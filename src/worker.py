import asyncio
import logging

from src.anomaly_detector import AnomalyDetector
from src.destinations.base import BaseDestination
from src.metrics import GatewayMetrics
from src.models import LapEvent
from src.settings import settings
from src.store_forward import StoreAndForward

log = logging.getLogger(__name__)
logging.basicConfig(level=settings.log_level_int, format=settings.LOG_FORMAT)


async def _drain_queue_into_batch(
    queue: asyncio.Queue,
    max_size: int,
    timeout: float,
) -> list:
    batch = []
    try:
        first = await asyncio.wait_for(queue.get(), timeout)
        batch.append(first)
    except TimeoutError:
        return batch
    await asyncio.sleep(0.01)
    while len(batch) < max_size:
        try:
            batch.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            break
    return batch


async def worker(
    queue: asyncio.Queue,
    store: StoreAndForward,
    destination: BaseDestination,
    alert_destination: BaseDestination,
    lap_destination: BaseDestination,
    metrics: GatewayMetrics,
    detector: AnomalyDetector,
    ac_signals: dict = {},
) -> None:
    """
    Extrae mensajes de la cola e intenta enviarlos.
    Si falla, persiste en SQLite.
    Si hay mensajes pendientes en SQLite, los reenvía primero.
    """
    log.info("Worker iniciado")
    broker_online = True
    while True:
        pendientes = await store.pending_count()
        if pendientes > 0 and broker_online:
            log.warning(
                "Detectados %d mensajes pendientes en SQLite. Iniciando forward.", pendientes
            )
            await forward_pending(store, destination, metrics)
        try:
            batch = await _drain_queue_into_batch(
                queue,
                max_size=settings.WORKER_BATCH_SIZE,
                timeout=settings.WORKER_QUEUE_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            log.info("Worker cancelado. Saliendo del bucle principal.")
            break

        if not batch:
            log.debug("Worker timeout esperando cola; reintentando")
            continue

        lap_events = [m for m in batch if isinstance(m, LapEvent)]
        sensor_readings = [m for m in batch if not isinstance(m, LapEvent)]

        for i, msg in enumerate(lap_events):
            log.info(
                "LapEvent recibido: lap=%d session=%s time=%dms",
                msg.lap_number,
                msg.session_id,
                msg.lap_time_ms,
            )
            sent = False
            try:
                if broker_online or i == 0:
                    sent = await asyncio.wait_for(
                        lap_destination.send(msg.to_mqtt_payload()), timeout=settings.WORKER_QUEUE_TIMEOUT_S
                    )
                    if i == 0 and sent:
                        if not broker_online:
                            log.info("Broker recuperado (LapEvent) — reanudando envío normal")
                        broker_online = True
                    elif not sent:
                        broker_online = False
                else:
                    sent = False
            except TimeoutError:
                log.warning("Timeout enviando LapEvent lap=%d — broker offline", msg.lap_number)
                sent = False
                broker_online = False
            except Exception:
                log.exception("Error inesperado enviando LapEvent lap=%d", msg.lap_number)
                sent = False
                broker_online = False
            finally:
                queue.task_done()

            if not sent:
                log.warning(
                    "LapEvent omitido/descartado — broker offline: lap=%d session=%s",
                    msg.lap_number,
                    msg.session_id,
                )
        failed_critical = []

        for i, msg in enumerate(sensor_readings):
            latencia = (msg.received_at - msg.timestamp).total_seconds()
            log.debug("Latencia medida para %s: %.4fs", msg.sensor_id, latencia)
            metrics.add_latency(latencia)

            sent = False
            try:
                if broker_online or i == 0:
                    sent = await asyncio.wait_for(
                        destination.send(msg.to_mqtt_payload()), timeout=settings.WORKER_QUEUE_TIMEOUT_S
                    )
                    if i == 0 and sent:
                        if not broker_online:
                            log.info("Broker recuperado (Sensores) — reanudando envío normal")
                        broker_online = True
                    elif not sent:
                        broker_online = False
                else:
                    sent = False
            except TimeoutError:
                log.warning("Timeout enviando SensorReading %s — broker offline", msg.sensor_id)
                sent = False
                broker_online = False
            except Exception:
                log.exception("Error inesperado enviando SensorReading %s", msg.sensor_id)
                sent = False
                broker_online = False
            finally:
                queue.task_done()

            if sent:
                log.debug("Mensaje de %s procesado correctamente", msg.sensor_id)
                metrics.cont_proce_total += 1
            else:
                config = ac_signals.get(msg.sensor_id, {})
                criticality = config.get("criticality", "CRITICAL")
                if criticality == "CRITICAL":
                    log.warning("Fallo al enviar señal %s; acumulando para SQLite", msg.sensor_id)
                    failed_critical.append(msg.to_mqtt_payload())
                else:
                    log.warning("Fallo al enviar señal BEST_EFFORT; descartando: %s", msg.sensor_id)
                metrics.cont_fallo_total += 1

            try:
                alert = detector.analyze(msg)
                log.debug("Analizando %s → %s", msg.sensor_id, alert)
                if alert is not None:
                    log.warning(
                        "Alerta detectada: sensor=%s tipo=%s valor=%.2f",
                        alert.sensor_id,
                        alert.type,
                        alert.value,
                    )
                    if broker_online:
                        await asyncio.wait_for(
                            alert_destination.send(
                                {
                                    "sensor_id": alert.sensor_id,
                                    "timestamp": alert.timestamp.isoformat(),
                                    "type": alert.type,
                                    "value": alert.value,
                                    "mean": alert.mean,
                                    "std": alert.std,
                                    "threshold": alert.threshold,
                                    "duration_samples": alert.duration_samples,
                                }
                            ),
                            timeout=0.5,
                        )
                    else:
                        log.warning(
                            "Alerta omitida — broker offline: sensor=%s tipo=%s",
                            alert.sensor_id,
                            alert.type,
                        )
            except TimeoutError:
                log.warning("Timeout enviando alerta para %s — broker offline", msg.sensor_id)
            except Exception:
                log.exception(
                    "Error en análisis de anomalías para %s — worker continúa", msg.sensor_id
                )

        if failed_critical:
            log.warning(
                "Guardando %d mensajes CRITICAL en SQLite (una transacción)",
                len(failed_critical),
            )
            await store.save_batch(failed_critical)


async def forward_pending(
    store: StoreAndForward,
    destination: BaseDestination,
    metrics: GatewayMetrics,
) -> None:
    """
    Recupera mensajes de SQLite y los reenvía al destino.
    Solo borra los registros DESPUÉS de confirmar la entrega.
    """
    pend = await store.get_batch()
    if not pend:
        log.debug("forward_pending llamado sin mensajes pendientes")
        return
    log.info("Forward SQLite->destino iniciado. Lote=%d", len(pend))
    payload_json_all = [
        {
            "id": msg[0],
            "opc_node_id": msg[1],
            "sensor_id": msg[2],
            "value": msg[3],
            "raw_value": msg[4],
            "unit": msg[5],
            "quality": msg[6],
            "timestamp": msg[7],
            "received_at": msg[8],
        }
        for msg in pend
    ]
    id_conf = []
    try:
        for payload_json in payload_json_all:
            msg_id = payload_json.pop("id")
            if await destination.send(payload_json) is True:
                metrics.cont_proce_total += 1
                id_conf.append(msg_id)
                log.debug("Confirmado reenviado id=%s", msg_id)
            else:
                log.warning("Conexion caida durante forward; se reintentara en siguiente ciclo")
                metrics.cont_fallo_total += 1
                break
    finally:
        log.info("Borrando confirmados de SQLite. Confirmados=%d", len(id_conf))
        await store.delete_batch(id_conf)
        log.info("Forward finalizado")
