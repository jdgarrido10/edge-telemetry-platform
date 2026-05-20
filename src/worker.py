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
    while True:
        pendientes = await store.pending_count()
        if pendientes > 0:
            log.warning(
                "Detectados %d mensajes pendientes en SQLite. Iniciando forward.", pendientes
            )
            await forward_pending(store, destination, metrics)
        try:
            msg = await asyncio.wait_for(queue.get(), settings.WORKER_QUEUE_TIMEOUT_S)
            if isinstance(msg, LapEvent):
                log.info(
                    "LapEvent recibido: lap=%d session=%s time=%dms",
                    msg.lap_number,
                    msg.session_id,
                    msg.lap_time_ms,
                )
                try:
                    sent = await lap_destination.send(msg.to_mqtt_payload())
                    if not sent:
                        log.warning(
                            "LapEvent BEST_EFFORT descartado — broker no disponible: "
                            "lap=%d session=%s",
                            msg.lap_number,
                            msg.session_id,
                        )
                except Exception:
                    log.exception("Error inesperado enviando LapEvent lap=%d", msg.lap_number)
                finally:
                    queue.task_done()
                continue

            latencia = (msg.received_at - msg.timestamp).total_seconds()
            log.debug("Latencia medida para %s: %.4fs", msg.sensor_id, latencia)
            metrics.add_latency(latencia)

        except TimeoutError:
            log.debug("Worker timeout esperando cola; reintentando")
            continue
        except asyncio.CancelledError:
            log.info("Worker cancelado. Saliendo del bucle principal.")
            break

        try:
            if await destination.send(msg.to_mqtt_payload()) is True:
                log.debug("Mensaje de %s procesado correctamente", msg.sensor_id)
                metrics.cont_proce_total += 1
            else:
                config = ac_signals.get(msg.sensor_id, {})
                criticality = config.get("criticality", "CRITICAL")
                if criticality == "CRITICAL":
                    log.warning(
                        "Fallo al enviar señal %s; guardando mensaje en SQLite", msg.sensor_id
                    )
                    await store.save_batch([msg.to_mqtt_payload()])
                else:
                    log.warning("Fallo al enviar señal BEST_EFFORT; descartando: %s", msg.sensor_id)
                metrics.cont_fallo_total += 1
        finally:
            queue.task_done()

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
                await alert_destination.send(
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
                )
        except Exception:
            log.exception("Error en análisis de anomalías para %s — worker continúa", msg.sensor_id)


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
