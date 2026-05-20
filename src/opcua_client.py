import asyncio
import logging
from datetime import UTC, datetime

from asyncua import Client
from pydantic import ValidationError

from src.config import load_sensor_map
from src.models import SensorReading
from src.settings import settings

logging.basicConfig(level=settings.log_level_int, format=settings.LOG_FORMAT)
log = logging.getLogger(__name__)


class DataChangeHandler:
    """
    Recibe notificaciones de todos los nodos suscritos.
    Usa node_meta_map para saber qué sensor_id y unit corresponde
    a cada node, sin hardcodear nada en el handler.
    """

    def __init__(self, queue: asyncio.Queue, reconnect_event: asyncio.Event, node_meta_map: dict):
        self.queue = queue
        self.reconnect_event = reconnect_event
        self.node_meta_map = node_meta_map

    def datachange_notification(self, node, val, data) -> None:
        data_value = data.monitored_item.Value

        estado = data_value.StatusCode
        if estado.is_good():
            status_text = "Good"
        elif estado.is_uncertain():
            status_text = "Uncertain"
        else:
            status_text = "Bad"

        source_time = data_value.SourceTimestamp
        if source_time is None:
            source_time = datetime.now(UTC)

        meta = self.node_meta_map.get(str(node))
        if meta is None:
            log.warning("Notificación de nodo desconocido: %s — ignorando", node)
            return

        try:
            reading = SensorReading.from_opc_notification(
                str(node),
                val,
                source_time,
                status_text,
                meta["sensor_id"],
                meta["unit"],
            )
            self.queue.put_nowait(reading)
            log.info("Encolado: %s = %s %s [%s]", meta["sensor_id"], val, meta["unit"], status_text)
        except ValidationError as e:
            log.error("Dato inválido nodo %s: %s", node, e)
        except asyncio.QueueFull:
            log.warning("Cola llena — descartando dato de %s", meta["sensor_id"])

    def status_change_notification(self, status) -> None:
        log.warning("Cambio de estado en la suscripción: %s", status)
        if status.Status.is_bad():
            self.reconnect_event.set()
            log.warning("Suscripción caída — señalizando reconexión")


async def subscribe(client: Client, queue: asyncio.Queue, event: asyncio.Event) -> None:
    uri_array = await client.get_namespace_array()
    idx = None
    for uri in uri_array:
        if uri == settings.OPC_UA_NAMESPACE_URI:
            idx = await client.get_namespace_index(uri)
            break

    if idx is None:
        raise RuntimeError(
            f"Namespace '{settings.OPC_UA_NAMESPACE_URI}' no encontrado en el servidor"
        )

    nodes = []
    node_meta_map = {}
    SENSOR_MAP = load_sensor_map(settings.SENSORS_CONFIG)
    for (maquina, variable), meta in SENSOR_MAP.items():
        node = await client.nodes.objects.get_child([f"{idx}:{maquina}", f"{idx}:{variable}"])
        nodes.append(node)
        node_meta_map[str(node)] = meta
        log.info("Nodo resuelto: %s → %s", node, meta["sensor_id"])

    handler = DataChangeHandler(queue, event, node_meta_map)
    subscription = await client.create_subscription(
        period=settings.OPC_UA_SUBSCRIPTION_PERIOD_MS, handler=handler
    )
    await subscription.subscribe_data_change(
        nodes, sampling_interval=settings.OPC_UA_SAMPLING_INTERVAL_MS
    )
    log.info("Suscrito a %d nodos", len(nodes))

    try:
        while not event.is_set():
            await asyncio.sleep(1)
    finally:
        await subscription.delete()
        log.info("Suscripción eliminada")


async def connect_with_retry(queue: asyncio.Queue, event: asyncio.Event) -> None:
    delay = settings.OPC_UA_RETRY_INITIAL_DELAY_S
    attempt = 0

    while True:
        attempt += 1
        try:
            log.info("Intento de conexión #%d a %s", attempt, settings.opcua_url)
            async with Client(settings.opcua_url) as client:
                event.clear()
                attempt = 0
                delay = settings.OPC_UA_RETRY_INITIAL_DELAY_S
                await subscribe(client, queue, event)
        except (ConnectionError, TimeoutError, OSError) as e:
            log.warning("Conexión fallida: %s — reintentando en %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(
                delay * settings.OPC_UA_RETRY_BACKOFF_FACTOR, settings.OPC_UA_RETRY_MAX_DELAY_S
            )
        except RuntimeError as e:
            log.error("Error irrecuperable: %s", e)
            await asyncio.sleep(delay)
            delay = min(
                delay * settings.OPC_UA_RETRY_BACKOFF_FACTOR, settings.OPC_UA_RETRY_MAX_DELAY_S
            )
