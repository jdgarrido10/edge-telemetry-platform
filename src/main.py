import asyncio
import contextlib
import logging

from src.ac_adapter import ac_adapter
from src.anomaly_detector import AnomalyDetector
from src.config import load_ac_signals, load_analysis_config
from src.destinations.mqtt_destination import MQTTDestination
from src.metrics import GatewayMetrics
from src.opcua_client import connect_with_retry
from src.server import http_server
from src.settings import settings
from src.store_forward import StoreAndForward
from src.worker import worker

log = logging.getLogger(__name__)
logging.basicConfig(level=settings.log_level_int, format=settings.LOG_FORMAT)


async def shutdown(tareas: dict, queue: asyncio.Queue, store: StoreAndForward) -> None:
    for nombre in ("t_ingesta", "t_ac", "t_http"):
        t = tareas.get(nombre)
        if t and not t.done():
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t

    try:
        await asyncio.wait_for(queue.join(), timeout=settings.SHUTDOWN_DRAIN_TIMEOUT_S)
    except TimeoutError:
        log.warning("%d items sin procesar — persistiendo", queue.qsize())
        items = []
        while not queue.empty():
            item = queue.get_nowait()
            queue.task_done()
            items.append(item)
        if items:
            await store.save_batch([i.to_mqtt_payload() for i in items])

    t = tareas.get("t_worker")
    if t and not t.done():
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t


async def main() -> None:
    store = StoreAndForward()
    destination = MQTTDestination(settings.MQTT_HOST, settings.MQTT_PORT, settings.MQTT_TOPIC_DATA)
    alert_destination = MQTTDestination(
        settings.MQTT_HOST, settings.MQTT_PORT, settings.MQTT_TOPIC_ALERTS
    )
    lap_destination = MQTTDestination(
        settings.MQTT_HOST, settings.MQTT_PORT, settings.MQTT_TOPIC_LAPS
    )
    ac_signals = load_ac_signals(settings.AC_SIGNALS_CONFIG)
    analysis_config = load_analysis_config(settings.ANALYSIS_CONFIG)
    detector = AnomalyDetector(analysis_config)
    queue = asyncio.Queue(maxsize=settings.QUEUE_MAX_SIZE)
    event = asyncio.Event()
    metrics = GatewayMetrics(store, queue, destination, event)
    tasks = {
        "t_ingesta": connect_with_retry(queue, event),
        "t_worker": worker(
            queue,
            store,
            destination,
            alert_destination,
            lap_destination,
            metrics,
            detector,
            ac_signals,
        ),
        "t_http": http_server(metrics, detector),
        "t_ac": ac_adapter(queue),
    }
    tarea = {}
    async with destination:
        async with alert_destination:
            async with lap_destination:
                async with asyncio.TaskGroup() as tg:
                    for nombre, task in tasks.items():
                        tarea[nombre] = tg.create_task(task)
                    try:
                        await asyncio.Future()
                    except (KeyboardInterrupt, asyncio.CancelledError):
                        log.info("Señal de apagado recibida.")
                        await shutdown(tarea, queue, store)


if __name__ == "__main__":
    asyncio.run(main())
