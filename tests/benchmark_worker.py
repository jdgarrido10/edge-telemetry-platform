import asyncio
import logging
import os
import time
from unittest.mock import MagicMock

from src.destinations.base import BaseDestination
from src.destinations.mqtt_destination import MQTTDestination
from src.metrics import GatewayMetrics
from src.models import SensorReading
from src.settings import settings
from src.store_forward import StoreAndForward
from src.worker import worker

logging.getLogger().setLevel(logging.WARNING)


class MockDestination(BaseDestination):
    def __init__(self):
        self.should_fail = False
        self.received_messages = []

    async def send(self, payload: dict) -> bool:
        if self.should_fail:
            return False
        self.received_messages.append(payload)
        return True

    async def is_available(self) -> bool:
        return not self.should_fail

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


async def benchmark(n_messages: int = 10000, use_real_mqtt: bool = True):
    store = StoreAndForward()
    queue = asyncio.Queue()

    # Inicializar los mocks necesarios
    mock = MockDestination()
    alert_mock = MockDestination()
    lap_mock = MockDestination()
    mock_detector = MagicMock()
    mock_detector.analyze.return_value = None
    event = asyncio.Event()

    payload = {
        "opc_node_id": "ns=2;i=2",
        "sensor_id": "planta1.maquina1.temperatura",
        "value": 42.5,
        "raw_value": 42.5,
        "unit": "°C",
        "quality": "Good",
        "timestamp": "2026-05-09T10:00:00+00:00",
        "received_at": "2026-05-09T10:00:00+00:00",
    }

    reading = SensorReading.from_opc_notification(
        payload["opc_node_id"],
        payload["value"],
        payload["timestamp"],
        payload["quality"],
        payload["sensor_id"],
        payload["unit"],
    )

    for _ in range(n_messages):
        queue.put_nowait(reading)

    if use_real_mqtt:
        destination = MQTTDestination(
            settings.MQTT_HOST, settings.MQTT_PORT, settings.MQTT_TOPIC_DATA
        )
    else:
        destination = mock

    metrics = GatewayMetrics(store, queue, destination, event)

    start = time.perf_counter()

    t_worker = asyncio.create_task(
        worker(queue, store, destination, alert_mock, lap_mock, metrics, mock_detector)
    )

    await queue.join()
    total_time = time.perf_counter() - start

    t_worker.cancel()
    print(f"{n_messages} mensajes → {n_messages / total_time:.1f} msg/s en {total_time:.2f}s")


async def main():
    db_path = "buffer.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        print("Borrando buffer.db antiguo...")

    await benchmark()


if __name__ == "__main__":
    asyncio.run(main())
