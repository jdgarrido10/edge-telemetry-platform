import asyncio
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.anomaly_detector import AnomalyDetector
from src.destinations.base import BaseDestination
from src.metrics import GatewayMetrics
from src.models import LapEvent, SensorReading
from src.store_forward import StoreAndForward
from src.worker import worker


class MockDestination(BaseDestination):
    def __init__(self):
        self.should_fail = False
        self.received_messages = []

    async def send(self, payload: dict) -> bool:
        if self.should_fail:
            return False
        else:
            self.received_messages.append(payload)
            return True

    async def is_available(self) -> bool:
        return not self.should_fail

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


ac_signals = {
    "ac.car.rpms": {"criticality": "CRITICAL"},
    "ac.car.tyre_temp_fl": {"criticality": "BEST_EFFORT"},
}

mock_detector = AnomalyDetector({"sensores": []})


@pytest.fixture
def store(tmp_path):
    return StoreAndForward(db_path=str(tmp_path / "test_buffer.db"))


@pytest.mark.asyncio
async def test_dato_valido_llega_al_destino(store):
    queue = asyncio.Queue()
    mock = MockDestination()
    alert_mock = MockDestination()
    lap_mock = MockDestination()
    event = asyncio.Event()
    metrics = GatewayMetrics(store, queue, mock, event)

    reading = SensorReading(
        sensor_id="test.sensor.1",
        opc_node_id="ns=2;i=2",
        value=42.5,
        raw_value=42.5,
        unit="°C",
        quality="Good",
        timestamp="2026-05-09T10:00:00+00:00",
        received_at="2026-05-09T10:00:00+00:00",
    )
    queue.put_nowait(reading)

    t = asyncio.create_task(
        worker(queue, store, mock, alert_mock, lap_mock, metrics, mock_detector)
    )
    await queue.join()
    t.cancel()

    assert len(mock.received_messages) == 1
    assert mock.received_messages[0]["sensor_id"] == "test.sensor.1"


@pytest.mark.asyncio
async def test_fallo_envio_guarda_en_sqlite(store):
    queue = asyncio.Queue()
    mock = MockDestination()
    alert_mock = MockDestination()
    lap_mock = MockDestination()
    mock.should_fail = True

    event = asyncio.Event()
    metrics = GatewayMetrics(store, queue, mock, event)

    reading = SensorReading(
        sensor_id="test.sensor.2",
        opc_node_id="ns=2;i=2",
        value=42.5,
        raw_value=42.5,
        unit="°C",
        quality="Good",
        timestamp=datetime.now(UTC),
        received_at=datetime.now(UTC),
    )
    queue.put_nowait(reading)

    t = asyncio.create_task(
        worker(queue, store, mock, alert_mock, lap_mock, metrics, mock_detector)
    )
    await queue.join()
    t.cancel()

    assert len(mock.received_messages) == 0
    assert await store.pending_count() == 1


@pytest.mark.asyncio
async def test_recuperacion_envia_pendientes_en_orden_fifo(store):
    queue = asyncio.Queue()
    mock = MockDestination()
    alert_mock = MockDestination()
    lap_mock = MockDestination()

    event = asyncio.Event()
    metrics = GatewayMetrics(store, queue, mock, event)

    msgList = []
    msg1 = {
        "sensor_id": "test.sensor.1",
        "opc_node_id": "ns=2;i=2",
        "value": 42.5,
        "raw_value": 42.5,
        "unit": "°C",
        "quality": "Good",
        "timestamp": datetime.now(UTC).isoformat(),
        "received_at": datetime.now(UTC).isoformat(),
    }
    msgList.append(msg1)

    msg2 = {
        "sensor_id": "test.sensor.2",
        "opc_node_id": "ns=2;i=2",
        "value": 42.5,
        "raw_value": 42.5,
        "unit": "°C",
        "quality": "Good",
        "timestamp": datetime.now(UTC).isoformat(),
        "received_at": datetime.now(UTC).isoformat(),
    }
    msgList.append(msg2)

    msg3 = {
        "sensor_id": "test.sensor.3",
        "opc_node_id": "ns=2;i=2",
        "value": 42.5,
        "raw_value": 42.5,
        "unit": "°C",
        "quality": "Good",
        "timestamp": datetime.now(UTC).isoformat(),
        "received_at": datetime.now(UTC).isoformat(),
    }
    msgList.append(msg3)

    await store.save_batch(msgList)

    t = asyncio.create_task(
        worker(queue, store, mock, alert_mock, lap_mock, metrics, mock_detector)
    )
    await asyncio.sleep(0.1)
    t.cancel()

    assert len(mock.received_messages) == 3
    assert mock.received_messages[0]["sensor_id"] == "test.sensor.1"
    assert mock.received_messages[1]["sensor_id"] == "test.sensor.2"
    assert mock.received_messages[2]["sensor_id"] == "test.sensor.3"


def test_rechaza_timestamp_naive():
    with pytest.raises(ValidationError):
        SensorReading(
            sensor_id="test.sensor.4",
            opc_node_id="ns=2;i=2",
            value=42.5,
            raw_value=42.5,
            unit="°C",
            quality="Good",
            timestamp=datetime.now(),
            received_at=datetime.now(),
        )


@pytest.mark.asyncio
async def test_acepta_y_envia_value_none_con_quality_bad(store):
    queue = asyncio.Queue()
    mock = MockDestination()
    alert_mock = MockDestination()
    lap_mock = MockDestination()
    event = asyncio.Event()
    metrics = GatewayMetrics(store, queue, mock, event)

    reading = SensorReading(
        sensor_id="test.sensor.5",
        opc_node_id="ns=2;i=2",
        value=None,
        raw_value=None,
        unit="°C",
        quality="Bad",
        timestamp=datetime.now(UTC),
        received_at=datetime.now(UTC),
    )
    queue.put_nowait(reading)

    t = asyncio.create_task(
        worker(queue, store, mock, alert_mock, lap_mock, metrics, mock_detector)
    )
    await queue.join()
    t.cancel()

    assert len(mock.received_messages) == 1
    assert mock.received_messages[0]["value"] is None


@pytest.mark.asyncio
async def test_critical_signal_persists_on_failure(store):
    queue = asyncio.Queue()
    mock = MockDestination()
    alert_mock = MockDestination()
    lap_mock = MockDestination()
    mock.should_fail = True

    event = asyncio.Event()
    metrics = GatewayMetrics(store, queue, mock, event)

    reading = SensorReading(
        sensor_id="ac.car.rpms",
        opc_node_id="ns=2;i=2",
        value=42.5,
        raw_value=42.5,
        unit="°C",
        quality="Good",
        timestamp=datetime.now(UTC),
        received_at=datetime.now(UTC),
    )
    queue.put_nowait(reading)

    t = asyncio.create_task(
        worker(queue, store, mock, alert_mock, lap_mock, metrics, mock_detector, ac_signals)
    )
    await queue.join()
    t.cancel()

    assert len(mock.received_messages) == 0
    assert await store.pending_count() == 1


@pytest.mark.asyncio
async def test_best_effort_signal_discarded_on_failure(store):
    queue = asyncio.Queue()
    mock = MockDestination()
    alert_mock = MockDestination()
    lap_mock = MockDestination()
    mock.should_fail = True

    event = asyncio.Event()
    metrics = GatewayMetrics(store, queue, mock, event)

    reading = SensorReading(
        sensor_id="ac.car.tyre_temp_fl",
        opc_node_id="ns=2;i=2",
        value=42.5,
        raw_value=42.5,
        unit="°C",
        quality="Good",
        timestamp=datetime.now(UTC),
        received_at=datetime.now(UTC),
    )
    queue.put_nowait(reading)

    t = asyncio.create_task(
        worker(queue, store, mock, alert_mock, lap_mock, metrics, mock_detector, ac_signals)
    )
    await queue.join()
    t.cancel()

    assert len(mock.received_messages) == 0
    assert await store.pending_count() == 0


@pytest.mark.asyncio
async def test_lap_event_llega_a_lap_destination(store):
    queue = asyncio.Queue()
    mock = MockDestination()
    alert_mock = MockDestination()
    lap_mock = MockDestination()
    event = asyncio.Event()
    metrics = GatewayMetrics(store, queue, mock, event)

    lap_event = LapEvent(
        session_id="20260518T081500_practice",
        lap_number=3,
        lap_time_ms=92450,
        timestamp=datetime.now(UTC),
    )
    queue.put_nowait(lap_event)

    t = asyncio.create_task(
        worker(queue, store, mock, alert_mock, lap_mock, metrics, mock_detector)
    )
    await queue.join()
    t.cancel()

    assert len(mock.received_messages) == 0
    assert len(alert_mock.received_messages) == 0
    assert len(lap_mock.received_messages) == 1
    assert lap_mock.received_messages[0]["lap_number"] == 3
    assert lap_mock.received_messages[0]["session_id"] == "20260518T081500_practice"


@pytest.mark.asyncio
async def test_lap_event_descartado_si_fallo_destino(store):
    """LapEvent es BEST_EFFORT: si lap_destination falla, se descarta sin persistir en SQLite."""
    queue = asyncio.Queue()
    mock = MockDestination()
    alert_mock = MockDestination()
    lap_mock = MockDestination()
    lap_mock.should_fail = True

    event = asyncio.Event()
    metrics = GatewayMetrics(store, queue, mock, event)

    lap_event = LapEvent(
        session_id="20260518T081500_practice",
        lap_number=5,
        lap_time_ms=91000,
        timestamp=datetime.now(UTC),
    )
    queue.put_nowait(lap_event)

    t = asyncio.create_task(
        worker(queue, store, mock, alert_mock, lap_mock, metrics, mock_detector)
    )
    await queue.join()
    t.cancel()

    assert len(lap_mock.received_messages) == 0
    assert len(mock.received_messages) == 0
    assert await store.pending_count() == 0
