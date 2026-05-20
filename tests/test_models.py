# tests/test_models.py
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.models import LapEvent, QualityStatus, SensorReading


def make_reading(**kwargs) -> SensorReading:
    """Factory con defaults válidos para tests. No tocar."""
    defaults = {
        "sensor_id": "test.sensor.1",
        "timestamp": datetime.now(UTC),
        "value": 42.0,
        "quality": QualityStatus.GOOD,
        "unit": "°C",
        "opc_node_id": "ns=2;s=Temperature",
        "received_at": datetime.now(UTC),
        "raw_value": 42.0,
    }
    defaults.update(kwargs)
    return SensorReading(**defaults)


class TestTimezoneValidation:
    def test_utc_timestamp_accepted(self):
        make_reading(timestamp=datetime.now(UTC))

    def test_naive_timestamp_rejected(self):
        """Un timestamp sin timezone debe ser rechazado con error descriptivo."""
        with pytest.raises(ValidationError):
            make_reading(timestamp=datetime.now())

    def test_iso_string_with_tz_accepted(self):
        make_reading(timestamp="2026-05-02T12:00:00Z")

    def test_naive_iso_string_rejected(self):
        """Un string ISO-8601 sin zona horaria debe ser parseado como naive y rechazado."""
        with pytest.raises(ValidationError):
            make_reading(timestamp="2026-05-02T12:00:00")


class TestValueValidation:
    def test_nan_rejected(self):
        """NaN debe rechazarse con ValidationError descriptivo."""
        with pytest.raises(ValidationError):
            make_reading(value=float("nan"))

    def test_positive_infinity_rejected(self):
        with pytest.raises(ValidationError):
            make_reading(value=float("inf"))

    def test_zero_accepted(self):
        make_reading(value=0.0)

    def test_negative_value_accepted(self):
        """Valores negativos son válidos (temperatura, diferencial de presión)."""
        make_reading(value=-1.0)


class TestQualityHandling:
    def test_bad_quality_preserves_raw_value(self):
        """Cuando quality=BAD, raw_value debe preservarse para diagnóstico."""
        lectura = make_reading(quality=QualityStatus.BAD, raw_value=55.5)
        assert lectura.raw_value == 55.5

    def test_bad_quality_accepts_none_value(self):
        """OPC UA envía Variant(Null) en lecturas Bad: value=None debe aceptarse."""
        lectura = make_reading(quality=QualityStatus.BAD, value=None, raw_value=None)
        assert lectura.value is None
        assert lectura.raw_value is None

    def test_good_quality_rejects_none_value(self):
        """Good con value=None es un bug del servidor — debe rechazarse."""
        with pytest.raises(ValidationError, match="value no puede ser None"):
            make_reading(quality=QualityStatus.GOOD, value=None)

    def test_uncertain_quality_rejects_none_value(self):
        """Uncertain con value=None también debe rechazarse."""
        with pytest.raises(ValidationError, match="value no puede ser None"):
            make_reading(quality=QualityStatus.UNCERTAIN, value=None)


class TestSerialisation:
    def test_mqtt_payload_has_required_fields(self):
        lectura = make_reading()
        payload = lectura.to_mqtt_payload()
        assert "sensor_id" in payload
        assert "timestamp" in payload
        assert "value" in payload
        assert "quality" in payload

    def test_timestamp_in_mqtt_payload_is_iso_string_with_timezone(self):
        lectura = make_reading()
        payload = lectura.to_mqtt_payload()
        timestamp_str = payload["timestamp"]
        assert isinstance(timestamp_str, str)
        parsed_dt = datetime.fromisoformat(timestamp_str)
        assert parsed_dt.tzinfo is not None


class TestLapEvent:
    def test_valid_lap_event(self):
        event = LapEvent(
            session_id="20260518T081500_practice",
            lap_number=3,
            lap_time_ms=92450,
            timestamp=datetime.now(UTC),
        )
        assert event.lap_number == 3

    def test_naive_timestamp_rejected(self):
        with pytest.raises(ValidationError):
            LapEvent(
                session_id="20260518T081500_practice",
                lap_number=1,
                lap_time_ms=90000,
                timestamp=datetime.now(),
            )

    def test_from_ac_packet(self):
        packet = {
            "session_id": "20260518T081500_practice",
            "lap_number": 2,
            "lap_time_ms": 91200,
            "timestamp": datetime.now(UTC).isoformat(),
        }
        event = LapEvent.from_ac_packet(packet)
        assert event.lap_number == 2

    def test_mqtt_payload(self):
        event = LapEvent(
            session_id="20260518T081500_practice",
            lap_number=1,
            lap_time_ms=89500,
            timestamp=datetime.now(UTC),
        )
        payload = event.to_mqtt_payload()
        assert "session_id" in payload
        assert "lap_number" in payload
        assert "lap_time_ms" in payload
