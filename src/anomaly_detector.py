import statistics
from collections import deque
from dataclasses import dataclass
from datetime import datetime

from src.models import QualityStatus, SensorReading
from src.settings import settings


@dataclass
class AlertData:
    sensor_id: str
    timestamp: datetime
    type: str
    value: float | None = None
    mean: float | None = None
    std: float | None = None
    threshold: float | None = None
    duration_samples: int | None = None


class AnomalyDetector:
    def __init__(self, config: dict) -> None:
        self._active_alerts: dict[str, AlertData] = {}
        self._config: dict[str, dict] = {s["sensor_id"]: s for s in config.get("sensores", [])}
        self._state: dict[str, dict] = {
            sensor_id: {
                "buffer": deque(maxlen=cfg["window_size"]),
                "consecutive_anomalies": 0,
            }
            for sensor_id, cfg in self._config.items()
        }

    def analyze(self, reading: SensorReading) -> AlertData | None:
        if reading.quality == QualityStatus.BAD:
            return AlertData(
                sensor_id=reading.sensor_id,
                timestamp=reading.timestamp,
                type="quality",
                value=reading.value,
            )

        cfg = self._config.get(reading.sensor_id)
        if cfg is None:
            return None

        state = self._state[reading.sensor_id]
        state["buffer"].append(reading.value)

        if len(state["buffer"]) < cfg["min_samples"]:
            return None

        if len(state["buffer"]) < 2:
            return None

        mean = statistics.mean(state["buffer"])
        std = statistics.stdev(state["buffer"])

        threshold_sup = mean + cfg["k_factor"] * std
        threshold_inf = mean - cfg["k_factor"] * std

        value = reading.value
        if value > threshold_sup or value < threshold_inf:
            state["consecutive_anomalies"] += 1
        else:
            state["consecutive_anomalies"] = 0
            self._active_alerts.pop(reading.sensor_id, None)

        if state["consecutive_anomalies"] >= settings.ANOMALY_CONSECUTIVE_THRESHOLD:
            alert = AlertData(
                sensor_id=reading.sensor_id,
                timestamp=reading.timestamp,
                type="statistical",
                value=reading.value,
                mean=round(mean, 4),
                std=round(std, 4),
                threshold=threshold_sup if value > threshold_sup else threshold_inf,
                duration_samples=state["consecutive_anomalies"],
            )
            self._active_alerts[reading.sensor_id] = alert
            return alert

        return None

    def get_active_alerts(self) -> dict:
        return {
            "alerts": [
                {
                    "sensor_id": a.sensor_id,
                    "timestamp": a.timestamp.isoformat(),
                    "type": a.type,
                    "value": a.value,
                    "mean": a.mean,
                    "std": a.std,
                    "threshold": a.threshold,
                    "duration_samples": a.duration_samples,
                }
                for a in self._active_alerts.values()
            ]
        }
