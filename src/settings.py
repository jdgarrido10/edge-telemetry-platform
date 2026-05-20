"""
settings.py — Fuente única de verdad para toda la configuración del gateway.

Carga en orden de prioridad (mayor a menor):
  1. Variables de entorno del proceso
  2. Archivo .env en el directorio de trabajo
  3. Valores por defecto declarados aquí

Uso:
    from src.settings import settings

    print(settings.MQTT_HOST)
    print(settings.opcua_url)          # propiedad calculada
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

_ENV_PATH = Path(__file__).parent.parent / ".env"
load_dotenv(_ENV_PATH)


def _get(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _get_int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


class Settings:
    """
    Configuración centralizada del gateway IoT.
    Instancia única importada como `settings` desde cualquier módulo.
    """

    # ── MQTT ─────────────────────────────────────────────────────────────────
    MQTT_HOST: str = _get("MQTT_HOST", "mosquitto")
    MQTT_PORT: int = _get_int("MQTT_PORT", 1883)

    # Topics MQTT
    MQTT_TOPIC_DATA: str = "gateway/data"
    MQTT_TOPIC_ALERTS: str = "gateway/alerts"
    MQTT_TOPIC_LAPS: str = "gateway/laps"

    # ── OPC UA ───────────────────────────────────────────────────────────────
    OPC_UA_HOST: str = _get("OPC_UA_HOST", "opc-ua-sim")
    OPC_UA_PORT: int = _get_int("OPC_UA_PORT", 4840)
    OPC_UA_PATH: str = _get("OPC_UA_PATH", "/edge")
    OPC_UA_NAMESPACE_URI: str = _get("OPC_UA_NAMESPACE_URI", "http://proyecto/Joaquin121")

    # Tiempos de suscripción OPC UA (ms)
    OPC_UA_SUBSCRIPTION_PERIOD_MS: int = 1000
    OPC_UA_SAMPLING_INTERVAL_MS: int = 500

    # Reintentos de conexión OPC UA
    OPC_UA_RETRY_INITIAL_DELAY_S: float = 2.0
    OPC_UA_RETRY_MAX_DELAY_S: float = 60.0
    OPC_UA_RETRY_BACKOFF_FACTOR: float = 2.0

    # ── HTTP Server ──────────────────────────────────────────────────────────
    HTTP_HOST: str = _get("HTTP_HOST", "0.0.0.0")
    HTTP_PORT: int = _get_int("HTTP_PORT", 8080)

    # Rutas HTTP
    HTTP_ROUTE_METRICS: str = "/metrics"
    HTTP_ROUTE_HEALTH: str = "/health"
    HTTP_ROUTE_ALERTS: str = "/alerts"

    # ── AC Adapter (UDP) ─────────────────────────────────────────────────────
    AC_UDP_HOST: str = _get("AC_UDP_HOST", "0.0.0.0")
    AC_UDP_PORT: int = _get_int("AC_UDP_PORT", 9000)

    # ── Cola interna ─────────────────────────────────────────────────────────
    QUEUE_MAX_SIZE: int = 2500

    # ── Worker ───────────────────────────────────────────────────────────────
    WORKER_QUEUE_TIMEOUT_S: float = 5.0

    # ── Shutdown ─────────────────────────────────────────────────────────────
    SHUTDOWN_DRAIN_TIMEOUT_S: float = 10.0

    # ── SQLite Store & Forward ───────────────────────────────────────────────
    DB_PATH: str = _get("DB_PATH", "buffer.db")
    DB_BATCH_SIZE: int = 100

    # ── Métricas / Latencia ──────────────────────────────────────────────────
    LATENCY_WINDOW_SIZE: int = 1000  # maxlen del deque de latencias
    LATENCY_MIN_SAMPLES: int = 100  # muestras mínimas para calcular percentiles
    QUEUE_DEGRADED_THRESHOLD: float = 0.75  # fracción del maxsize que activa estado degraded

    # ── Detector de anomalías ────────────────────────────────────────────────
    ANOMALY_CONSECUTIVE_THRESHOLD: int = 3  # muestras consecutivas fuera de rango → alerta

    # ── Paths de configuración YAML ─────────────────────────────────────────
    SENSORS_CONFIG: str = _get("SENSORS_CONFIG", "config/sensors.yaml")
    AC_SIGNALS_CONFIG: str = _get("AC_SIGNALS_CONFIG", "config/ac_signals.yaml")
    ANALYSIS_CONFIG: str = _get("ANALYSIS_CONFIG", "config/analysis.yaml")

    # ── Logging ──────────────────────────────────────────────────────────────
    LOG_LEVEL: str = _get("LOG_LEVEL", "DEBUG")
    LOG_FORMAT: str = _get("LOG_FORMAT", "%(asctime)s [%(levelname)s] %(message)s")

    # ── Simulador OPC UA ─────────────────────────────────────────────────────
    SIM_ENDPOINT_HOST: str = "0.0.0.0"
    SIM_ENDPOINT_PORT: int = 4840
    SIM_ENDPOINT_PATH: str = "/edge"
    SIM_PUBLISH_INTERVAL_S: float = 1.0

    SIM_SENSORS_CONFIG: str = "config/sensors.yaml"
    SIM_SIM_SENSORS_CONFIG: str = "config/sim_sensors.yaml"

    # ── Propiedades calculadas ────────────────────────────────────────────────

    @property
    def opcua_url(self) -> str:
        """URL completa del servidor OPC UA."""
        return f"opc.tcp://{self.OPC_UA_HOST}:{self.OPC_UA_PORT}{self.OPC_UA_PATH}"

    @property
    def sim_endpoint(self) -> str:
        """Endpoint del simulador OPC UA."""
        return (
            f"opc.tcp://{self.SIM_ENDPOINT_HOST}:{self.SIM_ENDPOINT_PORT}{self.SIM_ENDPOINT_PATH}"
        )

    @property
    def log_level_int(self) -> int:
        """Nivel de logging como entero para logging.basicConfig."""
        return getattr(logging, self.LOG_LEVEL.upper(), logging.DEBUG)


settings = Settings()
