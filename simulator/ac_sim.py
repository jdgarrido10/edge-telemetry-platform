"""
ac_sim.py — Simulador de telemetría Assetto Corsa vía UDP
          — Versión: Spectrally Rich Deterministic Telemetry Generator

Replica el protocolo exacto del script de shared-memory que corre en Windows:
  - Paquetes JSON: {"signal": ..., "value": ..., "unit": ..., "timestamp": ...}
  - Eventos de vuelta: {"event": "lap_completed", "session_id": ..., ...}
  - Emisión por umbral de cambio (delta threshold), igual que el original
  - Frecuencia base ~50 Hz

Cambios principales respecto a la versión anterior:
  - Motor de física convertido de "target interpolation" a "spectrally rich deterministic"
  - random.gauss() eliminado casi completamente; sustituido por osciladores armónicos
  - Nueva capa enrich_signals() con microdinámica determinista
  - harmonic_noise() — generador de ruido armónico sin random
  - Road profile determinista por segmento (firma vibracional distinta por sector)
  - ABS / TCS fake modulation determinista (alta frecuencia, muchos crossings)
  - Tau de suavizado reducido en señales clave (speed, G, slip, suspension)
  - Throughput objetivo: 35k–45k señales/min
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import socket
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

# ─── Configuración ────────────────────────────────────────────────────────────

GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "gateway")
GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "9000"))
PUBLISH_HZ = float(os.environ.get("SIM_HZ", "50"))
LAP_COUNT = int(os.environ.get("SIM_LAPS", "0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

DT = 1.0 / PUBLISH_HZ

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [ac-sim] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Umbrales de cambio ─────────────────────────

SIGNALS: dict[str, dict] = {
    "rpms": {"unit": "rpm", "threshold": 50, "sensor_id": "rpms"},
    "brake": {"unit": "ratio", "threshold": 0.01, "sensor_id": "brake"},
    "throttle": {"unit": "ratio", "threshold": 0.01, "sensor_id": "throttle"},
    "gear": {"unit": "", "threshold": 0, "sensor_id": "gear"},
    "speed_kmh": {"unit": "km/h", "threshold": 0.5, "sensor_id": "speed_kmh"},
    "fuel": {"unit": "kg", "threshold": 0.01, "sensor_id": "fuel"},
    "tyre_temp_fl": {"unit": "°C", "threshold": 0.5, "sensor_id": "tyre_temp_fl"},
    "tyre_temp_fr": {"unit": "°C", "threshold": 0.5, "sensor_id": "tyre_temp_fr"},
    "tyre_temp_rl": {"unit": "°C", "threshold": 0.5, "sensor_id": "tyre_temp_rl"},
    "tyre_temp_rr": {"unit": "°C", "threshold": 0.5, "sensor_id": "tyre_temp_rr"},
    "wheel_slip_fl": {"unit": "ratio", "threshold": 0.01, "sensor_id": "wheel_slip_fl"},
    "wheel_slip_fr": {"unit": "ratio", "threshold": 0.01, "sensor_id": "wheel_slip_fr"},
    "wheel_slip_rl": {"unit": "ratio", "threshold": 0.01, "sensor_id": "wheel_slip_rl"},
    "wheel_slip_rr": {"unit": "ratio", "threshold": 0.01, "sensor_id": "wheel_slip_rr"},
    "acc_g_x": {"unit": "g", "threshold": 0.01, "sensor_id": "acc_g_x"},
    "acc_g_y": {"unit": "g", "threshold": 0.01, "sensor_id": "acc_g_y"},
    "acc_g_z": {"unit": "g", "threshold": 0.01, "sensor_id": "acc_g_z"},
    "suspension_fl": {"unit": "m", "threshold": 0.001, "sensor_id": "suspension_fl"},
    "suspension_fr": {"unit": "m", "threshold": 0.001, "sensor_id": "suspension_fr"},
    "suspension_rl": {"unit": "m", "threshold": 0.001, "sensor_id": "suspension_rl"},
    "suspension_rr": {"unit": "m", "threshold": 0.001, "sensor_id": "suspension_rr"},
}

SESSION_TYPE_NAMES = {0: "practice", 1: "qualify", 2: "race"}

# ─── Tipos de fallo ───────────────────────────────────────────────────────────


class FaultType(StrEnum):
    STUCK = "STUCK"
    SPIKE = "SPIKE"
    NOISE = "NOISE"
    DROPOUT = "DROPOUT"
    FLATTY = "FLATTY"
    OFFSET = "OFFSET"


@dataclass
class FaultWindow:
    sensor_id: str
    fault_type: FaultType
    start_s: float
    duration_s: float
    spike_magnitude: float = 5.0
    noise_sigma: float = 3.0
    offset_value: float = 10.0
    flatty_sensor: str = ""


# ─── Calendario de fallos ─────────────────────────────────────────────────────

FAULT_SCHEDULE: list[FaultWindow] = [
    FaultWindow("rpms", FaultType.SPIKE, start_s=30, duration_s=0.1, spike_magnitude=2.5),
    FaultWindow("tyre_temp_fr", FaultType.STUCK, start_s=55, duration_s=15.0),
    FaultWindow("acc_g_y", FaultType.NOISE, start_s=80, duration_s=25.0, noise_sigma=2.0),
    FaultWindow(
        "tyre_temp_rl", FaultType.FLATTY, start_s=110, duration_s=40.0, flatty_sensor="tyre_temp_rl"
    ),
    FaultWindow("brake", FaultType.OFFSET, start_s=160, duration_s=30.0, offset_value=0.15),
    FaultWindow("wheel_slip_rr", FaultType.DROPOUT, start_s=200, duration_s=20.0),
]

# ─── Road profiles por segmento — firma vibracional determinista ──────────────

ROAD_PROFILE: dict[str, list[tuple[float, float, float]]] = {
    # La Source: asfalto pulido, baja vibración
    "La_Source": [
        (0.0006, 8.3, 0.0),
        (0.0003, 17.1, 1.2),
        (0.0002, 31.7, 2.5),
    ],
    # Eau Rouge / Raidillon: baches en el apex, comprensión fuerte
    "Eau_Rouge": [
        (0.0018, 6.5, 0.7),
        (0.0012, 13.2, 1.8),
        (0.0009, 24.7, 3.1),
        (0.0006, 41.3, 0.3),
    ],
    # Raidillon crest: crest bump específico
    "Raidillon": [
        (0.0022, 5.1, 1.1),
        (0.0015, 11.8, 2.3),
        (0.0008, 28.4, 0.9),
    ],
    # Kemmel: recta larga, perfil de alta velocidad
    "Kemmel": [
        (0.0010, 9.7, 0.4),
        (0.0007, 19.3, 1.6),
        (0.0004, 38.9, 2.8),
        (0.0003, 67.4, 0.1),
    ],
    # Les Combes: frenada fuerte, kerb hits
    "Les_Combes": [
        (0.0020, 7.2, 0.6),
        (0.0014, 15.6, 1.9),
        (0.0010, 29.1, 3.3),
        (0.0006, 53.8, 0.8),
    ],
    # Malmedy / Rivage: perfil mixto
    "Malmedy": [
        (0.0008, 10.4, 0.2),
        (0.0005, 22.7, 1.4),
        (0.0003, 44.1, 2.7),
    ],
    "Rivage": [
        (0.0015, 8.8, 0.9),
        (0.0010, 18.2, 2.1),
        (0.0007, 35.6, 0.5),
    ],
    # Pouhon: curva rápida sostenida, resonancia lateral
    "Pouhon": [
        (0.0012, 11.3, 1.3),
        (0.0009, 23.8, 2.6),
        (0.0006, 47.2, 0.7),
        (0.0004, 83.5, 1.9),
    ],
    # Stavelot: salida rápida
    "Stavelot": [
        (0.0009, 9.1, 0.3),
        (0.0006, 20.4, 1.5),
        (0.0004, 39.7, 2.9),
    ],
    # Blanchimont: recta de alta velocidad, aerodinámica dominante
    "Blanchimont": [
        (0.0008, 12.6, 0.8),
        (0.0006, 25.3, 2.0),
        (0.0004, 51.8, 0.4),
        (0.0002, 97.3, 1.7),
    ],
    # Bus Stop: chicane, kerb agresivo
    "BusStop": [
        (0.0025, 6.8, 0.5),
        (0.0018, 14.5, 1.7),
        (0.0013, 27.9, 3.0),
        (0.0009, 58.6, 0.2),
        (0.0005, 112.4, 1.1),
    ],
    # Main straight: recta de meta, perfil suave
    "Main_straight": [
        (0.0007, 8.9, 0.6),
        (0.0004, 18.7, 1.8),
        (0.0002, 36.4, 3.2),
    ],
    # Default: perfil genérico
    "_default": [
        (0.0008, 10.0, 0.0),
        (0.0005, 21.3, 1.5),
        (0.0003, 43.7, 3.0),
    ],
}


def _get_road_profile(seg_name: str) -> list[tuple[float, float, float]]:
    """Devuelve el road profile para un segmento dado, usando el nombre parcial."""
    for key in ROAD_PROFILE:
        if key in seg_name or seg_name.startswith(key):
            return ROAD_PROFILE[key]
    # fallback: buscar coincidencia parcial
    seg_lower = seg_name.lower()
    for key in ROAD_PROFILE:
        if key.lower() in seg_lower:
            return ROAD_PROFILE[key]
    return ROAD_PROFILE["_default"]


# ─── Modelo de pista — Spa-Francorchamps ─────────────────────────────────────


@dataclass
class TrackSegment:
    name: str
    duration_s: float
    seg_type: str
    target_speed: float
    target_gear: int
    lateral_g: float


LAP_SEGMENTS: list[TrackSegment] = [
    TrackSegment("La_Source_brake", 2.5, "BRAKING", 60, 2, 0.0),
    TrackSegment("La_Source_apex", 1.5, "APEX", 55, 2, -2.2),
    TrackSegment("La_Source_exit", 2.0, "EXIT", 100, 3, 1.5),
    TrackSegment("Eau_Rouge", 3.5, "STRAIGHT", 240, 5, 0.3),
    TrackSegment("Raidillon_crest", 2.0, "STRAIGHT", 270, 6, -0.4),
    TrackSegment("Kemmel", 6.0, "STRAIGHT", 310, 7, 0.1),
    TrackSegment("Les_Combes_brake", 2.0, "BRAKING", 100, 3, 0.0),
    TrackSegment("Les_Combes_apex", 1.5, "APEX", 90, 3, 2.0),
    TrackSegment("Les_Combes_exit", 1.5, "EXIT", 160, 5, -1.2),
    TrackSegment("Malmedy", 3.0, "STRAIGHT", 200, 6, -0.2),
    TrackSegment("Rivage_brake", 2.5, "BRAKING", 70, 2, 0.0),
    TrackSegment("Rivage_apex", 2.0, "APEX", 65, 2, -2.5),
    TrackSegment("Rivage_exit", 2.0, "EXIT", 130, 4, 1.8),
    TrackSegment("Pouhon_entry", 2.0, "APEX", 180, 5, 1.8),
    TrackSegment("Pouhon_mid", 1.5, "APEX", 175, 5, 2.0),
    TrackSegment("Pouhon_exit", 2.0, "EXIT", 220, 6, -1.0),
    TrackSegment("Stavelot_brake", 1.5, "BRAKING", 120, 4, 0.0),
    TrackSegment("Stavelot_apex", 1.5, "APEX", 110, 4, -1.5),
    TrackSegment("Stavelot_exit", 2.5, "EXIT", 200, 6, 1.0),
    TrackSegment("Blanchimont", 4.0, "STRAIGHT", 290, 7, 0.5),
    TrackSegment("BusStop_brake", 2.0, "BRAKING", 80, 2, 0.0),
    TrackSegment("BusStop_1", 1.5, "CHICANE", 75, 2, 2.5),
    TrackSegment("BusStop_2", 1.5, "CHICANE", 70, 2, -2.5),
    TrackSegment("BusStop_exit", 2.0, "EXIT", 140, 4, 1.0),
    TrackSegment("Main_straight", 6.0, "STRAIGHT", 290, 7, 0.0),
]

LAP_DURATION_S = sum(s.duration_s for s in LAP_SEGMENTS)

# ─── Estado de simulación ─────────────────────────────────────────────────────


@dataclass
class CarState:
    speed_kmh: float = 0.0
    gear: int = 1
    rpms: float = 800.0
    throttle: float = 0.0
    brake: float = 0.0
    fuel: float = 85.0

    tyre_temp_fl: float = 60.0
    tyre_temp_fr: float = 60.0
    tyre_temp_rl: float = 60.0
    tyre_temp_rr: float = 60.0

    wheel_slip_fl: float = 0.0
    wheel_slip_fr: float = 0.0
    wheel_slip_rl: float = 0.0
    wheel_slip_rr: float = 0.0

    acc_g_x: float = 0.0
    acc_g_y: float = 0.0
    acc_g_z: float = 1.0

    suspension_fl: float = 0.025
    suspension_fr: float = 0.025
    suspension_rl: float = 0.025
    suspension_rr: float = 0.025

    segment_idx: int = 0
    seg_elapsed: float = 0.0
    lap_number: int = 0
    lap_elapsed: float = 0.0

    stuck_values: dict = field(default_factory=dict)
    flatty_onset: dict = field(default_factory=dict)


# ─── Osciladores deterministas ────────────────────────────────────────────────


def harmonic_noise(
    t: float,
    harmonics: list[tuple[float, float, float]],
) -> float:
    """
    Genera ruido determinista como suma de osciladores sinusoidales.

    Args:
        t:          tiempo en segundos
        harmonics:  lista de (amplitud, frecuencia_Hz, phase_rad)

    Returns:
        Suma de componentes sinusoidales. Completamente reproducible dado t.

    Ejemplo:
        harmonic_noise(t, [(0.05, 18, 1.3), (0.03, 37, 0.7), (0.01, 91, 2.1)])
    """
    total = 0.0
    for amp, freq, phase in harmonics:
        total += amp * math.sin(2.0 * math.pi * freq * t + phase)
    return total


def _drivetrain_ripple(t: float, rpms: float, gear: int) -> float:
    """
    Ripple del tren de transmisión: frecuencia proporcional a las RPM.
    Simula las vibraciones de los engranajes y cardán.
    """
    f_base = rpms / 60.0
    return harmonic_noise(
        t,
        [
            (18.0, f_base * 0.5, 0.7),  # semi-orden
            (12.0, f_base * 1.0, 1.3),  # orden 1 (rotación)
            (8.0, f_base * 2.0, 2.1),  # orden 2 (firing 4-cil)
            (5.0, f_base * 4.0, 0.4),  # orden 4
            (3.0, f_base * 0.25, 3.1),  # orden bajo (TQ ripple)
        ],
    )


def _suspension_chatter(t: float, speed_kmh: float, seg_profile: list) -> float:
    """
    Chatter de suspensión: road profile + resonancia propia de la suspensión.
    Combina el perfil de carretera del segmento con resonancias estructurales.
    """
    structural = harmonic_noise(
        t,
        [
            (0.0012, 3.8, 1.1),
            (0.0008, 6.2, 2.4),
            (0.0005, 11.5, 0.8),
        ],
    )
    road = harmonic_noise(t, seg_profile)
    speed_factor = min(1.0, speed_kmh / 200.0)
    return structural + road * (0.5 + speed_factor)


def _aero_oscillation(t: float, speed_kmh: float) -> float:
    """
    Oscilación aerodinámica: flutter del alerón y estelas.
    Solo relevante a alta velocidad.
    """
    if speed_kmh < 100:
        return 0.0
    speed_norm = (speed_kmh - 100) / 200.0
    return harmonic_noise(
        t,
        [
            (0.0008 * speed_norm, 4.3, 0.9),
            (0.0005 * speed_norm, 9.7, 2.2),
            (0.0003 * speed_norm, 18.4, 1.4),
        ],
    )


def _abs_modulation(t: float, brake: float) -> float:
    """
    Modulación falsa de ABS: cuando brake > threshold, genera pulsos rápidos.
    Frecuencia característica: 8–15 Hz.
    """
    if brake < 0.3:
        return 0.0
    intensity = (brake - 0.3) / 0.7
    return harmonic_noise(
        t,
        [
            (0.04 * intensity, 11.3, 0.5),
            (0.02 * intensity, 8.7, 1.8),
            (0.015 * intensity, 15.2, 3.1),
        ],
    )


def _tcs_modulation(t: float, throttle: float, slip: float) -> float:
    """
    Modulación falsa de TCS: cuando hay slip + throttle alto.
    Genera reducción pulsante de throttle (plausible para telemetría).
    """
    if throttle < 0.6 or slip < 0.05:
        return 0.0
    intensity = min(1.0, slip * 5.0) * (throttle - 0.6) / 0.4
    return harmonic_noise(
        t,
        [
            (0.05 * intensity, 7.8, 1.2),
            (0.03 * intensity, 13.5, 2.7),
            (0.02 * intensity, 19.1, 0.3),
        ],
    )


def _wheel_hop(t: float, slip: float, corner_phase: float) -> float:
    """
    Wheel hop: resonancia de la rueda durante slip agresivo.
    Frecuencia característica: 12–20 Hz.
    """
    if slip < 0.08:
        return 0.0
    return harmonic_noise(
        t,
        [
            (0.015 * slip, 14.2 + corner_phase, 0.6),
            (0.009 * slip, 19.8 + corner_phase, 1.9),
            (0.005 * slip, 28.3 + corner_phase, 3.2),
        ],
    )


def _tyre_high_freq(t: float, speed_kmh: float, corner_phase: float) -> float:
    """
    Oscilación de alta frecuencia del neumático.
    Proporcional a la velocidad (standing waves en el neumático).
    """
    if speed_kmh < 30:
        return 0.0
    f_tyre = speed_kmh / 3.6 / 0.33 / (2 * math.pi)  # ~ Hz de rotación
    return harmonic_noise(
        t,
        [
            (0.002, f_tyre, corner_phase),
            (0.001, f_tyre * 2.0, corner_phase + 1.1),
            (0.0005, f_tyre * 4.0, corner_phase + 2.3),
        ],
    )


# ─── Helpers de marcha y RPM ──────────────────────────────────────────────────


def _gear_for_speed(speed: float) -> int:
    if speed < 70:
        return 2
    if speed < 120:
        return 3
    if speed < 175:
        return 4
    if speed < 230:
        return 5
    if speed < 270:
        return 6
    return 7


def _rpm_for_speed_gear(speed: float, gear: int) -> float:
    gear_ratios = {1: 55.0, 2: 40.0, 3: 28.0, 4: 20.0, 5: 14.5, 6: 10.5, 7: 7.5}
    ratio = gear_ratios.get(gear, 10.0)
    rpm = speed * ratio
    return max(800.0, min(9000.0, rpm))


def _tyre_temp_target(speed: float, slip: float, base: float = 85.0) -> float:
    heat_from_speed = speed * 0.18
    heat_from_slip = slip * 120.0
    return base + heat_from_speed + heat_from_slip


# ─── Motor de física — Spectrally Rich Deterministic ─────────────────────────

# Tabla de variación humana del conductor — determinista vía seno
# Simula la variación natural en la pisada sin usar random
_DRIVER_BRAKE_HARMONICS = [
    (0.07, 0.23, 0.7),  # variación lenta (respiración del piloto)
    (0.03, 0.61, 2.1),  # microajuste
    (0.015, 1.47, 0.4),  # tremor fino
]
_DRIVER_THROTTLE_HARMONICS = [
    (0.04, 0.31, 1.3),
    (0.02, 0.73, 2.8),
    (0.01, 1.83, 0.9),
]


def step_physics(state: CarState, seg: TrackSegment, dt: float, t_session: float) -> None:
    """
    Avanza el estado físico un paso dt.
    Motor convertido a "spectrally rich deterministic telemetry generator".
    """
    t = t_session
    target_speed = seg.target_speed

    lat_g_target = seg.lateral_g
    seg_type = seg.seg_type
    is_braking = seg_type == "BRAKING"
    is_accel = seg_type in ("EXIT", "STRAIGHT")
    is_apex = seg_type in ("APEX", "CHICANE")

    # ── Throttle / Brake — variación humana determinista ─────────────────
    # Los targets base dependen del segmento, la variación viene de osciladores
    if is_braking:
        brake_base = 0.80
        throttle_base = 0.0
    elif is_apex:
        brake_base = 0.07
        throttle_base = 0.45
    else:  # STRAIGHT / EXIT
        brake_base = 0.0
        throttle_base = 0.92

    # Variación humana determinista (sustituye random.uniform)
    brake_human = harmonic_noise(t, _DRIVER_BRAKE_HARMONICS)
    throttle_human = harmonic_noise(t, _DRIVER_THROTTLE_HARMONICS)

    brake_target = max(0.0, min(1.0, brake_base + brake_human))
    throttle_target = max(0.0, min(1.0, throttle_base + throttle_human))

    # ABS modulation en el target de freno (no en el valor final, sino en target)
    abs_mod = _abs_modulation(t, brake_target)
    brake_target = max(0.0, min(1.0, brake_target + abs_mod))

    # TCS modulation en throttle
    avg_rear_slip = (state.wheel_slip_rl + state.wheel_slip_rr) * 0.5
    tcs_mod = _tcs_modulation(t, throttle_target, avg_rear_slip)
    throttle_target = max(0.0, min(1.0, throttle_target - abs(tcs_mod)))

    # Tau reducido para más responsividad (más crossings)
    tau_input = 0.04
    state.throttle += (throttle_target - state.throttle) * (dt / tau_input)
    state.brake += (brake_target - state.brake) * (dt / tau_input)
    state.throttle = max(0.0, min(1.0, state.throttle))
    state.brake = max(0.0, min(1.0, state.brake))

    # ── Velocidad — tau reducido + microdinámica ───────────────────────────
    tau_speed = 2.5 if is_accel else (0.8 if is_braking else 1.8)
    state.speed_kmh += (target_speed - state.speed_kmh) * (dt / tau_speed)

    # Microoscilación de velocidad determinista (road texture, aero buffeting)
    speed_micro = harmonic_noise(
        t,
        [
            (0.25, 1.73, 0.6),
            (0.12, 3.41, 1.9),
            (0.06, 7.23, 3.1),
        ],
    )
    state.speed_kmh = max(0.0, state.speed_kmh + speed_micro)

    # ── Marcha y RPM ──────────────────────────────────────────────────────
    new_gear = _gear_for_speed(state.speed_kmh)
    if new_gear != state.gear:
        state.rpms *= 0.75
        state.gear = new_gear

    target_rpm = _rpm_for_speed_gear(state.speed_kmh, state.gear)
    if state.throttle > 0.9 and seg_type == "STRAIGHT":
        target_rpm = min(8800, target_rpm * 1.05)

    # Tau RPM reducido (más responsivo)
    state.rpms += (target_rpm - state.rpms) * (dt / 0.25)

    # Drivetrain ripple determinista (sustituye random.gauss(0, 15))
    ripple = _drivetrain_ripple(t, state.rpms, state.gear)
    state.rpms = max(800.0, state.rpms + ripple)

    # ── Combustible ──────────────────────────────────────────────────────
    fuel_rate = (state.rpms / 8500.0) * state.throttle * 0.0009
    state.fuel = max(0.0, state.fuel - fuel_rate * dt)

    # ── Wheel slip — con wheel hop y TCS ──────────────────────────────────
    accel_demand = state.throttle * (state.rpms / 8500.0)
    brake_demand = state.brake

    base_slip_rear = max(0.0, accel_demand - 0.4) * 0.8
    base_slip_front = brake_demand * 0.3

    # Microoscilación de slip determinista (sustituye random.gauss)
    slip_micro_f = harmonic_noise(t, [(0.008, 23.1, 0.5), (0.004, 47.3, 1.8)])
    slip_micro_r = harmonic_noise(t, [(0.012, 19.7, 1.1), (0.006, 41.2, 2.6)])

    state.wheel_slip_fl = max(0.0, base_slip_front + slip_micro_f)
    state.wheel_slip_fr = max(
        0.0, base_slip_front + harmonic_noise(t, [(0.008, 23.1, 1.5), (0.004, 47.3, 2.8)])
    )
    state.wheel_slip_rl = max(0.0, base_slip_rear + slip_micro_r)
    state.wheel_slip_rr = max(
        0.0, base_slip_rear + harmonic_noise(t, [(0.012, 19.7, 2.1), (0.006, 41.2, 0.6)])
    )

    # Wheel hop en traseras si hay slip
    hop_l = _wheel_hop(t, state.wheel_slip_rl, 0.0)
    hop_r = _wheel_hop(t, state.wheel_slip_rr, 1.57)
    state.wheel_slip_rl = max(0.0, state.wheel_slip_rl + abs(hop_l))
    state.wheel_slip_rr = max(0.0, state.wheel_slip_rr + abs(hop_r))

    # Pico de slip en cambio de marcha (determinista)
    if state.rpms < 3000 and state.throttle > 0.7:
        shift_spike = abs(harmonic_noise(t, [(0.25, 0.5, 0.0)]))
        state.wheel_slip_rl += shift_spike
        state.wheel_slip_rr += shift_spike

    # ── Temperaturas neumáticos ───────────────────────────────────────────
    lat_loading = abs(lat_g_target)
    outer_heat = lat_loading * 4.0
    inner_cool = lat_loading * 2.0

    if lat_g_target > 0:
        heat_fl = outer_heat
        heat_fr = -inner_cool
        heat_rl = outer_heat
        heat_rr = -inner_cool
    elif lat_g_target < 0:
        heat_fl = -inner_cool
        heat_fr = outer_heat
        heat_rl = -inner_cool
        heat_rr = outer_heat
    else:
        heat_fl = heat_fr = heat_rl = heat_rr = 0.0

    ambient = 25.0
    tau_tyre = 12.0

    for corner, attr, slip_attr, heat_extra, phase in [
        ("fl", "tyre_temp_fl", "wheel_slip_fl", heat_fl, 0.0),
        ("fr", "tyre_temp_fr", "wheel_slip_fr", heat_fr, 1.1),
        ("rl", "tyre_temp_rl", "wheel_slip_rl", heat_rl, 2.2),
        ("rr", "tyre_temp_rr", "wheel_slip_rr", heat_rr, 3.3),
    ]:
        slip = getattr(state, slip_attr)
        current = getattr(state, attr)
        target_temp = _tyre_temp_target(state.speed_kmh, slip) + heat_extra

        if state.speed_kmh < 30:
            target_temp = ambient + (current - ambient) * 0.7

        new_temp = current + (target_temp - current) * (dt / tau_tyre)

        # Microoscilación de temperatura determinista (sustituye random.gauss(0, 0.3))
        temp_micro = harmonic_noise(
            t,
            [
                (0.22, 0.83 + phase * 0.1, phase),
                (0.10, 2.17 + phase * 0.1, phase + 1.0),
            ],
        )
        new_temp += temp_micro

        # Tyre high-freq oscillation
        new_temp += _tyre_high_freq(t, state.speed_kmh, phase) * 80.0  # escalar a °C

        setattr(state, attr, max(ambient, new_temp))

    # ── G-forces — tau reducido + microdinámica ────────────────────────────
    g_long_target = state.brake * 4.5 - state.throttle * (state.rpms / 8500.0) * 1.8

    # Tau longitudinal reducido (0.08 → 0.05)
    state.acc_g_x += (g_long_target - state.acc_g_x) * (dt / 0.05)
    state.acc_g_x += harmonic_noise(
        t,
        [
            (0.035, 7.3, 0.4),
            (0.018, 15.1, 1.7),
            (0.009, 31.2, 3.0),
        ],
    )

    # Tau lateral reducido (0.3 → 0.15)
    state.acc_g_y += (lat_g_target - state.acc_g_y) * (dt / 0.15)
    state.acc_g_y += harmonic_noise(
        t,
        [
            (0.028, 6.8, 1.1),
            (0.014, 14.3, 2.4),
            (0.007, 28.9, 0.7),
        ],
    )

    # Vertical: downforce + aero oscillation + road bump
    downforce_g = (state.speed_kmh / 300.0) ** 2 * 0.8
    aero_osc = _aero_oscillation(t, state.speed_kmh)
    road_bump = harmonic_noise(
        t,
        [
            (0.025, 9.1, 0.3),
            (0.012, 21.7, 1.6),
            (0.006, 43.5, 2.9),
        ],
    )
    state.acc_g_z = 1.0 + downforce_g + aero_osc + road_bump

    # ── Suspensión — tau reducido + road profile ──────────────────────────
    pitch = state.brake * 0.020 - state.throttle * 0.008
    roll = lat_g_target * 0.004
    aero_comp = (state.speed_kmh / 300.0) ** 2 * 0.005
    base_travel = 0.025 + aero_comp

    # Road profile del segmento actual
    seg_profile = _get_road_profile(seg.name)

    # Chatter por esquina (cada rueda tiene fase distinta)
    chatter_fl = _suspension_chatter(t, state.speed_kmh, seg_profile)
    chatter_fr = _suspension_chatter(t + 0.013, state.speed_kmh, seg_profile)
    chatter_rl = _suspension_chatter(t + 0.027, state.speed_kmh, seg_profile)
    chatter_rr = _suspension_chatter(t + 0.041, state.speed_kmh, seg_profile)

    state.suspension_fl = base_travel + pitch + roll + chatter_fl
    state.suspension_fr = base_travel + pitch - roll + chatter_fr
    state.suspension_rl = base_travel - pitch + roll + chatter_rl
    state.suspension_rr = base_travel - pitch - roll + chatter_rr

    for attr in ("suspension_fl", "suspension_fr", "suspension_rl", "suspension_rr"):
        setattr(state, attr, max(0.002, min(0.060, getattr(state, attr))))


# ─── Signal Enrichment Layer ──────────────────────────────────────────────────


def enrich_signals(signals: dict, state: CarState, seg: TrackSegment, t: float) -> dict:
    """
    Capa de enriquecimiento de señal.

    Recibe señales base (post-physics) y añade microdinámica determinista
    SIN romper macro comportamiento, fault injection ni estructura actual.

    Enriquecimientos aplicados:
    - RPM: drivetrain ripple adicional (segundo armónico de orden)
    - Suspension: curb vibration si en CHICANE/APEX
    - acc_g_x/y: vibración de chasis adicional
    - wheel_slip_*: high-freq tyre oscillation
    - speed_kmh: road texture fino
    """
    is_apex_or_chicane = seg.seg_type in ("APEX", "CHICANE")

    # ── RPM enrichment — harmonic de orden de motor ──────────────────────
    rpm_val = signals.get("rpms", 0.0)
    if rpm_val and rpm_val > 800:
        # Orden 3 y 6 del motor (característicos de V8/flat-6)
        f_order = rpm_val / 60.0
        rpm_enrich = harmonic_noise(
            t,
            [
                (25.0, f_order * 3.0, 0.9),
                (12.0, f_order * 6.0, 2.1),
                (6.0, f_order * 0.5, 1.5),
            ],
        )
        signals["rpms"] = max(800.0, rpm_val + rpm_enrich)

    # ── Suspension enrichment — curb vibration ────────────────────────────
    if is_apex_or_chicane:
        curb_amp = 0.003 if seg.seg_type == "CHICANE" else 0.0015
        for key, phase in [
            ("suspension_fl", 0.0),
            ("suspension_fr", 0.7),
            ("suspension_rl", 1.4),
            ("suspension_rr", 2.1),
        ]:
            if key in signals and signals[key] is not None:
                curb = harmonic_noise(
                    t,
                    [
                        (curb_amp, 17.3 + phase, phase),
                        (curb_amp * 0.5, 34.7 + phase, phase + 1.0),
                        (curb_amp * 0.3, 68.1 + phase, phase + 2.0),
                    ],
                )
                signals[key] = max(0.002, min(0.060, signals[key] + curb))

    # ── acc_g enrichment — chassis resonance ─────────────────────────────
    chassis_x = harmonic_noise(
        t,
        [
            (0.012, 47.3, 1.2),
            (0.006, 93.7, 2.5),
        ],
    )
    chassis_y = harmonic_noise(
        t,
        [
            (0.010, 51.8, 0.8),
            (0.005, 103.4, 1.9),
        ],
    )
    if "acc_g_x" in signals and signals["acc_g_x"] is not None:
        signals["acc_g_x"] += chassis_x
    if "acc_g_y" in signals and signals["acc_g_y"] is not None:
        signals["acc_g_y"] += chassis_y

    # ── wheel_slip enrichment — tyre oscillation ──────────────────────────
    for key, phase in [
        ("wheel_slip_fl", 0.0),
        ("wheel_slip_fr", 1.1),
        ("wheel_slip_rl", 2.2),
        ("wheel_slip_rr", 3.3),
    ]:
        if key in signals and signals[key] is not None:
            base_slip = signals[key]
            tyre_osc = harmonic_noise(
                t,
                [
                    (0.006, 31.4 + phase * 2, phase),
                    (0.003, 62.8 + phase * 2, phase + 1.5),
                ],
            )
            signals[key] = max(0.0, base_slip + tyre_osc)

    # ── speed_kmh enrichment — road texture ──────────────────────────────
    if "speed_kmh" in signals and signals["speed_kmh"] is not None:
        speed_texture = harmonic_noise(
            t,
            [
                (0.15, 11.7, 0.3),
                (0.08, 23.4, 1.6),
                (0.04, 47.1, 2.9),
            ],
        )
        signals["speed_kmh"] = max(0.0, signals["speed_kmh"] + speed_texture)

    return signals


# ─── Aplicar fallos activos ───────────────────────────────────────────────────


def get_active_faults(t_session: float) -> list[FaultWindow]:
    return [fw for fw in FAULT_SCHEDULE if fw.start_s <= t_session < fw.start_s + fw.duration_s]


def apply_faults(signals: dict, state: CarState, t_session: float) -> dict:
    """Modifica el dict de señales según los fallos activos en t_session."""
    active = get_active_faults(t_session)

    for fw in active:
        sid = fw.sensor_id
        if sid not in signals:
            continue

        if fw.fault_type == FaultType.STUCK:
            if sid not in state.stuck_values:
                state.stuck_values[sid] = signals[sid]
            signals[sid] = state.stuck_values[sid]

        elif fw.fault_type == FaultType.SPIKE:
            elapsed = t_session - fw.start_s
            if elapsed < DT * 2:
                signals[sid] = signals[sid] * fw.spike_magnitude
                log.warning("[FAULT] SPIKE en %s → %.2f", sid, signals[sid])

        elif fw.fault_type == FaultType.NOISE:
            elapsed = t_session - fw.start_s
            grow = 1.0 + (elapsed / fw.duration_s)
            # Ruido de fallo: sí usamos random.gauss aquí porque el fallo
            # NOISE es intencionalmente no determinista (simula degradación real)
            signals[sid] += random.gauss(0, fw.noise_sigma * grow)

        elif fw.fault_type == FaultType.DROPOUT:
            signals[sid] = None

        elif fw.fault_type == FaultType.FLATTY:
            elapsed = t_session - fw.start_s
            progress = min(1.0, elapsed / fw.duration_s)
            current = signals[sid]
            target = 180.0
            signals[sid] = current + (target - current) * progress * 0.08
            if elapsed < DT * 3:
                log.warning("[FAULT] FLATTY onset en %s", sid)

        elif fw.fault_type == FaultType.OFFSET:
            signals[sid] += fw.offset_value

    recovered = {
        sid
        for sid in state.stuck_values
        if not any(fw.sensor_id == sid and fw.fault_type == FaultType.STUCK for fw in active)
    }
    for sid in recovered:
        del state.stuck_values[sid]

    return signals


def state_to_signals(state: CarState) -> dict:
    return {
        "rpms": state.rpms,
        "brake": state.brake,
        "throttle": state.throttle,
        "gear": float(state.gear),
        "speed_kmh": state.speed_kmh,
        "fuel": state.fuel,
        "tyre_temp_fl": state.tyre_temp_fl,
        "tyre_temp_fr": state.tyre_temp_fr,
        "tyre_temp_rl": state.tyre_temp_rl,
        "tyre_temp_rr": state.tyre_temp_rr,
        "wheel_slip_fl": state.wheel_slip_fl,
        "wheel_slip_fr": state.wheel_slip_fr,
        "wheel_slip_rl": state.wheel_slip_rl,
        "wheel_slip_rr": state.wheel_slip_rr,
        "acc_g_x": state.acc_g_x,
        "acc_g_y": state.acc_g_y,
        "acc_g_z": state.acc_g_z,
        "suspension_fl": state.suspension_fl,
        "suspension_fr": state.suspension_fr,
        "suspension_rl": state.suspension_rl,
        "suspension_rr": state.suspension_rr,
    }


# ─── Bucle principal ──────────────────────────────────────────────────────────


async def run_simulator() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    log.info(
        "AC Simulator iniciado — destino %s:%d @ %.0f Hz", GATEWAY_HOST, GATEWAY_PORT, PUBLISH_HZ
    )
    log.info(
        "Duración de vuelta simulada: %.1f s | Fallos configurados: %d",
        LAP_DURATION_S,
        len(FAULT_SCHEDULE),
    )
    for fw in FAULT_SCHEDULE:
        log.info(
            "  [FAULT] t=%.0fs %s → %s (%.0fs)",
            fw.start_s,
            fw.sensor_id,
            fw.fault_type.value,
            fw.duration_s,
        )

    await asyncio.sleep(3.0)

    state = CarState()
    prev = state_to_signals(state)
    session_id = _build_session_id(2)
    t_session = 0.0
    packets_sent = 0

    while True:
        await asyncio.sleep(DT)
        t_session += DT
        state.seg_elapsed += DT
        state.lap_elapsed += DT

        # ── Avanzar segmento de pista ──────────────────────────────────────
        seg = LAP_SEGMENTS[state.segment_idx]
        if state.seg_elapsed >= seg.duration_s:
            state.seg_elapsed = 0.0
            state.segment_idx = (state.segment_idx + 1) % len(LAP_SEGMENTS)

        # ── Detectar vuelta completada ────────────────────────────────────
        if state.lap_elapsed >= LAP_DURATION_S:
            state.lap_number += 1
            lap_time_ms = int(state.lap_elapsed * 1000)

            tyre_penalty = max(0, (state.tyre_temp_fl + state.tyre_temp_rl) / 2 - 90) * 50
            # Variación de tiempo de vuelta: determinista basada en número de vuelta
            lap_var = int(
                harmonic_noise(state.lap_number * 0.7, [(1500, 0.13, 0.0), (800, 0.29, 1.4)])
            )
            lap_time_ms += int(tyre_penalty) + lap_var

            lap_pkt = {
                "event": "lap_completed",
                "session_id": session_id,
                "lap_number": state.lap_number,
                "lap_time_ms": max(60_000, lap_time_ms),
                "timestamp": datetime.now(UTC).isoformat(),
            }
            _send(sock, lap_pkt)
            log.info("LapEvent enviado: lap=%d time=%.3fs", state.lap_number, lap_time_ms / 1000)

            state.lap_elapsed = 0.0

            if LAP_COUNT > 0 and state.lap_number >= LAP_COUNT:
                log.info("Límite de %d vueltas alcanzado. Simulador detenido.", LAP_COUNT)
                return

        # ── Física ────────────────────────────────────────────────────────
        current_seg = LAP_SEGMENTS[state.segment_idx]
        step_physics(state, current_seg, DT, t_session)

        # ── Señales base ──────────────────────────────────────────────────
        signals = state_to_signals(state)

        # ── Signal Enrichment Layer ───────────────────────────────────────
        signals = enrich_signals(signals, state, current_seg, t_session)

        # ── Fallos aplicados ──────────────────────────────────────────────
        signals = apply_faults(signals, state, t_session)

        # ── Emitir por umbral de cambio ───────────────────────────────────
        ts = datetime.now(UTC).isoformat()
        for name, meta in SIGNALS.items():
            value = signals.get(name)
            if value is None:
                continue
            prev_v = prev.get(name, 0.0)
            if abs(value - prev_v) >= meta["threshold"]:
                pkt = {
                    "signal": meta["sensor_id"],
                    "value": round(value, 4),
                    "unit": meta["unit"],
                    "timestamp": ts,
                }
                _send(sock, pkt)
                prev[name] = value
                packets_sent += 1

        if packets_sent % 500 == 0 and packets_sent > 0:
            active_faults = get_active_faults(t_session)
            fault_names = [f"{fw.fault_type.value}:{fw.sensor_id}" for fw in active_faults]
            log.info(
                "t=%.0fs lap=%d speed=%.0f rpm=%.0f | paquetes=%d | fallos_activos=%s",
                t_session,
                state.lap_number,
                state.speed_kmh,
                state.rpms,
                packets_sent,
                fault_names or "ninguno",
            )


def _build_session_id(session_type: int) -> str:
    tipo = SESSION_TYPE_NAMES.get(session_type, "unknown")
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    return f"{ts}_{tipo}"


def _send(sock: socket.socket, pkt: dict) -> None:
    try:
        sock.sendto(json.dumps(pkt).encode(), (GATEWAY_HOST, GATEWAY_PORT))
    except OSError as e:
        log.warning("Error enviando UDP: %s", e)


if __name__ == "__main__":
    try:
        asyncio.run(run_simulator())
    except KeyboardInterrupt:
        log.info("Simulador AC detenido.")
