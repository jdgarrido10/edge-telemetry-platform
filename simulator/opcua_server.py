import asyncio
import logging
import math
import random
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from asyncua import Server, ua
from asyncua.ua import DataValue, StatusCode, StatusCodes

from src.settings import settings

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=settings.log_level_int,
    format=settings.LOG_FORMAT,
)
log = logging.getLogger(__name__)

# ─── Modelos de estado de simulación ─────────────────────────────────────────


@dataclass
class TempState:
    """Estado interno del modelo de temperatura con deriva."""

    base: float
    drift_rate: float
    drift_limit: float
    noise_sigma: float
    drift_acc: float = 0.0
    drift_direction: int = 1

    def step(self, dt: float, extra_noise: float = 0.0, extra_drift: float = 0.0) -> float:
        effective_drift = (self.drift_rate + extra_drift) * self.drift_direction
        self.drift_acc += effective_drift * dt
        if abs(self.drift_acc) >= self.drift_limit:
            self.drift_acc = math.copysign(self.drift_limit, self.drift_acc)
            self.drift_direction *= -1
        sigma = self.noise_sigma + extra_noise
        return self.base + self.drift_acc + random.gauss(0, sigma)


@dataclass
class PressureState:
    base: float
    amplitude: float
    period: float
    rise_fraction: float
    noise_sigma: float
    phase: float = 0.0

    def step(self, dt: float) -> float:
        rise_time = self.period * self.rise_fraction
        fall_time = self.period * (1.0 - self.rise_fraction)
        in_rise = self.phase < math.pi
        if in_rise:
            dphi = (math.pi / rise_time) * dt
        else:
            dphi = (math.pi / fall_time) * dt
        self.phase = (self.phase + dphi) % (2 * math.pi)
        cycle_value = self.base + self.amplitude * math.sin(self.phase)
        return cycle_value + random.gauss(0, self.noise_sigma)


@dataclass
class VibrationState:
    base: float
    noise_sigma: float
    fault_cfg: dict = field(default_factory=dict)
    phase_fault: float = 0.0

    def step(self, dt: float, t: float) -> float:
        value = self.base + random.gauss(0, self.noise_sigma)
        fault = self.fault_cfg
        if not fault.get("enabled", False):
            return value
        start = fault.get("start_offset_s", 0)
        end = start + fault.get("duration_s", 0)
        if start <= t < end:
            period = fault.get("harmonic_period", 12.0)
            amp = fault.get("harmonic_amplitude", 0.3)
            self.phase_fault = (self.phase_fault + (2 * math.pi / period) * dt) % (2 * math.pi)
            value += amp * math.sin(self.phase_fault)
        return value


@dataclass
class DiscreteSpeedState:
    levels: list
    current_idx: int
    prob_change: float
    noise_sigma: float

    def step(self) -> float:
        if random.random() < self.prob_change:
            delta = random.choice([-1, 1])
            self.current_idx = (self.current_idx + delta) % len(self.levels)
        return self.levels[self.current_idx] + random.gauss(0, self.noise_sigma)


# ─── Gestión de ventanas BAD programadas ─────────────────────────────────────


class FaultWindowManager:
    def __init__(self, windows: list):
        self.windows = windows

    def is_bad(self, sensor_id: str, t: float) -> bool:
        for w in self.windows:
            if w["sensor_id"] != sensor_id:
                continue
            start = w.get("start_offset_s", 0)
            end = start + w.get("duration_s", 0)
            if start <= t < end:
                return True
        return False


# ─── Escritura de valores con StatusCode ─────────────────────────────────────


async def write_value(node, value: float, bad: bool = False) -> str:
    if bad:
        code = StatusCodes.BadNoCommunication
        label = "BAD"
    else:
        code = StatusCodes.Good
        label = "Good"
    dv = DataValue(
        Value=ua.Variant(value, ua.VariantType.Double),
        StatusCode_=StatusCode(code),
    )
    await node.write_value(dv)
    return label


# ─── Carga de configuración ───────────────────────────────────────────────────


def load_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_sim(cfg: dict, key: str, default=None):
    """Acceso seguro a parámetros de simulación."""
    return cfg.get("sim", {}).get(key, default)


# ─── Main ─────────────────────────────────────────────────────────────────────


async def main() -> None:
    yaml_path = Path(settings.SIM_SENSORS_CONFIG)
    sim_yaml_path = Path(settings.SIM_SIM_SENSORS_CONFIG)

    config = load_config(yaml_path)
    sim_config = load_config(sim_yaml_path)
    sensores = config.get("sensores", [])
    sim_sensores = sim_config.get("sensores", [])
    fault_windows = sim_config.get("fault_windows", [])

    fault_mgr = FaultWindowManager(fault_windows)

    server = Server()
    await server.init()
    server.set_endpoint(settings.sim_endpoint)
    idx = await server.register_namespace(settings.OPC_UA_NAMESPACE_URI)

    nodes: dict[str, object] = {}
    states: dict[str, object] = {}
    sensor_map: list[dict] = []
    machines: dict[str, object] = {}
    sim_index = {s["sensor_id"]: s for s in sim_sensores}

    for s in sensores:
        maquina_name = s["maquina"]
        variable_name = s["variable"]
        sensor_id = s["sensor_id"]
        sim_cfg = sim_index.get(sensor_id)
        if sim_cfg is None:
            log.warning("Sin config de simulación para %s — emitirá valor estático", sensor_id)

        tipo = sim_cfg.get("tipo", "generico") if sim_cfg else "generico"
        sim = sim_cfg.get("sim", {}) if sim_cfg else {}

        if maquina_name not in machines:
            machines[maquina_name] = await server.nodes.objects.add_object(idx, maquina_name)

        machine_node = machines[maquina_name]

        if tipo == "velocidad_discreta":
            initial = sim.get("niveles", [0.0])[sim.get("nivel_inicial", 0)]
        else:
            initial = sim.get("base", 0.0)

        node = await machine_node.add_variable(
            idx, variable_name, ua.Variant(initial, ua.VariantType.Double)
        )
        await node.set_writable()
        nodes[sensor_id] = node

        if tipo == "temperatura":
            states[sensor_id] = TempState(
                base=sim["base"],
                drift_rate=sim.get("drift_rate", 0.002),
                drift_limit=sim.get("drift_limit", 5.0),
                noise_sigma=sim.get("noise_sigma", 0.2),
            )
        elif tipo == "presion":
            states[sensor_id] = PressureState(
                base=sim["base"],
                amplitude=sim.get("amplitude", 0.3),
                period=sim.get("period", 180.0),
                rise_fraction=sim.get("rise_fraction", 0.25),
                noise_sigma=sim.get("noise_sigma", 0.05),
            )
        elif tipo == "vibracion":
            states[sensor_id] = VibrationState(
                base=sim.get("base", 0.45),
                noise_sigma=sim.get("noise_sigma", 0.03),
                fault_cfg=sim.get("fault", {}),
            )
        elif tipo == "velocidad_discreta":
            states[sensor_id] = DiscreteSpeedState(
                levels=sim.get("niveles", [1000.0]),
                current_idx=sim.get("nivel_inicial", 0),
                prob_change=sim.get("prob_cambio", 0.05),
                noise_sigma=sim.get("noise_sigma", 5.0),
            )

        sensor_map.append(s)

    log.info(
        "Servidor arrancado en %s | %d nodos | %d fault_windows",
        settings.sim_endpoint,
        len(nodes),
        len(fault_windows),
    )

    for s in sim_sensores:
        sim = s.get("sim", {})
        if sim.get("fault", {}).get("enabled", False):
            log.info(
                "  [FAULT] %s: armónico de fallo activo (inicio=%ss, duración=%ss)",
                s["sensor_id"],
                sim["fault"].get("start_offset_s"),
                sim["fault"].get("duration_s"),
            )
        if sim.get("degradation", {}).get("enabled", False):
            log.info(
                "  [DEGRAD] %s: degradación gradual activa (inicio=%ss)",
                s["sensor_id"],
                sim["degradation"].get("start_offset_s"),
            )
    for w in fault_windows:
        log.info(
            "  [BAD_WINDOW] %s: BAD en t=[%s, %s]s — %s",
            w["sensor_id"],
            w["start_offset_s"],
            w["start_offset_s"] + w["duration_s"],
            w.get("reason", ""),
        )

    t = 0.0
    dt = settings.SIM_PUBLISH_INTERVAL_S

    async with server:
        while True:
            t += dt
            log_parts = [f"t={t:.0f}s"]

            for s in sensor_map:
                sensor_id = s["sensor_id"]
                sim_cfg = sim_index.get(sensor_id)
                tipo = sim_cfg.get("tipo", "generico") if sim_cfg else "generico"
                sim = sim_cfg.get("sim", {}) if sim_cfg else {}
                state = states.get(sensor_id)
                node = nodes[sensor_id]

                extra_noise = 0.0
                extra_drift = 0.0
                degrad_cfg = sim.get("degradation", {})
                if degrad_cfg.get("enabled", False):
                    d_start = degrad_cfg.get("start_offset_s", 0)
                    if t >= d_start:
                        elapsed_degrad = t - d_start
                        extra_noise = degrad_cfg.get("noise_growth_rate", 0.001) * elapsed_degrad
                        extra_drift = degrad_cfg.get("drift_growth_rate", 0.0005) * elapsed_degrad

                if tipo == "temperatura" and isinstance(state, TempState):
                    value = state.step(dt, extra_noise=extra_noise, extra_drift=extra_drift)
                elif tipo == "presion" and isinstance(state, PressureState):
                    value = state.step(dt)
                elif tipo == "vibracion" and isinstance(state, VibrationState):
                    value = state.step(dt, t)
                elif tipo == "velocidad_discreta" and isinstance(state, DiscreteSpeedState):
                    value = state.step()
                else:
                    value = sim.get("base", 0.0)

                is_bad = fault_mgr.is_bad(sensor_id, t)
                status_label = await write_value(node, value, bad=is_bad)

                var_name = s["variable"]
                unit = s.get("unit", "")
                log_parts.append(f"{var_name}={value:.2f}{unit}[{status_label}]")

            log.info(" | ".join(log_parts))
            await asyncio.sleep(dt)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Simulador detenido.")
