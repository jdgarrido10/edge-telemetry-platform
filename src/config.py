"""
config.py — Loaders de archivos YAML de configuración.

Sin cambios en la lógica de negocio respecto al original.
Los paths por defecto ahora se toman de settings en lugar de estar hardcodeados.
"""

import yaml

from src.settings import settings


def load_sensor_map(path: str = settings.SENSORS_CONFIG) -> dict:
    with open(path) as f:
        config = yaml.safe_load(f)
        data_dict = {
            (s["maquina"], s["variable"]): {"sensor_id": s["sensor_id"], "unit": s["unit"]}
            for s in config["sensores"]
        }
    return data_dict


def load_ac_signals(path: str = settings.AC_SIGNALS_CONFIG) -> dict:
    with open(path) as f:
        config = yaml.safe_load(f)
        signal_dict = {
            s["signal"]: {
                "sensor_id": s["sensor_id"],
                "unit": s["unit"],
                "criticality": s["criticality"],
                "range": s["range"],
            }
            for s in config["signals"]
        }
    return signal_dict


def load_analysis_config(path: str = settings.ANALYSIS_CONFIG) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)
