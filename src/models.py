from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class QualityStatus(StrEnum):
    GOOD = "Good"
    BAD = "Bad"
    UNCERTAIN = "Uncertain"


class SensorReading(BaseModel):
    """
    Contrato canónico de una lectura de sensor.

    """

    sensor_id: str = Field(
        ..., min_length=3, description="Identificador único alfanumérico del sensor en la planta"
    )
    timestamp: datetime = Field(
        ..., description="Momento exacto de la lectura. Obligatorio incluir zona horaria (UTC)"
    )
    # None solo cuando quality=BAD y OPC UA envía Variant(Null) — el model_validator lo garantiza
    value: float | None = Field(
        None,
        description="Valor numérico de la lectura procesada. None si quality=BAD y el sensor no emitió dato válido",
    )
    quality: QualityStatus = Field(..., description="Estado de calidad de la lectura")
    unit: str = Field(..., description="Unidad de medida física (ej. '°C', 'bar')")
    opc_node_id: str | None = Field(
        None, description="Identificador del nodo OPC UA. None para fuentes no-OPC UA"
    )
    received_at: datetime = Field(
        ..., description="Marca de tiempo local de cuando el gateway recibió el dato"
    )
    raw_value: float | None = Field(
        None,
        description="Valor crudo original antes de cualquier conversión. None si quality=BAD y OPC UA no envió valor",
    )

    @field_validator("timestamp", mode="after")
    @classmethod
    def ensure_timezone_aware(cls, v: Any) -> datetime:
        """
        PREGUNTA: ¿por qué rechazamos timestamps naive?
        ¿Qué problema concreto evita este validador en un sistema distribuido?
        Escribe la respuesta como docstring antes de implementar.
        Rechazamos el timestamp naive porque tiene que haber un estandar si el sistema esta desplegado en varias zonas horarias, o la fabrica en un sitio y el servidor en otro
        """
        if v.tzinfo is None:
            raise ValueError("Error, datetime naive")
        return v

    @field_validator("value", mode="before")
    @classmethod
    def reject_non_finite(cls, v: Any) -> float | None:
        """
        PREGUNTA: ¿por qué NaN es especialmente peligroso en series temporales?
        ¿Qué pasa con NaN en una query de promedio en InfluxDB?
        """
        if v is None:
            return None
        if v != v:
            raise ValueError("El valor de la lectura no puede ser NaN")
        if v == float("inf") or v == float("-inf"):
            raise ValueError("El valor de la lectura no puede ser infinito")
        return v

    @model_validator(mode="after")
    def value_required_unless_bad(self) -> SensorReading:
        """
        Invariante: value solo puede ser None cuando quality=BAD.
        OPC UA envía Variant(Null) en lecturas Bad — es el único caso legítimo.
        Good y Uncertain siempre deben tener un número; si no, es un bug del servidor.
        """
        if self.quality != QualityStatus.BAD and self.value is None:
            raise ValueError(
                f"value no puede ser None cuando quality={self.quality.value}; "
                "solo se permite None con quality=BAD"
            )
        return self

    @classmethod
    def from_opc_notification(
        cls,
        node_id: str,
        value: Any,
        source_timestamp: datetime,
        status_text: str,
        sensor_id: str,
        unit: str | None = "N/A",
    ) -> SensorReading:
        """
        Constructor semántico: crea un SensorReading desde una notificación OPC UA.

        PREGUNTA DE DISEÑO: ¿por qué un constructor semántico en lugar de __init__ directamente?
        ¿Qué cambia si mañana el formato de la notificación OPC UA cambia?
        """
        if status_text == "Good":
            quality_value = QualityStatus.GOOD
        elif status_text == "Uncertain":
            quality_value = QualityStatus.UNCERTAIN
        else:
            quality_value = QualityStatus.BAD
        return cls(
            sensor_id=sensor_id,
            value=value,
            timestamp=source_timestamp,
            quality=quality_value,
            unit=unit,
            opc_node_id=node_id,
            received_at=datetime.now(UTC),
            raw_value=value,
        )

    def to_mqtt_payload(self) -> dict:
        """
        PREGUNTA: ¿incluyes todos los campos o solo los que necesita el broker?
        ¿Qué criterio usas para decidir qué excluir?
        """
        return self.model_dump(mode="json")

    @classmethod
    def from_ac_packet(cls, data: dict, signal_config: dict) -> SensorReading:
        rango = signal_config["range"]
        value = data["value"]

        if rango[0] <= value <= rango[1]:
            quality = QualityStatus.GOOD
        else:
            quality = QualityStatus.BAD

        return cls(
            sensor_id=signal_config["sensor_id"],
            value=value,
            raw_value=value,
            unit=signal_config["unit"],
            timestamp=data["timestamp"],
            opc_node_id=None,
            received_at=datetime.now(UTC),
            quality=quality,
        )


class LapEvent(BaseModel):
    session_id: str
    lap_number: int
    lap_time_ms: int
    timestamp: datetime

    @field_validator("timestamp", mode="after")
    @classmethod
    def ensure_timezone_aware(cls, v: Any) -> datetime:
        if v.tzinfo is None:
            raise ValueError("Error, datetime naive")
        return v

    @classmethod
    def from_ac_packet(cls, packet: dict) -> LapEvent:
        return cls(
            session_id=packet["session_id"],
            lap_number=packet["lap_number"],
            lap_time_ms=packet["lap_time_ms"],
            timestamp=datetime.fromisoformat(packet["timestamp"]),
        )

    def to_mqtt_payload(self) -> dict:
        return self.model_dump(mode="json")
