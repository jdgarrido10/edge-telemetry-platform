import json
import logging

import aiomqtt

from src.destinations.base import BaseDestination

log = logging.getLogger(__name__)

# Requiere: pip install aiomqtt
# IMPORTANTE: usar aiomqtt (async), no paho-mqtt (sync)
# Esta es exactamente la incompatibilidad que existía en el proyecto original


class MQTTDestination(BaseDestination):
    """
    Adaptador de salida: publica payloads en un broker MQTT.

    PREGUNTA ANTES DE IMPLEMENTAR:
    ¿Mantienes la conexión MQTT abierta permanentemente o abres/cierras por mensaje?
    ¿Qué trade-offs tiene cada opción?
    """

    def __init__(self, broker: str, port: int, topic: str) -> None:
        self.broker = broker
        self.port = port
        self.topic = topic
        # TODO: ¿necesitas mantener un cliente persistente aquí?
        self.client = aiomqtt.Client(hostname=self.broker, port=self.port)
        self._connected = False

    async def __aenter__(self):
        # Iniciar la conexión de red del cliente interno
        await self.client.__aenter__()
        self._connected = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Cerrar la conexión de red limpiamente
        await self.client.__aexit__(exc_type, exc_val, exc_tb)

    async def send(self, payload: dict) -> bool:
        # TODO: publicar payload serializado como JSON en self.topic
        # TODO: manejar timeout de conexión
        # TODO: devolver True/False según resultado
        try:
            str_payload = json.dumps(payload)
            await self.client.publish(self.topic, str_payload)
            self._connected = True
            return True
        except aiomqtt.MqttError:
            self._connected = False
            try:
                self.client = aiomqtt.Client(hostname=self.broker, port=self.port)
                await self.client.__aenter__()
                self._connected = True
            except aiomqtt.MqttError:
                pass
            return False

    async def is_available(self) -> bool:
        # TODO: intentar ping al broker o verificar conexión activa
        return self._connected
