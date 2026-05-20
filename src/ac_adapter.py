import asyncio
import json
import logging

from pydantic import ValidationError

from src.config import load_ac_signals
from src.models import LapEvent, SensorReading
from src.settings import settings

log = logging.getLogger(__name__)


class ACProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue, ac_signals: dict):
        self._queue = queue
        self._ac_signals = ac_signals

    def datagram_received(self, data: bytes, addr):
        log.debug("Paquete UDP recibido de %s", addr)
        try:
            packet = json.loads(data.decode())

            if packet.get("event") == "lap_completed":
                event = LapEvent.from_ac_packet(packet)
                log.info("LapEvent creado: lap=%d session=%s", event.lap_number, event.session_id)
                self._queue.put_nowait(event)
                return

            signal_name = packet.get("signal")
            signal_config = self._ac_signals.get(signal_name)
            if signal_config is None:
                log.warning("Señal desconocida ignorada: %s", signal_name)
                return

            reading = SensorReading.from_ac_packet(packet, signal_config)
            log.info("SensorReading creado: %s quality=%s", reading.sensor_id, reading.quality)
            self._queue.put_nowait(reading)

        except ValidationError as e:
            log.error("ValidationError: %s", e)
        except asyncio.QueueFull:
            log.warning("Cola llena — descartando dato")
        except Exception as e:
            log.error("Error inesperado: %s", e)


async def ac_adapter(
    queue: asyncio.Queue,
    host: str = settings.AC_UDP_HOST,
    port: int = settings.AC_UDP_PORT,
) -> None:
    ac_signals = load_ac_signals(settings.AC_SIGNALS_CONFIG)
    log.info(
        "Adaptador AC escuchando en %s:%d — %d señales configuradas", host, port, len(ac_signals)
    )
    loop = asyncio.get_event_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: ACProtocol(queue, ac_signals),
        local_addr=(host, port),
    )
    try:
        await asyncio.Future()
    finally:
        transport.close()
