import asyncio
import statistics
from collections import deque

from src.destinations.mqtt_destination import MQTTDestination
from src.settings import settings
from src.store_forward import StoreAndForward


class GatewayMetrics:
    def __init__(
        self,
        store: StoreAndForward,
        queue: asyncio.Queue,
        dest: MQTTDestination,
        avaliable_event: asyncio.Event,
    ) -> None:
        self._store = store
        self._queue = queue
        self.cont_proce_total = 0
        self.cont_fallo_total = 0
        self.destination = dest
        self.event = avaliable_event
        self.latency: deque[float] = deque(maxlen=settings.LATENCY_WINDOW_SIZE)

    async def snapshot(self) -> dict:
        p50 = p95 = p99 = 0
        pend = await self._store.pending_count()
        qsize = self._queue.qsize()
        if len(self.latency) >= settings.LATENCY_MIN_SAMPLES:
            cut = statistics.quantiles(self.latency, n=100)
            p50, p95, p99 = cut[49], cut[94], cut[98]
        return {
            "pending": pend,
            "queueSize": qsize,
            "cont_total": self.cont_proce_total,
            "cont_fails": self.cont_fallo_total,
            "latency_p50": p50,
            "latency_p95": p95,
            "latency_p99": p99,
        }

    async def health(self) -> dict:
        pend = await self._store.pending_count()
        mqtt_ok = await self.destination.is_available()
        if not mqtt_ok or self.event.is_set():
            status = "unhealthy"
        elif self._queue.qsize() >= self._queue.maxsize * settings.QUEUE_DEGRADED_THRESHOLD:
            status = "degraded"
        else:
            status = "healthy"
        return {
            "status": status,
            "queue_size": self._queue.qsize(),
            "sql_pending": pend,
            "mqtt_connected": mqtt_ok,
            "opc_ua_connected": not self.event.is_set(),
        }

    def add_latency(self, seconds: float) -> None:
        self.latency.append(seconds)
