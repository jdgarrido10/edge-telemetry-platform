import asyncio
import json

from src.anomaly_detector import AnomalyDetector
from src.metrics import GatewayMetrics
from src.settings import settings


async def http_server(metrics: GatewayMetrics, detector: AnomalyDetector):
    def make_response(status: str, body: dict) -> bytes:
        body_str = json.dumps(body)
        return f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\r\n{body_str}".encode()

    async def handle_request(reader, writer):
        line = await reader.readline()
        route = line.decode().split(" ")
        path = route[1]
        if path == settings.HTTP_ROUTE_METRICS:
            body = await metrics.snapshot()
            response = make_response("200 OK", body)
        elif path == settings.HTTP_ROUTE_HEALTH:
            body = await metrics.health()
            response = make_response("200 OK", body)
        elif path == settings.HTTP_ROUTE_ALERTS:
            body = detector.get_active_alerts()
            response = make_response("200 OK", body)
        else:
            response = make_response("404 Not Found", {"error": "not found"})

        writer.write(response)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle_request, settings.HTTP_HOST, settings.HTTP_PORT)
    async with server:
        await server.serve_forever()
