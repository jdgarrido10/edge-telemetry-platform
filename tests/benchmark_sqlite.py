import asyncio
import logging
import os
import time

from src.store_forward import StoreAndForward

logging.getLogger().setLevel(logging.WARNING)


async def benchmark(total_messages: int, batch_size: int, label: str):
    store = StoreAndForward()

    payload = {
        "opc_node_id": "ns=2;i=2",
        "sensor_id": "planta1.maquina1.temperatura",
        "value": 42.5,
        "raw_value": 42.5,
        "unit": "°C",
        "quality": "Good",
        "timestamp": "2026-05-09T10:00:00+00:00",
        "received_at": "2026-05-09T10:00:00+00:00",
    }

    batches = []
    current_batch = []
    for _ in range(total_messages):
        current_batch.append(payload)
        if len(current_batch) == batch_size:
            batches.append(current_batch)
            current_batch = []
    if current_batch:
        batches.append(current_batch)

    start = time.perf_counter()

    for batch in batches:
        await store.save_batch(batch)

    total_time = time.perf_counter() - start
    actual_rate = total_messages / total_time
    print(
        f"[{label}] {total_messages} msgs (Lotes de {batch_size}) → {actual_rate:.1f} msg/s en {total_time:.3f}s"
    )


async def main():
    db_path = "buffer.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        print("Borrando buffer.db antiguo...")

    print("\n--- INICIANDO BENCHMARK DE TRANSACCIONES SQLITE ---")

    await benchmark(total_messages=10000, batch_size=1, label="UNO A UNO")

    await benchmark(total_messages=10000, batch_size=500, label="BATCH 500")

    await benchmark(total_messages=50000, batch_size=1000, label="BATCH 1000")
    print("---------------------------------------------------\n")


if __name__ == "__main__":
    asyncio.run(main())
