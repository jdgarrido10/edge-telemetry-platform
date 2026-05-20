import asyncio
import logging
import sqlite3

from src.settings import settings

log = logging.getLogger(__name__)


class StoreAndForward:
    """
    Responsabilidad única: persistir mensajes que no pudieron enviarse
    y recuperarlos en orden FIFO cuando el destino vuelva a estar disponible.
    """

    def __init__(self, db_path: str = settings.DB_PATH) -> None:
        self.db_path = db_path
        self._conn = None
        self._lock = asyncio.Lock()
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn = conn
        cursor = conn.cursor()
        log.info("Inicializando SQLite store_forward en %s", self.db_path)
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS buffer ("
            "id INTEGER PRIMARY KEY,"
            "opc_node_id  TEXT,"
            "sensor_id    TEXT,"
            "value        REAL,"
            "raw_value    REAL,"
            "unit         TEXT,"
            "quality      TEXT,"
            "timestamp    TEXT,"
            "received_at  TEXT)"
        )
        conn.commit()
        log.info("SQLite lista: tabla buffer disponible")

    async def save_batch(self, messages: list[dict]) -> None:
        if self._conn is None:
            log.error("Base de datos no inicializada")
            return None
        if not messages:
            log.debug("save_batch llamado con lote vacío")
            return None
        async with self._lock:
            with self._conn:
                cursor = self._conn.cursor()
                datos_para_guardar = [
                    (
                        m["opc_node_id"],
                        m["sensor_id"],
                        m["value"],
                        m["raw_value"],
                        m["unit"],
                        m["quality"],
                        m["timestamp"],
                        m["received_at"],
                    )
                    for m in messages
                ]
                cursor.executemany(
                    "INSERT INTO buffer (opc_node_id, sensor_id, value, raw_value, unit, quality, timestamp, received_at) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    datos_para_guardar,
                )

    async def get_batch(self, size: int = settings.DB_BATCH_SIZE) -> list[tuple]:
        if self._conn is None:
            log.error("Base de datos no inicializada")
            return []
        async with self._lock:
            with self._conn:
                cursor = self._conn.cursor()
                cursor.execute("SELECT * FROM buffer ORDER BY id ASC LIMIT ?", (size,))
                result = cursor.fetchall()
                if result:
                    log.info("Recuperado lote FIFO de %d mensajes desde SQLite", len(result))
                return result

    async def delete_batch(self, ids: list[int]) -> None:
        if self._conn is None:
            log.error("Base de datos no inicializada")
            return None
        async with self._lock:
            with self._conn:
                cursor = self._conn.cursor()
                if len(ids) <= 0:
                    log.debug("delete_batch sin ids confirmados; no se borra nada")
                    return
                placeholders = ", ".join("?" for _ in ids)
                cursor.execute(f"DELETE FROM buffer WHERE id IN ({placeholders})", ids)
                log.info("Eliminados %d mensajes confirmados de SQLite", cursor.rowcount)

    async def pending_count(self) -> int:
        if self._conn is None:
            log.error("Base de datos no inicializada")
            return 0
        async with self._lock:
            with self._conn:
                cursor = self._conn.cursor()
                cursor.execute("SELECT COUNT(id) FROM buffer")
                result = cursor.fetchone()
                count = result[0] if result else 0
                log.debug("Pendientes actuales en SQLite: %d", count)
                return count
