import sqlite3
from contextlib import contextmanager
from typing import Iterable, Optional

from config import Config


CREATE_QUEUE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS queue (
    url TEXT PRIMARY KEY,
    category TEXT,
    publish_date TEXT,
    status TEXT DEFAULT 'PENDING',
    retries INTEGER DEFAULT 0,
    last_error TEXT
);
"""


@contextmanager
def get_conn():
    conn = sqlite3.connect(Config.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(CREATE_QUEUE_TABLE_SQL)


def enqueue_urls(rows: Iterable[tuple[str, str, Optional[str]]]) -> int:
    with get_conn() as conn:
        cur = conn.executemany(
            """
            INSERT OR IGNORE INTO queue(url, category, publish_date)
            VALUES (?, ?, ?)
            """,
            rows,
        )
        return cur.rowcount


def claim_pending(limit: int) -> list[sqlite3.Row]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT url, category, publish_date, retries
            FROM queue
            WHERE status='PENDING'
            ORDER BY publish_date IS NULL, publish_date
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE queue SET status='PROCESSING' WHERE url=?",
                (row["url"],),
            )
        return rows


def mark_done(url: str) -> None:
    with get_conn() as conn:
        conn.execute("UPDATE queue SET status='DONE', last_error=NULL WHERE url=?", (url,))


def mark_error(url: str, error_message: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE queue
            SET status=CASE WHEN retries + 1 >= ? THEN 'ERROR' ELSE 'PENDING' END,
                retries=retries + 1,
                last_error=?
            WHERE url=?
            """,
            (Config.RETRIES, error_message[:500], url),
        )


def stats() -> dict[str, int]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS total FROM queue GROUP BY status"
        ).fetchall()
        return {row["status"]: row["total"] for row in rows}
