import os

from db.client import db
from core.config import (
    DB_PATH,
    EXPORT_DIR,
    BACKUP_DIR,
)

async def init_db():

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(EXPORT_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)

    conn = await db()

    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA temp_store=MEMORY")

    await conn.execute('''
    CREATE TABLE IF NOT EXISTS topics (
        name TEXT PRIMARY KEY,
        enabled INTEGER DEFAULT 1,
        count INTEGER DEFAULT 0,
        reset_count_base INTEGER DEFAULT 0,
        created_at INTEGER,
        updated_at INTEGER
    )
    ''')

    # Backward-compatible migration for existing databases.
    try:
        await conn.execute(
            "ALTER TABLE topics ADD COLUMN reset_count_base INTEGER DEFAULT 0"
        )
    except Exception:
        pass

    await conn.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT,
        ntfy_id TEXT,
        message TEXT,
        priority INTEGER,
        created_at INTEGER,
        UNIQUE(topic, ntfy_id)
    )
    ''')

    await conn.execute('''
    CREATE TABLE IF NOT EXISTS errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER,
        component TEXT,
        topic TEXT,
        error TEXT
    )
    ''')

    await conn.execute('''
    CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
    USING fts5(topic, message)
    ''')

    await conn.execute('''
    CREATE TABLE IF NOT EXISTS telegram_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        attempts INTEGER DEFAULT 0,
        next_attempt_at INTEGER DEFAULT 0
    )
    ''')

    try:
        await conn.execute(
            "ALTER TABLE telegram_queue ADD COLUMN attempts INTEGER DEFAULT 0"
        )
    except Exception:
        pass

    try:
        await conn.execute(
            "ALTER TABLE telegram_queue ADD COLUMN next_attempt_at INTEGER DEFAULT 0"
        )
    except Exception:
        pass

    await conn.execute('''
    CREATE TABLE IF NOT EXISTS topic_status_counts (
        topic TEXT PRIMARY KEY,
        received INTEGER DEFAULT 0,
        filtered INTEGER DEFAULT 0,
        rate_limited INTEGER DEFAULT 0,
        disabled INTEGER DEFAULT 0,
        updated_at INTEGER
    )
    ''')

    try:
        await conn.execute(
            "ALTER TABLE topic_status_counts ADD COLUMN disabled INTEGER DEFAULT 0"
        )
    except Exception:
        pass

    await conn.execute('''
    CREATE TABLE IF NOT EXISTS telegram_dead_letter (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT NOT NULL,
        topic TEXT,
        last_error TEXT,
        attempts INTEGER DEFAULT 0,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )
    ''')

    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_errors_ts ON errors(ts)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_tgq_next_attempt ON telegram_queue(next_attempt_at, id)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_topics_enabled ON topics(enabled)"
    )

    await conn.commit()
    await conn.close()
