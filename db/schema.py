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
        created_at INTEGER,
        updated_at INTEGER
    )
    ''')

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
        created_at INTEGER NOT NULL
    )
    ''')

    await conn.commit()
    await conn.close()
