import sqlite3
import aiosqlite

from core.config import DB_PATH

async def db():

    conn = await aiosqlite.connect(DB_PATH)

    conn.row_factory = sqlite3.Row

    return conn
