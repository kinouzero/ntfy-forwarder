import json
import time

from db.client import db


def _loads_config(raw):
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


async def list_delivery_targets(*, include_disabled=True):
    conn = await db()
    sql = (
        "SELECT id, name, kind, config, enabled, is_default, created_at, updated_at "
        "FROM delivery_targets"
    )
    args = ()
    if not include_disabled:
        sql += " WHERE enabled = 1"
    sql += " ORDER BY is_default DESC, name ASC"
    cur = await conn.execute(sql, args)
    rows = await cur.fetchall()
    await conn.close()
    items = []
    for row in rows:
        items.append(
            {
                "id": int(row["id"]),
                "name": row["name"],
                "kind": row["kind"],
                "config": _loads_config(row["config"]),
                "enabled": bool(row["enabled"]),
                "is_default": bool(row["is_default"]),
                "created_at": int(row["created_at"] or 0),
                "updated_at": int(row["updated_at"] or 0),
            }
        )
    return items


async def get_delivery_target(target_id):
    conn = await db()
    cur = await conn.execute(
        (
            "SELECT id, name, kind, config, enabled, is_default, created_at, updated_at "
            "FROM delivery_targets WHERE id = ?"
        ),
        (int(target_id),),
    )
    row = await cur.fetchone()
    await conn.close()
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "kind": row["kind"],
        "config": _loads_config(row["config"]),
        "enabled": bool(row["enabled"]),
        "is_default": bool(row["is_default"]),
        "created_at": int(row["created_at"] or 0),
        "updated_at": int(row["updated_at"] or 0),
    }


async def create_delivery_target(name, kind, config=None, enabled=True, is_default=False):
    ts = int(time.time())
    conn = await db()
    if is_default:
        await conn.execute(
            "UPDATE delivery_targets SET is_default = 0 WHERE is_default = 1"
        )
    cur = await conn.execute(
        (
            "INSERT INTO delivery_targets("
            "name, kind, config, enabled, is_default, created_at, updated_at"
            ") "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        ),
        (
            name.strip(),
            kind.strip(),
            json.dumps(config or {}, ensure_ascii=True),
            1 if enabled else 0,
            1 if is_default else 0,
            ts,
            ts,
        ),
    )
    await conn.commit()
    target_id = int(getattr(cur, "lastrowid", cur._cursor.lastrowid))
    await conn.close()
    return target_id


async def update_delivery_target(target_id, *, name=None, kind=None, config=None, enabled=None):
    fields = []
    args = []
    if name is not None:
        fields.append("name = ?")
        args.append(name.strip())
    if kind is not None:
        fields.append("kind = ?")
        args.append(kind.strip())
    if config is not None:
        fields.append("config = ?")
        args.append(json.dumps(config, ensure_ascii=True))
    if enabled is not None:
        fields.append("enabled = ?")
        args.append(1 if enabled else 0)
    fields.append("updated_at = ?")
    args.append(int(time.time()))
    args.append(int(target_id))

    conn = await db()
    await conn.execute(
        f"UPDATE delivery_targets SET {', '.join(fields)} WHERE id = ?",
        tuple(args),
    )
    await conn.commit()
    await conn.close()


async def delete_delivery_target(target_id):
    conn = await db()
    await conn.execute(
        "DELETE FROM topic_target_map WHERE target_id = ?",
        (int(target_id),),
    )
    await conn.execute(
        "DELETE FROM delivery_targets WHERE id = ?",
        (int(target_id),),
    )
    await conn.commit()
    await conn.close()


async def set_default_delivery_target(target_id):
    conn = await db()
    await conn.execute("UPDATE delivery_targets SET is_default = 0 WHERE is_default = 1")
    if target_id is not None:
        await conn.execute(
            "UPDATE delivery_targets SET is_default = 1, updated_at = ? WHERE id = ?",
            (int(time.time()), int(target_id)),
        )
    await conn.commit()
    await conn.close()


async def get_default_delivery_target():
    conn = await db()
    cur = await conn.execute(
        (
            "SELECT id, name, kind, config, enabled, is_default, created_at, updated_at "
            "FROM delivery_targets WHERE is_default = 1 LIMIT 1"
        )
    )
    row = await cur.fetchone()
    await conn.close()
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "kind": row["kind"],
        "config": _loads_config(row["config"]),
        "enabled": bool(row["enabled"]),
        "is_default": bool(row["is_default"]),
        "created_at": int(row["created_at"] or 0),
        "updated_at": int(row["updated_at"] or 0),
    }


async def set_topic_delivery_target(topic, target_id):
    conn = await db()
    if target_id is None:
        await conn.execute(
            "DELETE FROM topic_target_map WHERE topic = ?",
            (topic,),
        )
    else:
        await conn.execute(
            (
                "INSERT INTO topic_target_map(topic, target_id, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(topic) DO UPDATE SET target_id = excluded.target_id, "
                "updated_at = excluded.updated_at"
            ),
            (topic, int(target_id), int(time.time())),
        )
    await conn.commit()
    await conn.close()


async def get_topic_delivery_target_id(topic):
    conn = await db()
    cur = await conn.execute(
        "SELECT target_id FROM topic_target_map WHERE topic = ?",
        (topic,),
    )
    row = await cur.fetchone()
    await conn.close()
    if row is None:
        return None
    value = row["target_id"]
    return None if value is None else int(value)


async def list_topic_delivery_target_ids():
    conn = await db()
    cur = await conn.execute(
        "SELECT topic, target_id FROM topic_target_map"
    )
    rows = await cur.fetchall()
    await conn.close()
    out = {}
    for row in rows:
        value = row["target_id"]
        out[row["topic"]] = None if value is None else int(value)
    return out


async def resolve_delivery_target_for_topic(topic):
    target_id = await get_topic_delivery_target_id(topic)
    if target_id is not None:
        target = await get_delivery_target(target_id)
        if target and target["enabled"]:
            return target
    default_target = await get_default_delivery_target()
    if default_target and default_target["enabled"]:
        return default_target
    return None
