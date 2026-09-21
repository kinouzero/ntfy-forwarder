import time

from db.client import db

SETTINGS_DEFAULTS = {
    "delivery_queue_max_attempts": 8,
    "delivery_queue_base_retry_seconds": 5,
    "delivery_queue_max_retry_seconds": 300,
    "quiet_hours_start": 23,
    "quiet_hours_end": 7,
    "db_batch_size": 1,
    "db_batch_flush_seconds": 1,
    "retention_days": 30,
    "error_retention_days": 7,
    "db_maintenance_interval_seconds": 3600,
    "aggregation_interval": 30,
    "aggregation_min_count": 10,
    "max_aggregation_buffer": 1000,
    "max_digest_buffer": 1000,
    "daily_summary_enabled": True,
    "daily_summary_hour": 8,
    "daily_summary_minute": 0,
}

_INT_KEYS = {
    k
    for k, v in SETTINGS_DEFAULTS.items()
    if isinstance(v, int) and not isinstance(v, bool)
}

_BOOL_KEYS = {
    k
    for k, v in SETTINGS_DEFAULTS.items()
    if isinstance(v, bool)
}

_CACHE = {"ts": 0.0, "values": dict(SETTINGS_DEFAULTS)}
_CACHE_TTL_SECONDS = 3.0


def _to_bool(value):
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _coerce_setting(key, value):
    if key in _BOOL_KEYS:
        return _to_bool(value)
    if key in _INT_KEYS:
        try:
            return int(value)
        except Exception:
            return int(SETTINGS_DEFAULTS[key])
    return value


def _sanitize_settings(values):
    out = {}
    for key, default in SETTINGS_DEFAULTS.items():
        out[key] = _coerce_setting(key, values.get(key, default))

    out["delivery_queue_max_attempts"] = max(1, out["delivery_queue_max_attempts"])
    out["delivery_queue_base_retry_seconds"] = max(1, out["delivery_queue_base_retry_seconds"])
    out["delivery_queue_max_retry_seconds"] = max(1, out["delivery_queue_max_retry_seconds"])
    out["quiet_hours_start"] = max(0, min(23, out["quiet_hours_start"]))
    out["quiet_hours_end"] = max(0, min(23, out["quiet_hours_end"]))
    out["db_batch_size"] = max(1, out["db_batch_size"])
    out["db_batch_flush_seconds"] = max(1, out["db_batch_flush_seconds"])
    out["retention_days"] = max(1, out["retention_days"])
    out["error_retention_days"] = max(1, out["error_retention_days"])
    out["db_maintenance_interval_seconds"] = max(30, out["db_maintenance_interval_seconds"])
    out["aggregation_interval"] = max(1, out["aggregation_interval"])
    out["aggregation_min_count"] = max(1, out["aggregation_min_count"])
    out["max_aggregation_buffer"] = max(10, out["max_aggregation_buffer"])
    out["max_digest_buffer"] = max(10, out["max_digest_buffer"])
    out["daily_summary_hour"] = max(0, min(23, out["daily_summary_hour"]))
    out["daily_summary_minute"] = max(0, min(59, out["daily_summary_minute"]))
    return out


async def _read_all_settings():
    conn = await db()
    cur = await conn.execute("SELECT key, value FROM app_settings")
    rows = await cur.fetchall()
    await conn.close()
    values = dict(SETTINGS_DEFAULTS)
    for row in rows:
        key = row["key"]
        if key not in SETTINGS_DEFAULTS:
            continue
        values[key] = _coerce_setting(key, row["value"])
    return _sanitize_settings(values)


async def get_settings_snapshot(force_refresh=False):
    now = time.monotonic()
    if not force_refresh and (now - float(_CACHE["ts"])) < _CACHE_TTL_SECONDS:
        return dict(_CACHE["values"])
    values = await _read_all_settings()
    _CACHE["ts"] = now
    _CACHE["values"] = dict(values)
    return values


async def list_settings_with_meta():
    values = await get_settings_snapshot()
    return {
        "values": values,
        "defaults": dict(SETTINGS_DEFAULTS),
    }


async def update_settings(values):
    if not isinstance(values, dict):
        return await get_settings_snapshot(force_refresh=True)

    conn = await db()
    for key, raw_value in values.items():
        if key not in SETTINGS_DEFAULTS:
            continue
        coerced = _coerce_setting(key, raw_value)
        await conn.execute(
            "INSERT INTO app_settings(key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET "
            "value = excluded.value, updated_at = excluded.updated_at",
            (key, str(coerced), int(time.time())),
        )
    await conn.commit()
    await conn.close()
    return await get_settings_snapshot(force_refresh=True)
