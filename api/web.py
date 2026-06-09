import asyncio
import csv
import io
import json
import time

from aiohttp import web
from prometheus_client import (
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from core.state import (
    workers,
    worker_last_seen,
    recent_events,
    topic_stats,
    topic_rates,
    aggregation_buffer,
    digest_buffer,
)
from core.metrics import telegram_queue_size
from core.metrics import telegram_dead_letter_size
from core.config import ADMIN_TOKEN
from core.config import TOPIC_ALLOWLIST, TOPIC_DENYLIST
from core.config import ADMIN_RECENT_EVENTS
from core.config import TELEGRAM_ENABLED
from core.config import HEALTH_TELEGRAM_CHECK_ENABLED
from core.logging import log
from db.topics import (
    list_topics,
    set_topic_enabled,
    add_topic,
    reset_topic_count_base,
    list_topic_status_counts,
)
from db.messages import count_messages_by_topic_since
from db.telegram_queue import count_telegram_queue
from db.errors import query_errors, count_errors_since, clear_errors
from db.dead_letter import (
    query_dead_letters,
    get_dead_letter,
    delete_dead_letter,
    delete_dead_letters,
    clear_dead_letters,
    count_dead_letters,
)
from db.telegram_queue import enqueue_telegram_item
from db.client import db
from services.telegram import tg_call
from services.ntfy import ntfy_worker

_HEALTH_TG_CACHE = {
    "ts": 0,
    "ok": None,
    "latency_ms": None,
    "error": None,
}

def _get_admin_token(request):
    return (
        request.headers.get("X-Admin-Token")
        or request.query.get("token", "")
        or request.cookies.get("admin_token", "")
    )


def _require_admin(request):
    if not ADMIN_TOKEN:
        raise web.HTTPServiceUnavailable(
            text="admin disabled",
        )
    token = _get_admin_token(request)
    if token != ADMIN_TOKEN:
        raise web.HTTPUnauthorized()
    return token


def _admin_html_response(request, token, html):
    resp = web.Response(
        text=html,
        content_type="text/html",
    )
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    if request.cookies.get("admin_token") != token:
        resp.set_cookie("admin_token", token, httponly=True)
    return resp


def _worker_running(name):
    task = workers.get(name)
    if task is None:
        return False
    if hasattr(task, "done"):
        return not task.done()
    return True


def _active_worker_count():
    return sum(1 for name in workers if _worker_running(name))


def _cleanup_topic_runtime(name):
    worker_last_seen.pop(name, None)
    aggregation_buffer.pop(name, None)
    digest_buffer.pop(name, None)


async def _stop_worker(name):
    task = workers.pop(name, None)
    _cleanup_topic_runtime(name)
    if task is None or not hasattr(task, "cancel"):
        log("DEBUG", "topic runtime stopped", topic=name, had_worker=bool(task))
        return
    if hasattr(task, "done") and task.done():
        log("DEBUG", "topic runtime stopped", topic=name, had_worker=True, done=True)
        return
    task.cancel()
    await asyncio.gather(
        task,
        return_exceptions=True,
    )
    log("INFO", "topic worker stopped", topic=name)


def _start_worker(name):
    if _worker_running(name):
        return False
    task = asyncio.create_task(
        ntfy_worker(name)
    )
    workers[name] = task
    log("INFO", "topic worker started from admin", topic=name)
    return True


def _topic_allowed(name):
    if TOPIC_ALLOWLIST and name not in TOPIC_ALLOWLIST:
        return False
    if TOPIC_DENYLIST and name in TOPIC_DENYLIST:
        return False
    return True

async def health(request):
    queue_count = await count_telegram_queue()
    dead_count = await count_dead_letters()
    telegram_queue_size.set(queue_count)
    telegram_dead_letter_size.set(dead_count)
    now = int(time.time())

    db_ok = True
    db_error = None
    try:
        conn = await db()
        await conn.execute("CREATE TEMP TABLE IF NOT EXISTS _health_rw(ts INTEGER)")
        await conn.execute("INSERT INTO _health_rw(ts) VALUES(?)", (now,))
        await conn.execute("DELETE FROM _health_rw WHERE ts = ?", (now,))
        await conn.commit()
        await conn.close()
    except Exception as exc:
        db_ok = False
        db_error = str(exc)

    worker_ages = {}
    stale_workers = 0
    for topic, ts in worker_last_seen.items():
        if not _worker_running(topic):
            continue
        age = max(0, now - int(ts))
        worker_ages[topic] = age
        if age > 120:
            stale_workers += 1
    max_worker_age = max(worker_ages.values()) if worker_ages else 0

    tg_status = {
        "enabled": bool(TELEGRAM_ENABLED and HEALTH_TELEGRAM_CHECK_ENABLED),
        "ok": None,
        "latency_ms": None,
        "error": None,
    }
    if tg_status["enabled"]:
        if now - int(_HEALTH_TG_CACHE["ts"]) >= 30:
            start = time.monotonic()
            try:
                await tg_call("getMe", {})
                _HEALTH_TG_CACHE.update(
                    {
                        "ts": now,
                        "ok": True,
                        "latency_ms": int((time.monotonic() - start) * 1000),
                        "error": None,
                    }
                )
            except Exception as exc:
                _HEALTH_TG_CACHE.update(
                    {
                        "ts": now,
                        "ok": False,
                        "latency_ms": int((time.monotonic() - start) * 1000),
                        "error": str(exc),
                    }
                )
        tg_status["ok"] = _HEALTH_TG_CACHE["ok"]
        tg_status["latency_ms"] = _HEALTH_TG_CACHE["latency_ms"]
        tg_status["error"] = _HEALTH_TG_CACHE["error"]

    status = "ok"
    if not db_ok or stale_workers > 0:
        status = "degraded"
    return web.json_response({
        "status": status,
        "workers": _active_worker_count(),
        "queue": queue_count,
        "dead_letters": dead_count,
        "checks": {
            "db_writable": {"ok": db_ok, "error": db_error},
            "telegram": tg_status,
            "workers": {
                "stale": stale_workers,
                "max_last_seen_age_seconds": max_worker_age,
            },
        },
    })

async def metrics(request):

    data = generate_latest()

    return web.Response(
        body=data,
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )

async def admin_page(request):
    token = _require_admin(request)
    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Forwarder Admin</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link
          href="https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&display=swap"
          rel="stylesheet"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
        >
        <style>
          :root {{
            color-scheme: light;
            --bg-a: #f6f8fc;
            --bg-b: #ecf4f2;
            --glow-a: #e2f2ed;
            --glow-b: #e8efff;
            --ink: #132239;
            --muted: #5e6a7c;
            --line: #d9e0ea;
            --card: #ffffff;
            --card-soft: #f9fbfe;
            --brand: #0f766e;
            --brand-2: #14532d;
            --hover-row: #f8fbff;
          }}
          html[data-theme="dark"] {{
            color-scheme: dark;
            --bg-a: #090f18;
            --bg-b: #0d1520;
            --glow-a: #102434;
            --glow-b: #1a2440;
            --ink: #e8edf5;
            --muted: #a1afc1;
            --line: #273447;
            --card: #121c29;
            --card-soft: #182638;
            --brand: #2dd4bf;
            --brand-2: #22c55e;
            --hover-row: #192537;
          }}
          body {{
            padding: 18px;
            font-family: "Manrope", sans-serif;
            color: var(--ink);
            background:
              radial-gradient(1200px 600px at 5% -5%, var(--glow-a) 0%, transparent 65%),
              radial-gradient(1000px 500px at 105% 0%, var(--glow-b) 0%, transparent 60%),
              linear-gradient(180deg, var(--bg-a), var(--bg-b));
          }}
          html[data-theme="dark"] body {{
            background: var(--bg-a);
          }}
          .text-muted {{ color: var(--muted)!important; }}
          .card {{
            --bs-card-bg: var(--card-soft);
            background-color: var(--card-soft)!important;
            border-color: var(--line)!important;
            color: var(--ink);
          }}
          .card .card-body {{ color: var(--ink); }}
          .list-group-item {{
            background: color-mix(in srgb, var(--card) 94%, transparent);
            color: var(--ink);
            border-color: var(--line);
          }}
          .editable-field.form-control,
          .editable-field.form-select {{
            background-color: color-mix(in srgb, var(--card) 95%, #000);
            color: var(--ink);
            border-color: var(--line);
            -webkit-text-fill-color: var(--ink);
          }}
          .editable-field.form-control::placeholder {{ color: var(--muted); }}
          .editable-field.form-control:focus,
          .editable-field.form-select:focus {{
            background-color: color-mix(in srgb, var(--card) 95%, #000)!important;
            color: var(--ink)!important;
            border-color: color-mix(in srgb, var(--brand) 45%, #fff);
            box-shadow: 0 0 0 .2rem color-mix(in srgb, var(--brand) 18%, transparent);
          }}
          input.editable-field.form-control:-webkit-autofill,
          input.editable-field.form-control:-webkit-autofill:hover,
          input.editable-field.form-control:-webkit-autofill:focus {{
            -webkit-text-fill-color: var(--ink)!important;
            -webkit-box-shadow: 0 0 0 1000px color-mix(in srgb, var(--card) 95%, #000) inset;
            transition: background-color 9999s ease-out 0s;
          }}
          .text-bg-light {{
            background-color: color-mix(in srgb, var(--card) 76%, #000)!important;
            color: var(--ink)!important;
            border: 1px solid var(--line);
          }}
          .btn-outline-secondary {{
            --bs-btn-color: var(--muted);
            --bs-btn-border-color: var(--line);
            --bs-btn-hover-color: var(--ink);
            --bs-btn-hover-bg: color-mix(in srgb, var(--card) 84%, #000);
            --bs-btn-hover-border-color: var(--line);
          }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{
            border-radius: 999px;
          }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, #f59e0b 45%, #fff);
            background: color-mix(in srgb, #f59e0b 14%, transparent);
            color: var(--ink);
          }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{
            border-radius: 999px;
          }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, #ef4444 45%, #fff);
            background: color-mix(in srgb, #ef4444 14%, transparent);
            color: var(--ink);
          }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{
            border-radius: 999px;
          }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, var(--brand) 45%, #fff);
            background: color-mix(in srgb, var(--brand) 18%, transparent);
            color: var(--ink);
          }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{
            border-radius: 999px;
          }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, var(--brand) 45%, #fff);
            background: color-mix(in srgb, var(--brand) 18%, transparent);
            color: var(--ink);
          }}
          .shell {{
            max-width: 1200px;
            margin: 0 auto;
            background: color-mix(in srgb, var(--card) 88%, transparent);
            border: 1px solid var(--line);
            border-radius: 18px;
            box-shadow: 0 10px 35px rgba(18, 37, 66, 0.08);
            overflow: hidden;
            animation: in .35s ease-out;
          }}
          .topbar {{
            padding: 16px 16px 10px 16px;
            border-bottom: 1px solid var(--line);
            background: linear-gradient(
              180deg,
              color-mix(in srgb, var(--card) 92%, #fff),
              color-mix(in srgb, var(--card) 98%, #000)
            );
          }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{
            border-radius: 999px;
          }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, var(--brand) 45%, #fff);
            background: color-mix(in srgb, var(--brand) 18%, transparent);
            color: var(--ink);
          }}
          .toolbar {{
            padding: 12px 16px;
            border-bottom: 1px solid var(--line);
            background: color-mix(in srgb, var(--card) 95%, #000);
          }}
          .table-wrap {{ padding: 8px 12px 14px 12px; }}
          .topics-table thead th {{
            border-bottom: 1px solid var(--line);
            color: var(--muted);
            font-size: .8rem;
            text-transform: uppercase;
            letter-spacing: .03em;
            font-weight: 700;
            background-color: transparent!important;
          }}
          .topics-table {{
            --bs-table-bg: transparent;
            --bs-table-striped-bg: transparent;
            --bs-table-color: var(--ink);
            color: var(--ink);
          }}
          .topics-table td {{
            background-color: transparent!important;
            color: var(--ink);
          }}
          .topics-table tbody tr {{ transition: background .2s ease; }}
          .topics-table tbody tr:hover {{ background: var(--hover-row); }}
          .topics-table td {{ vertical-align: middle; }}
          .topic-link {{
            color: var(--ink);
            text-decoration: none;
            font-weight: 800;
          }}
          .topic-link:hover {{ color: var(--brand); }}
          .count-block .small {{ color: var(--muted)!important; }}
          .btn-outline-primary {{
            --bs-btn-color: var(--brand);
            --bs-btn-border-color: color-mix(in srgb, var(--brand) 45%, #ffffff);
            --bs-btn-hover-bg: var(--brand);
            --bs-btn-hover-border-color: var(--brand);
          }}
          .btn-outline-theme {{
            --bs-btn-color: var(--ink);
            --bs-btn-border-color: var(--line);
            --bs-btn-hover-bg: color-mix(in srgb, var(--card) 84%, #000);
            --bs-btn-hover-border-color: var(--line);
          }}
          @keyframes in {{
            from {{ opacity: 0; transform: translateY(8px); }}
            to {{ opacity: 1; transform: translateY(0); }}
          }}
          #nav-toggle {{ display: none; }}
          @media (max-width: 860px) {{
            #nav-toggle {{ display: inline-flex; }}
            body {{ padding: 10px; }}
            .toolbar {{ padding: 10px; }}
            .table-wrap {{ padding: 8px; }}
            .topbar {{
              padding: 10px;
            }}
            .nav-actions {{
              width: 100%;
              justify-content: space-between;
            }}
            .nav-links {{
              width: 100%;
              display: none;
              margin-top: 8px;
            }}
            .nav-links.open {{
              display: flex;
            }}
            .nav-links .btn {{
              flex: 1 1 auto;
              justify-content: center;
            }}
            .topics-table thead {{ display: none; }}
            .topics-table tbody tr {{
              display: block;
              border: 1px solid var(--line);
              border-radius: 12px;
              margin-bottom: 10px;
              background: var(--card-soft);
              padding: 8px;
            }}
            .topics-table tbody tr:last-child {{
              margin-bottom: 0;
            }}
            .topics-table tbody td {{
              display: flex;
              justify-content: space-between;
              gap: 8px;
              border: 0;
              padding: 7px 6px;
              background-color: transparent!important;
            }}
            .topics-table tbody td::before {{
              content: attr(data-label);
              color: var(--muted);
              font-size: .78rem;
              font-weight: 700;
              text-transform: uppercase;
              letter-spacing: .03em;
            }}
            .topics-table tbody td[data-label="Action"] {{
              justify-content: flex-end;
            }}
            .topics-table tbody td[data-label="Action"]::before {{
              margin-right: auto;
            }}
            .topics-table .action-buttons {{
              display: inline-flex;
              gap: .4rem;
              margin-left: auto;
              justify-content: flex-end;
            }}
            .topics-table tbody td.count-block {{
              justify-content: space-between;
              align-items: flex-start;
            }}
            .topics-table .count-values {{
              display: inline-flex;
              flex-direction: column;
              gap: .2rem;
              margin-left: auto;
              align-items: flex-end;
              text-align: right;
            }}
            .topics-table .count-values > div:first-of-type {{
              display: inline-flex;
              align-items: center;
              gap: .35rem;
            }}
          }}
        </style>
      </head>
      <body>
        <div class="shell">
          <div class="topbar d-flex align-items-center justify-content-between gap-2 flex-wrap">
            <div class="nav-actions">
              <h2 class="m-0 fw-bold">Topics</h2>
              <div class="d-flex gap-2">
                <button id="pause-all" class="btn btn-outline-warning btn-sm"
                  title="Pause all" aria-label="Pause all">
                  <i class="bi bi-pause-fill"></i>
                </button>
                <button id="resume-all" class="btn btn-outline-success btn-sm"
                  title="Resume all" aria-label="Resume all">
                  <i class="bi bi-play-fill"></i>
                </button>
                <button id="clear-all" class="btn btn-outline-danger btn-sm"
                  title="Clear stats" aria-label="Clear stats">
                  <i class="bi bi-trash"></i>
                </button>
                <button
                  id="theme-toggle"
                  class="btn btn-outline-theme btn-sm"
                  aria-label="Toggle theme"
                ></button>
                <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                  aria-label="Toggle navigation">
                  <i class="bi bi-list"></i>
                </button>
              </div>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm active" href="/admin?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
            </div>
          </div>
          <div class="toolbar d-flex flex-wrap gap-2 align-items-center">
            <input id="filter" class="editable-field form-control form-control-sm"
              style="max-width: 220px;" placeholder="Filter topics" />
            <select id="sort" class="editable-field form-select form-select-sm"
              style="max-width: 180px;">
              <option value="name">Sort: Name</option>
              <option value="count">Sort: Count</option>
            </select>
            <button id="sort-dir" class="btn btn-outline-secondary btn-sm">Desc</button>
          </div>
          <div class="table-wrap">
            <table id="topics" class="topics-table table table-sm align-middle mb-0">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Status</th>
                  <th>Count</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
        <script>
          const token = {token!r};
          const sunIcon = '<i class="bi bi-sun-fill"></i>';
          const moonIcon = '<i class="bi bi-moon-stars-fill"></i>';
          const getTheme = () => localStorage.getItem('ui_theme');
          const applyTheme = (theme) => {{
            const resolved = theme || (
              window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
            );
            document.documentElement.setAttribute('data-theme', resolved);
            const btn = document.getElementById('theme-toggle');
            if (btn) {{
              const toLight = resolved === 'dark';
              btn.innerHTML = toLight ? sunIcon : moonIcon;
              btn.title = toLight ? 'Light mode' : 'Dark mode';
              btn.setAttribute('aria-label', btn.title);
            }}
          }};
          function getState() {{
            return {{
              filter: localStorage.getItem('topic_filter') || '',
              sort: localStorage.getItem('topic_sort') || 'name',
              dir: localStorage.getItem('topic_sort_dir') || 'desc'
            }};
          }}
          function setState(state) {{
            localStorage.setItem('topic_filter', state.filter);
            localStorage.setItem('topic_sort', state.sort);
            localStorage.setItem('topic_sort_dir', state.dir);
          }}
          function applyState() {{
            const state = getState();
            document.getElementById('filter').value = state.filter;
            document.getElementById('sort').value = state.sort;
            document.getElementById('sort-dir').innerText =
              state.dir === 'asc' ? 'Asc' : 'Desc';
          }}
          async function fetchTopics() {{
            const res = await fetch(
              '/api/topics?token=' + encodeURIComponent(token)
            );
            if (!res.ok) return;
            const data = await res.json();
            const tbody = document.querySelector('#topics tbody');
            tbody.innerHTML = '';
            const state = getState();
            const filter = state.filter.toLowerCase();
            const sort = state.sort;
            const dir = state.dir;
            let items = (data.items || []).filter(t =>
              !filter || (t.name || '').toLowerCase().includes(filter)
            );
            items.sort((a, b) => {{
              let v = 0;
              if (sort === 'count') v = (b.count || 0) - (a.count || 0);
              else v = (a.name || '').localeCompare(b.name || '');
              return dir === 'asc' ? -v : v;
            }});
            items.forEach(t => {{
              const tr = document.createElement('tr');
              const statusBadge = t.enabled ? 'success' : 'secondary';
              tr.innerHTML = `
                <td data-label="Topic">
                  <a class="topic-link" href="/admin/topic/${{encodeURIComponent(t.name)}}?token=" +
                    encodeURIComponent(token)>
                    ${{t.name}}
                  </a>
                </td>
                <td data-label="Status">
                  <span class="badge text-bg-${{statusBadge}}">
                    ${{t.enabled ? 'enabled' : 'disabled'}}
                  </span>
                </td>
                <td data-label="Count" class="count-block">
                  <div class="count-values">
                    <div><span class="badge text-bg-light">${{t.count_total}}</span> total</div>
                    <div class="small text-muted">+${{t.count_24h}} /24h</div>
                    <div class="small text-muted">${{t.count_since_reset}} since reset</div>
                    <div class="small text-muted">${{t.status_counts?.disabled ?? 0}} since disabled</div>
                  </div>
                </td>
                <td data-label="Action">
                  <div class="action-buttons">
                    <button class="btn btn-outline-primary btn-sm" data-name="${{t.name}}">
                      ${{t.enabled ? 'Disable' : 'Enable'}}
                    </button>
                    <button class="btn btn-outline-secondary btn-sm" data-reset="${{t.name}}">
                      Reset Count
                    </button>
                  </div>
                </td>
              `;
              tr.querySelector('[data-name]').onclick = async () => {{
                await fetch(
                  '/api/topics/' + encodeURIComponent(t.name) +
                  '/toggle?token=' + encodeURIComponent(token),
                  {{
                  method: 'POST'
                  }}
                );
                fetchTopics();
              }};
              tr.querySelector('[data-reset]').onclick = async () => {{
                await fetch(
                  '/api/topics/' + encodeURIComponent(t.name) +
                  '/reset_count?token=' + encodeURIComponent(token),
                  {{ method: 'POST' }}
                );
                fetchTopics();
              }};
              tbody.appendChild(tr);
            }});
          }}
          applyState();
          applyTheme(getTheme());
          document.getElementById('theme-toggle').onclick = () => {{
            const current = document.documentElement.getAttribute('data-theme') || 'light';
            const next = current === 'dark' ? 'light' : 'dark';
            localStorage.setItem('ui_theme', next);
            applyTheme(next);
          }};
          document.getElementById('nav-toggle').onclick = () => {{
            document.querySelector('.nav-links').classList.toggle('open');
          }};
          fetchTopics();
          setInterval(fetchTopics, 5000);
          document.getElementById('filter').addEventListener('input', (e) => {{
            const state = getState();
            state.filter = e.target.value;
            setState(state);
            fetchTopics();
          }});
          document.getElementById('sort').addEventListener('change', (e) => {{
            const state = getState();
            state.sort = e.target.value;
            setState(state);
            fetchTopics();
          }});
          document.getElementById('sort-dir').addEventListener('click', () => {{
            const state = getState();
            state.dir = state.dir === 'asc' ? 'desc' : 'asc';
            setState(state);
            applyState();
            fetchTopics();
          }});
          document.getElementById('pause-all').onclick = async () => {{
            await fetch(
              '/api/topics/pause_all?token=' + encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            fetchTopics();
          }};
          document.getElementById('resume-all').onclick = async () => {{
            await fetch(
              '/api/topics/resume_all?token=' + encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            fetchTopics();
          }};
          document.getElementById('clear-all').onclick = async () => {{
            await fetch(
              '/api/topics/clear_all?token=' + encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            fetchTopics();
          }};
        </script>
      </body>
    </html>
    """
    return _admin_html_response(request, token, html)

async def admin_topic_page(request):
    token = _require_admin(request)
    name = request.match_info["name"]
    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Topic {name}</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link
          href="https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&display=swap"
          rel="stylesheet"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
        >
        <style>
          :root {{
            color-scheme: light;
            --bg-a: #f7f9fc;
            --bg-b: #f7f9fc;
            --glow-a: #eef7f3;
            --glow-b: #eef3ff;
            --ink: #132239;
            --line: #dbe2ec;
            --muted: #607086;
            --card: #ffffff;
            --card-soft: #f9fbfe;
            --event-line: #edf1f7;
          }}
          html[data-theme="dark"] {{
            color-scheme: dark;
            --bg-a: #090f18;
            --bg-b: #0d1520;
            --glow-a: #102434;
            --glow-b: #1a2440;
            --ink: #e8edf5;
            --line: #273447;
            --muted: #a1afc1;
            --card: #121c29;
            --card-soft: #182638;
            --event-line: #223145;
          }}
          body {{
            padding: 16px;
            font-family: "Manrope", sans-serif;
            color: var(--ink);
            background:
              radial-gradient(1100px 600px at -5% -10%, var(--glow-a) 0%, transparent 65%),
              radial-gradient(900px 500px at 110% 0%, var(--glow-b) 0%, transparent 60%),
              linear-gradient(180deg, var(--bg-a), var(--bg-b));
          }}
          html[data-theme="dark"] body {{
            background: var(--bg-a);
          }}
          .text-muted {{ color: var(--muted)!important; }}
          .card {{
            --bs-card-bg: var(--card-soft);
            background-color: var(--card-soft)!important;
            border-color: var(--line)!important;
            color: var(--ink);
          }}
          .card .card-body {{ color: var(--ink); }}
          .list-group-item {{
            background: color-mix(in srgb, var(--card) 94%, transparent)!important;
            color: var(--ink)!important;
            border-color: var(--event-line)!important;
          }}
          .btn-outline-secondary {{
            --bs-btn-color: var(--muted);
            --bs-btn-border-color: var(--line);
            --bs-btn-hover-color: var(--ink);
            --bs-btn-hover-bg: color-mix(in srgb, var(--card) 84%, #000);
            --bs-btn-hover-border-color: var(--line);
          }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, #2dd4bf 45%, #fff);
            background: color-mix(in srgb, #2dd4bf 14%, transparent);
            color: var(--ink);
          }}
          .topic-shell {{
            max-width: 1200px;
            margin: 0 auto;
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 18px;
            box-shadow: 0 10px 35px rgba(18, 37, 66, 0.08);
            overflow: hidden;
          }}
          .meta {{
            margin-bottom: 0;
            padding: 14px;
            border-bottom: 1px solid var(--line);
            background: linear-gradient(
              180deg,
              color-mix(in srgb, var(--card) 92%, #fff),
              color-mix(in srgb, var(--card) 98%, #000)
            );
          }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{
            border-radius: 999px;
          }}
          .event {{
            padding: 10px 12px;
            border-bottom: 1px solid var(--event-line);
            font-size: .92rem;
          }}
          .event:last-child {{ border-bottom: 0; }}
          #events {{ max-height: 56vh; overflow: auto; }}
          #nav-toggle {{ display: none; }}
          @media (max-width: 860px) {{
            #nav-toggle {{ display: inline-flex; }}
            body {{ padding: 10px; }}
            .meta {{ padding: 10px; }}
            .nav-actions {{
              width: 100%;
              justify-content: space-between;
            }}
            .nav-links {{
              width: 100%;
              display: none;
              margin-top: 8px;
            }}
            .nav-links.open {{
              display: flex;
            }}
            .nav-links .btn {{
              flex: 1 1 auto;
              justify-content: center;
            }}
            #stats .card-body {{ padding: .55rem!important; }}
            #events {{ max-height: 62vh; }}
          }}
        </style>
      </head>
      <body>
        <div class="topic-shell">
        <div class="meta">
          <div class="d-flex flex-wrap align-items-center justify-content-between gap-2">
            <div class="nav-actions">
              <div class="d-flex gap-2">
                <a class="btn btn-outline-secondary btn-sm"
                  href="/admin?token={token}" title="Back"
                  aria-label="Back">←</a>
                <button id="clear-topic" class="btn btn-outline-danger btn-sm"
                  title="Clear stats" aria-label="Clear stats">
                  <i class="bi bi-trash"></i>
                </button>
                <button id="reset-topic-count" class="btn btn-outline-secondary btn-sm"
                  title="Reset count" aria-label="Reset count">
                  <i class="bi bi-recycle"></i>
                </button>
                <button
                  id="theme-toggle"
                  class="btn btn-outline-secondary btn-sm"
                  aria-label="Toggle theme"
                ></button>
              </div>
              <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                aria-label="Toggle navigation">
                <i class="bi bi-list"></i>
              </button>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm active" href="/admin?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
            </div>
          </div>
          <h2 class="m-0 fs-4 mt-2">Topic: {name}</h2>
          <div id="stats" class="row g-2 my-2"></div>
        </div>
        <div id="events" class="list-group list-group-flush"></div>
        </div>
        <script>
          const token = {token!r};
          const name = {name!r};
          const sunIcon = '<i class="bi bi-sun-fill"></i>';
          const moonIcon = '<i class="bi bi-moon-stars-fill"></i>';
          const getTheme = () => localStorage.getItem('ui_theme');
          const applyTheme = (theme) => {{
            const resolved = theme || (
              window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
            );
            document.documentElement.setAttribute('data-theme', resolved);
            const btn = document.getElementById('theme-toggle');
            if (btn) {{
              const toLight = resolved === 'dark';
              btn.innerHTML = toLight ? sunIcon : moonIcon;
              btn.title = toLight ? 'Light mode' : 'Dark mode';
              btn.setAttribute('aria-label', btn.title);
            }}
          }};
          const fmtDate = (ts) => {{
            if (!ts) return '';
            return new Date(ts * 1000).toLocaleString('fr-FR');
          }};
          const card = (label, value, tone) => `
            <div class="col-6 col-md-4 col-lg-3">
              <div class="card border-${{tone}}">
                <div class="card-body p-2">
                  <div class="text-muted small">${{label}}</div>
                  <div class="fw-bold">${{value}}</div>
                </div>
              </div>
            </div>`;
          async function fetchDetail() {{
            const res = await fetch(
              '/api/topics/' + encodeURIComponent(name) +
              '?token=' + encodeURIComponent(token)
            );
            if (!res.ok) return;
            const data = await res.json();
            const stats = data.stats || {{}};
            document.getElementById('stats').innerHTML =
              card('Enabled', data.enabled ? 'yes' : 'no', data.enabled ? 'success' : 'secondary') +
              card('Running', data.running ? 'yes' : 'no', data.running ? 'success' : 'secondary') +
              card('Count Total', data.count_total ?? 0, 'primary') +
              card('Count 24h', data.count_24h ?? 0, 'secondary') +
              card('Since Reset', data.count_since_reset ?? 0, 'primary') +
              card('Received', data.status_counts?.received ?? 0, 'info') +
              card('Inserted', data.count_total ?? 0, 'success') +
              card('Filtered', data.status_counts?.filtered ?? 0, 'warning') +
              card('Since disabled', data.status_counts?.disabled ?? 0, 'secondary') +
              card('Rate Limited', data.status_counts?.rate_limited ?? 0, 'warning') +
              card('Errors', stats.errors ?? 0, 'danger');
            const container = document.getElementById('events');
            container.innerHTML = '';
            (data.recent || []).slice().reverse().forEach(e => {{
              const div = document.createElement('div');
              div.className = 'event list-group-item';
              const title = e.title ? (e.title + ' - ') : '';
              div.innerText = `[${{fmtDate(e.ts)}}] p${{e.priority}} ${{title}}${{e.message}}`;
              container.appendChild(div);
            }});
          }}
          applyTheme(getTheme());
          document.getElementById('theme-toggle').onclick = () => {{
            const current = document.documentElement.getAttribute('data-theme') || 'light';
            const next = current === 'dark' ? 'light' : 'dark';
            localStorage.setItem('ui_theme', next);
            applyTheme(next);
          }};
          document.getElementById('nav-toggle').onclick = () => {{
            document.querySelector('.nav-links').classList.toggle('open');
          }};
          fetchDetail();
          setInterval(fetchDetail, 5000);
          document.getElementById('clear-topic').onclick = async () => {{
            await fetch(
              '/api/topics/' + encodeURIComponent(name) +
              '/clear?token=' + encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            fetchDetail();
          }};
          document.getElementById('reset-topic-count').onclick = async () => {{
            await fetch(
              '/api/topics/' + encodeURIComponent(name) +
              '/reset_count?token=' + encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            fetchDetail();
          }};
        </script>
      </body>
    </html>
    """
    return _admin_html_response(request, token, html)

async def topics_list(request):
    _require_admin(request)
    log("INFO", "admin topics_list")
    rows = await list_topics()
    counts_24h = await count_messages_by_topic_since(
        int(time.time()) - 86400
    )
    status_counts = await list_topic_status_counts()
    items = []
    for r in rows:
        name = r["name"]
        stats = topic_stats.get(name, {})
        count_total = int(r["count"])
        count_24h = counts_24h.get(name, 0)
        reset_base = int(r["reset_count_base"] or 0)
        count_since_reset = max(0, count_total - reset_base)
        items.append({
            "name": name,
            "enabled": bool(r["enabled"]),
            "count": count_total,
            "count_total": count_total,
            "count_24h": count_24h,
            "count_since_reset": count_since_reset,
            "running": _worker_running(name),
            "stats": stats,
            "status_counts": status_counts.get(
                name,
                {"received": 0, "filtered": 0, "rate_limited": 0, "disabled": 0},
            ),
        })
    return web.json_response({"items": items})

async def topic_detail(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "admin topic_detail", topic=name)
    rows = await list_topics()
    known = {r["name"]: r for r in rows}
    if name not in known:
        raise web.HTTPNotFound()
    r = known[name]
    recent = list(recent_events.get(name, []))[-ADMIN_RECENT_EVENTS:]
    count_total = int(r["count"])
    count_24h = (
        await count_messages_by_topic_since(
            int(time.time()) - 86400
        )
    ).get(name, 0)
    reset_base = int(r["reset_count_base"] or 0)
    status_counts = await list_topic_status_counts()
    return web.json_response(
        {
            "name": name,
            "enabled": bool(r["enabled"]),
            "count": count_total,
            "count_total": count_total,
            "count_24h": count_24h,
            "count_since_reset": max(0, count_total - reset_base),
            "running": _worker_running(name),
            "stats": topic_stats.get(name, {}),
            "status_counts": status_counts.get(
                name,
                {"received": 0, "filtered": 0, "rate_limited": 0, "disabled": 0},
            ),
            "recent": recent,
        }
    )

async def topic_toggle(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "admin topic_toggle", topic=name)
    if not _topic_allowed(name):
        raise web.HTTPForbidden()
    rows = await list_topics()
    known = {r["name"]: r for r in rows}
    if name not in known:
        await add_topic(name)
        enabled = True
    else:
        enabled = not bool(known[name]["enabled"])

    await set_topic_enabled(name, enabled)

    if enabled:
        await add_topic(name)
        started = _start_worker(name)
        log("INFO", "topic enabled", topic=name, worker_started=started)
    else:
        started = _start_worker(name)
        log("INFO", "topic disabled", topic=name, worker_started=started)

    return web.json_response(
        {"name": name, "enabled": enabled}
    )

async def pause_all(request):
    _require_admin(request)
    log("INFO", "admin pause_all")
    rows = await list_topics()
    for r in rows:
        name = r["name"]
        if TOPIC_ALLOWLIST and name not in TOPIC_ALLOWLIST:
            continue
        if TOPIC_DENYLIST and name in TOPIC_DENYLIST:
            continue
        await set_topic_enabled(name, False)
        _start_worker(name)
    return web.json_response({"paused": True})

async def resume_all(request):
    _require_admin(request)
    log("INFO", "admin resume_all")
    rows = await list_topics()
    for r in rows:
        name = r["name"]
        if TOPIC_ALLOWLIST and name not in TOPIC_ALLOWLIST:
            continue
        if TOPIC_DENYLIST and name in TOPIC_DENYLIST:
            continue
        await add_topic(name)
        await set_topic_enabled(name, True)
        _start_worker(name)
    return web.json_response({"resumed": True})

async def clear_topic(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "admin clear_topic", topic=name)
    topic_stats.pop(name, None)
    recent_events.pop(name, None)
    topic_rates.pop(name, None)
    return web.json_response({"cleared": name})


async def reset_topic_count(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "admin reset_topic_count", topic=name)
    rows = await list_topics()
    known = {r["name"]: r for r in rows}
    if name not in known:
        raise web.HTTPNotFound()
    await reset_topic_count_base(name)
    return web.json_response({"reset_count": name})

async def clear_all(request):
    _require_admin(request)
    log("INFO", "admin clear_all")
    topic_stats.clear()
    recent_events.clear()
    topic_rates.clear()
    return web.json_response({"cleared": True})


async def topics_export(request):
    _require_admin(request)
    rows = await list_topics()
    payload = {
        "version": 1,
        "items": [
            {
                "name": r["name"],
                "enabled": bool(r["enabled"]),
            }
            for r in rows
        ],
    }
    return web.Response(
        text=json.dumps(payload, ensure_ascii=True, indent=2),
        content_type="application/json",
    )


def _parse_import_topics(data):
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("items", [])
    else:
        raise web.HTTPBadRequest(text="invalid payload")

    parsed = []
    for item in items:
        if isinstance(item, str):
            name = item.strip()
            enabled = True
        elif isinstance(item, dict):
            name = str(item.get("name", "")).strip()
            enabled = bool(item.get("enabled", True))
        else:
            continue
        if not name:
            continue
        parsed.append((name, enabled))
    return parsed


async def topics_import(request):
    _require_admin(request)
    try:
        data = await request.json()
    except Exception as exc:
        raise web.HTTPBadRequest(text=f"invalid json: {exc}") from exc

    items = _parse_import_topics(data)
    if not items:
        raise web.HTTPBadRequest(text="no topics to import")

    imported = 0
    skipped = []
    for name, enabled in items:
        if not _topic_allowed(name):
            skipped.append({"name": name, "reason": "blocked_by_allow_deny"})
            continue
        await add_topic(name)
        await set_topic_enabled(name, enabled)
        _start_worker(name)
        imported += 1

    return web.json_response(
        {
            "imported": imported,
            "skipped": skipped,
            "total": len(items),
        }
    )

async def admin_stats_page(request):
    token = _require_admin(request)
    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Forwarder Stats</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link
          href="https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&display=swap"
          rel="stylesheet"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
        >
        <style>
          :root {{
            color-scheme: light;
            --bg-a: #f7f9fc;
            --bg-b: #f7f9fc;
            --glow-a: #eef7f3;
            --glow-b: #eef3ff;
            --ink: #132239;
            --line: #dbe2ec;
            --card: #ffffff;
            --muted: #607086;
            --card-soft: #f9fbfe;
          }}
          html[data-theme="dark"] {{
            color-scheme: dark;
            --bg-a: #090f18;
            --bg-b: #0d1520;
            --glow-a: #102434;
            --glow-b: #1a2440;
            --ink: #e8edf5;
            --line: #273447;
            --card: #121c29;
            --muted: #a1afc1;
            --card-soft: #182638;
          }}
          body {{
            padding: 16px;
            font-family: "Manrope", sans-serif;
            color: var(--ink);
            background:
              radial-gradient(1000px 550px at -8% -15%, var(--glow-a) 0%, transparent 65%),
              radial-gradient(950px 500px at 108% 0%, var(--glow-b) 0%, transparent 60%),
              linear-gradient(180deg, var(--bg-a), var(--bg-b));
          }}
          html[data-theme="dark"] body {{
            background: var(--bg-a);
          }}
          .text-muted {{ color: var(--muted)!important; }}
          .card {{
            --bs-card-bg: var(--card-soft);
            background-color: var(--card-soft)!important;
            border-color: var(--line)!important;
            color: var(--ink);
          }}
          .card .card-body {{ color: var(--ink); }}
          .btn-outline-secondary {{
            --bs-btn-color: var(--muted);
            --bs-btn-border-color: var(--line);
            --bs-btn-hover-color: var(--ink);
            --bs-btn-hover-bg: color-mix(in srgb, var(--card) 84%, #000);
            --bs-btn-hover-border-color: var(--line);
          }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{
            border-radius: 999px;
          }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, #2dd4bf 45%, #fff);
            background: color-mix(in srgb, #2dd4bf 14%, transparent);
            color: var(--ink);
          }}
          .stats-shell {{
            max-width: 1100px;
            margin: 0 auto;
            padding: 0;
            border-radius: 18px;
            border: 1px solid var(--line);
            background: var(--card);
            box-shadow: 0 10px 35px rgba(18, 37, 66, 0.08);
            overflow: hidden;
          }}
          .topbar {{
            padding: 14px;
            border-bottom: 1px solid var(--line);
            background: linear-gradient(
              180deg,
              color-mix(in srgb, var(--card) 92%, #fff),
              color-mix(in srgb, var(--card) 98%, #000)
            );
          }}
          .stats-body {{
            padding: 14px;
          }}
          #nav-toggle {{ display: none; }}
          @media (max-width: 860px) {{
            #nav-toggle {{ display: inline-flex; }}
            .topbar {{
              padding: 10px;
            }}
            .nav-actions {{
              width: 100%;
              justify-content: space-between;
            }}
            .nav-links {{
              width: 100%;
              display: none;
              margin-top: 8px;
            }}
            .nav-links.open {{
              display: flex;
            }}
            .nav-links .btn {{
              flex: 1 1 auto;
              justify-content: center;
            }}
          }}
        </style>
      </head>
      <body>
        <div class="stats-shell">
          <div class="topbar d-flex align-items-center justify-content-between gap-2 flex-wrap">
            <div class="nav-actions">
              <div class="d-flex gap-2">
                <a class="btn btn-outline-secondary btn-sm"
                  href="/admin?token={token}" title="Back"
                  aria-label="Back">←</a>
                <button
                  id="theme-toggle"
                  class="btn btn-outline-secondary btn-sm"
                  aria-label="Toggle theme"
                ></button>
              </div>
              <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                aria-label="Toggle navigation">
                <i class="bi bi-list"></i>
              </button>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm" href="/admin?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm active"
                href="/admin/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
            </div>
          </div>
          <div class="stats-body">
          <h2 class="fs-4">Global Stats</h2>
          <div id="stats" class="row g-2"></div>
          <div class="mt-3">
            <h3 class="fs-6 text-muted">Top Topics (24h)</h3>
            <div id="top-topics" class="small"></div>
          </div>
          </div>
        </div>
        <script>
          const token = {token!r};
          const sunIcon = '<i class="bi bi-sun-fill"></i>';
          const moonIcon = '<i class="bi bi-moon-stars-fill"></i>';
          const getTheme = () => localStorage.getItem('ui_theme');
          const applyTheme = (theme) => {{
            const resolved = theme || (
              window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
            );
            document.documentElement.setAttribute('data-theme', resolved);
            const btn = document.getElementById('theme-toggle');
            if (btn) {{
              const toLight = resolved === 'dark';
              btn.innerHTML = toLight ? sunIcon : moonIcon;
              btn.title = toLight ? 'Light mode' : 'Dark mode';
              btn.setAttribute('aria-label', btn.title);
            }}
          }};
          const card = (label, value, tone) => `
            <div class="col-6 col-md-4 col-lg-3">
              <div class="card border-${{tone}}">
                <div class="card-body p-2">
                  <div class="text-muted small">${{label}}</div>
                  <div class="fw-bold">${{value}}</div>
                </div>
              </div>
            </div>`;
          async function fetchStats() {{
            const res = await fetch('/api/stats?token=' + encodeURIComponent(token));
            if (!res.ok) return;
            const data = await res.json();
            document.getElementById('stats').innerHTML =
              card('Workers', data.workers, 'primary') +
              card('Stale workers', data.stale_workers, 'warning') +
              card('Max worker age (s)', data.max_last_seen_age_seconds, 'secondary') +
              card('Queue', data.queue, 'info') +
              card('Dead letters', data.dead_letters, 'danger') +
              card('Topics total', data.topics_total, 'secondary') +
              card('Topics enabled', data.topics_enabled, 'success') +
              card('Topics disabled', data.topics_disabled, 'warning') +
              card('Messages 24h', data.messages_24h, 'primary') +
              card('Errors 24h', data.errors_24h, 'danger') +
              card('Received total', data.received_total, 'info') +
              card('Filtered total', data.filtered_total, 'warning') +
              card('Total since disabled', data.disabled_total, 'secondary') +
              card('Rate-limited total', data.rate_limited_total, 'warning');

            const top = data.top_topics_24h || [];
            const topEl = document.getElementById('top-topics');
            if (!top.length) {{
              topEl.innerHTML = '<div class="text-muted">No traffic in last 24h</div>';
            }} else {{
              topEl.innerHTML = top.map((row, idx) =>
                `<div class="d-flex justify-content-between border-bottom py-1">` +
                `<span>${{idx + 1}}. ${{row.topic}}</span>` +
                `<strong>${{row.count}}</strong>` +
                `</div>`
              ).join('');
            }}
          }}
          applyTheme(getTheme());
          document.getElementById('theme-toggle').onclick = () => {{
            const current = document.documentElement.getAttribute('data-theme') || 'light';
            const next = current === 'dark' ? 'light' : 'dark';
            localStorage.setItem('ui_theme', next);
            applyTheme(next);
          }};
          document.getElementById('nav-toggle').onclick = () => {{
            document.querySelector('.nav-links').classList.toggle('open');
          }};
          fetchStats();
          setInterval(fetchStats, 5000);
        </script>
      </body>
    </html>
    """
    return _admin_html_response(request, token, html)


async def admin_errors_page(request):
    token = _require_admin(request)
    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Forwarder Errors</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link
          href="https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&display=swap"
          rel="stylesheet"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
        >
        <style>
          :root {{
            color-scheme: light;
            --bg-a: #f7f9fc;
            --bg-b: #f7f9fc;
            --glow-a: #eef7f3;
            --glow-b: #eef3ff;
            --ink: #132239;
            --line: #dbe2ec;
            --card: #ffffff;
            --muted: #607086;
            --card-soft: #f9fbfe;
          }}
          html[data-theme="dark"] {{
            color-scheme: dark;
            --bg-a: #090f18;
            --bg-b: #0d1520;
            --glow-a: #102434;
            --glow-b: #1a2440;
            --ink: #e8edf5;
            --line: #273447;
            --card: #121c29;
            --muted: #a1afc1;
            --card-soft: #182638;
          }}
          body {{
            padding: 16px;
            font-family: "Manrope", sans-serif;
            color: var(--ink);
            background:
              radial-gradient(1000px 550px at -8% -15%, var(--glow-a) 0%, transparent 65%),
              radial-gradient(950px 500px at 108% 0%, var(--glow-b) 0%, transparent 60%),
              linear-gradient(180deg, var(--bg-a), var(--bg-b));
          }}
          html[data-theme="dark"] body {{
            background: var(--bg-a);
          }}
          .errors-shell {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 0;
            border-radius: 18px;
            border: 1px solid var(--line);
            background: var(--card);
            box-shadow: 0 10px 35px rgba(18, 37, 66, 0.08);
            overflow: hidden;
          }}
          .topbar {{
            padding: 14px;
            border-bottom: 1px solid var(--line);
            background: linear-gradient(
              180deg,
              color-mix(in srgb, var(--card) 92%, #fff),
              color-mix(in srgb, var(--card) 98%, #000)
            );
          }}
          .errors-body {{
            padding: 14px;
          }}
          .text-muted {{ color: var(--muted)!important; }}
          .table {{
            --bs-table-bg: transparent;
            --bs-table-color: var(--ink);
            color: var(--ink);
          }}
          .table-striped > tbody > tr:nth-of-type(odd) > * {{
            color: var(--ink);
            background-color: color-mix(in srgb, var(--card-soft) 88%, transparent);
          }}
          .editable-field.form-control,
          .editable-field.form-select {{
            background-color: color-mix(in srgb, var(--card) 95%, #000);
            color: var(--ink);
            border-color: var(--line);
          }}
          .editable-field.form-control::placeholder {{
            color: var(--muted);
            opacity: 1;
          }}
          .editable-field.form-control:focus,
          .editable-field.form-select:focus {{
            background-color: color-mix(in srgb, var(--card) 95%, #000);
            color: var(--ink);
            border-color: color-mix(in srgb, #2dd4bf 45%, #fff);
            box-shadow: 0 0 0 .2rem color-mix(in srgb, #2dd4bf 18%, transparent);
          }}
          .btn-outline-secondary {{
            --bs-btn-color: var(--muted);
            --bs-btn-border-color: var(--line);
            --bs-btn-hover-color: var(--ink);
            --bs-btn-hover-bg: color-mix(in srgb, var(--card) 84%, #000);
            --bs-btn-hover-border-color: var(--line);
          }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{
            border-radius: 999px;
          }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, #ef4444 45%, #fff);
            background: color-mix(in srgb, #ef4444 14%, transparent);
            color: var(--ink);
          }}
          code {{
            color: var(--ink);
            background: color-mix(in srgb, var(--card-soft) 95%, #000);
            padding: .1rem .3rem;
            border-radius: .3rem;
          }}
          th.sortable {{
            cursor: pointer;
            user-select: none;
          }}
          th.sortable .sort-ind {{
            opacity: .55;
            margin-left: .25rem;
            font-size: .85em;
          }}
          th.sortable.active .sort-ind {{
            opacity: 1;
          }}
          th.sortable {{
            cursor: pointer;
            user-select: none;
          }}
          th.sortable .sort-ind {{
            opacity: .55;
            margin-left: .25rem;
            font-size: .85em;
          }}
          th.sortable.active .sort-ind {{
            opacity: 1;
          }}
          #nav-toggle {{ display: none; }}
          @media (max-width: 860px) {{
            #nav-toggle {{ display: inline-flex; }}
            .topbar {{
              padding: 10px;
            }}
            .nav-actions {{
              width: 100%;
              justify-content: space-between;
            }}
            .nav-links {{
              width: 100%;
              display: none;
              margin-top: 8px;
            }}
            .nav-links.open {{
              display: flex;
            }}
            .nav-links .btn {{
              flex: 1 1 auto;
              justify-content: center;
            }}
          }}
        </style>
      </head>
      <body>
        <div class="errors-shell">
          <div class="topbar d-flex align-items-center justify-content-between gap-2 flex-wrap">
            <div class="nav-actions">
              <div class="d-flex gap-2">
                <a class="btn btn-outline-secondary btn-sm"
                  href="/admin?token={token}" title="Back"
                  aria-label="Back">←</a>
                <button id="theme-toggle" class="btn btn-outline-secondary btn-sm"
                  aria-label="Toggle theme"></button>
                <button id="clear-errors" class="btn btn-outline-danger btn-sm"
                  title="Clear errors" aria-label="Clear errors">
                  <i class="bi bi-trash"></i>
                </button>
              </div>
              <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                aria-label="Toggle navigation">
                <i class="bi bi-list"></i>
              </button>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm" href="/admin?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm active" href="/admin/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
            </div>
          </div>
          <div class="errors-body">
          <h2 class="fs-4">Error History</h2>
          <div class="d-flex flex-wrap gap-2 mb-2">
            <input id="q" class="editable-field form-control form-control-sm"
              placeholder="Search error / component / topic" style="max-width: 420px;">
            <select id="limit" class="editable-field form-select form-select-sm"
              style="max-width: 120px;">
              <option value="50">50</option>
              <option value="100" selected>100</option>
              <option value="200">200</option>
            </select>
            <a id="export-json" class="btn btn-outline-secondary btn-sm">Export JSON</a>
            <a id="export-csv" class="btn btn-outline-secondary btn-sm">Export CSV</a>
          </div>
          <div class="d-flex align-items-center gap-2 mb-2">
            <button id="prev" class="btn btn-outline-secondary btn-sm">Prev</button>
            <button id="next" class="btn btn-outline-secondary btn-sm">Next</button>
            <span id="meta" class="text-muted small"></span>
          </div>
          <div class="table-responsive">
            <table class="table table-sm table-striped align-middle">
              <thead>
                <tr>
                  <th class="sortable" data-sort="id">ID <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="ts">Time <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="component">Component <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="topic">Topic <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="error">Error <span class="sort-ind">↕</span></th>
                </tr>
              </thead>
              <tbody id="rows"></tbody>
            </table>
          </div>
          </div>
        </div>
        <script>
          const token = {token!r};
          const sunIcon = '<i class="bi bi-sun-fill"></i>';
          const moonIcon = '<i class="bi bi-moon-stars-fill"></i>';
          const getTheme = () => localStorage.getItem('ui_theme');
          const applyTheme = (theme) => {{
            const resolved = theme || (
              window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
            );
            document.documentElement.setAttribute('data-theme', resolved);
            const btn = document.getElementById('theme-toggle');
            if (btn) {{
              const toLight = resolved === 'dark';
              btn.innerHTML = toLight ? sunIcon : moonIcon;
              btn.title = toLight ? 'Light mode' : 'Dark mode';
              btn.setAttribute('aria-label', btn.title);
            }}
          }};
          const state = {{
            offset: 0,
            limit: 100,
            q: '',
            sort_by: 'id',
            sort_dir: 'desc'
          }};
          function fmt(ts) {{
            return new Date((ts || 0) * 1000).toLocaleString('fr-FR');
          }}
          function queryString(withFormat) {{
            const params = new URLSearchParams();
            params.set('token', token);
            params.set('limit', state.limit);
            params.set('offset', state.offset);
            if (state.q) params.set('q', state.q);
            if (withFormat) params.set('format', withFormat);
            return params.toString();
          }}
          function applyFilters() {{
            state.q = document.getElementById('q').value.trim();
            state.limit = parseInt(document.getElementById('limit').value, 10);
            state.offset = 0;
            load();
          }}
          let filterTimer = null;
          function scheduleApply() {{
            if (filterTimer) clearTimeout(filterTimer);
            filterTimer = setTimeout(applyFilters, 220);
          }}
          function setSortIndicators() {{
            document.querySelectorAll('th.sortable').forEach(th => {{
              const active = th.dataset.sort === state.sort_by;
              th.classList.toggle('active', active);
              const ind = th.querySelector('.sort-ind');
              if (!ind) return;
              ind.textContent = active
                ? (state.sort_dir === 'asc' ? '↑' : '↓')
                : '↕';
            }});
          }}
          function sortItems(items) {{
            const key = state.sort_by;
            const dir = state.sort_dir === 'asc' ? 1 : -1;
            const numeric = new Set(['id', 'ts']);
            return items.slice().sort((a, b) => {{
              const av = a[key];
              const bv = b[key];
              if (numeric.has(key)) {{
                return ((Number(av || 0) - Number(bv || 0)) * dir);
              }}
              return String(av || '').localeCompare(String(bv || ''), 'fr', {{
                sensitivity: 'base'
              }}) * dir;
            }});
          }}
          async function load() {{
            const res = await fetch('/api/errors?' + queryString(''));
            if (!res.ok) return;
            const data = await res.json();
            const rows = document.getElementById('rows');
            document.getElementById('meta').innerText =
              `Total: ${{data.total}} | Offset: ${{data.offset}} | Limit: ${{data.limit}}`;
            rows.innerHTML = sortItems(data.items || []).map(e => `
              <tr>
                <td>${{e.id}}</td>
                <td>${{fmt(e.ts)}}</td>
                <td>${{e.component || ''}}</td>
                <td>${{e.topic || ''}}</td>
                <td><code>${{(e.error || '').slice(0, 300)}}</code></td>
              </tr>
            `).join('');
            setSortIndicators();
          }}
          document.getElementById('clear-errors').onclick = async () => {{
            if (!confirm('Clear all errors?')) return;
            await fetch(
              '/api/errors/clear?token=' + encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            state.offset = 0;
            await load();
          }};
          document.getElementById('q').addEventListener('input', scheduleApply);
          document.getElementById('limit').addEventListener('change', applyFilters);
          document.querySelectorAll('th.sortable').forEach(th => {{
            th.addEventListener('click', () => {{
              const key = th.dataset.sort;
              if (state.sort_by === key) {{
                state.sort_dir = state.sort_dir === 'asc' ? 'desc' : 'asc';
              }} else {{
                state.sort_by = key;
                state.sort_dir = key === 'id' || key === 'ts' ? 'desc' : 'asc';
              }}
              load();
            }});
          }});
          document.getElementById('prev').onclick = () => {{
            state.offset = Math.max(0, state.offset - state.limit);
            load();
          }};
          document.getElementById('next').onclick = () => {{
            state.offset += state.limit;
            load();
          }};
          document.getElementById('export-json').onclick = (e) => {{
            e.target.href = '/api/errors?' + queryString('');
          }};
          document.getElementById('export-csv').onclick = (e) => {{
            e.target.href = '/api/errors?' + queryString('csv');
          }};
          applyTheme(getTheme());
          document.getElementById('theme-toggle').onclick = () => {{
            const current = document.documentElement.getAttribute('data-theme') || 'light';
            const next = current === 'dark' ? 'light' : 'dark';
            localStorage.setItem('ui_theme', next);
            applyTheme(next);
          }};
          document.getElementById('nav-toggle').onclick = () => {{
            document.querySelector('.nav-links').classList.toggle('open');
          }};
          load();
          setInterval(load, 5000);
        </script>
      </body>
    </html>
    """
    return _admin_html_response(request, token, html)


async def admin_queue_page(request):
    token = _require_admin(request)
    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Forwarder Queue</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link
          href="https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&display=swap"
          rel="stylesheet"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        >
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
        >
        <style>
          :root {{
            color-scheme: light;
            --bg-a: #f7f9fc;
            --bg-b: #f7f9fc;
            --glow-a: #eef7f3;
            --glow-b: #eef3ff;
            --ink: #132239;
            --line: #dbe2ec;
            --card: #ffffff;
            --muted: #607086;
            --card-soft: #f9fbfe;
          }}
          html[data-theme="dark"] {{
            color-scheme: dark;
            --bg-a: #090f18;
            --bg-b: #0d1520;
            --glow-a: #102434;
            --glow-b: #1a2440;
            --ink: #e8edf5;
            --line: #273447;
            --card: #121c29;
            --muted: #a1afc1;
            --card-soft: #182638;
          }}
          body {{
            padding: 16px;
            font-family: "Manrope", sans-serif;
            color: var(--ink);
            background:
              radial-gradient(1000px 550px at -8% -15%, var(--glow-a) 0%, transparent 65%),
              radial-gradient(950px 500px at 108% 0%, var(--glow-b) 0%, transparent 60%),
              linear-gradient(180deg, var(--bg-a), var(--bg-b));
          }}
          html[data-theme="dark"] body {{
            background: var(--bg-a);
          }}
          .queue-shell {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 0;
            border-radius: 18px;
            border: 1px solid var(--line);
            background: var(--card);
            box-shadow: 0 10px 35px rgba(18, 37, 66, 0.08);
            overflow: hidden;
          }}
          .topbar {{
            padding: 14px;
            border-bottom: 1px solid var(--line);
            background: linear-gradient(
              180deg,
              color-mix(in srgb, var(--card) 92%, #fff),
              color-mix(in srgb, var(--card) 98%, #000)
            );
          }}
          .queue-body {{
            padding: 14px;
          }}
          .text-muted {{ color: var(--muted)!important; }}
          .table {{
            --bs-table-bg: transparent;
            --bs-table-color: var(--ink);
            color: var(--ink);
          }}
          .table-striped > tbody > tr:nth-of-type(odd) > * {{
            color: var(--ink);
            background-color: color-mix(in srgb, var(--card-soft) 88%, transparent);
          }}
          .editable-field.form-control,
          .editable-field.form-select {{
            background-color: color-mix(in srgb, var(--card) 95%, #000);
            color: var(--ink);
            border-color: var(--line);
          }}
          .editable-field.form-control::placeholder {{
            color: var(--muted);
            opacity: 1;
          }}
          .editable-field.form-control:focus,
          .editable-field.form-select:focus {{
            background-color: color-mix(in srgb, var(--card) 95%, #000);
            color: var(--ink);
            border-color: color-mix(in srgb, #2dd4bf 45%, #fff);
            box-shadow: 0 0 0 .2rem color-mix(in srgb, #2dd4bf 18%, transparent);
          }}
          .btn-outline-secondary {{
            --bs-btn-color: var(--muted);
            --bs-btn-border-color: var(--line);
            --bs-btn-hover-color: var(--ink);
            --bs-btn-hover-bg: color-mix(in srgb, var(--card) 84%, #000);
            --bs-btn-hover-border-color: var(--line);
          }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{
            border-radius: 999px;
          }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, #f59e0b 45%, #fff);
            background: color-mix(in srgb, #f59e0b 14%, transparent);
            color: var(--ink);
          }}
          code {{
            color: var(--ink);
            background: color-mix(in srgb, var(--card-soft) 95%, #000);
            padding: .1rem .3rem;
            border-radius: .3rem;
          }}
          .dlq-table th.sortable {{
            cursor: pointer;
            user-select: none;
          }}
          .dlq-table th.sortable .sort-ind {{
            opacity: .55;
            margin-left: .25rem;
            font-size: .85em;
          }}
          .dlq-table th.sortable.active .sort-ind {{
            opacity: 1;
          }}
          .queue-shell.dlq-compact .dlq-table thead {{
            display: none !important;
          }}
          .queue-shell.dlq-compact .dlq-table tbody tr {{
            display: block !important;
            border: 1px solid var(--line);
            border-radius: 12px;
            margin-bottom: 10px;
            padding: 8px;
            background: var(--card-soft);
          }}
          .queue-shell.dlq-compact .dlq-table tbody td {{
            display: flex !important;
            justify-content: space-between;
            gap: 10px;
            border: 0 !important;
            padding: 6px 4px;
            text-align: right;
            width: 100%;
          }}
          .queue-shell.dlq-compact .dlq-table tbody td::before {{
            content: attr(data-label);
            color: var(--muted);
            font-weight: 700;
            text-transform: uppercase;
            font-size: .75rem;
            letter-spacing: .02em;
            text-align: left;
          }}
          .queue-shell.dlq-compact .dlq-table tbody td[data-label="Action"] {{
            justify-content: flex-end;
            gap: 6px;
            padding-top: 8px;
          }}
          .queue-shell.dlq-compact .dlq-table tbody td[data-label="Action"]::before {{
            margin-right: auto;
          }}
          .queue-shell.dlq-compact .dlq-table tbody td[data-label="Message"] code {{
            max-width: 58vw;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
          }}
          #nav-toggle {{ display: none; }}
          @media (max-width: 1200px), (hover: none) and (pointer: coarse) {{
            #nav-toggle {{ display: inline-flex; }}
            .topbar {{
              padding: 10px;
            }}
            .queue-body {{
              padding: 10px;
            }}
            .nav-actions {{
              width: 100%;
              justify-content: space-between;
            }}
            .nav-links {{
              width: 100%;
              display: none;
              margin-top: 8px;
            }}
            .nav-links.open {{
              display: flex;
            }}
            .nav-links .btn {{
              flex: 1 1 auto;
              justify-content: center;
            }}
          }}
        </style>
      </head>
      <body>
        <div class="queue-shell">
          <div class="topbar d-flex align-items-center justify-content-between gap-2 flex-wrap">
            <div class="nav-actions">
              <div class="d-flex gap-2">
                <a class="btn btn-outline-secondary btn-sm"
                  href="/admin?token={token}" title="Back"
                  aria-label="Back">←</a>
                <button id="theme-toggle" class="btn btn-outline-secondary btn-sm"
                  aria-label="Toggle theme"></button>
                <button id="requeue-batch" class="btn btn-outline-success btn-sm"
                  title="Requeue filtered batch" aria-label="Requeue filtered batch">
                  <i class="bi bi-arrow-repeat"></i>
                </button>
                <button id="clear-dlq" class="btn btn-outline-danger btn-sm"
                  title="Clear filtered DLQ" aria-label="Clear filtered DLQ">
                  <i class="bi bi-trash"></i>
                </button>
              </div>
              <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                aria-label="Toggle navigation">
                <i class="bi bi-list"></i>
              </button>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm" href="/admin?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/admin/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm active" href="/admin/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
            </div>
          </div>
          <div class="queue-body">
          <h2 class="fs-4">Dead Letter Queue</h2>
          <div class="d-flex flex-wrap gap-2 mb-2">
            <input id="topic" class="editable-field form-control form-control-sm"
              placeholder="Search topic / reason / payload" style="max-width: 420px;">
            <select id="limit" class="editable-field form-select form-select-sm"
              style="max-width: 120px;">
              <option value="50">50</option>
              <option value="100" selected>100</option>
              <option value="200">200</option>
            </select>
          </div>
          <div class="d-flex align-items-center gap-2 mb-2">
            <button id="prev" class="btn btn-outline-secondary btn-sm">Prev</button>
            <button id="next" class="btn btn-outline-secondary btn-sm">Next</button>
            <span id="meta" class="text-muted small"></span>
          </div>
          <div class="table-responsive">
            <table class="table table-sm table-striped align-middle dlq-table">
              <thead>
                <tr>
                  <th class="sortable" data-sort="id">ID <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="topic">Topic <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="attempts">Attempts <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="last_error">Error <span class="sort-ind">↕</span></th>
                  <th>Message</th>
                  <th class="sortable" data-sort="updated_at">Updated <span class="sort-ind">↕</span></th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody id="rows"></tbody>
            </table>
          </div>
          <dialog id="payload-modal" class="rounded border-0 shadow" style="max-width:920px;width:92vw;">
            <div class="d-flex justify-content-between align-items-center mb-2">
              <h5 class="m-0">DLQ Message</h5>
              <button id="payload-close" class="btn btn-outline-secondary btn-sm">
                <i class="bi bi-x-lg"></i>
              </button>
            </div>
            <pre id="payload-content" class="mb-0"></pre>
          </dialog>
          </div>
        </div>
        <script>
          const token = {token!r};
          const sunIcon = '<i class="bi bi-sun-fill"></i>';
          const moonIcon = '<i class="bi bi-moon-stars-fill"></i>';
          const getTheme = () => localStorage.getItem('ui_theme');
          const applyTheme = (theme) => {{
            const resolved = theme || (
              window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
            );
            document.documentElement.setAttribute('data-theme', resolved);
            const btn = document.getElementById('theme-toggle');
            if (btn) {{
              const toLight = resolved === 'dark';
              btn.innerHTML = toLight ? sunIcon : moonIcon;
              btn.title = toLight ? 'Light mode' : 'Dark mode';
              btn.setAttribute('aria-label', btn.title);
            }}
          }};
          const state = {{
            offset: 0,
            limit: 100,
            q: '',
            sort_by: 'id',
            sort_dir: 'desc'
          }};
          const payloadById = new Map();
          function fmt(ts) {{
            return new Date((ts || 0) * 1000).toLocaleString('fr-FR');
          }}
          function applyCompactLayout() {{
            const shell = document.querySelector('.queue-shell');
            if (!shell) return;
            const compact = (
              window.innerWidth <= 1600 ||
              window.matchMedia('(hover: none) and (pointer: coarse)').matches
            );
            shell.classList.toggle('dlq-compact', compact);
          }}
          function escapeHtml(value) {{
            return String(value || '')
              .replaceAll('&', '&amp;')
              .replaceAll('<', '&lt;')
              .replaceAll('>', '&gt;');
          }}
          function messagePreview(item) {{
            const payload = item.payload || {{}};
            const title = payload.title ? String(payload.title).trim() : '';
            const message = payload.message ? String(payload.message).trim() : '';
            const text = [title, message].filter(Boolean).join(' - ');
            return text || '(empty)';
          }}
          function queryString() {{
            const params = new URLSearchParams();
            params.set('token', token);
            params.set('limit', state.limit);
            params.set('offset', state.offset);
            if (state.q) params.set('q', state.q);
            return params.toString();
          }}
          function applyFilters() {{
            state.q = document.getElementById('topic').value.trim();
            state.limit = parseInt(document.getElementById('limit').value, 10);
            state.offset = 0;
            load();
          }}
          let filterTimer = null;
          function scheduleApply() {{
            if (filterTimer) clearTimeout(filterTimer);
            filterTimer = setTimeout(applyFilters, 220);
          }}
          function setSortIndicators() {{
            document.querySelectorAll('th.sortable').forEach(th => {{
              const active = th.dataset.sort === state.sort_by;
              th.classList.toggle('active', active);
              const ind = th.querySelector('.sort-ind');
              if (!ind) return;
              ind.textContent = active
                ? (state.sort_dir === 'asc' ? '↑' : '↓')
                : '↕';
            }});
          }}
          function sortItems(items) {{
            const key = state.sort_by;
            const dir = state.sort_dir === 'asc' ? 1 : -1;
            const numeric = new Set(['id', 'attempts', 'updated_at']);
            return items.slice().sort((a, b) => {{
              const av = a[key];
              const bv = b[key];
              if (numeric.has(key)) {{
                return ((Number(av || 0) - Number(bv || 0)) * dir);
              }}
              return String(av || '').localeCompare(String(bv || ''), 'fr', {{
                sensitivity: 'base'
              }}) * dir;
            }});
          }}
          async function requeue(id) {{
            await fetch(
              '/api/queue/dead_letters/' + id + '/requeue?token=' +
                encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            await load();
          }}
          async function del(id) {{
            await fetch(
              '/api/queue/dead_letters/' + id + '/delete?token=' +
                encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            await load();
          }}
          function viewPayload(id) {{
            const payload = payloadById.get(id) || null;
            if (!payload) return;
            const el = document.getElementById('payload-content');
            el.textContent = JSON.stringify(payload, null, 2);
            const dlg = document.getElementById('payload-modal');
            if (typeof dlg.showModal === 'function') {{
              dlg.showModal();
            }}
          }}
          async function load() {{
            const res = await fetch('/api/queue/dead_letters?' + queryString());
            if (!res.ok) return;
            const data = await res.json();
            document.getElementById('meta').innerText =
              'Queue: ' + data.queue_size + ' | Dead letters: ' + data.dead_letters +
              ' | Total filtered: ' + data.total;
            const rows = document.getElementById('rows');
            payloadById.clear();
            rows.innerHTML = sortItems(data.items || []).map(e => {{
              payloadById.set(e.id, e.payload || null);
              return `
              <tr>
                <td data-label="ID">${{e.id}}</td>
                <td data-label="Topic">${{e.topic || ''}}</td>
                <td data-label="Attempts">${{e.attempts}}</td>
                <td data-label="Error"><code>${{(e.last_error || '').slice(0, 200)}}</code></td>
                <td data-label="Message"><code>${{escapeHtml(messagePreview(e).slice(0, 160))}}</code></td>
                <td data-label="Updated">${{fmt(e.updated_at)}}</td>
                <td data-label="Action">
                  <button class="btn btn-outline-secondary btn-sm"
                    onclick="viewPayload(${{e.id}})">
                    <i class="bi bi-eye"></i>
                  </button>
                  <button class="btn btn-outline-success btn-sm"
                    onclick="requeue(${{e.id}})">Requeue</button>
                  <button class="btn btn-outline-danger btn-sm"
                    onclick="del(${{e.id}})">Delete</button>
                </td>
              </tr>
            `;
            }}).join('');
            setSortIndicators();
          }}
          document.getElementById('topic').addEventListener('input', scheduleApply);
          document.getElementById('limit').addEventListener('change', applyFilters);
          document.getElementById('payload-close').onclick = () => {{
            document.getElementById('payload-modal').close();
          }};
          document.querySelectorAll('th.sortable').forEach(th => {{
            th.addEventListener('click', () => {{
              const key = th.dataset.sort;
              if (state.sort_by === key) {{
                state.sort_dir = state.sort_dir === 'asc' ? 'desc' : 'asc';
              }} else {{
                state.sort_by = key;
                state.sort_dir =
                  key === 'id' || key === 'updated_at' || key === 'attempts'
                    ? 'desc'
                    : 'asc';
              }}
              load();
            }});
          }});
          document.getElementById('prev').onclick = () => {{
            state.offset = Math.max(0, state.offset - state.limit);
            load();
          }};
          document.getElementById('next').onclick = () => {{
            state.offset += state.limit;
            load();
          }};
          document.getElementById('requeue-batch').onclick = async () => {{
            await fetch(
              '/api/queue/dead_letters/requeue_batch?token=' + encodeURIComponent(token),
              {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                  q: state.q || null,
                  limit: state.limit
                }})
              }}
            );
            state.offset = 0;
            load();
          }};
          document.getElementById('clear-dlq').onclick = async () => {{
            if (!confirm('Clear filtered DLQ items?')) return;
            await fetch(
              '/api/queue/dead_letters/clear?token=' + encodeURIComponent(token),
              {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                  q: state.q || null
                }})
              }}
            );
            state.offset = 0;
            load();
          }};
          applyTheme(getTheme());
          document.getElementById('theme-toggle').onclick = () => {{
            const current = document.documentElement.getAttribute('data-theme') || 'light';
            const next = current === 'dark' ? 'light' : 'dark';
            localStorage.setItem('ui_theme', next);
            applyTheme(next);
          }};
          document.getElementById('nav-toggle').onclick = () => {{
            document.querySelector('.nav-links').classList.toggle('open');
          }};
          window.addEventListener('resize', applyCompactLayout);
          window.addEventListener('orientationchange', applyCompactLayout);
          applyCompactLayout();
          load();
          setInterval(load, 5000);
        </script>
      </body>
    </html>
    """
    return _admin_html_response(request, token, html)

async def stats_api(request):
    _require_admin(request)
    log("INFO", "admin stats_api")
    now = int(time.time())
    since_24h = now - 86400
    topics_rows = await list_topics()
    status_counts = await list_topic_status_counts()
    counts_24h = await count_messages_by_topic_since(since_24h)
    queue_count = await count_telegram_queue()
    dead_letters = await count_dead_letters()
    workers_active = _active_worker_count()
    enabled_topics = sum(1 for r in topics_rows if bool(r["enabled"]))
    disabled_topics = len(topics_rows) - enabled_topics
    totals = {
        "received": sum(v.get("received", 0) for v in status_counts.values()),
        "filtered": sum(v.get("filtered", 0) for v in status_counts.values()),
        "rate_limited": sum(v.get("rate_limited", 0) for v in status_counts.values()),
        "disabled": sum(v.get("disabled", 0) for v in status_counts.values()),
    }
    total_messages_24h = sum(counts_24h.values())
    top_topics_24h = sorted(
        counts_24h.items(),
        key=lambda kv: kv[1],
        reverse=True,
    )[:10]
    stale_workers = 0
    max_worker_age = 0
    for topic, ts in worker_last_seen.items():
        if not _worker_running(topic):
            continue
        age = max(0, now - int(ts))
        max_worker_age = max(max_worker_age, age)
        if age > 120:
            stale_workers += 1
    return web.json_response(
        {
            "workers": workers_active,
            "queue": queue_count,
            "dead_letters": dead_letters,
            "topics_total": len(topics_rows),
            "topics_enabled": enabled_topics,
            "topics_disabled": disabled_topics,
            "messages_24h": total_messages_24h,
            "errors_24h": await count_errors_since(since_24h),
            "received_total": totals["received"],
            "filtered_total": totals["filtered"],
            "rate_limited_total": totals["rate_limited"],
            "disabled_total": totals["disabled"],
            "stale_workers": stale_workers,
            "max_last_seen_age_seconds": max_worker_age,
            "top_topics_24h": [
                {"topic": topic, "count": count}
                for topic, count in top_topics_24h
            ],
        }
    )


async def errors_api(request):
    _require_admin(request)
    limit = int(request.query.get("limit", "100"))
    limit = max(1, min(limit, 1000))
    offset = int(request.query.get("offset", "0"))
    offset = max(0, offset)
    query = request.query.get("q", "").strip() or None
    component = request.query.get("component", "").strip() or None
    topic = request.query.get("topic", "").strip() or None
    export_format = request.query.get("format", "").strip().lower()

    result = await query_errors(
        limit=limit,
        offset=offset,
        query=query,
        component=component,
        topic=topic,
    )
    if export_format == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id", "ts", "component", "topic", "error"])
        for row in result["items"]:
            writer.writerow([
                row["id"],
                row["ts"],
                row["component"],
                row["topic"],
                row["error"],
            ])
        return web.Response(
            text=buf.getvalue(),
            content_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=errors.csv",
            },
        )
    return web.json_response(
        {
            "items": result["items"],
            "total": result["total"],
            "limit": limit,
            "offset": offset,
        }
    )


async def errors_clear_api(request):
    _require_admin(request)
    log("INFO", "admin errors_clear_api")
    deleted = await clear_errors()
    return web.json_response({"cleared": True, "deleted": deleted})


async def dead_letters_api(request):
    _require_admin(request)
    limit = int(request.query.get("limit", "200"))
    limit = max(1, min(limit, 1000))
    offset = int(request.query.get("offset", "0"))
    offset = max(0, offset)
    topic = request.query.get("topic", "").strip() or None
    reason = request.query.get("reason", "").strip() or None
    query = request.query.get("q", "").strip() or None
    result = await query_dead_letters(
        limit=limit,
        offset=offset,
        topic=topic,
        reason=reason,
        query=query,
    )
    return web.json_response(
        {
            "items": result["items"],
            "total": result["total"],
            "limit": limit,
            "offset": offset,
            "queue_size": await count_telegram_queue(),
            "dead_letters": await count_dead_letters(),
        }
    )


async def dead_letter_requeue_api(request):
    _require_admin(request)
    item_id = int(request.match_info["id"])
    item = await get_dead_letter(item_id)
    if item is None:
        raise web.HTTPNotFound()
    await enqueue_telegram_item(item["payload"])
    await delete_dead_letter(item_id)
    return web.json_response({"requeued": item_id})


async def dead_letter_requeue_batch_api(request):
    _require_admin(request)
    data = await request.json()
    topic = str(data.get("topic", "")).strip() or None
    reason = str(data.get("reason", "")).strip() or None
    query = str(data.get("q", "")).strip() or None
    limit = int(data.get("limit", 100))
    limit = max(1, min(limit, 1000))
    result = await query_dead_letters(
        limit=limit,
        offset=0,
        topic=topic,
        reason=reason,
        query=query,
    )
    ids = []
    for item in result["items"]:
        await enqueue_telegram_item(item["payload"])
        ids.append(item["id"])
    await delete_dead_letters(ids)
    return web.json_response({"requeued": len(ids)})


async def dead_letter_delete_api(request):
    _require_admin(request)
    item_id = int(request.match_info["id"])
    await delete_dead_letter(item_id)
    return web.json_response({"deleted": item_id})


async def dead_letter_clear_api(request):
    _require_admin(request)
    data = await request.json()
    topic = str(data.get("topic", "")).strip() or None
    reason = str(data.get("reason", "")).strip() or None
    query = str(data.get("q", "")).strip() or None
    deleted = await clear_dead_letters(topic=topic, reason=reason, query=query)
    return web.json_response({"cleared": True, "deleted": deleted})

async def create_web_app():

    app = web.Application()

    app.router.add_get(
        "/health",
        health,
    )

    app.router.add_get(
        "/metrics",
        metrics,
    )
    app.router.add_get(
        "/admin",
        admin_page,
    )
    app.router.add_get(
        "/admin/stats",
        admin_stats_page,
    )
    app.router.add_get(
        "/admin/queue",
        admin_queue_page,
    )
    app.router.add_get(
        "/admin/errors",
        admin_errors_page,
    )
    app.router.add_get(
        "/admin/topic/{name}",
        admin_topic_page,
    )
    app.router.add_get(
        "/api/topics",
        topics_list,
    )
    app.router.add_get(
        "/api/topics/{name}",
        topic_detail,
    )
    app.router.add_post(
        "/api/topics/{name}/toggle",
        topic_toggle,
    )
    app.router.add_post(
        "/api/topics/pause_all",
        pause_all,
    )
    app.router.add_post(
        "/api/topics/resume_all",
        resume_all,
    )
    app.router.add_post(
        "/api/topics/{name}/clear",
        clear_topic,
    )
    app.router.add_post(
        "/api/topics/{name}/reset_count",
        reset_topic_count,
    )
    app.router.add_post(
        "/api/topics/clear_all",
        clear_all,
    )
    app.router.add_get(
        "/api/topics/export",
        topics_export,
    )
    app.router.add_post(
        "/api/topics/import",
        topics_import,
    )
    app.router.add_get(
        "/api/stats",
        stats_api,
    )
    app.router.add_get(
        "/api/errors",
        errors_api,
    )
    app.router.add_post(
        "/api/errors/clear",
        errors_clear_api,
    )
    app.router.add_get(
        "/api/queue/dead_letters",
        dead_letters_api,
    )
    app.router.add_post(
        "/api/queue/dead_letters/{id}/requeue",
        dead_letter_requeue_api,
    )
    app.router.add_post(
        "/api/queue/dead_letters/requeue_batch",
        dead_letter_requeue_batch_api,
    )
    app.router.add_post(
        "/api/queue/dead_letters/{id}/delete",
        dead_letter_delete_api,
    )
    app.router.add_post(
        "/api/queue/dead_letters/clear",
        dead_letter_clear_api,
    )

    return app
