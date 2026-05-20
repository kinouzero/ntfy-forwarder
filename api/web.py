import asyncio
import time

from aiohttp import web
from prometheus_client import (
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from core.state import (
    workers,
    telegram_queue,
    worker_last_seen,
    recent_events,
    topic_stats,
    topic_rates,
)
from core.metrics import telegram_queue_size
from core.config import ADMIN_TOKEN
from core.config import TOPIC_ALLOWLIST, TOPIC_DENYLIST
from core.config import ADMIN_RECENT_EVENTS
from core.logging import log
from db.topics import (
    list_topics,
    set_topic_enabled,
    add_topic,
)
from services.ntfy import ntfy_worker

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

async def health(request):

    telegram_queue_size.set(
        telegram_queue.qsize()
    )
    return web.json_response({
        "status": "ok",
        "workers": len(workers),
        "queue": telegram_queue.qsize(),
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
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        >
        <style>
          body {{ padding: 16px; }}
          .on {{ color: #0a0; }}
          .off {{ color: #a00; }}
          .spark {{ font-family: monospace; margin-left: 6px; }}
        </style>
      </head>
      <body>
        <div class="d-flex align-items-center justify-content-between mb-3">
          <h2 class="m-0">Topics</h2>
          <a class="btn btn-outline-secondary" href="/admin/stats?token={token}">
            Global Stats
          </a>
        </div>
        <div class="mb-3 d-flex flex-wrap gap-2 align-items-center">
          <button id="pause-all" class="btn btn-warning btn-sm">Pause All</button>
          <button id="resume-all" class="btn btn-success btn-sm">Resume All</button>
          <button id="clear-all" class="btn btn-outline-danger btn-sm">Clear Stats</button>
          <input id="filter" class="form-control form-control-sm"
            style="max-width: 220px;" placeholder="Filter topics" />
          <select id="sort" class="form-select form-select-sm" style="max-width: 180px;">
            <option value="name">Sort: Name</option>
            <option value="count">Sort: Count</option>
            <option value="rate">Sort: Rate</option>
            <option value="last_seen">Sort: Last Seen</option>
          </select>
          <button id="sort-dir" class="btn btn-outline-secondary btn-sm">Desc</button>
        </div>
        <table id="topics" class="table table-sm table-striped align-middle">
          <thead>
            <tr>
              <th>Name</th>
              <th>Status</th>
              <th>Count</th>
              <th>Last Seen</th>
              <th>Rate</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
        <script>
          const token = {token!r};
          const fmtDate = (ts) => {{
            if (!ts) return '';
            return new Date(ts * 1000).toLocaleString('fr-FR');
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
              else if (sort === 'rate') v = (b.rate_60s || 0) - (a.rate_60s || 0);
              else if (sort === 'last_seen') v = (b.last_seen || 0) - (a.last_seen || 0);
              else v = (a.name || '').localeCompare(b.name || '');
              return dir === 'asc' ? -v : v;
            }});
            items.forEach(t => {{
              const tr = document.createElement('tr');
              const statusBadge = t.enabled ? 'success' : 'secondary';
              tr.innerHTML = `
                <td>
                  <a href="/admin/topic/${{encodeURIComponent(t.name)}}?token=" +
                    encodeURIComponent(token)>
                    ${{t.name}}
                  </a>
                </td>
                <td>
                  <span class="badge text-bg-${{statusBadge}}">
                    ${{t.enabled ? 'enabled' : 'disabled'}}
                  </span>
                </td>
                <td><span class="badge text-bg-light">${{t.count}}</span></td>
                <td>${{fmtDate(t.last_seen)}}</td>
                <td>
                  ${{t.rate_60s ?? 0}}/min
                  <span class="spark" data-rate="${{(t.rate_spark || []).join(',')}}"></span>
                </td>
                <td>
                  <button class="btn btn-outline-primary btn-sm" data-name="${{t.name}}">
                    ${{t.enabled ? 'Disable' : 'Enable'}}
                  </button>
                </td>
              `;
              tr.querySelector('button').onclick = async () => {{
                await fetch(
                  '/api/topics/' + encodeURIComponent(t.name) +
                  '/toggle?token=' + encodeURIComponent(token),
                  {{
                  method: 'POST'
                  }}
                );
                fetchTopics();
              }};
              const sp = tr.querySelector('.spark');
              if (sp) {{
                const values = (sp.dataset.rate || '')
                  .split(',')
                  .filter(x => x)
                  .map(x => parseInt(x, 10));
                if (values.length) {{
                  sp.innerText = values.map(v => v ? '▇' : '▁').join('');
                }}
              }}
              tbody.appendChild(tr);
            }});
          }}
          applyState();
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
    resp = web.Response(
        text=html,
        content_type="text/html",
    )
    if request.cookies.get("admin_token") != token:
        resp.set_cookie("admin_token", token, httponly=True)
    return resp

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
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        >
        <style>
          body {{ padding: 16px; }}
          .meta {{ margin-bottom: 12px; }}
          .event {{ padding: 8px; border-bottom: 1px solid #ddd; }}
        </style>
      </head>
      <body>
        <div class="meta">
          <a class="btn btn-outline-secondary btn-sm" href="/admin?token={token}">Back</a>
          <h2>Topic: {name}</h2>
          <div id="stats" class="row g-2 mb-2"></div>
          <button id="clear-topic" class="btn btn-outline-danger btn-sm">Clear Stats</button>
        </div>
        <div id="events" class="list-group"></div>
        <script>
          const token = {token!r};
          const name = {name!r};
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
              card('Count', data.count ?? 0, 'primary') +
              card('Last Seen', fmtDate(data.last_seen), 'secondary') +
              card('Received', stats.received ?? 0, 'info') +
              card('Inserted', stats.inserted ?? 0, 'success') +
              card('Filtered', stats.filtered ?? 0, 'warning') +
              card('Rate Limited', stats.rate_limited ?? 0, 'warning') +
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
        </script>
      </body>
    </html>
    """
    resp = web.Response(
        text=html,
        content_type="text/html",
    )
    if request.cookies.get("admin_token") != token:
        resp.set_cookie("admin_token", token, httponly=True)
    return resp

async def topics_list(request):
    _require_admin(request)
    log("INFO", "admin topics_list")
    rows = await list_topics()
    items = []
    for r in rows:
        name = r["name"]
        stats = topic_stats.get(name, {})
        rates = topic_rates.get(name, [])
        rate_60s = 0
        if rates:
            now = int(time.time())
            rate_60s = sum(
                1 for ts in rates if now - ts <= 60
            )
        items.append({
            "name": name,
            "enabled": bool(r["enabled"]),
            "count": r["count"],
            "last_seen": worker_last_seen.get(name),
            "running": name in workers,
            "rate_60s": rate_60s,
            "rate_spark": list(rates)[-20:],
            "stats": stats,
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
    rates = list(topic_rates.get(name, []))[-60:]
    return web.json_response(
        {
            "name": name,
            "enabled": bool(r["enabled"]),
            "count": r["count"],
            "last_seen": worker_last_seen.get(name),
            "running": name in workers,
            "stats": topic_stats.get(name, {}),
            "recent": recent,
            "rates": rates,
        }
    )

async def topic_toggle(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "admin topic_toggle", topic=name)
    if TOPIC_ALLOWLIST and name not in TOPIC_ALLOWLIST:
        raise web.HTTPForbidden()
    if TOPIC_DENYLIST and name in TOPIC_DENYLIST:
        raise web.HTTPForbidden()
    rows = await list_topics()
    known = {r["name"]: r for r in rows}
    if name not in known:
        await add_topic(name)
        enabled = True
    else:
        enabled = not bool(known[name]["enabled"])

    await set_topic_enabled(name, enabled)

    if enabled and name not in workers:
        task = asyncio.create_task(
            ntfy_worker(name)
        )
        workers[name] = task
    if not enabled and name in workers:
        workers[name].cancel()
        workers.pop(name, None)

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
        if name in workers:
            workers[name].cancel()
            workers.pop(name, None)
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
        await set_topic_enabled(name, True)
        if name not in workers:
            task = asyncio.create_task(
                ntfy_worker(name)
            )
            workers[name] = task
    return web.json_response({"resumed": True})

async def clear_topic(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "admin clear_topic", topic=name)
    topic_stats.pop(name, None)
    recent_events.pop(name, None)
    topic_rates.pop(name, None)
    return web.json_response({"cleared": name})

async def clear_all(request):
    _require_admin(request)
    log("INFO", "admin clear_all")
    topic_stats.clear()
    recent_events.clear()
    topic_rates.clear()
    return web.json_response({"cleared": True})

async def admin_stats_page(request):
    token = _require_admin(request)
    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Forwarder Stats</title>
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        >
        <style>
          body {{ padding: 16px; }}
        </style>
      </head>
      <body>
        <a class="btn btn-outline-secondary btn-sm" href="/admin?token={token}">Back</a>
        <h2>Global Stats</h2>
        <div id="stats" class="row g-2"></div>
        <script>
          const token = {token!r};
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
              card('Queue', data.queue, 'info') +
              card('Topics', data.topics, 'secondary') +
              card('Active', data.active, 'success') +
              card('Rate (60s)', data.rate_60s, 'warning');
          }}
          fetchStats();
          setInterval(fetchStats, 5000);
        </script>
      </body>
    </html>
    """
    resp = web.Response(text=html, content_type="text/html")
    if request.cookies.get("admin_token") != token:
        resp.set_cookie("admin_token", token, httponly=True)
    return resp

async def stats_api(request):
    _require_admin(request)
    log("INFO", "admin stats_api")
    now = int(time.time())
    all_rates = []
    for rates in topic_rates.values():
        all_rates.extend(rates)
    rate_60s = sum(1 for ts in all_rates if now - ts <= 60)
    return web.json_response(
        {
            "workers": len(workers),
            "queue": telegram_queue.qsize(),
            "topics": len(topic_stats),
            "active": sum(1 for t in topic_stats.values() if t.get("received", 0) > 0),
            "rate_60s": rate_60s,
        }
    )

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
        "/api/topics/clear_all",
        clear_all,
    )
    app.router.add_get(
        "/api/stats",
        stats_api,
    )

    return app
