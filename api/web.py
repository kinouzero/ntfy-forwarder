import asyncio
import base64
import csv
import hashlib
import hmac
import html
import io
import json
import secrets
import time
from collections import deque
from urllib.parse import urlencode

import jwt
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
from core.config import ACCESS_TOKEN
from core.config import ADMIN_RECENT_EVENTS
from core.config import ACCESS_ALLOW_QUERY_TOKEN
from core.config import OIDC_ENABLED
from core.config import OIDC_ISSUER_URL
from core.config import OIDC_CLIENT_ID
from core.config import OIDC_CLIENT_SECRET
from core.config import OIDC_REDIRECT_URI
from core.config import OIDC_SCOPES
from core.config import ACCESS_SESSION_SECRET
from core.config import OIDC_SESSION_TTL_SECONDS
from core.config import OIDC_STATE_TTL_SECONDS
from core.config import OIDC_CLOCK_SKEW_SECONDS
from core.config import OIDC_ALLOWED_EMAILS
from core.config import OIDC_ALLOWED_DOMAINS
from core.config import OIDC_VERIFY_TLS
from core.config import OIDC_REQUIRE_VERIFIED_EMAIL
from core.config import ACCESS_LOCAL_ENABLED
from core.config import ACCESS_LOCAL_USERNAME
from core.config import ACCESS_LOCAL_PASSWORD
from core.config import ACCESS_LOCAL_SESSION_TTL_SECONDS
from core.config import OIDC_LOGIN_TEXT
from core.config import OIDC_LOGIN_ICON
from core.logging import log
from core.http import get_http_session
from db.topics import (
    list_topics,
    set_topic_enabled,
    add_topic,
    reset_topic_count_base,
    reset_all_topic_count_bases,
    clear_topic_status_counts,
    clear_all_topic_status_counts,
    hard_reset_all_topic_counts,
    list_topic_status_counts,
)
from db.messages import count_messages_by_topic_since, clear_all_messages
from db.telegram_queue import count_telegram_queue
from db.errors import query_errors, count_errors_since, clear_errors, log_error
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
from db.targets import (
    list_delivery_targets,
    get_delivery_target,
    create_delivery_target,
    update_delivery_target,
    delete_delivery_target,
    set_default_delivery_target,
    set_topic_delivery_target,
    list_topic_delivery_target_ids,
)
from db.settings import (
    list_settings_with_meta,
    update_settings,
)

_HEALTH_TARGET_CACHE = {}
_OIDC_DISCOVERY_CACHE = None
_OIDC_JWKS_CACHE = None
_AUTH_RATE_LIMIT = {}
OIDC_STATE_COOKIE = "oidc_state"
OIDC_SESSION_COOKIE = "access_session"


def _oidc_ready():
    return bool(
        OIDC_ENABLED
        and OIDC_ISSUER_URL
        and OIDC_CLIENT_ID
        and OIDC_CLIENT_SECRET
        and OIDC_REDIRECT_URI
        and ACCESS_SESSION_SECRET
    )


def _local_login_ready():
    return bool(
        ACCESS_LOCAL_ENABLED
        and ACCESS_LOCAL_USERNAME
        and ACCESS_LOCAL_PASSWORD
        and ACCESS_SESSION_SECRET
    )

def _get_access_token(request):
    header_token = request.headers.get("X-Access-Token", "")
    if header_token:
        return header_token
    cookie_token = request.cookies.get("access_token", "")
    if cookie_token:
        return cookie_token
    if ACCESS_ALLOW_QUERY_TOKEN:
        return request.query.get("token", "")
    return ""


def _token_matches(expected, provided):
    if not expected or not provided:
        return False
    return hmac.compare_digest(str(expected), str(provided))


def _request_is_secure(request):
    if request.scheme == "https":
        return True
    forwarded = request.headers.get("X-Forwarded-Proto", "")
    return forwarded.lower() == "https"


def _cookie_kwargs(request, max_age):
    return {
        "httponly": True,
        "secure": _request_is_secure(request),
        "samesite": "Lax",
        "path": "/",
        "max_age": int(max_age),
    }


def _client_ip(request):
    xff = request.headers.get("X-Forwarded-For", "").strip()
    if xff:
        return xff.split(",", 1)[0].strip()
    return (request.remote or "").strip() or "unknown"


def _enforce_auth_rate_limit(request, action, limit=30, window_seconds=60):
    now = time.time()
    key = f"{action}:{_client_ip(request)}"
    bucket = _AUTH_RATE_LIMIT.get(key, [])
    bucket = [v for v in bucket if (now - v) <= window_seconds]
    if len(bucket) >= limit:
        raise web.HTTPTooManyRequests(text="too many auth requests")
    bucket.append(now)
    _AUTH_RATE_LIMIT[key] = bucket


def _b64_encode(raw):
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64_decode(text):
    pad = "=" * ((4 - len(text) % 4) % 4)
    return base64.urlsafe_b64decode((text + pad).encode())


def _sign_payload(data):
    raw = json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
    sig = hmac.new(
        ACCESS_SESSION_SECRET.encode(),
        raw,
        hashlib.sha256,
    ).digest()
    return f"{_b64_encode(raw)}.{_b64_encode(sig)}"


def _unsign_payload(value):
    if "." not in value:
        return None
    encoded, encoded_sig = value.split(".", 1)
    try:
        raw = _b64_decode(encoded)
        sig = _b64_decode(encoded_sig)
    except Exception:
        return None
    expected = hmac.new(
        ACCESS_SESSION_SECRET.encode(),
        raw,
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        return json.loads(raw.decode())
    except Exception:
        return None


def _make_signed_cookie_value(data, ttl_seconds):
    payload = dict(data)
    payload["exp"] = int(time.time()) + int(ttl_seconds)
    return _sign_payload(payload)


def _read_signed_cookie_value(raw):
    if not raw or not ACCESS_SESSION_SECRET:
        return None
    payload = _unsign_payload(raw)
    if not isinstance(payload, dict):
        return None
    if int(payload.get("exp", 0)) <= int(time.time()):
        return None
    return payload


def _is_admin_html_request(request):
    return request.path in {"/", "/stats", "/errors", "/queue"} or request.path.startswith(
        "/topic/"
    )


def _build_login_redirect_target(request):
    next_path = request.rel_url.path_qs
    return "/auth/login?" + urlencode({"next": next_path})


def _build_local_login_redirect_target(request):
    next_path = request.rel_url.path_qs
    return "/login?" + urlencode({"next": next_path})


def _pkce_challenge(verifier):
    digest = hashlib.sha256(verifier.encode()).digest()
    return _b64_encode(digest)


def _sanitize_next_path(next_path):
    value = str(next_path or "/").strip()
    if not value.startswith("/"):
        return "/"
    if value.startswith("//"):
        return "/"
    return value


def _clean_optional_str(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _auth_error_page_html(title, message, details):
    details_html = "".join(f"<li>{d}</li>" for d in details)
    return f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{title}</title>
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
    <style>
      :root {{
        color-scheme: dark;
        --bg: #090f18;
        --card: #121c29;
        --line: #273447;
        --ink: #e8edf5;
        --muted: #a1afc1;
      }}
      body {{
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        padding: 20px;
        font-family: "Manrope", sans-serif;
        color: var(--ink);
        background: var(--bg);
      }}
      .auth-card {{
        width: min(760px, 100%);
        border: 1px solid var(--line);
        border-radius: 18px;
        background: var(--card);
        padding: 24px;
      }}
      .muted {{ color: var(--muted); }}
    </style>
  </head>
  <body>
    <div class="auth-card shadow">
      <h1 class="h3 mb-2">{title}</h1>
      <p class="muted mb-3">{message}</p>
      <ul class="mb-3">{details_html}</ul>
      <form method="get" action="/" class="d-flex gap-2 flex-wrap">
        <input
          name="token"
          class="form-control"
          style="max-width: 360px"
          placeholder="Paste ACCESS_TOKEN"
          autocomplete="off"
        />
        <button class="btn btn-outline-light" type="submit">Open Site</button>
      </form>
    </div>
  </body>
</html>"""


def _raise_admin_html_unauthorized():
    details = []
    if ACCESS_TOKEN:
        details.append("Provide a valid access token in URL, header, or cookie.")
    if _oidc_ready():
        details.append("OIDC is enabled: use browser login via /auth/login.")
    else:
        details.append("OIDC is not configured on this instance.")
    raise web.HTTPUnauthorized(
        text=_auth_error_page_html(
            "Authentication Required",
            "You cannot access this page without valid authentication.",
            details,
        ),
        content_type="text/html",
    )


def _raise_admin_html_unavailable():
    raise web.HTTPServiceUnavailable(
        text=_auth_error_page_html(
            "Access Disabled",
            "No authentication method is configured on this instance.",
            [
                "Set ACCESS_TOKEN to enable token-based access.",
                "Or configure ACCESS_LOCAL_* for classic login form.",
                "Or configure OIDC_* variables for SSO login.",
            ],
        ),
        content_type="text/html",
    )


def _login_page_html(next_path, error=""):
    error_html = ""
    if error:
        error_html = (
            '<div class="alert alert-danger py-2" role="alert">'
            + error
            + "</div>"
        )
    oidc_btn = ""
    if _oidc_ready():
        btn_text = html.escape(OIDC_LOGIN_TEXT or "Login with SSO")
        icon_raw = str(OIDC_LOGIN_ICON or "bi-shield-lock").strip()
        if icon_raw.startswith(("http://", "https://")):
            icon_html = (
                f'<img src="{html.escape(icon_raw)}" alt="" class="oidc-icon-img" />'
            )
        else:
            icon_class = html.escape(icon_raw or "bi-shield-lock")
            icon_html = f'<i class="bi {icon_class}"></i>'
        oidc_btn = (
            f'<a class="btn btn-outline-secondary w-100 d-flex align-items-center '
            f'justify-content-center gap-2 oidc-btn" href="/auth/login?next={next_path}">'
            f"{icon_html} {btn_text}"
            "</a>"
        )
    oidc_block = ""
    if oidc_btn:
        oidc_block = (
            '<div class="oidc-sep">or continue with SSO</div>'
            '<div class="d-grid mt-1">'
            + oidc_btn
            + "</div>"
        )
    return f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Login</title>
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
      body {{
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        background: #090f18;
        color: #e8edf5;
        font-family: "Manrope", sans-serif;
        padding: 20px;
      }}
      .cardx {{
        width: min(420px, 100%);
        border: 1px solid #273447;
        border-radius: 18px;
        padding: 20px;
        background: #121c29;
      }}
      .oidc-sep {{
        text-align: center;
        color: #8ea0b6;
        margin: 12px 0 10px 0;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: .08em;
      }}
      .oidc-btn {{
        min-height: 42px;
      }}
      .oidc-icon-img {{
        width: 18px;
        height: 18px;
        object-fit: contain;
        border-radius: 3px;
      }}
    </style>
  </head>
  <body>
    <div class="cardx shadow">
      <h1 class="h4 mb-3">Sign in</h1>
      {error_html}
      <form method="post" action="/login" class="d-grid gap-2">
        <input type="hidden" name="next" value="{next_path}" />
        <input class="form-control" name="username" placeholder="Username" required />
        <input class="form-control" name="password" type="password"
          placeholder="Password" required />
        <button class="btn btn-primary" type="submit">Login</button>
      </form>
      {oidc_block}
    </div>
  </body>
</html>"""


async def local_login_page(request):
    if not _local_login_ready():
        if _oidc_ready():
            raise web.HTTPFound(_build_login_redirect_target(request))
        _raise_admin_html_unavailable()
    next_path = _sanitize_next_path(request.query.get("next", "/"))
    return web.Response(
        text=_login_page_html(next_path=next_path, error=""),
        content_type="text/html",
    )


async def local_login_submit(request):
    if not _local_login_ready():
        if _oidc_ready():
            raise web.HTTPFound(_build_login_redirect_target(request))
        _raise_admin_html_unavailable()
    _enforce_auth_rate_limit(request, "local_login")
    form = await request.post()
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))
    next_path = _sanitize_next_path(form.get("next", "/"))
    if not (
        _token_matches(ACCESS_LOCAL_USERNAME, username)
        and _token_matches(ACCESS_LOCAL_PASSWORD, password)
    ):
        return web.Response(
            text=_login_page_html(
                next_path=next_path,
                error="Invalid username or password.",
            ),
            status=401,
            content_type="text/html",
        )
    session_cookie = _make_signed_cookie_value(
        {
            "sub": f"local:{username}",
            "name": username,
            "auth": "local",
        },
        ttl_seconds=ACCESS_LOCAL_SESSION_TTL_SECONDS,
    )
    resp = web.HTTPFound(next_path)
    resp.set_cookie(
        OIDC_SESSION_COOKIE,
        session_cookie,
        **_cookie_kwargs(request, ACCESS_LOCAL_SESSION_TTL_SECONDS),
    )
    raise resp


def _is_session_valid(request):
    if not ACCESS_SESSION_SECRET:
        return False
    session = _read_signed_cookie_value(
        request.cookies.get(OIDC_SESSION_COOKIE, "")
    )
    return bool(session and session.get("sub"))


def _require_oidc_admin(request):
    if not _oidc_ready():
        raise web.HTTPServiceUnavailable(text="access disabled")
    if _is_session_valid(request):
        return ""
    if _is_admin_html_request(request):
        raise web.HTTPFound(_build_login_redirect_target(request))
    raise web.HTTPUnauthorized(text="oidc login required")


def _require_admin(request):
    token = _get_access_token(request)
    if ACCESS_TOKEN and _token_matches(ACCESS_TOKEN, token):
        return token
    if _is_session_valid(request):
        return ""
    if not ACCESS_TOKEN and not _oidc_ready():
        if _is_admin_html_request(request):
            if _local_login_ready():
                raise web.HTTPFound(_build_local_login_redirect_target(request))
            _raise_admin_html_unavailable()
        raise web.HTTPServiceUnavailable(text="access disabled")
    if _is_admin_html_request(request):
        if _local_login_ready():
            raise web.HTTPFound(_build_local_login_redirect_target(request))
        if _oidc_ready():
            raise web.HTTPFound(_build_login_redirect_target(request))
    if _is_admin_html_request(request):
        _raise_admin_html_unauthorized()
    raise web.HTTPUnauthorized()


def _admin_html_response(request, token, html):
    resp = web.Response(
        text=html,
        content_type="text/html",
    )
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    if token and request.cookies.get("access_token") != token:
        resp.set_cookie("access_token", token, **_cookie_kwargs(request, 24 * 3600))
    return resp


async def _fetch_oidc_discovery():
    global _OIDC_DISCOVERY_CACHE
    if _OIDC_DISCOVERY_CACHE:
        return _OIDC_DISCOVERY_CACHE
    if not OIDC_ISSUER_URL:
        raise RuntimeError("OIDC_ISSUER_URL is not set")
    session = get_http_session()
    if session is None:
        raise RuntimeError("HTTP session not ready")
    url = OIDC_ISSUER_URL + "/.well-known/openid-configuration"
    async with session.get(url, ssl=OIDC_VERIFY_TLS, timeout=20) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(
                f"OIDC discovery failed: {resp.status} {text[:200]}"
            )
        _OIDC_DISCOVERY_CACHE = await resp.json()
    issuer = str(_OIDC_DISCOVERY_CACHE.get("issuer", "")).rstrip("/")
    configured = str(OIDC_ISSUER_URL).rstrip("/")
    if issuer not in {"", configured}:
        raise RuntimeError("OIDC issuer mismatch")
    for key in ("authorization_endpoint", "token_endpoint"):
        if not _OIDC_DISCOVERY_CACHE.get(key):
            raise RuntimeError(f"OIDC discovery missing {key}")
    return _OIDC_DISCOVERY_CACHE


async def _fetch_oidc_jwks(discovery):
    global _OIDC_JWKS_CACHE
    if _OIDC_JWKS_CACHE:
        return _OIDC_JWKS_CACHE
    jwks_uri = discovery.get("jwks_uri", "")
    if not jwks_uri:
        raise RuntimeError("OIDC discovery missing jwks_uri")
    session = get_http_session()
    if session is None:
        raise RuntimeError("HTTP session not ready")
    async with session.get(jwks_uri, ssl=OIDC_VERIFY_TLS, timeout=20) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(
                f"OIDC JWKS fetch failed: {resp.status} {text[:200]}"
            )
        _OIDC_JWKS_CACHE = await resp.json()
    return _OIDC_JWKS_CACHE


def _pick_jwk_key(jwks, token_header):
    if not isinstance(jwks, dict):
        return None
    keys = jwks.get("keys", [])
    if not isinstance(keys, list):
        return None
    kid = token_header.get("kid")
    alg = token_header.get("alg")
    for key in keys:
        if kid and key.get("kid") != kid:
            continue
        if alg and key.get("alg") and key.get("alg") != alg:
            continue
        return key
    return None


async def _validate_oidc_id_token(id_token, discovery, expected_nonce):
    try:
        token_header = jwt.get_unverified_header(id_token)
    except Exception as exc:
        raise web.HTTPUnauthorized(text=f"invalid id_token header: {exc}")
    alg = str(token_header.get("alg", "")).upper()
    if alg in {"NONE", "HS256", "HS384", "HS512"}:
        raise web.HTTPUnauthorized(text="unsupported id_token algorithm")
    jwks = await _fetch_oidc_jwks(discovery)
    jwk_key = _pick_jwk_key(jwks, token_header)
    if not jwk_key:
        raise web.HTTPUnauthorized(text="unable to resolve id_token signing key")
    try:
        verify_key = jwt.PyJWK.from_dict(jwk_key).key
        claims = jwt.decode(
            id_token,
            key=verify_key,
            algorithms=[alg],
            audience=OIDC_CLIENT_ID,
            leeway=OIDC_CLOCK_SKEW_SECONDS,
            options={
                "require": ["iss", "sub", "aud", "exp", "iat"],
            },
        )
    except Exception as exc:
        raise web.HTTPUnauthorized(text=f"invalid id_token: {exc}")
    if str(claims.get("iss", "")).rstrip("/") != str(OIDC_ISSUER_URL).rstrip("/"):
        raise web.HTTPUnauthorized(text="invalid id_token issuer")
    if expected_nonce and claims.get("nonce") != expected_nonce:
        raise web.HTTPUnauthorized(text="invalid id_token nonce")
    return claims


async def _fetch_oidc_userinfo(access_token, discovery):
    session = get_http_session()
    if session is None:
        raise RuntimeError("HTTP session not ready")
    userinfo_endpoint = discovery.get("userinfo_endpoint")
    if not userinfo_endpoint:
        return {}
    headers = {"Authorization": f"Bearer {access_token}"}
    async with session.get(
        userinfo_endpoint,
        headers=headers,
        ssl=OIDC_VERIFY_TLS,
        timeout=20,
    ) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(
                f"OIDC userinfo failed: {resp.status} {text[:200]}"
            )
        return await resp.json()


def _oidc_user_allowed(profile):
    email = str(profile.get("email", "")).strip().lower()
    if OIDC_REQUIRE_VERIFIED_EMAIL and not bool(profile.get("email_verified")):
        return False
    if OIDC_ALLOWED_EMAILS and email not in OIDC_ALLOWED_EMAILS:
        return False
    if OIDC_ALLOWED_DOMAINS:
        if "@" not in email:
            return False
        domain = email.split("@", 1)[1]
        if domain not in OIDC_ALLOWED_DOMAINS:
            return False
    return True


async def oidc_login(request):
    if not _oidc_ready():
        raise web.HTTPServiceUnavailable(text="oidc auth is not configured")
    _enforce_auth_rate_limit(request, "oidc_login")
    discovery = await _fetch_oidc_discovery()
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = _pkce_challenge(code_verifier)
    next_path = _sanitize_next_path(request.query.get("next", "/"))
    state_cookie = _make_signed_cookie_value(
        {
            "state": state,
            "nonce": nonce,
            "next": next_path,
            "code_verifier": code_verifier,
        },
        ttl_seconds=OIDC_STATE_TTL_SECONDS,
    )
    query = urlencode(
        {
            "response_type": "code",
            "client_id": OIDC_CLIENT_ID,
            "redirect_uri": OIDC_REDIRECT_URI,
            "scope": OIDC_SCOPES,
            "state": state,
            "nonce": nonce,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
    )
    location = discovery["authorization_endpoint"] + "?" + query
    resp = web.HTTPFound(location)
    resp.set_cookie(
        OIDC_STATE_COOKIE,
        state_cookie,
        **_cookie_kwargs(request, OIDC_STATE_TTL_SECONDS),
    )
    raise resp


async def oidc_callback(request):
    if not _oidc_ready():
        raise web.HTTPServiceUnavailable(text="oidc auth is not configured")
    _enforce_auth_rate_limit(request, "oidc_callback")
    state_cookie = _read_signed_cookie_value(
        request.cookies.get(OIDC_STATE_COOKIE, "")
    )
    state = request.query.get("state", "")
    if not state_cookie or state_cookie.get("state") != state:
        raise web.HTTPUnauthorized(text="invalid oidc state")
    code_verifier = str(state_cookie.get("code_verifier", "")).strip()
    if not code_verifier:
        raise web.HTTPUnauthorized(text="missing oidc code verifier")
    expected_nonce = str(state_cookie.get("nonce", "")).strip()
    if not expected_nonce:
        raise web.HTTPUnauthorized(text="missing oidc nonce")
    code = request.query.get("code", "")
    if not code:
        raise web.HTTPUnauthorized(text="missing oidc code")
    discovery = await _fetch_oidc_discovery()
    token_endpoint = discovery.get("token_endpoint")
    if not token_endpoint:
        raise web.HTTPServiceUnavailable(text="oidc token endpoint missing")
    session = get_http_session()
    if session is None:
        raise web.HTTPServiceUnavailable(text="HTTP session not ready")
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": OIDC_REDIRECT_URI,
        "client_id": OIDC_CLIENT_ID,
        "client_secret": OIDC_CLIENT_SECRET,
        "code_verifier": code_verifier,
    }
    async with session.post(
        token_endpoint,
        data=data,
        ssl=OIDC_VERIFY_TLS,
        timeout=20,
    ) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise web.HTTPUnauthorized(
                text=f"oidc token exchange failed: {resp.status} {text[:200]}"
            )
        token_response = await resp.json()
    access_token = token_response.get("access_token")
    if not access_token:
        raise web.HTTPUnauthorized(text="missing access token")
    id_token = token_response.get("id_token")
    if not id_token:
        raise web.HTTPUnauthorized(text="missing id_token")
    claims = await _validate_oidc_id_token(
        id_token=id_token,
        discovery=discovery,
        expected_nonce=expected_nonce,
    )
    try:
        profile = await _fetch_oidc_userinfo(access_token, discovery)
    except Exception:
        profile = {}
    merged_profile = dict(claims)
    merged_profile.update(
        {k: v for k, v in profile.items() if k not in {"sub"}}
    )
    sub = str(merged_profile.get("sub", "")).strip()
    if not sub:
        raise web.HTTPUnauthorized(text="missing oidc subject")
    if not _oidc_user_allowed(merged_profile):
        raise web.HTTPForbidden(text="oidc user not allowed")
    session_cookie = _make_signed_cookie_value(
        {
            "sub": sub,
            "email": str(merged_profile.get("email", "")).strip().lower(),
            "name": str(merged_profile.get("name", "")).strip(),
        },
        ttl_seconds=OIDC_SESSION_TTL_SECONDS,
    )
    next_path = _sanitize_next_path(state_cookie.get("next", "/"))
    resp = web.HTTPFound(next_path)
    resp.del_cookie(OIDC_STATE_COOKIE)
    resp.set_cookie(
        OIDC_SESSION_COOKIE,
        session_cookie,
        **_cookie_kwargs(request, OIDC_SESSION_TTL_SECONDS),
    )
    raise resp


async def oidc_logout(request):
    resp = web.HTTPFound("/login")
    secure = _request_is_secure(request)
    resp.del_cookie(OIDC_SESSION_COOKIE, path="/", samesite="Lax", secure=secure)
    resp.del_cookie(OIDC_STATE_COOKIE, path="/", samesite="Lax", secure=secure)
    resp.del_cookie("access_token", path="/", samesite="Lax", secure=secure)
    raise resp


@web.middleware
async def security_headers_middleware(request, handler):
    try:
        response = await handler(request)
    except web.HTTPException as exc:
        response = exc
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=()",
    )
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
    response.headers.setdefault(
        "Content-Security-Policy",
        (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net "
            "https://fonts.googleapis.com; "
            "font-src 'self' https://fonts.gstatic.com https://cdn.jsdelivr.net data:; "
            "img-src 'self' data: https:; "
            "connect-src 'self'; "
            "frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
        ),
    )
    return response


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
    log("INFO", "topic worker started from web", topic=name)
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

    async def check_target(target):
        cache_key = f"{target['id']}:{target['updated_at']}:{int(target['enabled'])}"
        cache = _HEALTH_TARGET_CACHE.setdefault(
            cache_key,
            {"ts": 0, "ok": None, "latency_ms": None, "error": None},
        )
        if now - int(cache["ts"]) < 30:
            return dict(cache)
        start = time.monotonic()
        ok = False
        err = None
        try:
            kind = str(target.get("kind") or "")
            cfg = target.get("config") or {}
            if kind == "telegram":
                bot_token = str(cfg.get("bot_token") or "").strip()
                if not bot_token:
                    raise RuntimeError("missing telegram bot_token")
                await tg_call("getMe", {}, token=bot_token)
                ok = True
            else:
                session = get_http_session()
                if session is None:
                    raise RuntimeError("HTTP session not ready")
                url = str(cfg.get("url") or "").strip()
                if not url:
                    raise RuntimeError("missing target url")
                headers = None
                auth_header = str(cfg.get("auth_header") or "").strip()
                if auth_header:
                    headers = {"Authorization": auth_header}
                async with session.options(url, headers=headers, timeout=10) as resp:
                    ok = int(resp.status) < 500
                    if not ok:
                        err = f"http_{resp.status}"
        except Exception as exc:
            ok = False
            err = str(exc)
        cache.update(
            {
                "ts": now,
                "ok": ok,
                "latency_ms": int((time.monotonic() - start) * 1000),
                "error": err,
            }
        )
        return dict(cache)

    target_checks = {}
    try:
        targets = await list_delivery_targets()
    except Exception:
        targets = []
    for target in targets:
        chk = await check_target(target)
        target_checks[target["name"]] = {
            "enabled": bool(target.get("enabled")),
            "kind": target.get("kind"),
            "is_default": bool(target.get("is_default")),
            "ok": chk["ok"],
            "latency_ms": chk["latency_ms"],
            "error": chk["error"],
        }

    status = "ok"
    any_target_failed = any(
        c["enabled"] and c["ok"] is False
        for c in target_checks.values()
    )
    if not db_ok or stale_workers > 0 or any_target_failed:
        status = "degraded"
    return web.json_response({
        "status": status,
        "workers": _active_worker_count(),
        "queue": queue_count,
        "dead_letters": dead_count,
        "checks": {
            "db_writable": {"ok": db_ok, "error": db_error},
            "targets": target_checks,
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
    logout_btn = "" if token else (
        "<a class=\"btn btn-outline-secondary btn-sm\" href=\"/logout\" "
        "title=\"Logout\" aria-label=\"Logout\">"
        "<i class=\"bi bi-box-arrow-right\"></i>"
        "</a>"
    )
    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Forwarder Topics</title>
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
          .topics-body {{ padding: 14px; }}
          .topics-controls {{ margin-bottom: .75rem; }}
          .table-responsive {{
            border: 1px solid var(--line);
            border-radius: 12px;
            overflow: hidden;
          }}
          .ui-table {{
            --bs-table-bg: transparent;
            --bs-table-striped-bg: color-mix(in srgb, var(--card-soft) 88%, transparent);
            --bs-table-color: var(--ink);
            color: var(--ink);
          }}
          .ui-table > :not(caption) > * > * {{
            border-bottom-color: var(--line);
          }}
          .ui-table thead th {{
            border-bottom: 1px solid var(--line);
            color: var(--muted);
            font-size: .8rem;
            text-transform: uppercase;
            letter-spacing: .03em;
            font-weight: 700;
            background-color: transparent!important;
          }}
          .ui-table td {{
            background-color: transparent!important;
            color: var(--ink);
          }}
          .ui-table tbody tr {{ transition: background .2s ease; }}
          .ui-table tbody tr:hover > * {{ background: var(--hover-row)!important; }}
          .topics-table {{
            --bs-table-striped-bg: transparent;
          }}
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
            .topics-body {{ padding: 10px; }}
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
              <div class="d-flex gap-2">
                {logout_btn}
                <button
                  id="theme-toggle"
                  class="btn btn-outline-theme btn-sm"
                  aria-label="Toggle theme"
                ></button>
                <button id="clear-all" class="btn btn-outline-danger btn-sm"
                  title="Clear stats" aria-label="Clear stats">
                  <i class="bi bi-trash"></i>
                </button>
                <button id="hard-clear-all" class="btn btn-outline-danger btn-sm"
                  title="Hard clear (including totals)" aria-label="Hard clear all stats">
                  <i class="bi bi-radioactive"></i>
                </button>
                <button id="pause-all" class="btn btn-outline-warning btn-sm"
                  title="Pause all" aria-label="Pause all">
                  <i class="bi bi-pause-fill"></i>
                </button>
                <button id="resume-all" class="btn btn-outline-success btn-sm"
                  title="Resume all" aria-label="Resume all">
                  <i class="bi bi-play-fill"></i>
                </button>
                <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                  aria-label="Toggle navigation">
                  <i class="bi bi-list"></i>
                </button>
              </div>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm active" href="/?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/targets?token={token}">
                <i class="bi bi-bullseye"></i> Targets
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/settings?token={token}">
                <i class="bi bi-sliders"></i> Settings
              </a>
            </div>
          </div>
          <div class="topics-body">
            <div class="card p-3 mb-3">
              <h3 class="h5 mb-0">Topics</h3>
            </div>
            <div class="d-flex flex-wrap gap-2 align-items-center topics-controls">
              <input id="filter" class="editable-field form-control form-control-sm"
                style="max-width: 220px;" placeholder="Filter topics" />
              <select id="sort" class="editable-field form-select form-select-sm"
                style="max-width: 180px;">
                <option value="name">Sort: Name</option>
                <option value="count">Sort: Count</option>
              </select>
              <button id="sort-dir" class="btn btn-outline-secondary btn-sm">Desc</button>
            </div>
            <div class="table-responsive">
              <table
                id="topics"
                class="topics-table ui-table table table-sm table-striped align-middle mb-0"
              >
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Status</th>
                    <th>Target</th>
                    <th>Count</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody></tbody>
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
            const targets = data.targets || [];
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
            if (!items.length) {{
              tbody.innerHTML = (
                '<tr><td colspan="5" class="text-center text-muted py-4">' +
                'No topics found' +
                '</td></tr>'
              );
              return;
            }}
            items.forEach(t => {{
              const tr = document.createElement('tr');
              const statusBadge = t.enabled ? 'success' : 'secondary';
              tr.innerHTML = `
                <td data-label="Topic">
                  <a class="topic-link" href="/topic/${{encodeURIComponent(t.name)}}?token=" +
                    encodeURIComponent(token)>
                    ${{t.name}}
                  </a>
                </td>
                <td data-label="Status">
                  <span class="badge text-bg-${{statusBadge}}">
                    ${{t.enabled ? 'enabled' : 'disabled'}}
                  </span>
                </td>
                <td data-label="Target">
                  <select
                    class="editable-field form-select form-select-sm"
                    data-target="${{t.name}}"
                  >
                    <option value="">Default target</option>
                    ${{targets.map(tt => `
                      <option value="${{tt.id}}" ${{tt.id === t.target_id ? 'selected' : ''}}>
                        ${{tt.name}}${{tt.is_default ? ' (default)' : ''}}
                        ${{tt.enabled ? '' : ' [disabled]'}}
                      </option>`).join('')}}
                  </select>
                </td>
                <td data-label="Count" class="count-block">
                  <div class="count-values">
                    <div><span class="badge text-bg-light">${{t.count_total}}</span> total</div>
                    <div class="small text-muted">+${{t.count_24h}} /24h</div>
                    <div class="small text-muted">${{t.count_since_reset}} since reset</div>
                    <div class="small text-muted">
                      ${{t.status_counts?.disabled ?? 0}} since disabled
                    </div>
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
              tr.querySelector('[data-target]').onchange = async (e) => {{
                await fetch(
                  '/api/topics/' + encodeURIComponent(t.name) +
                  '/target?token=' + encodeURIComponent(token),
                  {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ target_id: e.target.value || null }})
                  }}
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
          document.getElementById('hard-clear-all').onclick = async () => {{
            if (
              !confirm('Hard clear all stats and totals? This resets historical counters.')
            ) return;
            const res = await fetch(
              '/api/topics/hard_clear_all?token=' + encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            if (!res.ok) return;
            fetchTopics();
          }};
        </script>
      </body>
    </html>
    """
    return _admin_html_response(request, token, html)


async def admin_targets_page(request):
    token = _require_admin(request)
    logout_btn = "" if token else (
        "<a class=\"btn btn-outline-secondary btn-sm\" href=\"/logout\" "
        "title=\"Logout\" aria-label=\"Logout\">"
        "<i class=\"bi bi-box-arrow-right\"></i>"
        "</a>"
    )
    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Targets</title>
        <link rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
        <link rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
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
            color: var(--ink);
            background:
              radial-gradient(1000px 550px at -8% -15%, var(--glow-a) 0%, transparent 65%),
              radial-gradient(950px 500px at 108% 0%, var(--glow-b) 0%, transparent 60%),
              linear-gradient(180deg, var(--bg-a), var(--bg-b));
          }}
          html[data-theme="dark"] body {{ background: var(--bg-a); }}
          .text-muted {{ color: var(--muted)!important; }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{ border-radius: 999px; }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, #2dd4bf 45%, #fff);
            background: color-mix(in srgb, #2dd4bf 14%, transparent);
            color: var(--ink);
          }}
          .stats-shell {{
            max-width: 1100px;
            margin: 0 auto;
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
          .stats-body {{ padding: 14px; }}
          .card {{
            --bs-card-bg: var(--card-soft);
            background-color: var(--card-soft)!important;
            border-color: var(--line)!important;
            color: var(--ink);
          }}
          .table-responsive {{
            border: 1px solid var(--line);
            border-radius: 12px;
            overflow: hidden;
          }}
          .ui-table {{
            --bs-table-bg: transparent;
            --bs-table-striped-bg: color-mix(in srgb, var(--card-soft) 88%, transparent);
            --bs-table-color: var(--ink);
            color: var(--ink);
          }}
          .ui-table > :not(caption) > * > * {{ border-bottom-color: var(--line); }}
          .ui-table thead th {{
            border-bottom: 1px solid var(--line);
            color: var(--muted);
            font-size: .8rem;
            text-transform: uppercase;
            letter-spacing: .03em;
            font-weight: 700;
            background-color: transparent!important;
          }}
          .ui-table tbody tr:hover > * {{
            background: color-mix(in srgb, var(--card-soft) 78%, transparent)!important;
          }}
          .form-control,
          .form-select {{
            background: color-mix(in srgb, var(--card) 85%, #000);
            color: var(--ink);
            border-color: var(--line);
          }}
          .form-control:focus,
          .form-select:focus {{
            background: color-mix(in srgb, var(--card) 85%, #000);
            color: var(--ink);
            border-color: color-mix(in srgb, #2dd4bf 40%, var(--line));
            box-shadow: 0 0 0 .2rem color-mix(in srgb, #2dd4bf 20%, transparent);
          }}
          .form-control::placeholder {{ color: var(--muted); opacity: 1; }}
          .form-select option {{ background: var(--card); color: var(--ink); }}
          .modal-content {{
            background: var(--card-soft);
            border: 1px solid var(--line);
            color: var(--ink);
            border-radius: 14px;
            box-shadow: 0 20px 50px rgba(8, 17, 30, 0.35);
          }}
          .modal-header {{
            border-bottom-color: var(--line);
            background: color-mix(in srgb, var(--card) 92%, #000);
          }}
          .modal-footer {{
            border-top-color: var(--line);
            background: color-mix(in srgb, var(--card) 95%, #000);
          }}
          .modal-title-wrap {{
            display: flex;
            align-items: center;
            gap: .6rem;
          }}
          .modal-title-icon {{
            width: 30px;
            height: 30px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border-radius: 9px;
            border: 1px solid var(--line);
            color: #2dd4bf;
            background: color-mix(in srgb, #2dd4bf 12%, transparent);
          }}
          .modal-subtitle {{
            color: var(--muted);
            font-size: .86rem;
            margin-top: .1rem;
          }}
          .field-group {{
            border: 1px solid var(--line);
            border-radius: 12px;
            background: color-mix(in srgb, var(--card) 95%, #000);
            padding: .75rem;
          }}
          .field-group-title {{
            font-weight: 700;
            font-size: .86rem;
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: .02em;
            margin-bottom: .55rem;
          }}
          .modal-body .form-label {{
            margin-bottom: .35rem;
            font-size: .84rem;
            color: var(--muted);
          }}
          .modal-body .row {{
            --bs-gutter-x: .7rem;
            --bs-gutter-y: .7rem;
          }}
          .btn-close {{ filter: none; }}
          html[data-theme="dark"] .btn-close {{ filter: invert(1) grayscale(100%); }}
          .btn-outline-secondary {{
            --bs-btn-color: var(--muted);
            --bs-btn-border-color: var(--line);
            --bs-btn-hover-color: var(--ink);
            --bs-btn-hover-bg: color-mix(in srgb, var(--card) 84%, #000);
            --bs-btn-hover-border-color: var(--line);
          }}
          #nav-toggle {{ display: none; }}
          @media (max-width: 860px) {{
            #nav-toggle {{ display: inline-flex; }}
            .topbar {{ padding: 10px; }}
            .nav-actions {{ width: 100%; justify-content: space-between; }}
            .nav-links {{ width: 100%; display: none; margin-top: 8px; }}
            .nav-links.open {{ display: flex; }}
            .nav-links .btn {{ flex: 1 1 auto; justify-content: center; }}
          }}
        </style>
      </head>
      <body>
        <div class="stats-shell">
          <div class="topbar d-flex align-items-center justify-content-between gap-2 flex-wrap">
            <div class="nav-actions">
              <div class="d-flex gap-2">
                {logout_btn}
                <button id="theme-toggle" class="btn btn-outline-secondary btn-sm"></button>
                <button id="open-create" class="btn btn-outline-primary btn-sm"
                  title="Create target" aria-label="Create target">
                  <i class="bi bi-plus-lg"></i>
                </button>
              </div>
              <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                aria-label="Toggle navigation">
                <i class="bi bi-list"></i>
              </button>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm" href="/?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm active" href="/targets?token={token}">
                <i class="bi bi-bullseye"></i> Targets
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/settings?token={token}">
                <i class="bi bi-sliders"></i> Settings
              </a>
            </div>
          </div>
          <div class="stats-body">

          <div class="card p-3 mb-3">
            <h3 class="h5 mb-0">Configured Targets</h3>
          </div>

          <div class="table-responsive">
            <table class="ui-table table table-sm table-striped align-middle mb-0">
              <thead>
                <tr>
                  <th>Name</th><th>Kind</th><th>Config</th><th>Status</th><th>Default</th><th>Action</th>
                </tr>
              </thead>
              <tbody id="targets-body"></tbody>
            </table>
          </div>

          <div class="modal fade" id="target-modal" tabindex="-1" aria-hidden="true">
            <div class="modal-dialog modal-lg modal-dialog-centered">
              <div class="modal-content">
                <div class="modal-header">
                  <div>
                    <div class="modal-title-wrap">
                      <span class="modal-title-icon"><i class="bi bi-bullseye"></i></span>
                      <h5 class="modal-title mb-0" id="target-modal-title">Create target</h5>
                    </div>
                    <div class="modal-subtitle">Configure one delivery target.</div>
                  </div>
                  <button type="button" class="btn-close"
                    data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                  <div class="field-group mb-2">
                    <div class="field-group-title">General</div>
                    <div class="row">
                      <div class="col-md-4">
                        <label class="form-label" for="t-name">Name</label>
                        <input id="t-name" class="form-control" placeholder="Target name">
                      </div>
                      <div class="col-md-4">
                        <label class="form-label" for="t-kind">Type</label>
                        <select id="t-kind" class="form-select">
                          <option value="telegram">telegram</option>
                          <option value="webhook_generic">webhook_generic</option>
                          <option value="webhook_discord">webhook_discord</option>
                          <option value="webhook_slack">webhook_slack</option>
                        </select>
                      </div>
                      <div class="col-md-4">
                        <label class="form-label" for="t-enabled">Status</label>
                        <select id="t-enabled" class="form-select">
                          <option value="1">enabled</option>
                          <option value="0">disabled</option>
                        </select>
                      </div>
                    </div>
                  </div>
                  <div class="field-group">
                    <div class="field-group-title">Connection</div>
                    <div class="row">
                    <div class="col-md-6" id="field-chat-id">
                      <label class="form-label" for="t-chat-id">Telegram chat_id</label>
                      <input id="t-chat-id" class="form-control" placeholder="Chat ID">
                    </div>
                    <div class="col-md-6" id="field-bot-token">
                      <label class="form-label" for="t-bot-token">Telegram bot_token</label>
                      <input id="t-bot-token" class="form-control" placeholder="Bot token">
                    </div>
                    <div class="col-md-6" id="field-max-len">
                      <label class="form-label" for="t-max-len">Telegram max_message_length</label>
                      <input id="t-max-len" class="form-control" type="number" min="256" max="20000"
                        placeholder="Telegram max_message_length">
                    </div>
                    <div class="col-md-6" id="field-url">
                      <label class="form-label" for="t-url">Webhook URL</label>
                      <input id="t-url" class="form-control" placeholder="Webhook URL">
                    </div>
                    <div class="col-md-6" id="field-auth">
                      <label class="form-label" for="t-auth">Auth header</label>
                      <input id="t-auth" class="form-control" placeholder="Auth header (optional)">
                    </div>
                    </div>
                  </div>
                  <input type="hidden" id="t-id" value="">
                  <div class="small text-secondary mt-2" id="kind-help"></div>
                </div>
                <div class="modal-footer">
                  <button id="t-cancel" class="btn btn-outline-secondary" data-bs-dismiss="modal">
                    Cancel
                  </button>
                  <button id="t-save" class="btn btn-outline-primary">Create</button>
                </div>
              </div>
            </div>
          </div>
          </div>
        </div>
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js">
        </script>
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
            }}
          }};
          let targetModal = null;
          function ensureTargetModal() {{
            if (targetModal) return targetModal;
            const el = document.getElementById('target-modal');
            const bs = window.bootstrap || globalThis.bootstrap;
            if (el && bs && bs.Modal) {{
              targetModal = bs.Modal.getOrCreateInstance(el);
            }}
            return targetModal;
          }}
          function showTargetModal() {{
            const m = ensureTargetModal();
            if (m) m.show();
          }}
          function hideTargetModal() {{
            const m = ensureTargetModal();
            if (m) m.hide();
          }}
          function cfgPreview(kind, cfg) {{
            if (kind === 'telegram') {{
              const tokenState = (cfg.bot_token || '') ? 'set' : 'missing';
              const maxLen = Number(cfg.max_message_length || 4096);
              return (
                'chat_id=' + (cfg.chat_id || '') +
                ' token=' + tokenState +
                ' max=' + maxLen
              );
            }}
            return 'url=' + (cfg.url || '');
          }}
          function resetForm() {{
            document.getElementById('t-id').value = '';
            document.getElementById('t-name').value = '';
            document.getElementById('t-kind').value = 'telegram';
            document.getElementById('t-chat-id').value = '';
            document.getElementById('t-bot-token').value = '';
            document.getElementById('t-max-len').value = '4096';
            document.getElementById('t-url').value = '';
            document.getElementById('t-auth').value = '';
            document.getElementById('t-enabled').value = '1';
            document.getElementById('target-modal-title').innerText = 'Create target';
            document.getElementById('t-save').innerText = 'Create';
            applyKindFields();
          }}
          function applyKindFields() {{
            const kind = document.getElementById('t-kind').value;
            const isTelegram = kind === 'telegram';
            document.getElementById('field-chat-id').style.display = isTelegram ? '' : 'none';
            document.getElementById('field-bot-token').style.display = isTelegram ? '' : 'none';
            document.getElementById('field-max-len').style.display = isTelegram ? '' : 'none';
            document.getElementById('field-url').style.display = isTelegram ? 'none' : '';
            document.getElementById('field-auth').style.display = isTelegram ? 'none' : '';
            const help = document.getElementById('kind-help');
            if (isTelegram) {{
              help.innerText = 'Telegram: fill chat_id, bot_token, and max message length.';
              document.getElementById('t-chat-id').placeholder = 'Telegram chat_id';
              document.getElementById('t-bot-token').placeholder = 'Telegram bot_token';
            }} else if (kind === 'webhook_discord') {{
              help.innerText = 'Discord: fill webhook URL.';
              document.getElementById('t-url').placeholder = 'Discord webhook URL';
            }} else if (kind === 'webhook_slack') {{
              help.innerText = 'Slack: fill webhook URL.';
              document.getElementById('t-url').placeholder = 'Slack webhook URL';
            }} else {{
              help.innerText = 'Generic webhook: fill URL (+ optional auth header).';
              document.getElementById('t-url').placeholder = 'Webhook URL';
            }}
          }}
          function loadIntoForm(t) {{
            document.getElementById('t-id').value = String(t.id);
            document.getElementById('t-name').value = t.name || '';
            document.getElementById('t-kind').value = t.kind || 'telegram';
            document.getElementById('t-chat-id').value = (t.config && t.config.chat_id) || '';
            document.getElementById('t-bot-token').value =
              (t.config && t.config.bot_token) || '';
            document.getElementById('t-max-len').value =
              (t.config && t.config.max_message_length) || 4096;
            document.getElementById('t-url').value = (t.config && t.config.url) || '';
            document.getElementById('t-auth').value = (t.config && t.config.auth_header) || '';
            document.getElementById('t-enabled').value = t.enabled ? '1' : '0';
            document.getElementById('target-modal-title').innerText = 'Edit target';
            document.getElementById('t-save').innerText = 'Update';
            applyKindFields();
            showTargetModal();
          }}
          function formPayload() {{
            const kind = document.getElementById('t-kind').value;
            const cfg = {{}};
            if (kind === 'telegram') {{
              cfg.chat_id = document.getElementById('t-chat-id').value.trim();
              cfg.bot_token = document.getElementById('t-bot-token').value.trim();
              cfg.max_message_length = Number(document.getElementById('t-max-len').value || 4096);
            }} else {{
              cfg.url = document.getElementById('t-url').value.trim();
              cfg.auth_header = document.getElementById('t-auth').value.trim();
            }}
            return {{
              name: document.getElementById('t-name').value.trim(),
              kind,
              config: cfg,
              enabled: document.getElementById('t-enabled').value === '1',
            }};
          }}
          async function fetchTargets() {{
            const res = await fetch('/api/targets?token=' + encodeURIComponent(token));
            if (!res.ok) return;
            const data = await res.json();
            const body = document.getElementById('targets-body');
            body.innerHTML = '';
            const items = data.items || [];
            if (!items.length) {{
              body.innerHTML = (
                '<tr><td colspan="6" class="text-center text-muted py-4">' +
                'No targets configured' +
                '</td></tr>'
              );
              return;
            }}
            items.forEach(t => {{
              const tr = document.createElement('tr');
              tr.innerHTML = `
                <td>${{t.name}}</td>
                <td><code>${{t.kind}}</code></td>
                <td><code>${{cfgPreview(t.kind, t.config || {{}})}}</code></td>
                <td>${{t.enabled ? 'enabled' : 'disabled'}}</td>
                <td>
                  ${{t.is_default ? '<span class="badge text-bg-success">default</span>' : ''}}
                </td>
                <td class="d-flex gap-1">
                  <button class="btn btn-outline-secondary btn-sm" data-edit="${{t.id}}">
                    Edit
                  </button>
                  <button class="btn btn-outline-success btn-sm" data-default="${{t.id}}">
                    Default
                  </button>
                  <button class="btn btn-outline-danger btn-sm" data-del="${{t.id}}">
                    Delete
                  </button>
                </td>`;
              tr.querySelector('[data-edit]').onclick = () => loadIntoForm(t);
              tr.querySelector('[data-default]').onclick = async () => {{
                await fetch(
                  '/api/targets/' + t.id + '/default?token=' + encodeURIComponent(token),
                  {{ method: 'POST' }}
                );
                fetchTargets();
              }};
              tr.querySelector('[data-del]').onclick = async () => {{
                if (!confirm('Delete target ' + t.name + '?')) return;
                await fetch(
                  '/api/targets/' + t.id + '?token=' + encodeURIComponent(token),
                  {{ method: 'DELETE' }}
                );
                fetchTargets();
              }};
              body.appendChild(tr);
            }});
          }}
          document.getElementById('open-create').onclick = () => {{
            resetForm();
            showTargetModal();
          }};
          document.getElementById('t-cancel').onclick = resetForm;
          document.getElementById('t-kind').onchange = applyKindFields;
          document.getElementById('t-save').onclick = async () => {{
            const id = document.getElementById('t-id').value;
            const payload = formPayload();
            const url = id
              ? '/api/targets/' + id + '?token=' + encodeURIComponent(token)
              : '/api/targets?token=' + encodeURIComponent(token);
            const method = id ? 'PUT' : 'POST';
            const res = await fetch(url, {{
              method,
              headers: {{ 'Content-Type': 'application/json' }},
              body: JSON.stringify(payload),
            }});
            if (!res.ok) {{
              alert(await res.text());
              return;
            }}
            hideTargetModal();
            resetForm();
            fetchTargets();
          }};
          applyTheme(getTheme());
          ensureTargetModal();
          document.getElementById('theme-toggle').onclick = () => {{
            const current = document.documentElement.getAttribute('data-theme') || 'light';
            const next = current === 'dark' ? 'light' : 'dark';
            localStorage.setItem('ui_theme', next);
            applyTheme(next);
          }};
          document.getElementById('nav-toggle').onclick = () => {{
            document.querySelector('.nav-links').classList.toggle('open');
          }};
          resetForm();
          fetchTargets();
          setInterval(fetchTargets, 5000);
        </script>
      </body>
    </html>
    """
    return _admin_html_response(request, token, html)


async def admin_settings_page(request):
    token = _require_admin(request)
    logout_btn = "" if token else (
        "<a class=\"btn btn-outline-secondary btn-sm\" href=\"/logout\" "
        "title=\"Logout\" aria-label=\"Logout\">"
        "<i class=\"bi bi-box-arrow-right\"></i>"
        "</a>"
    )
    html = f"""
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Settings</title>
        <link rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
        <link rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
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
            color: var(--ink);
            background:
              radial-gradient(1000px 550px at -8% -15%, var(--glow-a) 0%, transparent 65%),
              radial-gradient(950px 500px at 108% 0%, var(--glow-b) 0%, transparent 60%),
              linear-gradient(180deg, var(--bg-a), var(--bg-b));
          }}
          html[data-theme="dark"] body {{ background: var(--bg-a); }}
          .text-muted {{ color: var(--muted)!important; }}
          .stats-shell {{
            max-width: 1100px;
            margin: 0 auto;
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
          .stats-body {{ padding: 14px; }}
          .nav-actions,
          .nav-links {{
            display: flex;
            gap: .5rem;
            flex-wrap: wrap;
            align-items: center;
          }}
          .nav-links .btn {{ border-radius: 999px; }}
          .nav-links .btn.active {{
            border-color: color-mix(in srgb, #2dd4bf 45%, #fff);
            background: color-mix(in srgb, #2dd4bf 14%, transparent);
            color: var(--ink);
          }}
          .card {{
            --bs-card-bg: var(--card-soft);
            background-color: var(--card-soft)!important;
            border-color: var(--line)!important;
            color: var(--ink);
          }}
          .form-control,
          .form-select {{
            background: color-mix(in srgb, var(--card) 85%, #000);
            color: var(--ink);
            border-color: var(--line);
          }}
          .form-control:focus,
          .form-select:focus {{
            background: color-mix(in srgb, var(--card) 85%, #000);
            color: var(--ink);
            border-color: color-mix(in srgb, #2dd4bf 40%, var(--line));
            box-shadow: 0 0 0 .2rem color-mix(in srgb, #2dd4bf 20%, transparent);
          }}
          .form-control::placeholder {{ color: var(--muted); opacity: 1; }}
          .form-select option {{ background: var(--card); color: var(--ink); }}
          .btn-outline-secondary {{
            --bs-btn-color: var(--muted);
            --bs-btn-border-color: var(--line);
            --bs-btn-hover-color: var(--ink);
            --bs-btn-hover-bg: color-mix(in srgb, var(--card) 84%, #000);
            --bs-btn-hover-border-color: var(--line);
          }}
          .page-head {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            flex-wrap: wrap;
            margin-bottom: .9rem;
          }}
          .page-head p {{ margin: 0; color: var(--muted); }}
          .settings-grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: .9rem;
          }}
          .settings-section {{
            border: 1px solid var(--line);
            border-radius: 12px;
            background: color-mix(in srgb, var(--card-soft) 95%, #000);
            padding: .9rem;
          }}
          .section-title {{
            font-size: 1rem;
            font-weight: 700;
            margin-bottom: .1rem;
          }}
          .section-subtitle {{
            font-size: .85rem;
            color: var(--muted);
            margin-bottom: .7rem;
          }}
          .field-hint {{
            font-size: .77rem;
            color: var(--muted);
            margin-top: .25rem;
          }}
          .action-bar {{
            margin-top: 1rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: .75rem;
            flex-wrap: wrap;
            border-top: 1px solid var(--line);
            padding-top: .8rem;
          }}
          #save-status {{
            font-size: .85rem;
            color: var(--muted);
          }}
          #nav-toggle {{ display: none; }}
          @media (max-width: 980px) {{
            .settings-grid {{ grid-template-columns: 1fr; }}
          }}
          @media (max-width: 860px) {{
            #nav-toggle {{ display: inline-flex; }}
            .topbar {{ padding: 10px; }}
            .nav-actions {{ width: 100%; justify-content: space-between; }}
            .nav-links {{ width: 100%; display: none; margin-top: 8px; }}
            .nav-links.open {{ display: flex; }}
            .nav-links .btn {{ flex: 1 1 auto; justify-content: center; }}
          }}
        </style>
      </head>
      <body>
        <div class="stats-shell">
          <div class="topbar d-flex align-items-center justify-content-between gap-2 flex-wrap">
            <div class="nav-actions">
              <div class="d-flex gap-2">
                {logout_btn}
                <button id="theme-toggle" class="btn btn-outline-secondary btn-sm"></button>
              </div>
              <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                aria-label="Toggle navigation">
                <i class="bi bi-list"></i>
              </button>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm" href="/?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/targets?token={token}">
                <i class="bi bi-bullseye"></i> Targets
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
              <a class="btn btn-outline-secondary btn-sm active" href="/settings?token={token}">
                <i class="bi bi-sliders"></i> Settings
              </a>
            </div>
          </div>
          <div class="stats-body">
          <div class="card p-3 mb-3">
            <h3 class="h5 mb-0">Settings</h3>
          </div>

          <div class="card p-3">
            <div class="page-head">
              <p>Centralized runtime tuning for queue, performance, and summaries.</p>
            </div>
            <div class="settings-grid">
              <section class="settings-section">
                <div class="section-title">Retry / Queue</div>
                <div class="section-subtitle">Delivery retry policy and backoff.</div>
                <div class="row g-3">
                  <div class="col-md-4">
                    <label class="form-label">Max attempts</label>
                    <input id="delivery_queue_max_attempts" type="number" class="form-control">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label">Base retry (s)</label>
                    <input id="delivery_queue_base_retry_seconds" type="number"
                      class="form-control">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label">Max retry (s)</label>
                    <input id="delivery_queue_max_retry_seconds" type="number" class="form-control">
                  </div>
                </div>
              </section>
              <section class="settings-section">
                <div class="section-title">Behavior</div>
                <div class="section-subtitle">Control quiet-hour forwarding window.</div>
                <div class="row g-3">
                  <div class="col-md-6">
                    <label class="form-label">Quiet start (0-23)</label>
                    <input id="quiet_hours_start" type="number" class="form-control">
                  </div>
                  <div class="col-md-6">
                    <label class="form-label">Quiet end (0-23)</label>
                    <input id="quiet_hours_end" type="number" class="form-control">
                  </div>
                </div>
              </section>
              <section class="settings-section">
                <div class="section-title">Performance / Maintenance</div>
                <div class="section-subtitle">Batching and retention management.</div>
                <div class="row g-3">
                  <div class="col-md-4">
                    <label class="form-label">DB batch size</label>
                    <input id="db_batch_size" type="number" class="form-control">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label">DB batch flush (s)</label>
                    <input id="db_batch_flush_seconds" type="number" class="form-control">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label">DB maintenance interval (s)</label>
                    <input id="db_maintenance_interval_seconds" type="number" class="form-control">
                  </div>
                  <div class="col-md-6">
                    <label class="form-label">Messages retention (days)</label>
                    <input id="retention_days" type="number" class="form-control">
                  </div>
                  <div class="col-md-6">
                    <label class="form-label">Errors retention (days)</label>
                    <input id="error_retention_days" type="number" class="form-control">
                  </div>
                </div>
              </section>
              <section class="settings-section">
                <div class="section-title">Aggregation / Digest / Summary</div>
                <div class="section-subtitle">Digest buffers and daily summary schedule.</div>
                <div class="row g-3">
                  <div class="col-md-4">
                    <label class="form-label">Aggregation interval (s)</label>
                    <input id="aggregation_interval" type="number" class="form-control">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label">Aggregation min count</label>
                    <input id="aggregation_min_count" type="number" class="form-control">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label">Max aggregation buffer</label>
                    <input id="max_aggregation_buffer" type="number" class="form-control">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label">Max digest buffer</label>
                    <input id="max_digest_buffer" type="number" class="form-control">
                  </div>
                  <div class="col-md-4">
                    <label class="form-label">Daily summary enabled (0/1)</label>
                    <input id="daily_summary_enabled" type="number" class="form-control">
                    <div class="field-hint">`1` enabled, `0` disabled</div>
                  </div>
                  <div class="col-md-2">
                    <label class="form-label">Daily hour</label>
                    <input id="daily_summary_hour" type="number" class="form-control">
                  </div>
                  <div class="col-md-2">
                    <label class="form-label">Daily minute</label>
                    <input id="daily_summary_minute" type="number" class="form-control">
                  </div>
                </div>
              </section>
            </div>
            <div class="action-bar">
              <div id="save-status">Ready</div>
              <div class="d-flex gap-2">
                <button id="reload" class="btn btn-outline-secondary">Reload</button>
                <button id="save" class="btn btn-outline-success">
                  <i class="bi bi-check2-circle"></i> Save
                </button>
              </div>
            </div>
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
            }}
          }};
          const keys = [
            'delivery_queue_max_attempts',
            'delivery_queue_base_retry_seconds',
            'delivery_queue_max_retry_seconds',
            'quiet_hours_start',
            'quiet_hours_end',
            'db_batch_size',
            'db_batch_flush_seconds',
            'retention_days',
            'error_retention_days',
            'db_maintenance_interval_seconds',
            'aggregation_interval',
            'aggregation_min_count',
            'max_aggregation_buffer',
            'max_digest_buffer',
            'daily_summary_enabled',
            'daily_summary_hour',
            'daily_summary_minute'
          ];
          async function loadSettings() {{
            setStatus('Loading settings...');
            const res = await fetch('/api/settings?token=' + encodeURIComponent(token));
            if (!res.ok) {{
              setStatus('Load failed', true);
              return;
            }}
            const data = await res.json();
            const values = data.values || {{}};
            keys.forEach(k => {{
              const el = document.getElementById(k);
              if (el) el.value = values[k];
            }});
            setStatus('Loaded');
          }}
          function setStatus(msg, isError = false) {{
            const el = document.getElementById('save-status');
            if (!el) return;
            el.textContent = msg;
            el.style.color = isError ? '#ef4444' : 'var(--muted)';
          }}
          async function saveSettings() {{
            setStatus('Saving...');
            const payload = {{}};
            keys.forEach(k => {{
              const el = document.getElementById(k);
              if (!el) return;
              payload[k] = Number(el.value);
            }});
            const res = await fetch('/api/settings?token=' + encodeURIComponent(token), {{
              method: 'POST',
              headers: {{ 'Content-Type': 'application/json' }},
              body: JSON.stringify(payload),
            }});
            if (!res.ok) {{
              setStatus('Save failed', true);
              alert(await res.text());
              return;
            }}
            await loadSettings();
            setStatus('Saved');
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
          document.getElementById('save').onclick = saveSettings;
          document.getElementById('reload').onclick = loadSettings;
          loadSettings();
        </script>
      </body>
    </html>
    """
    return _admin_html_response(request, token, html)


async def settings_get_api(request):
    _require_admin(request)
    data = await list_settings_with_meta()
    return web.json_response(data)


async def settings_update_api(request):
    _require_admin(request)
    try:
        payload = await request.json()
    except Exception as exc:
        raise web.HTTPBadRequest(text=f"invalid json: {exc}") from exc
    values = await update_settings(payload)
    return web.json_response({"saved": True, "values": values})


async def admin_topic_page(request):
    token = _require_admin(request)
    logout_btn = "" if token else (
        "<a class=\"btn btn-outline-secondary btn-sm\" href=\"/logout\" "
        "title=\"Logout\" aria-label=\"Logout\">"
        "<i class=\"bi bi-box-arrow-right\"></i>"
        "</a>"
    )
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
                  href="/?token={token}" title="Back"
                  aria-label="Back">←</a>
                {logout_btn}
                <button
                  id="theme-toggle"
                  class="btn btn-outline-secondary btn-sm"
                  aria-label="Toggle theme"
                ></button>
                <button id="clear-topic" class="btn btn-outline-danger btn-sm"
                  title="Clear stats" aria-label="Clear stats">
                  <i class="bi bi-trash"></i>
                </button>
                <button id="reset-topic-count" class="btn btn-outline-secondary btn-sm"
                  title="Reset count" aria-label="Reset count">
                  <i class="bi bi-recycle"></i>
                </button>
              </div>
              <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                aria-label="Toggle navigation">
                <i class="bi bi-list"></i>
              </button>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm active" href="/?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/targets?token={token}">
                <i class="bi bi-bullseye"></i> Targets
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/settings?token={token}">
                <i class="bi bi-sliders"></i> Settings
              </a>
            </div>
          </div>
          <div class="card p-3 mt-2 mb-2">
            <h3 class="h5 mb-0">Topic: {name}</h3>
          </div>
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
    log("INFO", "ui topics_list")
    rows = await list_topics()
    topic_target_ids = await list_topic_delivery_target_ids()
    targets = await list_delivery_targets()
    targets_by_id = {int(t["id"]): t for t in targets}
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
        topic_target_id = topic_target_ids.get(name)
        topic_target = targets_by_id.get(topic_target_id) if topic_target_id else None
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
            "target_id": topic_target_id,
            "target_name": topic_target["name"] if topic_target else None,
        })
    return web.json_response(
        {
            "items": items,
            "targets": [
                {
                    "id": int(t["id"]),
                    "name": t["name"],
                    "kind": t["kind"],
                    "enabled": bool(t["enabled"]),
                    "is_default": bool(t["is_default"]),
                }
                for t in targets
            ],
        }
    )

async def topic_detail(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "ui topic_detail", topic=name)
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
    topic_target_ids = await list_topic_delivery_target_ids()
    targets = await list_delivery_targets()
    target_id = topic_target_ids.get(name)
    target = None
    for t in targets:
        if int(t["id"]) == int(target_id or -1):
            target = t
            break
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
            "target_id": target_id,
            "target_name": target["name"] if target else None,
        }
    )

async def topic_toggle(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "ui topic_toggle", topic=name)
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


def _validate_target_payload(data):
    name = str(data.get("name", "")).strip()
    kind = str(data.get("kind", "")).strip()
    config = data.get("config")
    enabled = bool(data.get("enabled", True))
    is_default = bool(data.get("is_default", False))
    if not name:
        raise web.HTTPBadRequest(text="missing target name")
    allowed_kinds = {"telegram", "webhook_generic", "webhook_discord", "webhook_slack"}
    if kind not in allowed_kinds:
        raise web.HTTPBadRequest(text="invalid target kind")
    if not isinstance(config, dict):
        config = {}
    if kind == "telegram":
        chat_id = str(config.get("chat_id", "")).strip()
        bot_token = str(config.get("bot_token", "")).strip()
        try:
            max_message_length = int(config.get("max_message_length", 4096))
        except Exception as exc:
            raise web.HTTPBadRequest(
                text="telegram config.max_message_length must be an integer"
            ) from exc
        max_message_length = max(256, min(max_message_length, 20000))
        if not chat_id or not bot_token:
            raise web.HTTPBadRequest(
                text="telegram target requires config.chat_id + config.bot_token"
            )
        config = {
            "chat_id": chat_id,
            "bot_token": bot_token,
            "max_message_length": max_message_length,
        }
    else:
        url = str(config.get("url", "")).strip()
        if not url:
            raise web.HTTPBadRequest(text="webhook target requires config.url")
        auth_header = str(config.get("auth_header", "")).strip()
        config = {"url": url, "auth_header": auth_header}
    return name, kind, config, enabled, is_default


async def targets_list(request):
    _require_admin(request)
    items = await list_delivery_targets()
    return web.json_response({"items": items})


async def targets_create(request):
    _require_admin(request)
    try:
        data = await request.json()
    except Exception as exc:
        raise web.HTTPBadRequest(text=f"invalid json: {exc}") from exc
    name, kind, config, enabled, is_default = _validate_target_payload(data)
    target_id = await create_delivery_target(
        name,
        kind,
        config=config,
        enabled=enabled,
        is_default=is_default,
    )
    return web.json_response({"id": target_id})


async def targets_update(request):
    _require_admin(request)
    target_id = int(request.match_info["id"])
    target = await get_delivery_target(target_id)
    if target is None:
        raise web.HTTPNotFound()
    try:
        data = await request.json()
    except Exception as exc:
        raise web.HTTPBadRequest(text=f"invalid json: {exc}") from exc
    name, kind, config, enabled, _is_default = _validate_target_payload(data)
    await update_delivery_target(
        target_id,
        name=name,
        kind=kind,
        config=config,
        enabled=enabled,
    )
    return web.json_response({"updated": target_id})


async def targets_delete(request):
    _require_admin(request)
    target_id = int(request.match_info["id"])
    target = await get_delivery_target(target_id)
    if target is None:
        raise web.HTTPNotFound()
    await delete_delivery_target(target_id)
    return web.json_response({"deleted": target_id})


async def targets_set_default(request):
    _require_admin(request)
    target_id = int(request.match_info["id"])
    target = await get_delivery_target(target_id)
    if target is None:
        raise web.HTTPNotFound()
    await set_default_delivery_target(target_id)
    return web.json_response({"default_id": target_id})


async def topic_set_target(request):
    _require_admin(request)
    name = request.match_info["name"]
    rows = await list_topics()
    known = {r["name"]: r for r in rows}
    if name not in known:
        raise web.HTTPNotFound()
    try:
        data = await request.json()
    except Exception as exc:
        raise web.HTTPBadRequest(text=f"invalid json: {exc}") from exc
    target_id_raw = data.get("target_id")
    target_id = None
    if target_id_raw not in (None, "", "null"):
        target_id = int(target_id_raw)
        target = await get_delivery_target(target_id)
        if target is None:
            raise web.HTTPBadRequest(text="unknown target_id")
    await set_topic_delivery_target(name, target_id)
    return web.json_response({"topic": name, "target_id": target_id})

async def pause_all(request):
    _require_admin(request)
    log("INFO", "ui pause_all")
    rows = await list_topics()
    for r in rows:
        name = r["name"]
        await set_topic_enabled(name, False)
        _start_worker(name)
    return web.json_response({"paused": True})

async def resume_all(request):
    _require_admin(request)
    log("INFO", "ui resume_all")
    rows = await list_topics()
    for r in rows:
        name = r["name"]
        await add_topic(name)
        await set_topic_enabled(name, True)
        _start_worker(name)
    return web.json_response({"resumed": True})

async def clear_topic(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "ui clear_topic", topic=name)
    await reset_topic_count_base(name)
    await clear_topic_status_counts(name)
    topic_stats[name] = {
        "received": 0,
        "filtered": 0,
        "rate_limited": 0,
        "disabled": 0,
        "inserted": 0,
        "errors": 0,
    }
    recent_events[name] = deque(maxlen=ADMIN_RECENT_EVENTS)
    topic_rates[name] = deque(maxlen=60)
    return web.json_response({"cleared": name})


async def reset_topic_count(request):
    _require_admin(request)
    name = request.match_info["name"]
    log("INFO", "ui reset_topic_count", topic=name)
    rows = await list_topics()
    known = {r["name"]: r for r in rows}
    if name not in known:
        raise web.HTTPNotFound()
    await reset_topic_count_base(name)
    return web.json_response({"reset_count": name})

async def clear_all(request):
    _require_admin(request)
    log("INFO", "ui clear_all")
    await reset_all_topic_count_bases()
    await clear_all_topic_status_counts()
    for name in list(topic_stats.keys()):
        topic_stats[name] = {
            "received": 0,
            "filtered": 0,
            "rate_limited": 0,
            "disabled": 0,
            "inserted": 0,
            "errors": 0,
        }
    for name in list(recent_events.keys()):
        recent_events[name] = deque(maxlen=ADMIN_RECENT_EVENTS)
    for name in list(topic_rates.keys()):
        topic_rates[name] = deque(maxlen=60)
    return web.json_response({"cleared": True})


async def hard_clear_all(request):
    _require_admin(request)
    log("INFO", "ui hard_clear_all")
    await hard_reset_all_topic_counts()
    await clear_all_topic_status_counts()
    await clear_all_messages()
    for name in list(topic_stats.keys()):
        topic_stats[name] = {
            "received": 0,
            "filtered": 0,
            "rate_limited": 0,
            "disabled": 0,
            "inserted": 0,
            "errors": 0,
        }
    for name in list(recent_events.keys()):
        recent_events[name] = deque(maxlen=ADMIN_RECENT_EVENTS)
    for name in list(topic_rates.keys()):
        topic_rates[name] = deque(maxlen=60)
    return web.json_response({"hard_cleared": True})


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
    logout_btn = "" if token else (
        "<a class=\"btn btn-outline-secondary btn-sm\" href=\"/logout\" "
        "title=\"Logout\" aria-label=\"Logout\">"
        "<i class=\"bi bi-box-arrow-right\"></i>"
        "</a>"
    )
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
                {logout_btn}
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
              <a class="btn btn-outline-secondary btn-sm" href="/?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/targets?token={token}">
                <i class="bi bi-bullseye"></i> Targets
              </a>
              <a class="btn btn-outline-secondary btn-sm active"
                href="/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/settings?token={token}">
                <i class="bi bi-sliders"></i> Settings
              </a>
            </div>
          </div>
          <div class="stats-body">
          <div class="card p-3 mb-3">
            <h3 class="h5 mb-0">Global Stats</h3>
          </div>
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
    logout_btn = "" if token else (
        "<a class=\"btn btn-outline-secondary btn-sm\" href=\"/logout\" "
        "title=\"Logout\" aria-label=\"Logout\">"
        "<i class=\"bi bi-box-arrow-right\"></i>"
        "</a>"
    )
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
          .card {{
            --bs-card-bg: var(--card-soft);
            background-color: var(--card-soft)!important;
            border-color: var(--line)!important;
            color: var(--ink);
          }}
          .table-responsive {{
            border: 1px solid var(--line);
            border-radius: 12px;
            overflow: hidden;
          }}
          .ui-table {{
            --bs-table-bg: transparent;
            --bs-table-striped-bg: color-mix(in srgb, var(--card-soft) 88%, transparent);
            --bs-table-color: var(--ink);
            color: var(--ink);
          }}
          .ui-table > :not(caption) > * > * {{
            border-bottom-color: var(--line);
          }}
          .ui-table thead th {{
            border-bottom: 1px solid var(--line);
            color: var(--muted);
            font-size: .8rem;
            text-transform: uppercase;
            letter-spacing: .03em;
            font-weight: 700;
            background-color: transparent!important;
          }}
          .ui-table tbody tr:hover > * {{
            background: color-mix(in srgb, var(--card-soft) 78%, transparent)!important;
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
                {logout_btn}
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
              <a class="btn btn-outline-secondary btn-sm" href="/?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/targets?token={token}">
                <i class="bi bi-bullseye"></i> Targets
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm active" href="/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/settings?token={token}">
                <i class="bi bi-sliders"></i> Settings
              </a>
            </div>
          </div>
          <div class="errors-body">
          <div class="card p-3 mb-3">
            <h3 class="h5 mb-0">Error History</h3>
          </div>
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
            <table class="ui-table table table-sm table-striped align-middle">
              <thead>
                <tr>
                  <th class="sortable" data-sort="id">ID <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="ts">Time <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="component">
                    Component <span class="sort-ind">↕</span>
                  </th>
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
          function updatePager(total, offset, limit, pageCount) {{
            const prev = document.getElementById('prev');
            const next = document.getElementById('next');
            const hasPrev = offset > 0;
            const hasNext = (offset + pageCount) < total && pageCount >= limit;
            prev.disabled = !hasPrev;
            next.disabled = !hasNext;
          }}
          async function load() {{
            const res = await fetch('/api/errors?' + queryString(''));
            if (!res.ok) return;
            const data = await res.json();
            const rows = document.getElementById('rows');
            document.getElementById('meta').innerText =
              `Total: ${{data.total}} | Offset: ${{data.offset}} | Limit: ${{data.limit}}`;
            const items = sortItems(data.items || []);
            if (!items.length) {{
              rows.innerHTML = (
                '<tr><td colspan="5" class="text-center text-muted py-4">' +
                'No errors found' +
                '</td></tr>'
              );
              updatePager(data.total || 0, data.offset || 0, data.limit || state.limit, 0);
              setSortIndicators();
              return;
            }}
            rows.innerHTML = items.map(e => `
              <tr>
                <td>${{e.id}}</td>
                <td>${{fmt(e.ts)}}</td>
                <td>${{e.component || ''}}</td>
                <td>${{e.topic || ''}}</td>
                <td><code>${{(e.error || '').slice(0, 300)}}</code></td>
              </tr>
            `).join('');
            updatePager(data.total || 0, data.offset || 0, data.limit || state.limit, items.length);
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
            if (document.getElementById('prev').disabled) return;
            state.offset = Math.max(0, state.offset - state.limit);
            load();
          }};
          document.getElementById('next').onclick = () => {{
            if (document.getElementById('next').disabled) return;
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
    logout_btn = "" if token else (
        "<a class=\"btn btn-outline-secondary btn-sm\" href=\"/logout\" "
        "title=\"Logout\" aria-label=\"Logout\">"
        "<i class=\"bi bi-box-arrow-right\"></i>"
        "</a>"
    )
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
          .card {{
            --bs-card-bg: var(--card-soft);
            background-color: var(--card-soft)!important;
            border-color: var(--line)!important;
            color: var(--ink);
          }}
          .table-responsive {{
            border: 1px solid var(--line);
            border-radius: 12px;
            overflow: hidden;
          }}
          .ui-table {{
            --bs-table-bg: transparent;
            --bs-table-striped-bg: color-mix(in srgb, var(--card-soft) 88%, transparent);
            --bs-table-color: var(--ink);
            color: var(--ink);
          }}
          .ui-table > :not(caption) > * > * {{
            border-bottom-color: var(--line);
          }}
          .ui-table thead th {{
            border-bottom: 1px solid var(--line);
            color: var(--muted);
            font-size: .8rem;
            text-transform: uppercase;
            letter-spacing: .03em;
            font-weight: 700;
            background-color: transparent!important;
          }}
          .ui-table tbody tr:hover > * {{
            background: color-mix(in srgb, var(--card-soft) 78%, transparent)!important;
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
          .queue-shell.dlq-compact.dlq-has-rows .dlq-table thead {{
            display: none !important;
          }}
          .queue-shell.dlq-compact.dlq-has-rows .dlq-table tbody tr {{
            display: block !important;
            border: 1px solid var(--line);
            border-radius: 12px;
            margin-bottom: 10px;
            padding: 8px;
            background: var(--card-soft);
          }}
          .queue-shell.dlq-compact.dlq-has-rows .dlq-table tbody td {{
            display: flex !important;
            justify-content: space-between;
            gap: 10px;
            border: 0 !important;
            padding: 6px 4px;
            text-align: right;
            width: 100%;
          }}
          .queue-shell.dlq-compact.dlq-has-rows .dlq-table tbody td::before {{
            content: attr(data-label);
            color: var(--muted);
            font-weight: 700;
            text-transform: uppercase;
            font-size: .75rem;
            letter-spacing: .02em;
            text-align: left;
          }}
          .queue-shell.dlq-compact.dlq-has-rows .dlq-table tbody td[data-label="Action"] {{
            justify-content: flex-end;
            gap: 6px;
            padding-top: 8px;
          }}
          .queue-shell.dlq-compact.dlq-has-rows .dlq-table tbody td[data-label="Action"]::before {{
            margin-right: auto;
          }}
          .queue-shell.dlq-compact.dlq-has-rows .dlq-table tbody td[data-label="Message"] code {{
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
                {logout_btn}
                <button id="theme-toggle" class="btn btn-outline-secondary btn-sm"
                  aria-label="Toggle theme"></button>
                <button id="clear-dlq" class="btn btn-outline-danger btn-sm"
                  title="Clear filtered DLQ" aria-label="Clear filtered DLQ">
                  <i class="bi bi-trash"></i>
                </button>
                <button id="requeue-batch" class="btn btn-outline-success btn-sm"
                  title="Requeue filtered batch" aria-label="Requeue filtered batch">
                  <i class="bi bi-arrow-repeat"></i>
                </button>
              </div>
              <button id="nav-toggle" class="btn btn-outline-secondary btn-sm"
                aria-label="Toggle navigation">
                <i class="bi bi-list"></i>
              </button>
            </div>
            <div class="nav-links">
              <a class="btn btn-outline-secondary btn-sm" href="/?token={token}">
                <i class="bi bi-grid-3x3-gap"></i> Topics
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/targets?token={token}">
                <i class="bi bi-bullseye"></i> Targets
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/stats?token={token}">
                <i class="bi bi-bar-chart"></i> Global Stats
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/errors?token={token}">
                <i class="bi bi-exclamation-triangle"></i> Errors
              </a>
              <a class="btn btn-outline-secondary btn-sm active" href="/queue?token={token}">
                <i class="bi bi-inboxes"></i> Queue
              </a>
              <a class="btn btn-outline-secondary btn-sm" href="/settings?token={token}">
                <i class="bi bi-sliders"></i> Settings
              </a>
            </div>
          </div>
          <div class="queue-body">
          <div class="card p-3 mb-3">
            <h3 class="h5 mb-0">Dead Letter Queue</h3>
          </div>
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
            <table class="ui-table table table-sm table-striped align-middle dlq-table">
              <thead>
                <tr>
                  <th class="sortable" data-sort="id">ID <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="topic">Topic <span class="sort-ind">↕</span></th>
                  <th class="sortable" data-sort="attempts">
                    Attempts <span class="sort-ind">↕</span>
                  </th>
                  <th class="sortable" data-sort="last_error">
                    Error <span class="sort-ind">↕</span>
                  </th>
                  <th>Message</th>
                  <th class="sortable" data-sort="updated_at">
                    Updated <span class="sort-ind">↕</span>
                  </th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody id="rows"></tbody>
            </table>
          </div>
          <dialog
            id="payload-modal"
            class="rounded border-0 shadow"
            style="max-width:920px;width:92vw;"
          >
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
          function updateDlqLayout(rowsCount) {{
            const shell = document.querySelector('.queue-shell');
            if (!shell) return;
            shell.classList.toggle('dlq-has-rows', Number(rowsCount || 0) > 0);
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
          function updatePager(total, offset, limit, pageCount) {{
            const prev = document.getElementById('prev');
            const next = document.getElementById('next');
            const hasPrev = offset > 0;
            const hasNext = (offset + pageCount) < total && pageCount >= limit;
            prev.disabled = !hasPrev;
            next.disabled = !hasNext;
          }}
          async function requeue(id) {{
            await fetch(
              '/api/queue/dead_letters/' + id + '/requeue?token=' +
                encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            await load();
          }}
          async function deleteDlqItem(id) {{
            const res = await fetch(
              '/api/queue/dead_letters/' + id + '/delete?token=' +
                encodeURIComponent(token),
              {{ method: 'POST' }}
            );
            if (!res.ok) {{
              let msg = 'delete failed';
              try {{
                msg = await res.text();
              }} catch (_e) {{}}
              alert(msg || 'delete failed');
              return;
            }}
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
            const items = sortItems(data.items || []);
            if (!items.length) {{
              rows.innerHTML = (
                '<tr><td colspan="7" class="text-center text-muted py-4">' +
                'No dead letters found' +
                '</td></tr>'
              );
              updateDlqLayout(0);
              updatePager(data.total || 0, data.offset || 0, data.limit || state.limit, 0);
              setSortIndicators();
              return;
            }}
            rows.innerHTML = items.map(e => {{
              payloadById.set(e.id, e.payload || null);
              return `
              <tr>
                <td data-label="ID">${{e.id}}</td>
                <td data-label="Topic">${{e.topic || ''}}</td>
                <td data-label="Attempts">${{e.attempts}}</td>
                <td data-label="Error"><code>${{(e.last_error || '').slice(0, 200)}}</code></td>
                <td data-label="Message">
                  <code>${{escapeHtml(messagePreview(e).slice(0, 160))}}</code>
                </td>
                <td data-label="Updated">${{fmt(e.updated_at)}}</td>
                <td data-label="Action">
                  <button class="btn btn-outline-secondary btn-sm"
                    onclick="viewPayload(${{e.id}})">
                    <i class="bi bi-eye"></i>
                  </button>
                  <button class="btn btn-outline-success btn-sm"
                    onclick="requeue(${{e.id}})">Requeue</button>
                  <button class="btn btn-outline-danger btn-sm"
                    onclick="deleteDlqItem(${{e.id}})">Delete</button>
                </td>
              </tr>
            `;
            }}).join('');
            updateDlqLayout(items.length);
            updatePager(data.total || 0, data.offset || 0, data.limit || state.limit, items.length);
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
            if (document.getElementById('prev').disabled) return;
            state.offset = Math.max(0, state.offset - state.limit);
            load();
          }};
          document.getElementById('next').onclick = () => {{
            if (document.getElementById('next').disabled) return;
            state.offset += state.limit;
            load();
          }};
          document.getElementById('requeue-batch').onclick = async () => {{
            const q = document.getElementById('topic').value.trim();
            const res = await fetch(
              '/api/queue/dead_letters/requeue_batch?token=' + encodeURIComponent(token),
              {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                  q: q || '',
                  limit: 0
                }})
              }}
            );
            if (!res.ok) {{
              let msg = 'requeue failed';
              try {{
                msg = await res.text();
              }} catch (_e) {{}}
              alert(msg || 'requeue failed');
              return;
            }}
            try {{
              const payload = await res.json();
              if ((payload.requeued || 0) === 0 && (payload.failed || 0) === 0) {{
                alert('No DLQ item matched your filter.');
              }} else if ((payload.failed || 0) > 0) {{
                alert(
                  'Requeued: ' + payload.requeued + ' | Failed: ' + payload.failed
                );
              }}
            }} catch (_e) {{}}
            state.offset = 0;
            load();
          }};
          document.getElementById('clear-dlq').onclick = async () => {{
            if (!confirm('Clear filtered DLQ items?')) return;
            const q = document.getElementById('topic').value.trim();
            const res = await fetch(
              '/api/queue/dead_letters/clear?token=' + encodeURIComponent(token),
              {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                  q: q || ''
                }})
              }}
            );
            if (!res.ok) {{
              let msg = 'clear failed';
              try {{
                msg = await res.text();
              }} catch (_e) {{}}
              alert(msg || 'clear failed');
              return;
            }}
            try {{
              const payload = await res.json();
              alert('Cleared: ' + (payload.deleted || 0));
            }} catch (_e) {{}}
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
    log("INFO", "ui stats_api")
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
    log("INFO", "ui errors_clear_api")
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
    deleted = await delete_dead_letter(item_id)
    if deleted <= 0:
        raise web.HTTPNotFound()
    return web.json_response({"requeued": item_id})


async def dead_letter_requeue_batch_api(request):
    _require_admin(request)
    data = await request.json()
    topic = _clean_optional_str(data.get("topic"))
    reason = _clean_optional_str(data.get("reason"))
    query = _clean_optional_str(data.get("q"))
    max_items = int(data.get("limit", 0))
    if max_items < 0:
        max_items = 0
    batch_size = 200
    requeued = 0
    failed = 0
    while True:
        if max_items > 0:
            remaining = max_items - requeued
            if remaining <= 0:
                break
            current_limit = max(1, min(remaining, batch_size))
        else:
            current_limit = batch_size
        result = await query_dead_letters(
            limit=current_limit,
            offset=0,
            topic=topic,
            reason=reason,
            query=query,
        )
        items = result["items"]
        if not items:
            break
        ids = []
        for item in items:
            try:
                await enqueue_telegram_item(item["payload"])
                ids.append(item["id"])
                requeued += 1
            except Exception as exc:
                failed += 1
                await log_error(
                    "dead_letter_requeue_batch",
                    item.get("topic"),
                    str(exc),
                )
        if ids:
            await delete_dead_letters(ids)
        else:
            break
    return web.json_response(
        {
            "requeued": requeued,
            "failed": failed,
        }
    )


async def dead_letter_delete_api(request):
    _require_admin(request)
    item_id = int(request.match_info["id"])
    deleted = await delete_dead_letter(item_id)
    if deleted <= 0:
        raise web.HTTPNotFound()
    return web.json_response({"deleted": item_id})


async def dead_letter_clear_api(request):
    _require_admin(request)
    try:
        data = await request.json()
    except Exception:
        data = {}
    topic = _clean_optional_str(data.get("topic"))
    reason = _clean_optional_str(data.get("reason"))
    query = _clean_optional_str(data.get("q"))
    deleted = await clear_dead_letters(topic=topic, reason=reason, query=query)
    return web.json_response({"cleared": True, "deleted": deleted})


async def create_web_app():

    app = web.Application(
        middlewares=[security_headers_middleware],
    )

    app.router.add_get(
        "/health",
        health,
    )

    app.router.add_get(
        "/metrics",
        metrics,
    )
    app.router.add_get(
        "/auth/login",
        oidc_login,
    )
    app.router.add_get(
        "/auth/callback",
        oidc_callback,
    )
    app.router.add_get(
        "/auth/logout",
        oidc_logout,
    )
    app.router.add_get(
        "/login",
        local_login_page,
    )
    app.router.add_post(
        "/login",
        local_login_submit,
    )
    app.router.add_get(
        "/logout",
        oidc_logout,
    )
    app.router.add_get(
        "/",
        admin_page,
    )
    app.router.add_get(
        "/stats",
        admin_stats_page,
    )
    app.router.add_get(
        "/queue",
        admin_queue_page,
    )
    app.router.add_get(
        "/targets",
        admin_targets_page,
    )
    app.router.add_get(
        "/errors",
        admin_errors_page,
    )
    app.router.add_get(
        "/settings",
        admin_settings_page,
    )
    app.router.add_get(
        "/topic/{name}",
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
        "/api/topics/{name}/target",
        topic_set_target,
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
    app.router.add_post(
        "/api/topics/hard_clear_all",
        hard_clear_all,
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
        "/api/targets",
        targets_list,
    )
    app.router.add_post(
        "/api/targets",
        targets_create,
    )
    app.router.add_put(
        "/api/targets/{id}",
        targets_update,
    )
    app.router.add_delete(
        "/api/targets/{id}",
        targets_delete,
    )
    app.router.add_post(
        "/api/targets/{id}/default",
        targets_set_default,
    )
    app.router.add_get(
        "/api/stats",
        stats_api,
    )
    app.router.add_get(
        "/api/settings",
        settings_get_api,
    )
    app.router.add_post(
        "/api/settings",
        settings_update_api,
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
