import base64
import hashlib
import hmac
import json
import time
from urllib.parse import parse_qs, urlsplit

import jwt
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer, make_mocked_request
from cryptography.hazmat.primitives.asymmetric import rsa

from api import web as api
from core.http import create_http_session


@pytest.fixture(scope="module")
def signing_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest_asyncio.fixture
async def oidc_provider(web_client, monkeypatch, signing_key):
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(signing_key.public_key()))
    jwk.update(kid="test-key", alg="RS256")
    provider = {"status": {}, "calls": [], "jwk": jwk, "token_extra": {},
                "userinfo": {"sub": "user-1", "email": "user@example.test"}}

    async def endpoint(request):
        provider["calls"].append(request.path)
        path = request.path
        status = provider["status"].get(path, 200)
        if path == "/.well-known/openid-configuration":
            body = provider["discovery"]
        elif path == "/jwks":
            body = {"keys": [jwk]}
        elif path == "/userinfo":
            body = provider["userinfo"]
            assert request.headers["Authorization"] == "Bearer access-token"
        else:
            data = await request.post()
            assert data["grant_type"] == "authorization_code"
            assert data["code_verifier"]
            body = {"access_token": "access-token", "id_token": jwt.encode(
                provider["claims"], signing_key, algorithm="RS256", headers={"kid": "test-key"})}
            body.update(provider["token_extra"])
        return web.json_response(body, status=status)

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", endpoint)
    async with TestServer(app) as server:
        base = str(server.make_url("")).rstrip("/")
        provider["discovery"] = {"issuer": base, "authorization_endpoint": base + "/authorize",
                                 "token_endpoint": base + "/token", "jwks_uri": base + "/jwks",
                                 "userinfo_endpoint": base + "/userinfo"}
        provider["claims"] = {"iss": base, "sub": "user-1", "aud": "client-1",
                              "exp": int(time.time()) + 600, "iat": int(time.time()),
                              "email": "user@example.test"}
        for key, value in {"OIDC_ENABLED": True, "OIDC_ISSUER_URL": base,
                           "OIDC_CLIENT_ID": "client-1", "OIDC_CLIENT_SECRET": "client-secret",
                           "OIDC_REDIRECT_URI": str(web_client.make_url("/auth/callback")),
                           "ACCESS_SESSION_SECRET": "s" * 64, "_OIDC_DISCOVERY_CACHE": None,
                           "_OIDC_JWKS_CACHE": None, "OIDC_ALLOWED_EMAILS": (),
                           "OIDC_ALLOWED_DOMAINS": (),
                           "OIDC_REQUIRE_VERIFIED_EMAIL": False}.items():
            monkeypatch.setattr(api, key, value)
        await create_http_session()
        yield provider


async def start_login(client, provider):
    response = await client.get("/auth/login?next=/targets", allow_redirects=False)
    assert response.status == 302
    query = parse_qs(urlsplit(response.headers["Location"]).query)
    provider["claims"]["nonce"] = query["nonce"][0]
    assert query["code_challenge_method"] == ["S256"]
    assert query["client_id"] == ["client-1"]
    return query["state"][0]


@pytest.mark.asyncio
async def test_oidc_login_callback_and_session(web_client, oidc_provider):
    state = await start_login(web_client, oidc_provider)
    response = await web_client.get(
        f"/auth/callback?state={state}&code=valid", allow_redirects=False
    )
    assert response.status == 302, await response.text()
    assert response.headers["Location"] == "/targets"
    cookie = response.cookies[api.OIDC_SESSION_COOKIE]
    assert cookie["httponly"] and cookie["samesite"] == "Lax"
    assert api._read_signed_cookie_value(cookie.value)["sub"] == "user-1"
    response = await web_client.get("/api/topics", headers={"X-Access-Token": ""})
    assert response.status == 200
    response = await web_client.get("/logout", allow_redirects=False)
    assert response.status == 302
    assert response.cookies[api.OIDC_SESSION_COOKIE]["max-age"] == "0"


@pytest.mark.parametrize("change,value,status,message", [
    ("nonce", "wrong", 401, "nonce"), ("iss", "https://wrong.test", 401, "issuer"),
    ("aud", "wrong", 401, "id_token"), ("exp", 0, 401, "id_token"),
    ("sub", "", 401, "subject"),
])
@pytest.mark.asyncio
async def test_oidc_rejects_invalid_claims(
        web_client, oidc_provider, change, value, status, message):
    state = await start_login(web_client, oidc_provider)
    oidc_provider["claims"][change] = value
    if change == "sub":
        oidc_provider["userinfo"] = {}
    response = await web_client.get(
        f"/auth/callback?state={state}&code=valid", allow_redirects=False
    )
    assert response.status == status
    assert message in await response.text()


@pytest.mark.parametrize("token_extra,message", [({"access_token": None}, "access token"),
                                                 ({"id_token": None}, "id_token")])
@pytest.mark.asyncio
async def test_oidc_rejects_missing_tokens(web_client, oidc_provider, token_extra, message):
    state = await start_login(web_client, oidc_provider)
    oidc_provider["token_extra"] = token_extra
    response = await web_client.get(f"/auth/callback?state={state}&code=valid")
    assert response.status == 401 and message in await response.text()


@pytest.mark.asyncio
async def test_oidc_user_restrictions_and_subject_consistency(
        web_client, oidc_provider, monkeypatch):
    state = await start_login(web_client, oidc_provider)
    oidc_provider["userinfo"]["sub"] = "different-user"
    response = await web_client.get(f"/auth/callback?state={state}&code=valid")
    assert response.status == 401
    oidc_provider["userinfo"]["sub"] = "user-1"
    monkeypatch.setattr(api, "OIDC_ALLOWED_DOMAINS", ("another.test",))
    response = await web_client.get(f"/auth/callback?state={state}&code=valid")
    assert response.status == 403
    assert not api._oidc_user_allowed({"email": "missing-at"})
    monkeypatch.setattr(api, "OIDC_ALLOWED_DOMAINS", ("example.test",))
    assert api._oidc_user_allowed({"email": "user@example.test"})
    monkeypatch.setattr(api, "OIDC_ALLOWED_EMAILS", ("allowed@example.test",))
    assert not api._oidc_user_allowed({"email": "user@example.test"})


@pytest.mark.asyncio
async def test_oidc_userinfo_failure_uses_verified_id_token(web_client, oidc_provider):
    state = await start_login(web_client, oidc_provider)
    oidc_provider["status"]["/userinfo"] = 503
    response = await web_client.get(
        f"/auth/callback?state={state}&code=valid", allow_redirects=False
    )
    assert response.status == 302


@pytest.mark.parametrize("cookie,query,message", [
    ({}, "state=x&code=c", "state"),
    ({"state": "x"}, "state=x&code=c", "verifier"),
    ({"state": "x", "code_verifier": "v"}, "state=x&code=c", "nonce"),
    ({"state": "x", "code_verifier": "v", "nonce": "n"}, "state=x", "code"),
])
@pytest.mark.asyncio
async def test_oidc_callback_requires_state_and_pkce(
        web_client, oidc_provider, cookie, query, message):
    value = api._make_signed_cookie_value(cookie, 300)
    response = await web_client.get("/auth/callback?" + query,
                                    cookies={api.OIDC_STATE_COOKIE: value})
    assert response.status == 401 and message in await response.text()


@pytest.mark.asyncio
async def test_oidc_endpoint_errors_and_discovery_not_cached_until_valid(web_client, oidc_provider,
                                                                         monkeypatch):
    original = dict(oidc_provider["discovery"])
    for broken in ({"issuer": "wrong"}, {"issuer": original["issuer"]}):
        oidc_provider["discovery"] = broken
        with pytest.raises(RuntimeError):
            await api._fetch_oidc_discovery()
        assert api._OIDC_DISCOVERY_CACHE is None
    oidc_provider["discovery"] = original
    oidc_provider["status"]["/.well-known/openid-configuration"] = 503
    with pytest.raises(RuntimeError, match="discovery failed"):
        await api._fetch_oidc_discovery()
    oidc_provider["status"].clear()
    discovery = await api._fetch_oidc_discovery()
    assert await api._fetch_oidc_discovery() is discovery
    oidc_provider["status"]["/jwks"] = 503
    with pytest.raises(RuntimeError, match="JWKS fetch failed"):
        await api._fetch_oidc_jwks(discovery)
    oidc_provider["status"].clear()
    assert (await api._fetch_oidc_jwks(discovery))["keys"]
    assert (await api._fetch_oidc_jwks(discovery))["keys"]
    assert await api._fetch_oidc_userinfo("access-token", {}) == {}
    state = await start_login(web_client, oidc_provider)
    oidc_provider["status"]["/token"] = 400
    response = await web_client.get(f"/auth/callback?state={state}&code=valid")
    assert response.status == 401 and "exchange failed" in await response.text()
    monkeypatch.setattr(api, "get_http_session", lambda: None)
    response = await web_client.get(f"/auth/callback?state={state}&code=valid")
    assert response.status == 503
    monkeypatch.setattr(api, "_OIDC_DISCOVERY_CACHE", {})
    with pytest.raises(RuntimeError, match="not ready"):
        await api._fetch_oidc_discovery()
    monkeypatch.setattr(api, "OIDC_ISSUER_URL", "")
    with pytest.raises(RuntimeError, match="not set"):
        await api._fetch_oidc_discovery()
    monkeypatch.setattr(api, "_OIDC_JWKS_CACHE", None)
    with pytest.raises(RuntimeError, match="jwks_uri"):
        await api._fetch_oidc_jwks({})
    with pytest.raises(RuntimeError, match="not ready"):
        await api._fetch_oidc_jwks(discovery)
    with pytest.raises(RuntimeError, match="not ready"):
        await api._fetch_oidc_userinfo("access-token", discovery)


@pytest.mark.asyncio
async def test_oidc_invalid_signatures_algorithms_and_key_selection(oidc_provider, signing_key):
    discovery = oidc_provider["discovery"]
    for token in ("bad", jwt.encode({"sub": "a"}, "x" * 64, algorithm="HS256"),
                  jwt.encode(oidc_provider["claims"], signing_key, algorithm="RS256",
                             headers={"kid": "unknown"})):
        with pytest.raises(web.HTTPUnauthorized):
            await api._validate_oidc_id_token(token, discovery, "nonce")
    for keys in (None, {"keys": None}, {"keys": [{"alg": "RS512"}]}):
        assert api._pick_jwk_key(keys, {"alg": "RS256"}) is None


def test_signed_cookie_tamper_expiry_and_corruption(monkeypatch):
    monkeypatch.setattr(api, "ACCESS_SESSION_SECRET", "test-secret")
    good = api._make_signed_cookie_value({"sub": "me"}, 60)
    assert api._read_signed_cookie_value(good)["sub"] == "me"
    assert api._read_signed_cookie_value(good + "bad") is None
    assert api._read_signed_cookie_value(api._make_signed_cookie_value({}, -1)) is None
    for value in ("plain", "x.x", api._sign_payload([1, 2]), "%%%.$$"):
        assert api._read_signed_cookie_value(value) is None
    raw = b"not json"
    sig = hmac.new(b"test-secret", raw, hashlib.sha256).digest()
    value = base64.urlsafe_b64encode(raw).decode() + "." + api._b64_encode(sig)
    assert api._unsign_payload(value) is None
    monkeypatch.setattr(api, "ACCESS_SESSION_SECRET", "")
    assert api._read_signed_cookie_value(good) is None


def test_auth_rate_limit_and_forwarded_headers(monkeypatch):
    monkeypatch.setattr(api, "_AUTH_RATE_LIMIT", {})
    req = make_mocked_request("GET", "/", headers={"X-Forwarded-For": "1.2.3.4, 5.6.7.8",
                                                   "X-Forwarded-Proto": "https"})
    assert api._request_is_secure(req)
    assert api._client_ip(req) == "1.2.3.4"
    api._enforce_auth_rate_limit(req, "test", limit=1)
    with pytest.raises(web.HTTPTooManyRequests):
        api._enforce_auth_rate_limit(req, "test", limit=1)


@pytest.mark.asyncio
async def test_oidc_only_login_redirects_and_guard(web_client, oidc_provider, monkeypatch):
    for method in ("GET", "POST"):
        response = await web_client.request(method, "/login", allow_redirects=False)
        assert response.status == 302 and response.headers["Location"].startswith("/auth/login")
    req = make_mocked_request("GET", "/api/topics")
    with pytest.raises(web.HTTPUnauthorized):
        api._require_oidc_admin(req)
    req = make_mocked_request("GET", "/", headers={"Accept": "text/html"})
    with pytest.raises(web.HTTPFound):
        api._require_oidc_admin(req)
    with pytest.raises(web.HTTPUnauthorized):
        api._raise_admin_html_unauthorized()
    monkeypatch.setattr(api, "_is_session_valid", lambda request: True)
    assert api._require_oidc_admin(req) == ""
    monkeypatch.setattr(api, "OIDC_ENABLED", False)
    with pytest.raises(web.HTTPServiceUnavailable):
        api._require_oidc_admin(req)
    monkeypatch.setattr(api, "ACCESS_TOKEN", "")
    with pytest.raises(web.HTTPUnauthorized):
        api._raise_admin_html_unauthorized()


@pytest.mark.asyncio
async def test_oidc_callback_requires_token_endpoint(web_client, oidc_provider):
    state = await start_login(web_client, oidc_provider)
    api._OIDC_DISCOVERY_CACHE.pop("token_endpoint")
    response = await web_client.get(f"/auth/callback?state={state}&code=valid")
    assert response.status == 503


def test_session_rejects_bad_expiration(monkeypatch):
    monkeypatch.setattr(api, "ACCESS_SESSION_SECRET", "secret")
    assert api._read_signed_cookie_value(api._sign_payload({"exp": "bad"})) is None
