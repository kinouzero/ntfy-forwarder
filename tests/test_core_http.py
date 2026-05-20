import pytest

from core import http


@pytest.mark.asyncio
async def test_http_session_lifecycle():
    session = await http.create_http_session()
    assert session is http.get_http_session()

    await http.close_http_session()
    assert http.get_http_session() is None
