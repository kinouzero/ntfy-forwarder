import aiohttp

http_session = None

async def create_http_session():

    global http_session

    if http_session is None:
        http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(
                limit=100,
                ttl_dns_cache=300,
            ),
        )

    return http_session


def get_http_session():
    return http_session


async def close_http_session():

    global http_session

    if http_session:
        await http_session.close()
        http_session = None
