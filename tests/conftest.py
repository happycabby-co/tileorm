import asyncio
import subprocess
import time
import pytest

from pyle38 import Tile38
import pytest_asyncio

# Port 9852 to avoid clashing with a local Tile38 on default 9851
TILE38_URL = "redis://localhost:9852"
TILE38_READY_TIMEOUT = 15.0
TILE38_READY_POLL_INTERVAL = 0.2


def _wait_for_tile38(url: str = TILE38_URL, timeout: float = TILE38_READY_TIMEOUT) -> None:
    """Poll until Tile38 accepts connections and responds to PING."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async def check():
                client = Tile38(url)
                await client.ping()
                await client.quit()

            asyncio.run(check())
            return
        except Exception:
            time.sleep(TILE38_READY_POLL_INTERVAL)
    raise TimeoutError(f"Tile38 at {url} did not become ready within {timeout}s")


@pytest.fixture(scope="session")
def tile38_running():
    subprocess.run(["docker", "compose", "up", "-d"], check=True)
    _wait_for_tile38()
    yield
    subprocess.run(["docker", "compose", "down"], check=True)


@pytest_asyncio.fixture
async def tile38(tile38_running):
    """Tile38 client; DB is flushed after each test so tests can reuse IDs."""
    client = Tile38(TILE38_URL)
    await client.readonly(False)
    try:
        yield client
    finally:
        try:
            await client.flushdb()
            await client.readonly(False)
        except Exception:
            await client.flushdb()
        await client.quit()
