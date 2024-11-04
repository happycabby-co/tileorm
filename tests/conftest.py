import subprocess
import time
import pytest

from pyle38 import Tile38
import pytest_asyncio


@pytest.fixture(scope="session")
def tile38_running():
    subprocess.run(["docker", "compose", "up", "-d"], check=True)
    time.sleep(1)
    yield
    subprocess.run(["docker", "compose", "down"], check=True)


@pytest.fixture()
def create_tile38(request, event_loop, tile38_running):
    async def f(url: str = "redis://localhost:9852"):
        tile38 = Tile38(url)
        # make sure to reset readonly
        await tile38.readonly(False)

        def teardown():
            async def ateardown():
                try:
                    await tile38.flushdb()
                    await tile38.readonly(False)
                # TODO: find explicit exception
                except Exception:
                    await tile38.flushdb()

                await tile38.quit()

            if event_loop.is_running():
                event_loop.create_task(ateardown())
            else:
                event_loop.run_until_complete(ateardown())

        request.addfinalizer(teardown)
        return tile38

    return f


@pytest_asyncio.fixture
async def tile38(create_tile38):
    yield await create_tile38()
