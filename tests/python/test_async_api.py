from datetime import datetime, timedelta, timezone
import asyncio

import pytest

import kron


async def wait_until(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition was not met before timeout")


def test_async_start_status_list_and_shutdown(tmp_path):
    async def scenario():
        calls = []

        def task():
            calls.append("ran")

        kron.schedule(
            "async_once",
            fn=task,
            at=datetime.now(timezone.utc) + timedelta(milliseconds=100),
            max_attempts=1,
        )

        await asyncio.wait_for(kron.astart(data_dir=str(tmp_path)), timeout=3.0)
        try:
            ticked = False

            async def ticker():
                nonlocal ticked
                await asyncio.sleep(0)
                ticked = True

            await asyncio.gather(ticker(), wait_until(lambda: calls == ["ran"]))
            assert ticked is True

            status = await asyncio.wait_for(kron.astatus("async_once"), timeout=3.0)
            assert status["last_status"] == "OK"

            timers = await asyncio.wait_for(kron.alist(), timeout=3.0)
            assert "async_once" in {timer["id"] for timer in timers}
        finally:
            await asyncio.wait_for(kron.ashutdown(), timeout=3.0)
            await asyncio.wait_for(kron.ashutdown(), timeout=3.0)

    asyncio.run(scenario())


def test_async_double_start_raises_runtime_error(tmp_path):
    async def scenario():
        await asyncio.wait_for(kron.astart(data_dir=str(tmp_path)), timeout=3.0)
        try:
            with pytest.raises(RuntimeError, match="already started"):
                await asyncio.wait_for(kron.astart(data_dir=str(tmp_path)), timeout=3.0)
        finally:
            await asyncio.wait_for(kron.ashutdown(), timeout=3.0)

    asyncio.run(scenario())
