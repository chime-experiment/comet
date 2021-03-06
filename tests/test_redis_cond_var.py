import asyncio
import aioredis
import pytest

from comet.redis_async_locks import Lock, Condition


import logging

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.asyncio
async def test_cond_variable():
    """Test that a single condition variable works as expected."""
    redis = await aioredis.create_pool(
        ("127.0.0.1", 6379), encoding="utf-8", maxsize=30
    )

    lock = await Lock.create(redis, "testlock")
    cond = await Condition.create(lock, "testcond")

    await redis.execute("del", "testkey")

    # Create a task that waits on the variable and reads a key. This key won't
    # exist until after the task has been notified
    async def task():
        r = await cond.acquire()
        await cond.wait()
        assert await r.execute("get", "testkey") == "1"
        await cond.release()

    # Create tasks
    tasks = [asyncio.create_task(task()) for i in range(20)]
    await asyncio.sleep(0.2)

    # Check that they are blocked
    for t in tasks:
        assert not t.done()

    # Set the key and notify
    r = await cond.acquire()
    await r.execute("set", "testkey", 1)
    await cond.notify_all()
    await cond.release()

    # Sleep to allow the tasks to finish up
    await asyncio.sleep(0.2)

    # Check that the tasks are finished and exited normally
    for t in tasks:
        assert t.done()
        assert t.exception() is None

    redis.close()
    await redis.wait_closed()


@pytest.mark.asyncio
async def test_cond_context_manager():
    """Test that a single condition variable works as expected."""
    redis = await aioredis.create_pool(
        ("127.0.0.1", 6379), encoding="utf-8", maxsize=30
    )

    lock = await Lock.create(redis, "testlock")
    cond = await Condition.create(lock, "testcond")

    await redis.execute("del", "testkey")

    # Create a task that waits on the variable and reads a key. This key won't
    # exist until after the task has been notified
    async def task():
        async with cond as r:
            await cond.wait()
            assert await r.execute("get", "testkey") == "1"

    # Create tasks
    tasks = [asyncio.create_task(task()) for i in range(20)]
    await asyncio.sleep(0.2)

    # Check that they are blocked
    for t in tasks:
        assert not t.done()

    # Set the key and notify
    async with cond as r:
        await r.execute("set", "testkey", 1)
        await cond.notify_all()

    # Give some time for tasks to finish
    await asyncio.sleep(0.2)

    # Check that the tasks are finished and exited normally
    for t in tasks:
        assert t.exception() is None
        assert t.done()

    redis.close()
    await redis.wait_closed()


@pytest.mark.asyncio
async def test_cond_two_variables():
    """Similar to the above test, but check that two condition variables can
    work at the same time."""

    redis = await aioredis.create_pool(
        ("127.0.0.1", 6379), encoding="utf-8", maxsize=45
    )

    lock = await Lock.create(redis, "testlock")
    cond_a = await Condition.create(lock, "testcond_a")
    cond_b = await Condition.create(lock, "testcond_b")

    redis.execute("del", "testkey_a")
    redis.execute("del", "testkey_b")

    async def task_a():
        async with cond_a as r:
            await cond_a.wait()
            assert await r.execute("get", "testkey_a") == "1"

    async def task_b():
        async with cond_b as r:
            await cond_b.wait()
            assert await r.execute("get", "testkey_b") == "1"

    tasks_a = [asyncio.create_task(task_a()) for i in range(20)]
    tasks_b = [asyncio.create_task(task_b()) for i in range(20)]

    await asyncio.sleep(0.2)

    for t in tasks_a + tasks_b:
        assert not t.done()

    async with cond_a as r:
        await r.execute("set", "testkey_a", 1)
        await cond_a.notify_all()

    # Wait for task A's to finish
    await asyncio.sleep(0.2)

    for t in tasks_a:
        assert t.done()
        assert t.exception() is None

    async with cond_b as r:
        await r.execute("set", "testkey_b", 1)
        await cond_b.notify_all()

    # Wait for task B's to finish
    await asyncio.sleep(0.2)

    for t in tasks_b:
        assert t.done()
        assert t.exception() is None

    redis.close()
    await redis.wait_closed()
