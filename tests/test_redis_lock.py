import os
import asyncio
import aioredis
import pytest

import logging

logging.basicConfig(level=logging.DEBUG)

from comet.redis_async_locks import Lock

REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
REDIS_HOST = os.environ.get("REDIS_HOST", "0.0.0.0")
REDIS_URL = "redis://{0}:{1}".format(REDIS_HOST, REDIS_PORT)


@pytest.mark.asyncio
async def test_lock():
    """Test that the locks work when called directly."""

    redis = aioredis.from_url(REDIS_URL, encoding="utf-8")
    lock = await Lock.create(redis, "test1")

    task1_end = False

    async def task1():
        r = await lock.acquire()
        await r.lpush("testlist1", 1)
        # Block until told to end
        while not task1_end:
            await asyncio.sleep(0.05)
        await lock.release()

    async def task2():
        r = await lock.acquire()
        assert await r.execute_command("del", "testlist1") == 1

    # Create and start up task1
    task1_future = asyncio.create_task(task1())
    await asyncio.sleep(0.2)

    # Start task2, and check that it is blocked
    task2_future = asyncio.create_task(task2())
    await asyncio.sleep(0.2)
    assert not task1_future.done()
    assert not task2_future.done()

    # Tell task1 to finish and check that both tasks finish
    task1_end = True
    await asyncio.sleep(0.2)
    assert task1_future.exception() is None
    assert task2_future.exception() is None
    assert task1_future.done()
    assert task2_future.done()

    await redis.close()


#    await redis.wait_closed()


@pytest.mark.asyncio
async def test_lock_manager():
    """Test that the locks work when used as a context manager."""

    redis = aioredis.from_url(REDIS_URL, encoding="utf-8")
    lock = await Lock.create(redis, "test1")

    task1_end = False

    async def task1():
        async with lock as r:
            await r.lpush("testlist1", 1)
            while not task1_end:
                await asyncio.sleep(0.05)

    async def task2():
        async with lock as r:
            assert await r.execute_command("del", "testlist1") == 1

    # Create and start up task1
    task1_future = asyncio.create_task(task1())
    await asyncio.sleep(0.2)

    assert await lock.locked()

    # Start task2, and check that it is blocked
    task2_future = asyncio.create_task(task2())
    await asyncio.sleep(0.2)
    assert not task1_future.done()
    assert not task2_future.done()
    assert await lock.locked()

    # Tell task1 to finish and check that both tasks finish
    task1_end = True
    await asyncio.sleep(0.2)
    assert task1_future.exception() is None
    assert task2_future.exception() is None
    assert task1_future.done()
    assert task2_future.done()
    assert not await lock.locked()

    # Test that the lock entries are removed when it is closed
    await lock.close()
    assert not await redis.execute_command("exists", lock.lockname)

    await redis.close()


#    await redis.wait_closed()


@pytest.mark.asyncio
async def test_large():
    """Test that the locks work when used as a context manager."""

    key = "test1"
    ntask = 256

    # Deliberately set a number of simultaneous connections much smaller than
    # the number of tasks to test connection starvation
    redis = aioredis.from_url(REDIS_URL, encoding="utf-8")
    # Create the lock and reset the test value
    lock = await Lock.create(redis, key)
    await redis.execute_command("del", key)

    # Perform a non-atomic increment of a variable in redis
    # This is obviously stupid except for testing the locking
    async def task():
        # with await redis as r:  ## Use this to see how badly the test fails
        async with lock as r:
            val = await r.get(key)
            val = int(val) if val is not None else 0
            # Deliberately sleep to give other tasks a chance to break things
            # (if the lock wasn't here)
            await asyncio.sleep(0.01)
            await r.set(key, val + 1)

    tasks = [task() for _ in range(ntask)]
    await asyncio.gather(*tasks)

    val = await redis.get(key)
    assert int(val) == ntask

    await redis.close()
