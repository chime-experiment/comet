"""Condition variable using redis."""
import asyncio
import aioredis
import logging
import asyncio as CancelledError

logger = logging.getLogger(__name__)


class LockError(Exception):
    """An error that happened while using a lock."""

    def __init__(self, message: str):
        """
        Error with sync redis lock.

        Parameters
        ----------
        message : str
            Description of the error.
        """
        self.message = message


class Lock:
    """Async context manager for redis lock.

    This lock has a similar API to `asyncio.Lock`. The main difference is
    that acquiring the lock returns an exclusive `redis` connection for use
    by the locking task. As this is an open and active connection it can and
    should be used within the locked region to prevent other tasks starving
    the locking task of connections preventing it from unlocking the region.

    The lock requires a minimum of one free connection to function.
    """

    def __init__(self, redis, name):
        """Create the lock.

        This should probably not be called directly. Use `Lock.create`
        instead.
        """
        self.name = name
        self.redis = redis
        self._redis_conn = None

    @classmethod
    async def create(cls, redis, name):
        """Create a distributed Lock using redis.

        Parameters
        ----------
        redis : aioredis.ConnectionsPool
            A connections pool instance that will be used to connect to the redis database.
        name : str
            A name for the lock. This must be unique (i.e. not clash with
            other locks), and be set across all processes that want to use
            the same lock.

        Returns
        -------
        lock : Lock
            The created lock.
        """
        self = cls(redis, name)

        await redis.execute(
            "eval",
            "redis.call('del', KEYS[1]); redis.call('lpush', KEYS[1], '1')",
            1,
            self.lockname,
        )

        return self

    async def close(self):
        """Clean up the database entries for the lock.

        This will acquire the lock before removing it. Raises a LockError if the lock
        can't get acquired.
        """
        r = await self.acquire(no_block=True)
        if r is None:
            raise LockError("Failure closing lock: Can't acquire lock.")
        self.redis.release(self._redis_conn)
        self.redis = None
        logger.debug("Closed lock {}".format(self.name))

    @property
    def lockname(self):
        """Name of the lock variable."""
        # TODO: mangle to avoid name clashes
        return f"lock_{self.name}"

    # TODO: can we do this synchronously?
    async def locked(self):
        """Tells if the lock is already acquired."""
        return int(await self.redis.execute("llen", self.lockname)) == 0

    async def acquire(self, r=None, no_block=False):
        """Acquire the lock.

        Parameters
        ----------
        r : aioredis.Connection, optional
            A pre-existing connection to the redis database. If not set, one
            will be claimed from the pool (default). This can be used to
            re-use connections. Warning, if this happens you almost certainly
            want to use `.release(close=False)` to ensure this isn't closed by
            the lock when it is released.
        no_block : bool
            Turn on try_lock mode: Instead of blocking, just return `None` in case the
            lock can't directly be acquired.

        Returns
        -------
        r : aioredis.Connection
            A connection to use for accessing redis while in the locked
            region. This in a exclusive connection to guarantee that the
            worker holding the lock cannot be starved of access by other
            workers using all the redis connections.
        """
        # Acquire a connection to perform the blocking operation on the database
        logger.debug(f"Acquiring lock {self.name}.")
        if r is None:
            if self.redis is None:
                raise LockError(
                    f"Failure acquiring lock: {self.name} (No redis connection pool)"
                )
            r = await self.redis.acquire()
        if no_block:
            if await r.execute("lpop", self.lockname) != "1":
                return None
        else:
            if (await r.execute("blpop", self.lockname, 0)) != [self.lockname, "1"]:
                raise LockError(
                    f"Failure acquiring lock: {self.name} (unexpected value in redis lock)"
                )

        # Check there is no active connection (there shouldn't be, this is just a consistency check)
        if self._redis_conn is not None:
            raise LockError(
                f"Failure acquiring lock: {self.name} (connection not cleared)"
            )

        # Now we hold the lock, we can set the internal connection copy
        self._redis_conn = r
        logger.debug(f"Acquired lock {self.name}.")
        return r

    async def release(self, close=True):
        """Release the lock.

        Parameters
        ----------
        close : boolean, optional
            If True, the redis connection being used by the lock currently is
            closed (default). This should be True if the connection was
            opened by the lock itself, but can be useful to set to False for
            better connection management.
        """
        logger.debug(f"Releasing lock {self.name}")

        # Check we have an active connection
        if (
            not isinstance(self._redis_conn, aioredis.connection.RedisConnection)
            or self._redis_conn.closed
        ):
            raise LockError(
                f"Failure releasing lock: {self.name} (no active redis connection)."
            )

        # Change the internal connection *before* releasing the lock in redis,
        # but keep a reference so that we can still use it in here
        r = self._redis_conn
        self._redis_conn = None

        # Release the lock in redis
        if (await r.execute("lpush", self.lockname, 1)) != 1:
            raise LockError(f"Failure releasing lock: {self.name} (released twice?)")

        # Close our copy of the connection
        if close:
            self.redis.release(r)
        logger.debug(f"Released lock {self.name}.")

    async def __aenter__(self):
        """
        Acquire lock.

        Shielded from cancellation. In case of cancellation, the lock acquisition is
        awaited anyways and then the lock is released.
        """
        task = asyncio.ensure_future(self.acquire())
        try:
            return await asyncio.shield(task)
        except CancelledError:
            logger.debug(
                "Acquisition of lock {} cancelled. Releasing...".format(self.name)
            )
            await self.release()
            raise

    async def __aexit__(self, exc_type, exc, tb):
        """
        Release lock.

        Shielded from cancellation.
        """
        await asyncio.shield(self.release())


class Condition:
    """A redis condition variable.

    This aims to have a similar API to the `asyncio.Condition` except that it
    only supports `notify_all`. Additionally the `.acquire()` or context
    manager will return an exclusive redis connection for use by the locking
    task. Note that in although a `.wait()` call will release and re-acquire
    the lock, the redis connection from the lock will remain valid after the
    `.wait()` has returned.

    This requires a minimum of one available connection in the pool per
    waiting task, and one extra for the notify call because it needs to acquire
    the lock itself.
    """

    def __init__(self, lock, name):
        """Create the condition variable.

        Don't call this directly. Use `Condition.create`.
        """
        self.lock = lock
        self.name = name

        self.locked = lock.locked
        self.acquire = lock.acquire
        self.release = lock.release

    @classmethod
    async def create(cls, lock, name):
        """Create a distributed condition variable using redis.

        Parameters
        ----------
        lock : Lock
            A lock instance.
        name : str
            Name of the condition variable.


        Returns
        -------
        cond : Condition
            The created condition variable.
        """
        self = cls(lock, name)
        await self.lock.redis.execute("hset", "WAITING", self.condname, 0)
        return self

    async def close(self):
        """Clean up the database state of the condition variable.

        This will not do anything to tasks waiting on the variable. These
        should be cleaned up before calling this. It will also not close the
        underlying `Lock`, but does need to acquire it to close the condition
        variable. Raises a LockError if it can't acquire the lock.
        """
        r = await self.lock.acquire(no_block=True)
        if r is None:
            raise LockError("Failed closing condition variable: Can't acquire lock.")
        await r.execute("del", "WAITING")
        await r.execute("del", self.condname)
        await self.lock.release()
        logger.debug("Closed condition variable {}".format(self.name))

    async def __aenter__(self):
        """Acquire lock."""
        return await self.lock.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        """Release lock."""
        await self.lock.release()

    @property
    def condname(self):
        """Get name of the condition variable."""
        return f"cond_{self.name}"

    @property
    def redis(self):
        """Get underlying redis connection pool."""
        return self.lock.redis

    async def notify(self, n=1):
        """Notify one task.

        Not implemented.
        """
        raise NotImplementedError(
            "only notify_all is supported for a redis condition variable."
        )

    async def notify_all(self):
        """Notify all processes waiting for the condition variable."""

        #
        # PSEUDOCODE:
        #
        # if waiting[name] > 0
        #     name.append(1)  # Appends to a list called name
        redis_notify_cond = """
local x = redis.call('SPOP', KEYS[1])
while x do
    redis.call('LPUSH', x, 1)
    x = redis.call('SPOP', KEYS[1])
end
"""
#         redis_notify_cond = """
# if redis.call('hget', 'WAITING', KEYS[1]) ~= 0 then
#     redis.call('lpush', KEYS[1], "1")
# end
#        """
        if not await self.locked():
            raise LockError(
                f"Failure notifying condition {self.name}: lock not acquired."
            )

        # Use the internal redis connection
        task = asyncio.ensure_future(
            self.lock._redis_conn.execute("eval", redis_notify_cond, 1, self.condname)
        )
        try:
            await asyncio.shield(task)
        except CancelledError:
            await task
            raise

    async def wait(self, timeout=0, id_=None):
        """
        Wait for the condition variable to signal.

        Will cancel waiting and raise a TimeoutError after <timeout> seconds. If
        timeout is `0`, it will never cancel waiting.

        Guarantees to hold the lock when it returns. For this some of the coroutines are
        shielded. I.e. the calling client could cancel the request. If the code calling
        this holds the lock, it expects the lock to be still held when wait() returns,
        otherwise it might try to release the lock without holding it.

        Parameters
        ----------
        id : str
            A unique ID to use for the unlocking. If not set, uses a random 16 byte string.
        timeout : int
            Timeout in seconds.

        Raises
        ------
        TimeoutError
            If there was no signal after the number of seconds specified by timeout have
            passed.
        ValueError
            If timeout is not an int.
        """

        # Generate a random ID
        if id_ is None:
            import random
            import string
            id_ = "".join(random.choices(string.ascii_letters + string.digits, k=16))
            print(id_)

        waiting_list_name = f"waiting_{self.condname}_{id_}"

        if not await self.locked():
            raise LockError(
                f"Failure waiting condition {self.name}: lock not acquired at start."
            )

        if not isinstance(timeout, int):
            raise ValueError(
                "Parameter timeout is of type {} (expected int).".format(type(timeout))
            )

        # Save a reference to the connection so that we can preserve it through the release/acquire cycle
        r = self.lock._redis_conn

        if timeout < 0:
            raise TimeoutError

        # register as a waiting process
        #
        # PSEUDOCODE:
        #
        # waiting = dict()
        # waiting[name] += 1
        # task = asyncio.ensure_future(r.execute("hincrby", "WAITING", self.condname, 1))
        # try:
        #     asyncio.shield(task)
        # except CancelledError:
        #     await task
        #     raise
        await r.execute("sadd", self.condname, waiting_list_name)

        # release the lock while waiting
        cancelled = None
        try:
            # Shield against cancellation
            await asyncio.shield(self.lock.release(close=False))
        except CancelledError as err:
            # In case of cancellation, continue but remember cancellation
            cancelled = err

        # Wait for notification
        #
        # PSEUDOCODE:
        #
        # while(True):
        #     if name:
        #         name.pop()
        timed_out = False
        if not cancelled:
            try:
                # allow this to be cancelled, but catch to reacquire lock etc
                ret = await r.execute("blpop", waiting_list_name, timeout)
            except CancelledError as err:
                # In case of cancellation, continue but remember cancellation
                await r.execute("srem", waiting_list_name)
                cancelled = err
            else:
                if ret is None:
                    timed_out = True

        # reacquire the lock
        try:
            # shield against cancellation
            await asyncio.shield(self.lock.acquire(r))
        except CancelledError as err:
            # In case of cancellation, continue but remember cancellation
            cancelled = err



        # Now we can tell the caller about everything that went wrong
        if timed_out:
            raise TimeoutError
        if cancelled:
            raise cancelled
