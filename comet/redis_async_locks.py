"""Condition variable using redis."""
import asyncio
import aioredis
import logging

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
        redis : aioredis.ConnectionPool
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
        await redis.delete(self.lockname)
        await redis.lpush(self.lockname, "1")

        return self

    async def close(self):
        """Clean up the database entries for the lock.

        This will acquire the lock before removing it. Raises a LockError if the lock
        can't get acquired.
        """
        r = await self.acquire(no_block=True)
        if r is None:
            raise LockError("Failure closing lock: Can't acquire lock.")
        self.redis = None
        logger.debug("Closed lock {}".format(self.name))

    @property
    def lockname(self):
        """Name of the lock variable."""
        # TODO: mangle to avoid name clashes
        return f"lock_{self.name}"

    @property
    def blockname(self):
        """Name of lock variable, as byte string."""
        return bytes(self.lockname, encoding="utf-8")

    # TODO: can we do this synchronously?
    async def locked(self):
        """Tells if the lock is already acquired."""
        return int(await self.redis.execute_command("llen", self.lockname)) == 0

    async def acquire(self, no_block=False):
        """Acquire the lock.

        Parameters
        ----------
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
        if self.redis is None:
            raise LockError(
                f"Failure acquiring lock: {self.name} (No redis connection available)"
            )
        if no_block:
            if await self.redis.lpop(self.lockname) != b"1":
                return None
        else:
            if (await self.redis.blpop(self.lockname, 0)) != (self.blockname, b"1"):
                raise LockError(
                    f"Failure acquiring lock: {self.name} (unexpected value in redis lock)"
                )

        # Check there is no active connection (there shouldn't be, this is just a consistency check)
        logger.debug(f"Acquired lock {self.name}.")
        return self.redis

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

        # Release the lock in redis
        if (await self.redis.lpush(self.lockname, 1)) != 1:
            raise LockError(f"Failure releasing lock: {self.name} (released twice?)")

        # Close our copy of the connection
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
        except asyncio.CancelledError:
            logger.debug(
                "Acquisition of lock {} cancelled. Releasing...".format(self.name)
            )
            await task
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
        await self.redis.execute_command("hset", "WAITING", self.condname, 0)
        await self.redis.execute_command("del", self.condname)
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
        await r.execute_command("del", "WAITING")
        await r.execute_command("del", self.condname)
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

        # Notify all registered waiters. And remove them from the waiting counter.
        #
        # PSEUDOCODE:
        #
        # for 1 .. waiting[name]
        #     name.append(1)  # Appends to a list called name
        # waiting[name] = 0
        redis_notify_cond = """
for i=1,redis.call('hget', 'WAITING', KEYS[1]) do
    redis.call('lpush', KEYS[1], "1")
end
redis.call('HSET', 'WAITING', KEYS[1], 0)
        """
        if not await self.locked():
            raise LockError(
                f"Failure notifying condition {self.name}: lock not acquired."
            )

        # Use the internal redis connection
        task = asyncio.ensure_future(
            self.lock.redis.execute_command("eval", redis_notify_cond, 1, self.condname)
        )
        # If the request gets cancelled while doing this, we have to make sure to await
        # the shielded task, because directly after, the context manager will release
        # the lock.
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def wait(self, timeout=0):
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

        if not await self.locked():
            raise LockError(
                f"Failure waiting condition {self.name}: lock not acquired at start."
            )

        if not isinstance(timeout, int):
            raise ValueError(
                "Parameter timeout is of type {} (expected int).".format(type(timeout))
            )

        if timeout < 0:
            raise TimeoutError

        # Save any caught CancelledError's in here to let them out again after cleaning
        # up.
        cancelled = None

        # register as a waiting process
        #
        # If this gets cancelled, we want to wait for the shielded task, before we
        # remove our process from the waiting list again.
        #
        # PSEUDOCODE:
        #
        # waiting = dict()
        # waiting[name] += 1
        task = asyncio.ensure_future(self.redis.hincrby("WAITING", self.condname, 1))
        try:
            # Shield against cancellation
            asyncio.shield(task)
        except asyncio.CancelledError as err:
            # In case of cancellation, continue but remember cancellation
            # Wait for the lock acquisition to complete
            await task
            cancelled = err

        # remember if we acquired the lock or got cancelled before
        have_lock = False
        if not cancelled:
            # release the lock while waiting
            task = asyncio.ensure_future(self.lock.release(close=False))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as err:
                await task
                cancelled = err
            finally:
                have_lock = True

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
                ret = await self.redis.blpop(self.condname, timeout)
            except asyncio.CancelledError as err:
                # In case of cancellation, continue but remember cancellation
                cancelled = err
            else:
                if ret is None:
                    timed_out = True

        if have_lock:
            # reacquire the lock
            task = asyncio.ensure_future(self.lock.acquire())
            try:
                # shield against cancellation
                await asyncio.shield(task)
            except asyncio.CancelledError as err:
                # In case of cancellation, wait for lock acquisition and continue but
                # remember cancellation
                await task
                cancelled = err

        # Now we can tell the caller about everything that went wrong
        if cancelled:
            raise cancelled
        if timed_out:
            raise TimeoutError
