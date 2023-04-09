import asyncio
import collections
import contextlib
import typing


class QueueClosedError(Exception):
    """Raised when trying to use a closed queue."""


class QueueEmptyError(Exception):
    """Raised if queue is empty."""


class QueueFullError(Exception):
    """Raised if queue is full."""


class Queue:
    """Asyncio queue which implements AsyncIterable and can be closed.

    Interface and implementation are based on `asyncio.Queue`.

    If `maxsize` is less than or equal to zero, the queue size is infinite.

    Args:
        maxsize: maximum number of items in the queue

    Example::

        queue = Queue(maxsize=1)

        async def producer():
            for i in range(4):
                await queue.put(i)
            queue.close()

        async def consumer():
            result = 0
            async for i in queue:
                result += i
            return result

        asyncio.ensure_future(producer())
        result = await consumer()
        assert result == 6

    """

    def __init__(self,
                 maxsize: int = 0):
        self._maxsize = maxsize
        self._queue = collections.deque()
        self._getters = collections.deque()
        self._putters = collections.deque()
        self._closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.get()

        except QueueClosedError:
            raise StopAsyncIteration

    def __str__(self):
        return (f'<{type(self).__name__}'
                f' _closed={self._closed} '
                f' _queue={list(self._queue)}>')

    def __len__(self):
        return len(self._queue)

    @property
    def maxsize(self) -> int:
        """Maximum number of items in the queue."""
        return self._maxsize

    @property
    def is_closed(self) -> bool:
        """Is queue closed."""
        return self._closed

    def empty(self) -> bool:
        """``True`` if queue is empty, ``False`` otherwise."""
        return not self._queue

    def full(self) -> bool:
        """``True`` if queue is full, ``False`` otherwise."""
        if self._maxsize > 0:
            return len(self._queue) >= self._maxsize

        return False

    def qsize(self) -> int:
        """Number of items currently in the queue."""
        return len(self._queue)

    def close(self):
        """Close the queue."""
        if self._closed:
            return

        self._closed = True
        self._wakeup_all(self._putters)
        self._wakeup_next(self._getters)

    def get_nowait(self) -> typing.Any:
        """Return an item if one is immediately available, else raise
        `QueueEmptyError`.

        Raises:
            QueueEmptyError

        """
        if self.empty():
            raise QueueEmptyError()

        item = self._queue.popleft()
        self._wakeup_next(self._putters)
        return item

    def put_nowait(self, item: typing.Any):
        """Put an item into the queue without blocking.

        If no free slot is immediately available, raise `QueueFullError`.

        Raises:
            QueueFullError

        """
        if self._closed:
            raise QueueClosedError()

        if self.full():
            raise QueueFullError()

        self._queue.append(item)
        self._wakeup_next(self._getters)

    async def get(self) -> typing.Any:
        """Remove and return an item from the queue.

        If queue is empty, wait until an item is available.

        Raises:
            QueueClosedError

        """
        loop = asyncio.get_running_loop()

        while self.empty():
            if self._closed:
                self._wakeup_all(self._getters)
                raise QueueClosedError()

            getter = loop.create_future()
            self._getters.append(getter)

            try:
                await getter

            except BaseException:
                getter.cancel()

                with contextlib.suppress(ValueError):
                    self._getters.remove(getter)

                if not getter.cancelled():
                    if not self.empty() or self._closed:
                        self._wakeup_next(self._getters)

                raise

        return self.get_nowait()

    async def put(self, item: typing.Any):
        """Put an item into the queue.

        If the queue is full, wait until a free slot is available before adding
        the item.

        Raises:
            QueueClosedError

        """
        loop = asyncio.get_running_loop()

        while not self._closed and self.full():
            putter = loop.create_future()
            self._putters.append(putter)

            try:
                await putter

            except BaseException:
                putter.cancel()

                with contextlib.suppress(ValueError):
                    self._putters.remove(putter)

                if not self.full() and not putter.cancelled():
                    self._wakeup_next(self._putters)

                raise

        return self.put_nowait(item)

    async def get_until_empty(self) -> typing.Any:
        """Empty the queue and return the last item.

        If queue is empty, wait until at least one item is available.

        Raises:
            QueueClosedError

        """
        item = await self.get()

        while not self.empty():
            item = self.get_nowait()

        return item

    def get_nowait_until_empty(self) -> typing.Any:
        """Empty the queue and return the last item if at least one
        item is immediately available, else raise `QueueEmptyError`.

        Raises:
            QueueEmptyError

        """
        item = self.get_nowait()

        while not self.empty():
            item = self.get_nowait()

        return item

    def _wakeup_next(self, waiters):
        while waiters:
            waiter = waiters.popleft()

            if not waiter.done():
                waiter.set_result(None)
                break

    def _wakeup_all(self, waiters):
        while waiters:
            waiter = waiters.popleft()

            if not waiter.done():
                waiter.set_result(None)
