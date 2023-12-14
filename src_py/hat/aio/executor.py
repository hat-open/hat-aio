import asyncio
import concurrent.futures
import functools
import typing

from hat.aio.group import Resource, Group
from hat.aio.misc import call_on_cancel, uncancellable


class Executor(Resource):
    """Executor wrapping `asyncio.loop.run_in_executor`.

    Wrapped executor is created from `executor_cls` with provided `args`.

    If `wait_futures` is ``True``, executor will be closed once all running
    tasks finishes.

    `log_exceptions` is delegated to `async_group`.

    Args:
        args: executor args
        executor_cls: executor class
        log_exceptions: log exceptions

    Example::

        executor1 = Executor()
        executor2 = Executor()
        tid1 = await executor1.spawn(threading.get_ident)
        tid2 = await executor2.spawn(threading.get_ident)
        assert tid1 != tid2

    """

    def __init__(self,
                 *args,
                 executor_cls: typing.Type[concurrent.futures.Executor] = concurrent.futures.ThreadPoolExecutor,  # NOQA
                 log_exceptions: bool = True,
                 wait_futures: bool = True):
        self._wait_futures = wait_futures
        self._executor = executor_cls(*args)
        self._loop = asyncio.get_running_loop()
        self._async_group = Group(log_exceptions)

        self.async_group.spawn(call_on_cancel, self._executor.shutdown, False)

    @property
    def async_group(self):
        """Async group"""
        return self._async_group

    def spawn(self,
              fn: typing.Callable,
              *args, **kwargs
              ) -> asyncio.Task:
        """Spawn new task"""
        return self.async_group.spawn(self._spawn, fn, args, kwargs)

    async def _spawn(self, fn, args, kwargs):
        func = functools.partial(fn, *args, **kwargs)
        coro = self._loop.run_in_executor(self._executor, func)

        if self._wait_futures:
            coro = uncancellable(coro)

        return await coro


def create_executor(*args,
                    executor_cls: typing.Type[concurrent.futures.Executor] = concurrent.futures.ThreadPoolExecutor,  # NOQA
                    loop: asyncio.AbstractEventLoop | None = None
                    ) -> typing.Callable[..., typing.Awaitable]:
    """Create `asyncio.loop.run_in_executor` wrapper.

    Wrapped executor is created from `executor_cls` with provided `args`.

    This function returns coroutine that takes a function and its arguments,
    executes the function in executor and returns the result.

    Args:
        args: executor args
        executor_cls: executor class
        loop: asyncio loop

    Returns:
        executor coroutine

    Example::

        executor1 = create_executor()
        executor2 = create_executor()
        tid1 = await executor1(threading.get_ident)
        tid2 = await executor2(threading.get_ident)
        assert tid1 != tid2

    """
    executor = executor_cls(*args)

    async def executor_wrapper(func, /, *args, **kwargs):
        _loop = loop or asyncio.get_running_loop()
        fn = functools.partial(func, *args, **kwargs)
        return await _loop.run_in_executor(executor, fn)

    return executor_wrapper
