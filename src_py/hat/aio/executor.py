import asyncio
import concurrent.futures
import functools
import typing

from hat.aio.group import Resource, Group
from hat.aio.misc import uncancellable


class Executor(Resource):
    """Executor wrapping `asyncio.loop.run_in_executor`.

    Wrapped executor is created from `executor_cls` with provided `args`.

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
                 *args: typing.Any,
                 executor_cls: typing.Type = concurrent.futures.ThreadPoolExecutor,  # NOQA
                 log_exceptions: bool = True):
        self._executor = executor_cls(*args)
        self._async_group = Group(log_exceptions)

    @property
    def async_group(self):
        """Async group"""
        return self._async_group

    def spawn(self,
              fn: typing.Callable,
              *args, **kwargs
              ) -> asyncio.Task:
        """Spawn new task"""
        return self.async_group.spawn(self._spawn, fn, *args, **kwargs)

    async def _spawn(self, fn, *args, **kwargs):
        loop = asyncio.get_running_loop()
        func = functools.partial(fn, *args, **kwargs)
        return await uncancellable(loop.run_in_executor(self._executor, func))


def create_executor(*args: typing.Any,
                    executor_cls: typing.Type = concurrent.futures.ThreadPoolExecutor,  # NOQA
                    loop: typing.Optional[asyncio.AbstractEventLoop] = None
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
