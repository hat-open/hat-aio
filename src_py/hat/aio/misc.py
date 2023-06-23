import asyncio
import collections
import collections.abc
import contextlib
import inspect
import signal
import sys
import typing


T = typing.TypeVar('T')


async def first(xs: typing.AsyncIterable[T],
                fn: typing.Callable[[T], typing.Any] = lambda _: True,
                default: T | None = None
                ) -> T | None:
    """Return the first element from async iterable that satisfies
    predicate `fn`, or `default` if no such element exists.

    Result of predicate `fn` can be of any type. Predicate is satisfied if it's
    return value is truthy.

    Args:
        xs: async collection
        fn: predicate
        default: default value

    Example::

        async def async_range(x):
            for i in range(x):
                await asyncio.sleep(0)
                yield i

        assert await first(async_range(3)) == 0
        assert await first(async_range(3), lambda x: x > 1) == 2
        assert await first(async_range(3), lambda x: x > 2) is None
        assert await first(async_range(3), lambda x: x > 2, 123) == 123

    """
    async for i in xs:
        if fn(i):
            return i

    return default


async def uncancellable(obj: typing.Awaitable,
                        raise_cancel: bool = True
                        ) -> typing.Any:
    """Uncancellable execution of a awaitable object.

    Object is scheduled as task, shielded and its execution cannot be
    interrupted.

    If `raise_cancel` is `True` and the object gets canceled,
    `asyncio.CancelledError` is reraised after the Future finishes.

    Warning:
        If `raise_cancel` is `False`, this method suppresses
        `asyncio.CancelledError` and stops its propagation. Use with
        caution.

    Args:
        obj: awaitable object
        raise_cancel: raise CancelledError flag

    Returns:
        object execution result

    """
    exception = None
    loop = asyncio.get_running_loop()

    if asyncio.iscoroutine(obj):
        task = loop.create_task(obj)

    else:
        task = asyncio.ensure_future(obj, loop=loop)

    while not task.done():
        try:
            await asyncio.shield(task)

        except asyncio.CancelledError as e:
            if raise_cancel:
                exception = e

        except Exception:
            pass

    if exception:
        raise exception

    return task.result()


# TODO: AsyncCallable rewrite needed
class _AsyncCallableType(type(typing.Callable), _root=True):

    def __init__(self):
        super().__init__(origin=collections.abc.Callable,
                         nparams=(..., typing.Awaitable | None))

    def __getitem__(self, params):
        if len(params) == 2:
            params = params[0], params[1] | typing.Awaitable[params[1]]

        return super().__getitem__(params)


AsyncCallable = _AsyncCallableType()
"""Async callable"""


async def call(fn: AsyncCallable, *args, **kwargs) -> typing.Any:
    """Call a function or a coroutine (or other callable object).

    Call the `fn` with `args` and `kwargs`. If result of this call is
    awaitable, it is awaited and returned. Otherwise, result is immediately
    returned.

    Args:
        fn: callable object
        args: additional positional arguments
        kwargs: additional keyword arguments

    Returns:
        awaited result or result

    Example:

        def f1(x):
            return x

        def f2(x):
            f = asyncio.Future()
            f.set_result(x)
            return f

        async def f3(x):
            return x

        assert 'f1' == await hat.aio.call(f1, 'f1')
        assert 'f2' == await hat.aio.call(f2, 'f2')
        assert 'f3' == await hat.aio.call(f3, 'f3')

    """
    result = fn(*args, **kwargs)

    if inspect.isawaitable(result):
        result = await result

    return result


async def call_on_cancel(fn: AsyncCallable, *args, **kwargs) -> typing.Any:
    """Call a function or a coroutine when canceled.

    When canceled, `fn` is called with `args` and `kwargs` by using
    `call` coroutine.

    Args:
        fn: function or coroutine
        args: additional function arguments
        kwargs: additional function keyword arguments

    Returns:
        function result

    Example::

        f = asyncio.Future()
        group = Group()
        group.spawn(call_on_cancel, f.set_result, 123)
        assert not f.done()
        await group.async_close()
        assert f.result() == 123

    """
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.get_running_loop().create_future()

    return await call(fn, *args, *kwargs)


async def call_on_done(f: typing.Awaitable,
                       fn: AsyncCallable,
                       *args, **kwargs
                       ) -> typing.Any:
    """Call a function or a coroutine when awaitable is done.

    When `f` is done, `fn` is called with `args` and `kwargs` by using
    `call` coroutine.

    If this coroutine is canceled before `f` is done, `f` is canceled and `fn`
    is not called.

    If this coroutine is canceled after `f` is done, `fn` call is canceled.

    Args:
        f: awaitable future
        fn: function or coroutine
        args: additional function arguments
        kwargs: additional function keyword arguments

    Returns:
        function result

    Example::

        f = asyncio.Future()
        group = Group()
        group.spawn(call_on_done, f, group.close)
        assert group.is_open
        f.set_result(None)
        await group.wait_closed()
        assert group.is_closed

    """
    with contextlib.suppress(Exception):
        await f

    return await call(fn, *args, *kwargs)


def init_asyncio(policy: asyncio.AbstractEventLoopPolicy | None = None):
    """Initialize asyncio.

    Sets event loop policy (if ``None``, instance of
    `asyncio.DefaultEventLoopPolicy` is used).

    On Windows, `asyncio.WindowsProactorEventLoopPolicy` is used as default
    policy.

    """

    def get_default_policy():
        if sys.platform == 'win32':
            return asyncio.WindowsProactorEventLoopPolicy()

        # TODO: evaluate usage of uvloop
        # with contextlib.suppress(ModuleNotFoundError):
        #     import uvloop
        #     return uvloop.EventLoopPolicy()

        return asyncio.DefaultEventLoopPolicy()

    asyncio.set_event_loop_policy(policy or get_default_policy())


def run_asyncio(future: typing.Awaitable, *,
                handle_signals: bool = True,
                loop: asyncio.AbstractEventLoop | None = None
                ) -> typing.Any:
    """Run asyncio loop until the `future` is completed and return the result.

    If `handle_signals` is ``True``, SIGINT and SIGTERM handlers are
    temporarily overridden. Instead of raising ``KeyboardInterrupt`` on every
    signal reception, Future is canceled only once. Additional signals are
    ignored. On Windows, SIGBREAK (CTRL_BREAK_EVENT) handler is also
    overridden.

    If `loop` is set to ``None``, new event loop is created and set
    as thread's default event loop. Newly created loop is closed when this
    coroutine returns. Running tasks or async generators, other than provided
    `future`, are not canceled prior to loop closing.

    On Windows, asyncio loop gets periodically woken up (every 0.5 seconds).

    Args:
        future: future or coroutine
        handle_signals: handle signals flag
        loop: event loop

    Returns:
        future's result

    Example::

        async def run():
            await asyncio.sleep(0)
            return 123

        result = run_asyncio(run())
        assert result == 123

    """
    close_loop = loop is None
    if loop is None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    task = asyncio.ensure_future(future, loop=loop)

    if sys.platform == 'win32':

        async def task_wrapper(task):
            try:
                while not task.done():
                    await asyncio.wait([task], timeout=0.5)
            except asyncio.CancelledError:
                task.cancel()
            return await task

        task = asyncio.ensure_future(task_wrapper(task), loop=loop)

    if not handle_signals:
        return loop.run_until_complete(task)

    canceled = False
    signalnums = [signal.SIGINT, signal.SIGTERM]
    if sys.platform == 'win32':
        signalnums += [signal.SIGBREAK]

    def signal_handler(*args):
        nonlocal canceled
        if canceled:
            return
        loop.call_soon_threadsafe(task.cancel)
        canceled = True

    handlers = {signalnum: signal.getsignal(signalnum) or signal.SIG_DFL
                for signalnum in signalnums}
    for signalnum in signalnums:
        signal.signal(signalnum, signal_handler)

    try:
        return loop.run_until_complete(task)

    finally:
        for signalnum, handler in handlers.items():
            signal.signal(signalnum, handler)
        if close_loop:
            loop.close()
