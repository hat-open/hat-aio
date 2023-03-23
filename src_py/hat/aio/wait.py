import asyncio
import typing

from hat.aio.group import Group
from hat.aio.misc import call_on_done, uncancellable


class CancelledWithResultError(asyncio.CancelledError):
    """CancelledError with associated result or exception"""

    def __init__(self,
                 result: typing.Any,
                 exception: typing.Optional[BaseException]):
        super().__init__()
        self.__result = result
        self.__exception = exception

    @property
    def result(self) -> typing.Any:
        """Result"""
        return self.__result

    @property
    def exception(self) -> typing.Optional[BaseException]:
        return self.__exception


async def wait_for(f: typing.Awaitable,
                   timeout: float
                   ) -> typing.Any:
    """"Wait for the single Future or coroutine to complete, with timeout.

    Implementation `asyncio.wait_for` that ensure propagation of
    CancelledError.

    If task is cancelled with future's result available, instead of
    returning result, this implementation raises `CancelledWithResultError`.

    """
    group = Group(log_exceptions=False)
    group.spawn(call_on_done, asyncio.sleep(timeout), group.close)

    f = group.wrap(f)
    group.spawn(call_on_done, asyncio.shield(f), group.close)

    exc = None

    try:
        await group.wait_closing()
    except asyncio.CancelledError as e:
        exc = e

    try:
        await uncancellable(group.async_close())
    except asyncio.CancelledError as e:
        exc = e

    if exc:
        if f.cancelled():
            raise exc
        result = None if f.exception() else f.result()
        raise CancelledWithResultError(result, f.exception()) from exc

    if f.cancelled():
        raise asyncio.TimeoutError()

    return f.result()
