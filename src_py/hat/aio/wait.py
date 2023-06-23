import asyncio
import typing

from hat.aio.group import Group
from hat.aio.misc import call_on_done, uncancellable


class CancelledWithResultError(asyncio.CancelledError):
    """CancelledError with associated result or exception"""

    def __init__(self,
                 result: typing.Any | None,
                 exception: BaseException | None):
        super().__init__()
        self.__result = result
        self.__exception = exception

    @property
    def result(self) -> typing.Any | None:
        """Result"""
        return self.__result

    @property
    def exception(self) -> BaseException | None:
        return self.__exception


async def wait_for(obj: typing.Awaitable,
                   timeout: float
                   ) -> typing.Any:
    """"Wait for the awaitable object to complete, with timeout.

    Implementation `asyncio.wait_for` that ensure propagation of
    CancelledError.

    If task is cancelled with objects's result available, instead of
    returning result, this implementation raises `CancelledWithResultError`.

    """
    group = Group(log_exceptions=False)
    group.spawn(call_on_done, asyncio.sleep(timeout), group.close)

    task = group.wrap(obj)
    group.spawn(call_on_done, asyncio.shield(task), group.close)

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
        if task.cancelled():
            raise exc

        result = None if task.exception() else task.result()
        raise CancelledWithResultError(result, task.exception()) from exc

    if task.cancelled():
        raise asyncio.TimeoutError()

    return task.result()
