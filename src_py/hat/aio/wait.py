from collections.abc import Awaitable
import asyncio
import typing

from hat.aio.group import Group
from hat.aio.misc import call_on_done, uncancellable, CancelledWithResultError


T = typing.TypeVar('T')


async def wait_for(obj: Awaitable[T],
                   timeout: float
                   ) -> T:
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

    cancelled = False

    try:
        await group.wait_closing()

    except asyncio.CancelledError:
        cancelled = True

    try:
        await uncancellable(group.async_close())

    except asyncio.CancelledError:
        cancelled = True

    if cancelled:
        if task.cancelled():
            raise asyncio.CancelledError()

        result = None if task.exception() else task.result()
        raise CancelledWithResultError(result, task.exception())

    if task.cancelled():
        raise asyncio.TimeoutError()

    return task.result()
