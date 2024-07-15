import asyncio

import pytest

from hat import aio


async def test_wait_for():
    loop = asyncio.get_running_loop()

    async def return_result(delay):
        await asyncio.sleep(delay)
        return 123

    async def raise_exception(delay):
        await asyncio.sleep(delay)
        raise Exception()

    result = await aio.wait_for(return_result(0), 0.001)
    assert result == 123

    with pytest.raises(Exception):
        await aio.wait_for(raise_exception(0), 0.001)

    with pytest.raises(asyncio.TimeoutError):
        await aio.wait_for(return_result(0.001), 0)

    f = asyncio.ensure_future(aio.wait_for(return_result(0.001), 0))
    loop.call_soon(f.cancel)
    with pytest.raises(asyncio.CancelledError):
        await f

    async def f1():
        try:
            await aio.wait_for(return_result(0), 0.001)
        except aio.CancelledWithResultError as e:
            assert e.result == 123
            assert e.exception is None
        else:
            assert False

    f = asyncio.ensure_future(f1())
    loop.call_soon(f.cancel)
    await f

    async def f2():
        try:
            await aio.wait_for(raise_exception(0), 0.001)
        except aio.CancelledWithResultError as e:
            assert e.result is None
            assert e.exception is not None
        else:
            assert False

    f = asyncio.ensure_future(f2())
    loop.call_soon(f.cancel)
    await f
