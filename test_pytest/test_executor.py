import threading

import pytest

from hat import aio


async def test_executor():
    executor = aio.Executor()

    result = await executor.spawn(lambda: threading.current_thread().name)
    assert threading.current_thread().name != result

    await executor.async_close()

    with pytest.raises(Exception):
        executor.spawn(lambda: 123)


async def test_create_executor():
    executor = aio.create_executor()
    result = await executor(lambda: threading.current_thread().name)
    assert threading.current_thread().name != result


async def test_create_executor_example():
    executor1 = aio.create_executor()
    executor2 = aio.create_executor()
    tid1 = await executor1(threading.get_ident)
    tid2 = await executor2(threading.get_ident)
    assert tid1 != tid2
