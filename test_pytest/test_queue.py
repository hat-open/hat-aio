import asyncio
import collections

import pytest

from hat import aio


async def test_queue():
    queue = aio.Queue()
    assert not queue.is_closed
    f = asyncio.ensure_future(queue.get())
    assert not f.done()
    queue.put_nowait(1)
    assert 1 == await f
    for _ in range(5):
        queue.close()
        assert queue.is_closed
    with pytest.raises(aio.QueueClosedError):
        await queue.get()


async def test_queue_str():
    queue = aio.Queue()
    result = str(queue)
    assert isinstance(result, str)
    assert result


async def test_queue_len():
    count = 10
    queue = aio.Queue()
    assert len(queue) == 0

    for i in range(count):
        queue.put_nowait(None)
        assert len(queue) == i + 1
        assert queue.qsize() == i + 1

    for i in range(count):
        queue.get_nowait()
        assert queue.qsize() == count - i - 1

    assert len(queue) == 0


async def test_queue_get_nowait():
    queue = aio.Queue()
    with pytest.raises(aio.QueueEmptyError):
        queue.get_nowait()


async def test_queue_get_until_empty():
    queue = aio.Queue()
    queue.put_nowait(1)
    queue.put_nowait(2)
    queue.put_nowait(3)
    result = await queue.get_until_empty()
    assert result == 3


async def test_queue_get_nowait_until_empty():
    queue = aio.Queue()
    queue.put_nowait(1)
    queue.put_nowait(2)
    queue.put_nowait(3)
    result = queue.get_nowait_until_empty()
    assert result == 3


async def test_queue_with_size():
    queue_size = 5
    queue = aio.Queue(queue_size)
    assert queue.maxsize == queue_size

    for _ in range(queue_size):
        queue.put_nowait(None)

    with pytest.raises(aio.QueueFullError):
        queue.put_nowait(None)


async def test_queue_put():
    queue = aio.Queue(1)
    await queue.put(1)
    put_future = asyncio.ensure_future(queue.put(1))
    asyncio.get_event_loop().call_soon(queue.close)
    with pytest.raises(aio.QueueClosedError):
        await put_future


async def test_queue_put_cancel():
    queue = aio.Queue(1)
    await queue.put(0)
    f1 = asyncio.ensure_future(queue.put(1))
    f2 = asyncio.ensure_future(queue.put(2))
    await asyncio.sleep(0)
    assert 0 == queue.get_nowait()
    f1.cancel()
    assert 2 == await queue.get()
    with pytest.raises(asyncio.CancelledError):
        await f1
    await f2


@pytest.mark.parametrize('item_count', [0, 1, 2, 10])
async def test_queue_async_iterable(item_count):
    queue = aio.Queue()
    data = collections.deque()

    for i in range(10):
        queue.put_nowait(i)
        data.append(i)

    asyncio.get_running_loop().call_later(0.001, queue.close)

    async for i in queue:
        assert i == data.popleft()

    assert queue.empty()
    assert len(data) == 0


async def test_queue_get_canceled():
    queue = aio.Queue()
    f1 = asyncio.ensure_future(queue.get())
    f2 = asyncio.ensure_future(queue.get())
    await asyncio.sleep(0)
    queue.put_nowait(123)
    f1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await f1
    assert 123 == await f2


async def test_queue_example():
    queue = aio.Queue(maxsize=1)

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
