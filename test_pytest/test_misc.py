import asyncio
import signal
import subprocess
import sys
import threading
import time

import pytest

from hat import aio


async def test_first():
    queue = aio.Queue()
    queue.put_nowait(1)
    queue.put_nowait(2)
    queue.put_nowait(3)
    queue.close()

    result = await aio.first(queue, lambda i: i == 2)
    assert result == 2

    assert not queue.empty()
    assert queue.get_nowait() == 3
    assert queue.empty()

    queue = aio.Queue()
    queue.put_nowait(1)
    queue.put_nowait(2)
    queue.put_nowait(3)
    queue.close()
    result = await aio.first(queue, lambda i: i == 4)
    assert result is None
    assert queue.empty()


async def test_first_example():

    async def async_range(x):
        for i in range(x):
            await asyncio.sleep(0)
            yield i

    assert await aio.first(async_range(3)) == 0
    assert await aio.first(async_range(3), lambda x: x > 1) == 2
    assert await aio.first(async_range(3), lambda x: x > 2) is None
    assert await aio.first(async_range(3), lambda x: x > 2, 123) == 123


async def test_first_example_docs():
    queue = aio.Queue()
    queue.put_nowait(1)
    queue.put_nowait(2)
    queue.put_nowait(3)
    queue.close()

    assert 1 == await aio.first(queue)
    assert 3 == await aio.first(queue, lambda x: x > 2)
    assert 123 == await aio.first(queue, default=123)


async def test_uncancellable():
    f1 = asyncio.Future()

    async def f():
        return await f1

    f2 = aio.uncancellable(f(), raise_cancel=False)
    f3 = asyncio.ensure_future(f2)
    asyncio.get_event_loop().call_soon(f3.cancel)
    f1.set_result(123)
    result = await f3
    assert result == 123


async def test_uncancellable_exception():
    f1 = asyncio.Future()
    e = Exception()

    async def f():
        await asyncio.sleep(0)
        return await f1

    f2 = aio.uncancellable(f(), raise_cancel=False)
    f3 = asyncio.ensure_future(f2)
    asyncio.get_event_loop().call_soon(f3.cancel)
    f1.set_exception(e)
    try:
        await f3
    except Exception as ex:
        exc = ex
    assert exc is e


async def test_uncancellable_vs_shield():

    async def set_future(f, value):
        await asyncio.sleep(0.001)
        f.set_result(value)

    future = asyncio.Future()
    t1 = asyncio.shield(set_future(future, 1))
    t2 = asyncio.ensure_future(t1)
    asyncio.get_event_loop().call_soon(t2.cancel)
    with pytest.raises(asyncio.CancelledError):
        await t2
    assert not future.done()
    await future
    assert future.result() == 1

    future = asyncio.Future()
    t1 = aio.uncancellable(set_future(future, 1), raise_cancel=True)
    t2 = asyncio.ensure_future(t1)
    asyncio.get_event_loop().call_soon(t2.cancel)
    with pytest.raises(asyncio.CancelledError):
        await t2
    assert future.done()
    assert future.result() == 1

    future = asyncio.Future()
    t1 = aio.uncancellable(set_future(future, 1), raise_cancel=False)
    t2 = asyncio.ensure_future(t1)
    asyncio.get_event_loop().call_soon(t2.cancel)
    await t2
    assert future.done()
    assert future.result() == 1


async def test_call():

    def f1(x):
        return x

    async def f2(x):
        await asyncio.sleep(0)
        return x

    result = await aio.call(f1, 123)
    assert result == 123

    result = await aio.call(f2, 123)
    assert result == 123


async def test_call_on_cancel():
    called = asyncio.Future()
    group = aio.Group()

    async def closing(called):
        called.set_result(True)
        assert group.is_closing
        assert not group.is_closed

    group.spawn(aio.call_on_cancel, closing, called)
    assert not called.done()

    await group.async_close()
    assert called.done()


async def test_call_on_cancel_example():
    f = asyncio.Future()
    group = aio.Group()
    group.spawn(aio.call_on_cancel, f.set_result, 123)
    assert not f.done()
    await group.async_close()
    assert f.result() == 123


async def test_call_on_done():
    f1 = asyncio.Future()
    f2 = asyncio.Future()
    f3 = asyncio.ensure_future(aio.call_on_done(f1, f2.set_result, 123))

    await asyncio.sleep(0)
    assert not f1.done()
    assert not f2.done()
    assert not f3.done()

    f1.set_result(None)
    await asyncio.wait_for(f3, 0.001)
    assert f2.result() == 123
    assert f3.result() is None


async def test_call_on_done_example():
    f = asyncio.Future()
    group = aio.Group()
    group.spawn(aio.call_on_done, f, group.close)
    assert group.is_open
    f.set_result(None)
    await group.wait_closed()
    assert group.is_closed


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="pthread_kill not supported")
def test_run_asyncio():
    ident = threading.get_ident()

    def thread():
        time.sleep(0.01)
        signal.pthread_kill(ident, signal.SIGINT)

    t = threading.Thread(target=thread)
    t.start()

    async def f():
        await asyncio.Future()

    with pytest.raises(asyncio.CancelledError):
        aio.run_asyncio(f())
    t.join()


# TODO check implementation for posible deadlock
def test_run_asyncio_with_subprocess(tmp_path):

    py_path = tmp_path / 'temp.py'
    run_path = tmp_path / 'temp.run'
    with open(py_path, 'w', encoding='utf-8') as f:
        f.write(f"from hat import aio\n"
                f"import asyncio\n"
                f"import sys\n"
                f"async def f():\n"
                f"    open(r'{run_path}', 'w').close()\n"
                f"    await asyncio.Future()\n"
                f"try:\n"
                f"    aio.run_asyncio(f())\n"
                f"except asyncio.CancelledError:\n"
                f"    sys.exit(0)\n"
                f"except Exception:\n"
                f"    sys.exit(10)\n"
                f"sys.exit(5)\n")

    p = subprocess.Popen([sys.executable, str(py_path)],
                         creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP
                                        if sys.platform == 'win32' else 0))
    while not run_path.exists():
        assert p.poll() is None
        time.sleep(0.01)
    p.send_signal(signal.CTRL_BREAK_EVENT
                  if sys.platform == 'win32'
                  else signal.SIGINT)
    assert p.wait() == 0


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="pthread_kill not supported")
def test_run_asyncio_with_multiple_signals():
    ident = threading.get_ident()

    def thread():
        time.sleep(0.01)
        for _ in range(5):
            signal.pthread_kill(ident, signal.SIGINT)
            time.sleep(0.001)

    t = threading.Thread(target=thread)
    t.start()

    async def f():
        await aio.uncancellable(asyncio.sleep(0.02), raise_cancel=False)
        return 1

    assert aio.run_asyncio(f()) == 1
    t.join()


# TODO: test run_asyncio with `handle_signals` and `loop`


def test_run_async_example():

    async def run():
        await asyncio.sleep(0)
        return 123

    result = aio.run_asyncio(run())
    assert result == 123
