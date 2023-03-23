import asyncio
import unittest.mock

import pytest

from hat import aio


async def test_group():
    group = aio.Group()
    futures = [group.wrap(asyncio.Future()) for _ in range(100)]
    assert not any(future.done() for future in futures)
    await group.async_close()
    assert all(future.done() for future in futures)
    assert not group.is_open
    assert group.is_closing
    assert group.is_closed
    await group.wait_closing()
    await group.wait_closed()


async def test_group_spawn_async_close():

    async def task():
        group.spawn(group.async_close)
        await asyncio.Future()

    group = aio.Group()
    group.spawn(task)
    await group.wait_closed()


async def test_group_subgroup():
    g1 = aio.Group()
    g2 = g1.create_subgroup()
    f = g2.wrap(asyncio.Future())
    g1.close()

    assert g1.is_closing
    assert g2.is_closing
    assert not f.done()
    assert not g1.is_closed
    assert not g2.is_closed

    with pytest.raises(Exception):
        g1.create_subgroup()
    await g1.async_close()
    with pytest.raises(Exception):
        g1.create_subgroup()

    assert f.done()
    assert g1.is_closed
    assert g2.is_closed


async def test_group_async_close_subgroup_without_tasks():
    g1 = aio.Group()
    g2 = g1.create_subgroup()
    await g1.async_close()

    assert g1.is_closed
    assert g2.is_closed


async def test_group_spawn_subgroup_in_closing_subgroup():
    g1 = aio.Group()
    g2 = g1.create_subgroup()

    async def task():
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            with pytest.raises(Exception):
                g1.create_subgroup()

    g2.spawn(task)
    await g1.async_close()
    assert g1.is_closed
    assert g2.is_closed


async def test_group_spawn_when_not_open():
    g = aio.Group()
    g.spawn(asyncio.Future)
    g.close()
    with pytest.raises(Exception):
        g.spawn(asyncio.Future)
    await g.async_close()
    with pytest.raises(Exception):
        g.spawn(asyncio.Future)
    with pytest.raises(Exception):
        g.wrap(asyncio.Future())


async def test_group_close_empty_group():
    g = aio.Group()
    assert not g.is_closing
    assert not g.is_closed
    g.close()
    assert g.is_closing
    assert g.is_closed


async def test_group_context():
    async with aio.Group() as g:
        f = g.spawn(asyncio.Future)
        assert not f.done()
    assert f.done()


@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_group_exception_handler():

    async def f():
        raise e

    e = Exception()
    g = aio.Group()
    with unittest.mock.patch('hat.aio.group.mlog.error') as mock:
        with pytest.raises(Exception):
            await g.spawn(f)
    await g.async_close()

    _, kwargs = mock.call_args
    assert kwargs['exc_info'] is e


async def test_group_example_docs_spawn():

    async def f1(x):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            return x

    async def f2(x):
        await asyncio.sleep(0)
        return x

    async with aio.Group() as group:
        f = group.spawn(f1, 'f1')
        assert 'f2' == await group.spawn(f2, 'f2')
    assert 'f1' == await f


async def test_group_example_docs_subgroup():
    group = aio.Group()
    subgroup1 = group.create_subgroup()
    subgroup2 = group.create_subgroup()

    f1 = subgroup1.spawn(asyncio.Future)
    f2 = subgroup2.spawn(asyncio.Future)

    assert not f1.done()
    assert not f2.done()

    await group.async_close()

    assert f1.done()
    assert f2.done()


async def test_resource():

    class Mock(aio.Resource):

        def __init__(self):
            self._async_group = aio.Group()
            self._async_group.spawn(asyncio.sleep, 1)

        @property
        def async_group(self):
            return self._async_group

    mock = Mock()
    assert mock.is_open
    assert not mock.is_closing
    assert not mock.is_closed

    mock.close()
    assert not mock.is_open
    assert mock.is_closing
    assert not mock.is_closed

    await mock.wait_closing()
    await mock.wait_closed()
    assert not mock.is_open
    assert mock.is_closing
    assert mock.is_closed

    await mock.async_close()
