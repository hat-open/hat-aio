import abc
import asyncio
import logging
import typing
import warnings


mlog: logging.Logger = logging.getLogger(__name__)
"""Module logger"""


class Resource(abc.ABC):
    """Resource with lifetime control based on `Group`."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.async_close()

    @property
    @abc.abstractmethod
    def async_group(self) -> 'Group':
        """Group controlling resource's lifetime."""

    @property
    def is_open(self) -> bool:
        """``True`` if not closing or closed, ``False`` otherwise."""
        return self.async_group.is_open

    @property
    def is_closing(self) -> bool:
        """Is resource closing or closed."""
        return self.async_group.is_closing

    @property
    def is_closed(self) -> bool:
        """Is resource closed."""
        return self.async_group.is_closed

    async def wait_closing(self):
        """Wait until closing is ``True``."""
        await self.async_group.wait_closing()

    async def wait_closed(self):
        """Wait until closed is ``True``."""
        await self.async_group.wait_closed()

    def close(self):
        """Close resource."""
        self.async_group.close()

    async def async_close(self):
        """Close resource and wait until closed is ``True``."""
        await self.async_group.async_close()


class GroupClosedError(Exception):
    """Group closed exception"""


class Group(Resource):
    """Group of asyncio Tasks.

    Group enables creation and management of related asyncio Tasks. The
    Group ensures uninterrupted execution of Tasks and Task completion upon
    Group closing.

    Group can contain subgroups, which are independent Groups managed by the
    parent Group.

    If a Task raises exception, other Tasks continue to execute.

    If `log_exceptions` is ``True``, exceptions raised by spawned tasks are
    logged with level ERROR.

    """

    def __init__(self,
                 log_exceptions: bool = True,
                 *,
                 loop: asyncio.AbstractEventLoop | None = None):
        self._log_exceptions = log_exceptions
        self._loop = loop or asyncio.get_running_loop()
        self._closing = self._loop.create_future()
        self._closed = self._loop.create_future()
        self._tasks = set()
        self._parent = None
        self._children = set()

    @property
    def async_group(self):
        """Async group"""
        return self

    @property
    def is_open(self) -> bool:
        """``True`` if group is not closing or closed, ``False`` otherwise."""
        return not self._closing.done()

    @property
    def is_closing(self) -> bool:
        """Is group closing or closed."""
        return self._closing.done()

    @property
    def is_closed(self) -> bool:
        """Is group closed."""
        return self._closed.done()

    async def wait_closing(self):
        """Wait until closing is ``True``."""
        await asyncio.shield(self._closing)

    async def wait_closed(self):
        """Wait until closed is ``True``."""
        await asyncio.shield(self._closed)

    def create_subgroup(self,
                        log_exceptions: bool | None = None
                        ) -> 'Group':
        """Create new Group as a child of this Group. Return the new Group.

        When a parent Group gets closed, all of its children are closed.
        Closing of a subgroup has no effect on the parent Group.

        If `log_exceptions` is ``None``, subgroup inherits `log_exceptions`
        from its parent.

        """
        if self._closing.done():
            raise GroupClosedError("can't create subgroup of closed group")

        child = Group(
            log_exceptions=(self._log_exceptions if log_exceptions is None
                            else log_exceptions),
            loop=self._loop)
        child._parent = self
        self._children.add(child)
        return child

    def wrap(self,
             obj: typing.Awaitable
             ) -> asyncio.Task:
        """Wrap the awaitable object into a Task and schedule its execution.
        Return the Task object.

        Resulting task is shielded and can be canceled only with
        `Group.async_close`.

        """
        if self._closing.done():
            raise GroupClosedError("can't wrap object in closed group")

        if asyncio.iscoroutine(obj):
            task = self._loop.create_task(obj)

        else:
            task = asyncio.ensure_future(obj, loop=self._loop)

        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)

        return asyncio.shield(task)

    def spawn(self,
              fn: typing.Callable[..., typing.Awaitable],
              *args, **kwargs
              ) -> asyncio.Task:
        """Wrap the result of a `fn` into a Task and schedule its execution.
        Return the Task object.

        Function `fn` is called with provided `args` and `kwargs`.
        Resulting Task is shielded and can be canceled only with
        `Group.async_close`.

        """
        if self._closing.done():
            raise GroupClosedError("can't spawn task in closed group")

        future = fn(*args, **kwargs)
        return self.wrap(future)

    def close(self):
        """Schedule Group closing.

        Closing Future is set immediately. All subgroups are closed, and all
        running tasks are canceled. Once closing of all subgroups
        and execution of all tasks is completed, closed Future is set.

        """
        if self._closing.done():
            return

        self._closing.set_result(True)

        for child in list(self._children):
            child.close()

        for task in self._tasks:
            self._loop.call_soon(task.cancel)

        futures = [*self._tasks,
                   *(child._closed for child in self._children)]
        if futures:
            waiting_task = self._loop.create_task(asyncio.wait(futures))
            waiting_task.add_done_callback(lambda _: self._on_closed())

        else:
            self._on_closed()

    async def async_close(self):
        """Close Group and wait until closed is ``True``."""
        self.close()
        await self.wait_closed()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.async_close()

    def _on_closed(self):
        if self._parent is not None:
            self._parent._children.remove(self)
            self._parent = None

        self._closed.set_result(True)

    def _on_task_done(self, task):
        self._tasks.remove(task)

        if task.cancelled():
            return

        e = task.exception()
        if e and self._log_exceptions:
            mlog.error('unhandled exception in async group: %s', e, exc_info=e)
            warnings.warn('unhandled exception in async group')
