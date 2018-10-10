from collections.abc import MutableMapping, MutableSet
from asyncio.locks import Event


class EmptyWaitMixin:
    def __init__(self):
        self._empty = Event()
        self._is_empty()

    async def wait_until_empty(self):
        await self._empty.wait()

    def _is_empty(self):
        self._empty.set()

    def _is_not_empty(self):
        self._empty.clear()


class AsyncDict(MutableMapping, EmptyWaitMixin):
    "Tiny dict wrapper that lets us await until it's empty."
    def __init__(self):
        EmptyWaitMixin.__init__(self)
        self._dict = {}
        self._added = Event()

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self._dict[key] = value
        self._is_not_empty()
        self._added.set()
        self._added.clear()

    def __delitem__(self, key):
        del self._dict[key]
        if not self._dict:
            self._is_empty()

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    async def wait_for_key(self, key):
        while True:
            if key in self:
                return self[key]
            await self._added.wait()


class AsyncSet(MutableSet, EmptyWaitMixin):
    "Tiny set wrapper that lets us await until it's empty."
    def __init__(self):
        EmptyWaitMixin.__init__(self)
        self._set = set()

    def __contains__(self, item):
        return item in self._set

    def add(self, item):
        self._set.add(item)
        self._is_not_empty()

    def discard(self, item):
        self._set.discard(item)
        if not self._set:
            self._is_empty()

    def __iter__(self):
        return iter(self._set)

    def __len__(self):
        return len(self._set)
