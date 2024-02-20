from asyncio import Task, Event, create_task, sleep
from typing import Coroutine, Optional
from xml.etree.ElementTree import Element


class CaseInsensitiveDict(dict):
    def __init__(self, d: dict = None, **data):
        super().__init__()
        for k, v in data.items():
            self[k] = v
        if isinstance(d, dict):  # pragma: no cover
            for k, v in d.items():
                self[k] = v

    def __setitem__(self, key: str, value):
        super(CaseInsensitiveDict, self).__setitem__(key.lower(), value)

    def __getitem__(self, key: str):  # pragma: no cover
        return super(CaseInsensitiveDict, self).__getitem__(key.lower())

    def __contains__(self, item: str):
        return super().__contains__(item.lower())


NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
NS_URL = NS[1:-1]


def get_xml_attr(element: Element, name: str, get_all: bool = False, ns: str = NS) -> Element:
    path = f".//{ns}{name}"
    return element.findall(path) if get_all else element.find(path)


class AsyncTaskPool:
    def __init__(self, limit: int = 4):
        self._limit = limit
        self._scheduled_tasks: list[Coroutine] = []
        self._running_tasks: list[Task] = []
        self._pool_task: Optional[Task] = None
        self._results = []

    def add(self, coro: Coroutine) -> None:
        self._scheduled_tasks.append(coro)
        if self._pool_task is None:
            self._pool_task = create_task(self._task())

    async def _task(self):
        while self._scheduled_tasks or self._running_tasks:
            await sleep(0)

            indexes = [idx for idx, task in enumerate(self._running_tasks) if task.done()]
            removed = 0
            for idx in indexes:
                await sleep(0)
                self._results.append(await self._running_tasks.pop(idx - removed))
                removed += 1

            if len(self._running_tasks) >= self._limit:
                continue

            while len(self._running_tasks) < self._limit and self._scheduled_tasks:
                await sleep(0)
                self._running_tasks.append(create_task(self._scheduled_tasks.pop(0)))

        self._pool_task = None

    async def results(self):
        while self._pool_task is not None:
            await sleep(0)

        return self._results
