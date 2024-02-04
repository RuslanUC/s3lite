from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from s3lite.client import Client
    from s3lite.object import Object


class Bucket:
    __slots__ = ["name", "_client"]

    def __init__(self, name: str, *, client: Client):
        self.name = name
        self._client = client

    def __repr__(self) -> str:
        return f"Bucket(name={self.name!r})"

    async def ls(self) -> list[Object]:
        return await self._client.ls_bucket(self.name)
