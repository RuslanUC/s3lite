from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:  # pragma: no cover
    from s3lite.client import Client
    from s3lite.object import Object


class Bucket:
    __slots__ = ["name", "_client"]

    def __init__(self, name: str, *, client: Client):
        self.name = name
        self._client = client

    def __repr__(self) -> str:  # pragma: no cover
        return f"Bucket(name={self.name!r})"

    async def ls(self, prefix: str | None = None, max_keys: int | None = None) -> list[Object]:
        return await self._client.ls_bucket(self.name, prefix, max_keys)

    async def upload(self, key: str, file: str | BinaryIO) -> Object:
        return await self._client.upload_object(self, key, file)

    async def get_policy(self) -> dict:
        return await self._client.get_bucket_policy(self)

    async def put_policy(self, policy: dict) -> None:
        return await self._client.put_bucket_policy(self, policy)

    async def delete_policy(self) -> None:
        return await self._client.delete_bucket_policy(self)

    async def get_object(self, key: str) -> Object | None:
        return await self._client.get_object(self, key)
