from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from s3lite.client import Client
    from s3lite.bucket import Bucket


class Object:
    __slots__ = ["bucket", "name", "last_modified", "size", "_client"]

    def __init__(self, bucket: Bucket, name: str, last_modified: datetime, size: int, *, client: Client):
        self.bucket = bucket
        self.name = name
        self.last_modified = last_modified
        self.size = size
        self._client = client

    def __repr__(self) -> str:  # pragma: no cover
        return f"Object(bucket={self.bucket!r}, name={self.name!r}, size={self.size!r})"

    async def download(self, path: str | None = None, in_memory: bool = False, offset: int = 0,
                       limit: int = 0) -> str | BytesIO:
        return await self._client.download_object(
            bucket=self.bucket,
            key=self.name,
            path=path,
            in_memory=in_memory,
            offset=offset,
            limit=limit,
        )

    async def delete(self) -> None:
        await self._client.delete_object(self.bucket, self.name)

    def share(self, ttl: int = 86400) -> str:
        return self._client.share(self.bucket, self.name, ttl)
