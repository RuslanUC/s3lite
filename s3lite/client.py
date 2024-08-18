from __future__ import annotations

import asyncio
import json
from datetime import datetime
from io import BytesIO, SEEK_END
from pathlib import Path
from typing import BinaryIO, Union, Callable, AsyncIterator
from xml.etree import ElementTree

from dateutil import parser
from httpx import Response

from s3lite.auth import AWSSigV4, SignedClient
from s3lite.bucket import Bucket
from s3lite.exceptions import S3Exception
from s3lite.object import Object
from s3lite.utils import get_xml_attr, NS_URL

IGNORED_ERRORS = {"BucketAlreadyOwnedByYou"}
SignedClientClassOrFactory = Union[type[SignedClient], Callable[[AWSSigV4, ...], SignedClient]]


class ClientConfig:
    """
    Parameters:
        multipart_threshold:
            Minimum size of file to upload it with multipart upload

        max_concurrency:
            Maximum number of async tasks for multipart uploads. Does not affect multipart uploads or downloads
    """

    __slots__ = ("multipart_threshold", "max_concurrency")

    def __init__(self, multipart_threshold: int = 16 * 1024 * 1024, max_concurrency: int = 6):
        self.multipart_threshold = multipart_threshold
        self.max_concurrency = max_concurrency


class Client:
    __slots__ = ("_access_key_id", "_secret_access_key", "_endpoint", "_signer", "config", "_client_cls")

    def __init__(
            self, access_key_id: str, secret_access_key: str, endpoint: str, region: str="us-east-1",
            config: ClientConfig = None, httpx_client: SignedClientClassOrFactory = SignedClient,
    ):
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._endpoint = endpoint

        self._signer = AWSSigV4(access_key_id, secret_access_key, region)

        self.config = config or ClientConfig()
        self._client_cls = httpx_client

    @staticmethod
    def _check_error(response: Response) -> None:
        if response.status_code < 400:
            return

        try:
            error = ElementTree.parse(BytesIO(response.text.encode("utf8"))).getroot()
        except Exception as e:
            raise S3Exception("S3liteError", "Failed to parse response xml.") from e

        error_code = get_xml_attr(error, "Code", ns="").text
        error_message = get_xml_attr(error, "Message", ns="").text

        if error_code not in IGNORED_ERRORS:
            raise S3Exception(error_code, error_message)

    async def ls_buckets(self) -> list[Bucket]:
        buckets = []
        async with self._client_cls(self._signer) as client:
            resp = await client.get(f"{self._endpoint}/")
            self._check_error(resp)
            res = ElementTree.parse(BytesIO(resp.text.encode("utf8"))).getroot()

        for obj in get_xml_attr(res, "Bucket", True):
            name = get_xml_attr(obj, "Name").text

            buckets.append(Bucket(name, client=self))

        return buckets

    async def create_bucket(self, bucket_name: str) -> Bucket:
        body = (f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                f"<CreateBucketConfiguration xmlns=\"{NS_URL}\"></CreateBucketConfiguration>").encode("utf8")

        async with self._client_cls(self._signer) as client:
            resp = await client.put(f"{self._endpoint}/{bucket_name}", content=body)
            self._check_error(resp)

        return Bucket(bucket_name, client=self)

    async def ls_bucket(
            self, bucket_name: str, prefix: str | None = None, max_keys: int | None = None
    ) -> list[Object]:
        return [obj async for obj in self.ls_bucket_iter(bucket_name, prefix, max_keys)]

    async def ls_bucket_iter(
            self, bucket_name: str, prefix: str | None = None, max_keys: int | None = None
    ) -> AsyncIterator[Object]:
        more_objects = True
        marker = None
        got_objects = 0

        while more_objects and (max_keys is None or got_objects < max_keys):
            params = {}
            if prefix is not None:
                params["prefix"] = prefix
            if max_keys is not None:
                params["max-keys"] = min(max_keys - got_objects, 1000)
            if marker is not None:
                params["marker"] = marker

            async with self._client_cls(self._signer) as client:
                resp = await client.get(f"{self._endpoint}/{bucket_name}", params=params)
                self._check_error(resp)
                res = ElementTree.parse(BytesIO(resp.text.encode("utf8"))).getroot()

            more_objects = get_xml_attr(res, "IsTruncated").text.lower() == "true"

            for obj in get_xml_attr(res, "Contents", True):
                name = marker = get_xml_attr(obj, "Key").text
                last_modified = parser.parse(get_xml_attr(obj, "LastModified").text)
                size = int(get_xml_attr(obj, "Size").text)

                yield Object(Bucket(bucket_name, client=self), name, last_modified, size, client=self)
                got_objects += 1

    async def download_object(self, bucket: str | Bucket, key: str, path: str | None = None,
                              in_memory: bool = False, offset: int = 0, limit: int = 0) -> str | BytesIO:
        key = key.lstrip("/")
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        if key.startswith("/"): key = key[1:]
        headers = {}
        if offset > 0 or limit > 0:
            offset = max(offset, 0)
            limit = max(limit, 0)
            headers["Range"] = f"bytes={offset}-{offset + limit - 1}" if limit else f"bytes={offset}-"

        async with self._client_cls(self._signer) as client:
            resp = await client.get(f"{self._endpoint}/{bucket}/{key}", headers=headers)
            self._check_error(resp)
            content = await resp.aread()

        if in_memory:
            return BytesIO(content)

        save_path = Path(path)
        if save_path.is_dir() or path.endswith("/"):
            save_path.mkdir(parents=True, exist_ok=True)
            save_path /= key

        with open(save_path, "wb") as f:
            f.write(content)

        return str(save_path)

    async def get_object(self, bucket: str | Bucket, key: str) -> Object | None:
        key = key.lstrip("/")
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        async with self._client_cls(self._signer) as client:
            resp = await client.head(f"{self._endpoint}/{bucket}/{key}")
            if resp.status_code != 200:
                return

        last_modified = parser.parse(resp.headers["Last-Modified"]) if "Last-Modified" else datetime(1970, 1, 1)
        size = int(resp.headers["Content-Length"])
        return Object(Bucket(bucket, client=self), key, last_modified, size, client=self)

    async def _upload_object_multipart(self, bucket: str, key: str, file: BinaryIO) -> Object | None:
        key = key.lstrip("/")
        async with self._client_cls(self._signer) as client:
            # Create multipart upload
            resp = await client.post(f"{self._endpoint}/{bucket}/{key}?uploads=")
            self._check_error(resp)
            res = ElementTree.parse(BytesIO(resp.text.encode("utf8"))).getroot()
            upload_id = get_xml_attr(res, "UploadId").text

            sem = asyncio.Semaphore(self.config.max_concurrency)

            async def _upload_task(part_number: int, content: bytes):
                await sem.acquire()

                url = f"{self._endpoint}/{bucket}/{key}?partNumber={part_number}&uploadId={upload_id}"
                resp_ = await client.put(url, content=content, headers={})
                self._check_error(resp_)
                etag = resp_.headers["ETag"]

                sem.release()
                return part_number, f"<Part><ETag>{etag}</ETag><PartNumber>{part_number}</PartNumber></Part>"

            # Upload parts
            part = 1
            total_size = 0
            tasks = []
            while data := file.read(self.config.multipart_threshold):
                total_size += len(data)
                tasks.append(asyncio.create_task(_upload_task(part, data)))
                part += 1

            parts = sorted(await asyncio.gather(*tasks))
            parts = "".join([part[1] for part in parts])

            # Complete upload
            body = (f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    f"<CompleteMultipartUpload xmlns=\"{NS_URL}\">{parts}</CompleteMultipartUpload>").encode("utf8")
            resp = await client.post(f"{self._endpoint}/{bucket}/{key}?uploadId={upload_id}", content=body)
            self._check_error(resp)

        return Object(Bucket(bucket, client=self), key, datetime.now(), total_size, client=self)

    async def upload_object(self, bucket: str | Bucket, key: str, file: str | BinaryIO) -> Object | None:
        key = key.lstrip("/")
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        close = False
        if not hasattr(file, "read"):
            file = open(file, "rb")
            close = True

        file.seek(0, SEEK_END)
        file_size = file.tell()
        file.seek(0)

        if key.startswith("/"): key = key[1:]

        if file_size > self.config.multipart_threshold:
            return await self._upload_object_multipart(bucket, key, file)

        file_body = file.read()
        async with self._client_cls(self._signer) as client:
            resp = await client.put(f"{self._endpoint}/{bucket}/{key}", content=file_body)
            self._check_error(resp)

        if close:
            file.close()

        return Object(Bucket(bucket, client=self), key, datetime.now(), file_size, client=self)

    def share(self, bucket: str | Bucket, key: str, ttl: int = 86400, upload: bool = False) -> str:
        key = key.lstrip("/")
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        return self._signer.presign(f"{self._endpoint}/{bucket}/{key}", upload, ttl)

    async def delete_object(self, bucket: str | Bucket, key: str) -> None:
        key = key.lstrip("/")
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        async with self._client_cls(self._signer) as client:
            resp = await client.delete(f"{self._endpoint}/{bucket}/{key}")
            self._check_error(resp)

    async def delete_bucket(self, bucket: str | Bucket) -> None:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        async with self._client_cls(self._signer) as client:
            resp = await client.delete(f"{self._endpoint}/{bucket}/")
            self._check_error(resp)

    async def get_bucket_policy(self, bucket: str | Bucket) -> dict:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        async with self._client_cls(self._signer) as client:
            resp = await client.get(f"{self._endpoint}/{bucket}/?policy=")
            self._check_error(resp)
            return resp.json()

    async def put_bucket_policy(self, bucket: str | Bucket, policy: dict) -> None:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        policy_bytes = json.dumps(policy).encode("utf8")

        async with self._client_cls(self._signer) as client:
            resp = await client.put(f"{self._endpoint}/{bucket}/?policy=", content=policy_bytes)
            self._check_error(resp)

    async def delete_bucket_policy(self, bucket: str | Bucket) -> None:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        async with self._client_cls(self._signer) as client:
            resp = await client.delete(f"{self._endpoint}/{bucket}/?policy=")
            self._check_error(resp)

    # Aliases for boto3 compatibility

    list_buckets = ls_buckets
    upload_file = upload_object
    upload_fileobj = upload_object
    download_file = download_object
    download_fileobj = download_object
    generate_presigned_url = share
