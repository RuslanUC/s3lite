from __future__ import annotations

from datetime import datetime
from io import BytesIO, SEEK_END
from pathlib import Path
from typing import BinaryIO
from xml.etree import ElementTree

from dateutil import parser
from httpx import Response

from s3lite.auth import AWSSigV4, SignedClient
from s3lite.bucket import Bucket
from s3lite.exceptions import S3Exception
from s3lite.object import Object
from s3lite.utils import get_xml_attr, NS_URL

IGNORED_ERRORS = {"BucketAlreadyOwnedByYou"}


class ClientConfig:
    __slots__ = ["multipart_threshold"]

    def __init__(self, multipart_threshold: int = 16 * 1024 * 1024):
        self.multipart_threshold = multipart_threshold


class Client:
    def __init__(self, access_key_id: str, secret_access_key: str, endpoint: str, region: str="us-east-1",
                 config: ClientConfig = None):
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._endpoint = endpoint

        self._signer = AWSSigV4(access_key_id, secret_access_key, region)

        self.config = config or ClientConfig()

    def _check_error(self, response: Response) -> None:
        if response.status_code < 400:
            return

        error = ElementTree.parse(BytesIO(response.text.encode("utf8"))).getroot()
        error_code = get_xml_attr(error, "Code", ns="").text
        error_message = get_xml_attr(error, "Message", ns="").text

        if error_code not in IGNORED_ERRORS:
            raise S3Exception(error_code, error_message)

    async def ls_buckets(self) -> list[Bucket]:
        buckets = []
        async with SignedClient(self._signer) as client:
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

        async with SignedClient(self._signer) as client:
            resp = await client.put(f"{self._endpoint}/{bucket_name}", content=body)
            self._check_error(resp)

        return Bucket(bucket_name, client=self)

    async def ls_bucket(self, bucket_name: str) -> list[Object]:
        objs = []
        async with SignedClient(self._signer) as client:
            resp = await client.get(f"{self._endpoint}/{bucket_name}")
            self._check_error(resp)
            res = ElementTree.parse(BytesIO(resp.text.encode("utf8"))).getroot()

        for obj in get_xml_attr(res, "Contents", True):
            name = get_xml_attr(obj, "Key").text
            last_modified = parser.parse(get_xml_attr(obj, "LastModified").text)
            size = int(get_xml_attr(obj, "Size").text)

            objs.append(Object(Bucket(bucket_name, client=self), name, last_modified, size, client=self))

        return objs

    async def download_object(self, bucket: str | Bucket, key: str, path: str | None = None,
                              in_memory: bool = False, offset: int = 0, limit: int = 0) -> str | BytesIO:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        if key.startswith("/"): key = key[1:]
        headers = {}
        if offset > 0 or limit > 0:
            offset = max(offset, 0)
            limit = max(limit, 0)
            headers["Range"] = f"bytes={offset}-{offset + limit - 1}" if limit else f"bytes={offset}-"

        async with SignedClient(self._signer) as client:
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

    async def _upload_object_multipart(self, bucket: str, key: str, file: BinaryIO) -> Object | None:
        async with SignedClient(self._signer) as client:
            # Create multipart upload
            resp = await client.post(f"{self._endpoint}/{bucket}/{key}?uploads=")
            self._check_error(resp)
            res = ElementTree.parse(BytesIO(resp.text.encode("utf8"))).getroot()
            upload_id = get_xml_attr(res, "UploadId").text

            # Upload parts
            part = 1
            total_size = 0
            parts = ""
            while data := file.read(self.config.multipart_threshold):
                total_size += len(data)
                headers = {}
                url = f"{self._endpoint}/{bucket}/{key}?partNumber={part}&uploadId={upload_id}"

                resp = await client.put(url, content=data, headers=headers)
                self._check_error(resp)

                etag = resp.headers["ETag"]
                parts += f"<Part><ETag>{etag}</ETag><PartNumber>{part}</PartNumber></Part>"

                part += 1

            # Complete upload
            body = (f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    f"<CompleteMultipartUpload xmlns=\"{NS_URL}\">{parts}</CompleteMultipartUpload>").encode("utf8")
            resp = await client.post(f"{self._endpoint}/{bucket}/{key}?uploadId={upload_id}", content=body)
            self._check_error(resp)

        return Object(Bucket(bucket, client=self), key, datetime.now(), total_size, client=self)

    async def upload_object(self, bucket: str | Bucket, key: str, file: str | BinaryIO) -> Object | None:
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
        async with SignedClient(self._signer) as client:
            resp = await client.put(f"{self._endpoint}/{bucket}/{key}", content=file_body)
            self._check_error(resp)

        if close:
            file.close()

        return Object(Bucket(bucket, client=self), key, datetime.now(), file_size, client=self)

    def share(self, bucket: str | Bucket, key: str, ttl: int = 86400) -> str:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        return self._signer.presign(f"{self._endpoint}/{bucket}/{key}", False, ttl)

    async def delete_object(self, bucket: str | Bucket, key: str) -> None:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        if key.startswith("/"): key = key[1:]
        async with SignedClient(self._signer) as client:
            resp = await client.delete(f"{self._endpoint}/{bucket}/{key}")
            self._check_error(resp)

    async def delete_bucket(self, bucket: str | Bucket) -> None:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        async with SignedClient(self._signer) as client:
            resp = await client.delete(f"{self._endpoint}/{bucket}/")
            self._check_error(resp)

    # Aliases for boto3 compatibility

    list_buckets = ls_buckets
    upload_file = upload_object
    upload_fileobj = upload_object
    download_file = download_object
    download_fileobj = download_object
    generate_presigned_url = share
