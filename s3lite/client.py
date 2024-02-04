from __future__ import annotations

from datetime import datetime
from io import BytesIO, SEEK_END
from pathlib import Path
from typing import BinaryIO, Optional
from xml.etree import ElementTree

from dateutil import parser
from httpx import AsyncClient, Response

from s3lite.auth import AWSSigV4
from s3lite.bucket import Bucket
from s3lite.exceptions import S3Exception
from s3lite.object import Object
from s3lite.utils import get_xml_attr, NS_URL

IGNORED_ERRORS = {"BucketAlreadyOwnedByYou"}


class Client:
    def __init__(self, access_key_id: str, secret_access_key: str, endpoint: str, region: str="us-east-1"):
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._endpoint = endpoint

        self._signer = AWSSigV4(access_key_id, secret_access_key, region)

    def _check_error(self, response: Response) -> None:
        if response.status_code < 400:
            return

        error = ElementTree.parse(BytesIO(response.text.encode("utf8"))).getroot()
        error_code = get_xml_attr(error, "Code", ns="").text
        error_message = get_xml_attr(error, "Message", ns="").text

        if error_code not in IGNORED_ERRORS:
            raise S3Exception(error_code, error_message)

    async def ls_buckets(self) -> list[Bucket]:
        url = f"{self._endpoint}/"
        _, headers = self._signer.sign(url, add_signature=True)

        buckets = []
        async with AsyncClient() as client:
            resp = await client.get(url, headers=headers)
            self._check_error(resp)
            res = ElementTree.parse(BytesIO(resp.text.encode("utf8"))).getroot()

        for obj in get_xml_attr(res, "Buckets", True):
            name = get_xml_attr(obj, "Name").text

            buckets.append(Bucket(name, client=self))

        return buckets

    async def create_bucket(self, bucket_name: str) -> Bucket:
        body = (f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                f"<CreateBucketConfiguration xmlns=\"{NS_URL}\"></CreateBucketConfiguration>").encode("utf8")

        url = f"{self._endpoint}/{bucket_name}"
        _, headers = self._signer.sign(url, method="PUT", body=body, add_signature=True)

        async with AsyncClient() as client:
            resp = await client.put(url, content=body, headers=headers)
            self._check_error(resp)

        return Bucket(bucket_name, client=self)

    async def ls_bucket(self, bucket_name: str) -> list[Object]:
        url = f"{self._endpoint}/{bucket_name}"
        _, headers = self._signer.sign(url, add_signature=True)

        objs = []
        async with AsyncClient() as client:
            resp = await client.get(url, headers=headers)
            self._check_error(resp)
            res = ElementTree.parse(BytesIO(resp.text.encode("utf8"))).getroot()

        for obj in get_xml_attr(res, "Contents", True):
            name = get_xml_attr(obj, "Key").text
            last_modified = parser.parse(get_xml_attr(obj, "LastModified").text)
            size = int(get_xml_attr(obj, "Size").text)

            objs.append(Object(Bucket(bucket_name, client=self), name, last_modified, size, client=self))

        return objs

    async def download_object(self, bucket: str | Bucket, name: str, path: str | None = None,
                              in_memory: bool = False, offset: int = 0, limit: int = 0) -> str | BytesIO:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        if name.startswith("/"): name = name[1:]
        url = f"{self._endpoint}/{bucket}/{name}"
        headers = {}
        if offset > 0 or limit > 0:
            offset = max(offset, 0)
            limit = max(limit, 0)
            headers["Range"] = f"bytes={offset}-{offset + limit - 1}" if limit else f"bytes={offset}-"

        _, headers_ = self._signer.sign(url, headers, add_signature=True)
        headers |= headers_

        async with AsyncClient() as client:
            resp = await client.get(url, headers=headers)
            self._check_error(resp)
            content = await resp.aread()

        if in_memory:
            return BytesIO(content)

        save_path = Path(path)
        if save_path.is_dir() or path.endswith("/"):
            save_path.mkdir(parents=True, exist_ok=True)
            save_path /= name

        with open(save_path, "wb") as f:
            f.write(content)

        return str(save_path)

    async def upload_object(self, bucket: str, name: str, file: str | BinaryIO) -> Optional[Object]:
        close = False
        if not hasattr(file, "read"):
            file = open(file, "rb")
            close = True

        file.seek(0, SEEK_END)
        file_size = file.tell()
        file.seek(0)

        if name.startswith("/"): name = name[1:]
        url = f"{self._endpoint}/{bucket}/{name}"

        file_body = file.read()
        _, headers = self._signer.sign(url, method="PUT", body=file_body, add_signature=True)

        # TODO: multipart uploads
        async with AsyncClient() as client:
            resp = await client.put(url, content=file_body, headers=headers)
            self._check_error(resp)

        if close:
            file.close()

        return Object(Bucket(bucket, client=self), name, datetime.now(), file_size, client=self)

    def share(self, bucket: str | Bucket, name: str, ttl: int = 86400) -> str:
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        return self._signer.presign(f"{self._endpoint}/{bucket}/{name}", False, ttl)

    # Aliases for boto3 compatibility

    list_buckets = ls_buckets
    upload_file = upload_object
    upload_fileobj = upload_object
    download_file = download_object
    download_fileobj = download_object
    generate_presigned_url = share
    # TODO: generate_presigned_post
