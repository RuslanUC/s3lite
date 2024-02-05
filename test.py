from io import BytesIO
from os import environ, urandom
from time import time

import pytest as pt
from httpx import AsyncClient

from s3lite import Client, S3Exception

KEY_ID = environ["ACCESS_KEY_ID"]
ACCESS_KEY = environ["SECRET_ACCESS_KEY"]
ENDPOINT = "http://127.0.0.1:9000"


@pt.mark.asyncio
async def test_create_delete_bucket():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    name = f"test-{int(time())}"

    bucket = await client.create_bucket(name)
    assert bucket is not None
    assert bucket.name == name
    assert await bucket.ls() == []

    await client.delete_bucket(bucket)


@pt.mark.asyncio
async def test_ls_buckets():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)

    bucket = await client.create_bucket(f"test-{int(time())}")
    assert bucket.name in [b.name for b in await client.ls_buckets()]

    await client.delete_bucket(bucket)


@pt.mark.asyncio
async def test_ls_bucket():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")
    obj = await bucket.upload("/test.txt", BytesIO(b"test"))

    assert len(await bucket.ls()) == 1

    await obj.delete()
    await client.delete_bucket(bucket)


@pt.mark.asyncio
async def test_delete_bucket_with_objects():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")
    obj = await bucket.upload("/test.txt", BytesIO(b"test"))

    with pt.raises(S3Exception):
        await client.delete_bucket(bucket)

    await obj.delete()
    await client.delete_bucket(bucket)


@pt.mark.asyncio
async def test_upload_download_object():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")
    content_sp = urandom(1024 * 32)
    content_mp = urandom(1024 * 1024 * 32)

    obj_sp = await bucket.upload("/test-singlepart.txt", BytesIO(content_sp))
    obj_mp = await bucket.upload("/test-multipart.txt", BytesIO(content_mp))

    assert (await obj_sp.download(in_memory=True)).getvalue() == content_sp
    assert (await obj_mp.download(in_memory=True)).getvalue() == content_mp
    assert (await obj_mp.download(in_memory=True, offset=1025, limit=42)).getvalue() == content_mp[1025:1025 + 42]

    await obj_sp.delete()
    await obj_mp.delete()

    await client.delete_bucket(bucket)


@pt.mark.asyncio
async def test_presigned_url():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")
    content = urandom(1024 * 32)

    obj = await bucket.upload("/test-presigned.txt", BytesIO(content))

    url = obj.share()
    async with AsyncClient() as cl:
        resp = await cl.get(url)
        assert await resp.aread() == content

    await obj.delete()
    await client.delete_bucket(bucket)
