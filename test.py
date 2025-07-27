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
async def test_ls_bucket_prefix():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")
    obj1 = await bucket.upload("/a/test.txt", BytesIO(b"test"))
    obj2 = await bucket.upload("/a/test2.txt", BytesIO(b"test"))
    obj3 = await bucket.upload("/b/test3.txt", BytesIO(b"test"))

    assert len(await bucket.ls("a/")) == 2
    assert len(await bucket.ls("b/")) == 1

    await obj1.delete()
    await obj2.delete()
    await obj3.delete()
    await client.delete_bucket(bucket)


@pt.mark.asyncio
async def test_ls_bucket_max_keys():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")
    obj1 = await bucket.upload("/a/test.txt", BytesIO(b"test"))
    obj2 = await bucket.upload("/a/test2.txt", BytesIO(b"test"))
    obj3 = await bucket.upload("/b/test3.txt", BytesIO(b"test"))

    assert len(await bucket.ls(max_keys=1)) == 1
    assert (await bucket.ls(max_keys=1))[0].name == "a/test.txt"

    async for obj in client.ls_bucket_iter(bucket.name, "b/", 2):
        assert obj.name == "b/test3.txt"

    await obj1.delete()
    await obj2.delete()
    await obj3.delete()
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


@pt.mark.asyncio
async def test_bucket_policies():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")
    content = urandom(1024 * 32)
    policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Principal': {'AWS': ['*']},
            'Action': ['s3:GetObject'],
            'Resource': [f'arn:aws:s3:::{bucket.name}/*']
        }]
    }

    obj = await bucket.upload("/test.txt", BytesIO(content))

    async with AsyncClient() as cl:
        resp = await cl.get(f"{ENDPOINT}/{bucket.name}/test.txt")
        assert resp.status_code == 403

    await bucket.put_policy(policy)
    assert await bucket.get_policy() == policy

    async with AsyncClient() as cl:
        resp = await cl.get(f"{ENDPOINT}/{bucket.name}/test.txt")
        assert resp.status_code == 200
        assert await resp.aread() == content

    await bucket.delete_policy()
    with pt.raises(S3Exception):
        await bucket.get_policy()

    await obj.delete()
    await client.delete_bucket(bucket)


@pt.mark.asyncio
async def test_get_object():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")
    obj1 = await bucket.upload("/a/test.txt", BytesIO(b"test"))

    obj = await client.get_object(bucket, "a/test.txt")
    assert obj.name == obj1.name
    assert obj.size == obj1.size

    assert await client.get_object(bucket, "nonexistent") is None

    await obj1.delete()
    await client.delete_bucket(bucket)


@pt.mark.asyncio
async def test_presigned_url_upload():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")

    url = client.share(bucket, "/test-upload-presigned.bin", 60, True)
    content = urandom(1024 * 32)

    async with AsyncClient() as cl:
        resp = await cl.put(url, content=content)
        assert resp.status_code == 200

    obj = await client.download_object(bucket, "/test-upload-presigned.bin", in_memory=True)
    assert obj.getvalue() == content

    await (await bucket.get_object("test-upload-presigned.bin")).delete()
    await client.delete_bucket(bucket)


@pt.mark.asyncio
async def test_presigned_url_custom_filename():
    client = Client(KEY_ID, ACCESS_KEY, ENDPOINT)
    bucket = await client.create_bucket(f"test-{int(time())}")
    content = urandom(1024 * 32)

    obj = await bucket.upload("/test-presigned.txt", BytesIO(content))

    url = obj.share(download_filename="test-custom.txt")
    async with AsyncClient() as cl:
        resp = await cl.get(url)
        assert resp.headers["content-disposition"] == "attachment; filename=\"test-custom.txt\""
        assert await resp.aread() == content

    await obj.delete()
    await client.delete_bucket(bucket)
