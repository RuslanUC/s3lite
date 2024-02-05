# S3Lite
Minimal async s3 client implementation in python.

# Installation
**Requirements:**
 - Python 3.9+

```bash
pip install s3lite
```

# Examples

### List buckets
```python
from s3lite import Client

client = Client("key-id", "secret-key", "https://s3-endpoint")
for bucket in await client.ls_buckets():
    print(bucket.name)
```

### Create new bucket
```python
from s3lite import Client

client = Client("key-id", "secret-key", "https://s3-endpoint")
bucket = await client.create_bucket("new-bucket")
```

### Upload object
```python
from io import BytesIO
from s3lite import Client

client = Client("key-id", "secret-key", "https://s3-endpoint")
object_from_path = await client.upload_object("new-bucket", "test-image.jpg", "./local-image.png")

with open("document.txt", "rb") as f:  # Make sure to open file in read-binary mode
    object_from_file = await client.upload_object("new-bucket", "document.txt", f)

bytes_io = BytesIO(b"test file content")
object_from_bytesio = await client.upload_object("new-bucket", "test.txt", bytes_io)
```

### Download object
```python
from s3lite import Client

client = Client("key-id", "secret-key", "https://s3-endpoint")
saved_path = await client.download_object("new-bucket", "test-image.jpg", "./local-image.png")

bytes_io = await client.download_object("new-bucket", "document.txt", in_memory=True)

partial_bytes = await client.download_object("new-bucket", "test.txt", in_memory=True, offset=4, limit=6)
```

### Generate presigned url
```python
from s3lite import Client

client = Client("key-id", "secret-key", "https://s3-endpoint")

obj = await client.upload_object("new-bucket", "test-image.jpg", "./local-image.png")
presigned_url = obj.share()  # Url will be valid for 1 day

presigned_url_1h = client.share("new-bucket", "test.txt", 3600)  # Url will be valid for 1 hour
```

### Delete object
```python
from s3lite import Client

client = Client("key-id", "secret-key", "https://s3-endpoint")
saved_path = await client.delete_object("new-bucket", "test-image.jpg")
```