from __future__ import annotations

import hashlib
import hmac
from datetime import datetime
from urllib.parse import urlparse, quote

from httpx import AsyncClient, Response
from httpx._types import URLTypes, HeaderTypes, RequestContent

from s3lite.utils import CaseInsensitiveDict


def sign_msg(key: bytes, msg: str):
    """ Sign message using key """
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


# https://github.com/andrewjroth/requests-auth-aws-sigv4/blob/bf46a91a2ce4a7ffeb4038694412bc514731603f/requests_auth_aws_sigv4/__init__.py#L31
class AWSSigV4:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region: str):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region
        self.payload_signing_enabled = True

    def _sign(self, date: datetime, credential_scope: str, canonical_request: str) -> str:
        amzdate = date.strftime('%Y%m%dT%H%M%SZ')
        datestamp = date.strftime('%Y%m%d')

        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256",
            amzdate,
            credential_scope,
            hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        ])

        k_date = sign_msg(f"AWS4{self.aws_secret_access_key}".encode('utf-8'), datestamp)
        k_region = sign_msg(k_date, self.region)
        k_service = sign_msg(k_region, "s3")
        k_signing = sign_msg(k_service, "aws4_request")
        return sign_msg(k_signing, string_to_sign).hex()

    def sign(self, url: str, headers: dict = None, method: str = "GET", body: bytes = b"",
             add_signature: bool = False) -> tuple[str, dict]:
        headers = CaseInsensitiveDict(**(headers or {}))
        new_headers = CaseInsensitiveDict()

        # Create a date for headers and the credential string
        t = datetime.utcnow()
        amzdate = t.strftime('%Y%m%dT%H%M%SZ')
        datestamp = t.strftime('%Y%m%d')

        # Parse request to get URL parts
        url_parts = urlparse(url)
        host = url_parts.hostname
        uri = url_parts.path
        if len(url_parts.query) > 0:
            qs = dict(map(lambda i: i.split('='), url_parts.query.split('&')))
        else:
            qs = {}

        # Setup Headers
        if "Host" not in headers:
            new_headers["Host"] = host
        if "Content-Type" not in headers:
            new_headers["Content-Type"] = "application/x-www-form-urlencoded; charset=utf-8; application/json"
        if "User-Agent" not in headers:
            new_headers["User-Agent"] = 's3lite'
        new_headers["X-AMZ-Date"] = amzdate

        # Task 1: Create Canonical Request
        # Ref: http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
        # Query string values must be URL-encoded (space=%20) and be sorted by name.
        canonical_querystring = "&".join(map(lambda p: "=".join(p), sorted(qs.items())))

        # Create payload hash (hash of the request body content).
        if not self.payload_signing_enabled:  # pragma: no cover
            payload_hash = 'UNSIGNED-PAYLOAD'
        elif method == 'GET' and not body:
            payload_hash = hashlib.sha256(b'').hexdigest()
        elif body:
            payload_hash = hashlib.sha256(body).hexdigest()
        else:
            payload_hash = hashlib.sha256(b'').hexdigest()

        new_headers["x-amz-content-sha256"] = payload_hash

        # Create the canonical headers and signed headers. Header names
        # must be trimmed and lowercase, and sorted in code point order from
        # low to high. Note that there is a trailing \n.
        headers_to_sign = sorted(
            filter(
                lambda h: h.startswith("x-amz-") or h == "host",
                map(lambda h_key: h_key.lower(), (headers | new_headers).keys())
            )
        )
        canonical_headers = ''.join(map(lambda h: ":".join((h, (headers | new_headers)[h])) + '\n', headers_to_sign))
        signed_headers = ";".join(headers_to_sign)

        # Combine elements to create canonical request
        canonical_request = '\n'.join([
            method, uri, canonical_querystring, canonical_headers, signed_headers, payload_hash
        ])

        # Task 2: Create string to sign
        credential_scope = f"{datestamp}/{self.region}/s3/aws4_request"
        signature = self._sign(t, credential_scope, canonical_request)

        # Task 4: Add signing information to request
        signature = (f"AWS4-HMAC-SHA256 Credential={self.aws_access_key_id}/{credential_scope}, "
                     f"SignedHeaders={signed_headers}, Signature={signature}")
        if add_signature:
            new_headers["Authorization"] = signature

        return signature, new_headers

    def presign(self, url: str, upload: bool = False, ttl: int = 86400) -> str:
        method = "PUT" if upload else "GET"

        t = datetime.utcnow()
        amzdate = t.strftime('%Y%m%dT%H%M%SZ')
        datestamp = t.strftime('%Y%m%d')

        credential_scope = f"{datestamp}/{self.region}/s3/aws4_request"

        url_parts = urlparse(url)
        host = url_parts.netloc
        uri = url_parts.path
        qs = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": quote(f"{self.aws_access_key_id}/{credential_scope}", safe=""),
            "X-Amz-Date": amzdate,
            "X-Amz-Expires": f"{ttl}",
            "X-Amz-SignedHeaders": "host",
        }

        canonical_querystring = "&".join(map(lambda p: "=".join(p), sorted(qs.items())))
        canonical_request = f"{method}\n{uri}\n{canonical_querystring}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD"

        signature = self._sign(t, credential_scope, canonical_request)
        url += f"?{canonical_querystring}"
        url += f"&X-Amz-Signature={signature}"

        return url


class SignedClient(AsyncClient):
    def __init__(self, signer: AWSSigV4):
        super().__init__()
        self._signer = signer

    async def get(self, url: URLTypes, *, headers: HeaderTypes | None = None, **kwargs) -> Response:
        if headers is None:
            headers = {}
        _, headers_ = self._signer.sign(url, headers, add_signature=True)
        headers |= headers_
        return await super().get(url=url, headers=headers, **kwargs)

    async def put(
            self,
            url: URLTypes, *,
            content: RequestContent | None = None,
            headers: HeaderTypes | None = None,
            **kwargs
    ) -> Response:
        if headers is None:
            headers = {}
        _, headers_ = self._signer.sign(url, headers, "PUT", content or b"", add_signature=True)
        headers |= headers_
        return await super().put(url=url, content=content, headers=headers, **kwargs)

    async def post(
            self,
            url: URLTypes, *,
            content: RequestContent | None = None,
            headers: HeaderTypes | None = None,
            **kwargs
    ) -> Response:
        if headers is None:
            headers = {}
        _, headers_ = self._signer.sign(url, headers, "POST", content or b"", add_signature=True)
        headers |= headers_
        return await super().post(url=url, content=content, headers=headers, **kwargs)

    async def delete(
            self,
            url: URLTypes, *,
            headers: HeaderTypes | None = None,
            **kwargs
    ) -> Response:
        if headers is None:
            headers = {}
        _, headers_ = self._signer.sign(url, headers, "DELETE", add_signature=True)
        headers |= headers_
        return await super().delete(url=url, headers=headers, **kwargs)
