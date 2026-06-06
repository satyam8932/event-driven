from __future__ import annotations

import io

import aioboto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from app.config import get_settings
from app.domain.errors import StorageError
from app.logging import get_logger

log = get_logger(__name__)


def _make_session(public: bool = False) -> aioboto3.Session:
    return aioboto3.Session()


def _s3_kwargs(public: bool = False) -> dict:
    settings = get_settings()
    endpoint = settings.minio_public_endpoint if public else settings.minio_endpoint
    scheme = "https" if settings.minio_secure else "http"
    return {
        "service_name": "s3",
        "endpoint_url": f"{scheme}://{endpoint}",
        "aws_access_key_id": settings.minio_access_key,
        "aws_secret_access_key": settings.minio_secret_key,
        "config": Config(signature_version="s3v4"),
    }


async def put_object(key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    settings = get_settings()
    session = _make_session()
    try:
        async with session.client(**_s3_kwargs()) as s3:
            await s3.put_object(
                Bucket=settings.minio_bucket,
                Key=key,
                Body=io.BytesIO(data),
                ContentType=content_type,
            )
        log.debug("storage_put", key=key, size=len(data))
        return key
    except (BotoCoreError, ClientError) as exc:
        raise StorageError(f"PUT {key} failed: {exc}") from exc


async def get_object(key: str) -> bytes:
    settings = get_settings()
    session = _make_session()
    try:
        async with session.client(**_s3_kwargs()) as s3:
            response = await s3.get_object(Bucket=settings.minio_bucket, Key=key)
            return await response["Body"].read()
    except (BotoCoreError, ClientError) as exc:
        raise StorageError(f"GET {key} failed: {exc}") from exc


async def presign_url(key: str, expires: int = 3600) -> str:
    settings = get_settings()
    session = _make_session()
    try:
        async with session.client(**_s3_kwargs(public=True)) as s3:
            return await s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": settings.minio_bucket, "Key": key},
                ExpiresIn=expires,
            )
    except (BotoCoreError, ClientError) as exc:
        raise StorageError(f"presign {key} failed: {exc}") from exc
