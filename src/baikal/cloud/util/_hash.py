import hashlib
import logging

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

from aiofiles import open as aio_open
from aiofiles.threadpool.binary import AsyncBufferedReader
from botocore.exceptions import ClientError
from types_aiobotocore_s3 import S3Client

from baikal.common.system import FileSystemUtil


async def local_sha256(path: Path, chunk_size: int = 4096) -> str:
    if not FileSystemUtil.is_file_exist(path):
        return ""

    sha256_hash = hashlib.sha256()
    async with aio_open(path, "rb") as aio_file:
        async for block in AIOChunkReader(aio_file, chunk_size):
            sha256_hash.update(block)

    return sha256_hash.hexdigest()


async def bucket_sha256(client: S3Client, bucket: str, key: str, sha_key: str) -> str:
    try:
        object_info = await client.head_object(Bucket=bucket, Key=key)
        metadata = object_info["Metadata"]

        if sha_key not in metadata:
            logging.warning(
                f"No sha256 key ({sha_key}) found in {key} object metadata."
            )

        return metadata.get(sha_key, "")

    except ClientError:
        return ""


class AIOChunkReader:
    def __init__(self, aio_file: AsyncBufferedReader, chunk_size: int) -> None:
        self._aio_file = aio_file
        self._chunk_size = chunk_size

    async def __aiter__(self) -> AsyncGenerator[bytes, Any]:
        while len(buffer := await self._aio_file.read(self._chunk_size)):
            yield buffer
