from collections.abc import AsyncGenerator

from types_aiobotocore_s3 import S3Client
from types_aiobotocore_s3.type_defs import ObjectTypeDef


async def list_objects(
    client: S3Client, bucket: str, prefix: str
) -> AsyncGenerator[ObjectTypeDef]:
    paginator = client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    )

    async for page in paginator:
        if "Contents" not in page:
            continue

        for item in page["Contents"]:
            if "Size" in item and item["Size"]:
                yield item
