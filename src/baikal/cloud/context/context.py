from pathlib import Path

from attrs import define
from types_aiobotocore_s3 import S3Client
from types_aiobotocore_s3.type_defs import ObjectTypeDef


@define
class WorkerTask:
    cloud_file: ObjectTypeDef | None
    local_file: Path | None


@define
class WorkerContext:
    data_dir: Path
    client: S3Client
    bucket: str


@define
class LocalInfo:
    path: Path
    sha256: str


@define
class CloudInfo:
    info: ObjectTypeDef
    sha256: str


@define
class ProcessorTask:
    cloud_info: CloudInfo | None
    local_info: LocalInfo | None
