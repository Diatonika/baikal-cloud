from contextlib import AsyncExitStack
from pathlib import Path
from types import TracebackType

from aioboto3 import Session
from types_aiobotocore_s3.client import S3Client
from types_aiobotocore_s3.type_defs import ObjectTypeDef

from baikal.cloud._synchronize import synchronize
from baikal.cloud.cloud_storage_config import CloudStorageConfig
from baikal.cloud.context import WorkerContext
from baikal.cloud.processors import pull_processor, push_processor
from baikal.cloud.util import list_objects


class CloudStorage:
    _client: S3Client
    _exit_stack: AsyncExitStack

    _config: CloudStorageConfig

    def __init__(self, config: CloudStorageConfig) -> None:
        self._config = config.model_copy()

    async def pull(self, cloud_path: Path) -> None:
        cloud_files = await self._fetch_cloud_files(cloud_path)
        local_files = await self._fetch_local_files(cloud_path)

        await synchronize(
            worker_count=self._config.local_worker_count,
            worker_context=self._worker_context,
            processor=pull_processor,
            cloud_files=cloud_files,
            local_files=local_files,
        )

    async def push(self, cloud_path: Path) -> None:
        cloud_files = await self._fetch_cloud_files(cloud_path)
        local_files = await self._fetch_local_files(cloud_path)

        await synchronize(
            worker_count=self._config.local_worker_count,
            worker_context=self._worker_context,
            processor=push_processor,
            cloud_files=cloud_files,
            local_files=local_files,
        )

    @property
    def _worker_context(self) -> WorkerContext:
        return WorkerContext(
            data_dir=self._config.local_data_dir,
            client=self._client,
            bucket=self._config.cloud_bucket,
        )

    async def _fetch_cloud_files(self, cloud_path: Path) -> dict[Path, ObjectTypeDef]:
        return {
            Path(obj["Key"]): obj
            async for obj in list_objects(
                self._client,
                self._config.cloud_bucket,
                cloud_path.as_posix(),
            )
        }

    async def _fetch_local_files(self, cloud_path: Path) -> dict[Path, Path]:
        local_path = self._config.local_data_dir / cloud_path
        return {
            file.relative_to(self._config.local_data_dir): file
            for file in local_path.rglob("*")
            if file.is_file()
        }

    async def __aenter__(self) -> None:
        self._exit_stack = AsyncExitStack()
        self._client = await self._exit_stack.enter_async_context(
            Session().client(
                service_name="s3",
                endpoint_url=self._config.cloud_url.unicode_string(),
                aws_access_key_id=self._config.cloud_key_id,
                aws_secret_access_key=self._config.cloud_key_secret.get_secret_value(),
            )
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
