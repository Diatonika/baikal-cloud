from pathlib import Path

from baikal.cloud.context import (
    CloudInfo,
    ProcessorTask,
    ProgressTracker,
    WorkerContext,
)
from baikal.cloud.processors._callback_factory import callback_factory
from baikal.common.system import remove_path


async def pull_processor(
    worker_context: WorkerContext,
    processor_context: ProcessorTask,
    progress_tracker: ProgressTracker,
) -> None:
    cloud_info = processor_context.cloud_info
    local_info = processor_context.local_info

    if cloud_info is None and local_info is not None:
        remove_path(local_info.path)
        progress_tracker.update_status(f"removed 0x{local_info.sha256[:12]}")

        return

    if cloud_info is not None and local_info is None:
        file_path = worker_context.data_dir / Path(cloud_info.info["Key"])
        await _download(worker_context, cloud_info, file_path, progress_tracker)
        progress_tracker.update_status(f"created 0x{cloud_info.sha256[:12]}")

        return

    if cloud_info is not None and local_info is not None:
        if cloud_info.sha256 == local_info.sha256:
            progress_tracker.update_status(f"skipped 0x{cloud_info.sha256[:12]}")
            return

        await _download(worker_context, cloud_info, local_info.path, progress_tracker)
        progress_tracker.update_status(
            f"updated 0x{local_info.sha256[:12]} → 0x{cloud_info.sha256[:12]}"
        )

        return

    exception_message = "Invariant violation. cloud_info and local_info are None"
    raise ValueError(exception_message)


async def _download(
    worker_context: WorkerContext,
    cloud_info: CloudInfo,
    download_path: Path,
    progress_tracker: ProgressTracker,
) -> None:
    await worker_context.client.download_file(
        Bucket=worker_context.bucket,
        Key=cloud_info.info["Key"],
        Filename=download_path.as_posix(),
        Callback=callback_factory(progress_tracker),
    )
