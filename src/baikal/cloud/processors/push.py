from baikal.cloud.context import (
    LocalInfo,
    ProcessorTask,
    ProgressTracker,
    WorkerContext,
)
from baikal.cloud.processors._callback_factory import callback_factory


async def push_processor(
    worker_context: WorkerContext,
    processor_context: ProcessorTask,
    progress_tracker: ProgressTracker,
) -> None:
    cloud_info = processor_context.cloud_info
    local_info = processor_context.local_info

    if cloud_info is None and local_info is not None:
        await _upload(
            worker_context,
            local_info,
            local_info.path.relative_to(worker_context.data_dir).as_posix(),
            progress_tracker,
        )

        progress_tracker.update_status(f"created 0x{local_info.sha256[:12]}")
        return

    if cloud_info is not None and local_info is None:
        await worker_context.client.delete_object(
            Bucket=worker_context.bucket,
            Key=cloud_info.info["Key"],
        )

        progress_tracker.update_status(f"removed 0x{cloud_info.sha256[:12]}")
        return

    if cloud_info is not None and local_info is not None:
        if cloud_info.sha256 == local_info.sha256:
            progress_tracker.update_status(f"skipped 0x{local_info.sha256[:12]}")
            return

        await _upload(
            worker_context,
            local_info,
            cloud_info.info["Key"],
            progress_tracker,
        )

        progress_tracker.update_status(
            f"updated 0x{cloud_info.sha256[12]} → 0x{local_info.sha256[:12]}"
        )

        return

    exception_message = "Invariant violation. cloud_info and local_info are None"
    raise ValueError(exception_message)


async def _upload(
    worker_context: WorkerContext,
    local_info: LocalInfo,
    upload_path: str,
    progress_tracker: ProgressTracker,
) -> None:
    await worker_context.client.upload_file(
        Filename=local_info.path.as_posix(),
        Bucket=worker_context.bucket,
        Key=upload_path,
        ExtraArgs={"Metadata": {"sha256": local_info.sha256}},
        Callback=callback_factory(progress_tracker),
    )
