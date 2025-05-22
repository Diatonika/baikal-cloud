from asyncio import Queue

from rich.progress import Progress, TaskID

from baikal.cloud.context import (
    CloudInfo,
    LocalInfo,
    ProcessorTask,
    ProgressTracker,
    WorkerContext,
    WorkerTask,
)
from baikal.cloud.processors import Processor
from baikal.cloud.util import bucket_sha256, local_sha256


async def worker(
    progress: Progress,
    queue: Queue[WorkerTask],
    worker_context: WorkerContext,
    processor: Processor,
) -> None:
    while task := await queue.get():
        local_file = task.local_file
        cloud_file = task.cloud_file

        rich_task = _create_task(progress, worker_context, task)

        local_info: LocalInfo | None = None
        if local_file is not None:
            local_info = LocalInfo(local_file, await local_sha256(local_file))

        cloud_info: CloudInfo | None = None
        if cloud_file is not None:
            cloud_info = CloudInfo(
                cloud_file,
                await bucket_sha256(
                    worker_context.client,
                    worker_context.bucket,
                    cloud_file["Key"],
                    "sha256",
                ),
            )

        processor_task = ProcessorTask(
            cloud_info=cloud_info,
            local_info=local_info,
        )

        progress.start_task(rich_task)
        await processor(
            worker_context,
            processor_task,
            ProgressTracker(progress, rich_task),
        )

        progress.update(rich_task, visible=False)
        queue.task_done()


def _create_task(
    progress: Progress,
    worker_context: WorkerContext,
    worker_task: WorkerTask,
) -> TaskID:
    if worker_task.local_file is not None:
        return progress.add_task(
            worker_task.local_file.relative_to(worker_context.data_dir).as_posix(),
            total=None,
            extra="",
        )

    if worker_task.cloud_file is not None:
        return progress.add_task(
            worker_task.cloud_file["Key"],
            total=None,
            extra="",
        )

    exception_message = "No local or cloud file found"
    raise ValueError(exception_message)
