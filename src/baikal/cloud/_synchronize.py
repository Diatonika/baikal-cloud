from asyncio import Queue, Task, create_task, gather
from pathlib import Path

from types_aiobotocore_s3.type_defs import ObjectTypeDef

from baikal.cloud._worker import worker
from baikal.cloud.context import (
    WorkerContext,
    WorkerTask,
    create_progress,
)
from baikal.cloud.processors import Processor


async def synchronize(
    worker_count: int,
    worker_context: WorkerContext,
    processor: Processor,
    cloud_files: dict[Path, ObjectTypeDef],
    local_files: dict[Path, Path],
) -> None:
    with create_progress() as progress:
        async_queue = Queue[WorkerTask]()

        for path in set(cloud_files) | set(local_files):
            worker_task = WorkerTask(
                cloud_file=cloud_files.get(path),
                local_file=local_files.get(path),
            )

            async_queue.put_nowait(worker_task)

        tasks: list[Task[None]] = []
        for _ in range(worker_count):
            worker_coro = worker(progress, async_queue, worker_context, processor)
            tasks.append(create_task(worker_coro))

        await async_queue.join()
        for task in tasks:
            task.cancel()

        await gather(*tasks, return_exceptions=True)

        for rich_task in progress.tasks:
            progress.update(rich_task.id, visible=True)
