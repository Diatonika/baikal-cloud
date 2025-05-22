from collections.abc import Awaitable, Callable

from baikal.cloud.context import ProcessorTask, ProgressTracker, WorkerContext
from baikal.cloud.processors.pull import pull_processor
from baikal.cloud.processors.push import push_processor

type Processor = Callable[
    [WorkerContext, ProcessorTask, ProgressTracker], Awaitable[None]
]

__all__ = ["pull_processor", "push_processor", "Processor"]
