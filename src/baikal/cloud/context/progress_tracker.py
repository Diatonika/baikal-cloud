from attrs import define
from rich.progress import Progress, TaskID


@define
class ProgressTracker:
    progress: Progress
    task_id: TaskID

    def advance(self, advance: int) -> None:
        self.progress.update(self.task_id, advance=advance)

    def update_status(self, status: str) -> None:
        self.progress.update(self.task_id, extra=status)
