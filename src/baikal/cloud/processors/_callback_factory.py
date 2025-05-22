from collections.abc import Callable

from baikal.cloud.context import ProgressTracker


def callback_factory(progress_tracker: ProgressTracker) -> Callable[[int], None]:
    def callback(bytes_transferred: int) -> None:
        progress_tracker.advance(bytes_transferred)

    return callback
