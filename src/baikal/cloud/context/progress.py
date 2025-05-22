from rich.progress import FileSizeColumn, Progress, TextColumn

from baikal.common.rich import RichConsoleStack

COLUMNS = (
    TextColumn("[progress.description]{task.description}", justify="left"),
    TextColumn("｜", justify="center"),
    FileSizeColumn(),
    TextColumn("•", justify="center"),
    TextColumn("{task.fields[extra]}", justify="left"),
)


def create_progress() -> Progress:
    console = RichConsoleStack.active_console()
    return Progress(*COLUMNS, console=console)
