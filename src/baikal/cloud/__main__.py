import asyncio

from pathlib import Path

import click

from click import command, group, option
from dynaconf import Dynaconf

from baikal.cloud import CloudStorage, CloudStorageConfig


def _create_storage(settings: Path, secrets: Path) -> CloudStorage:
    config = Dynaconf(
        settings_files=[settings.as_posix(), secrets.as_posix()],
        environments=True,
        merge_enabled=True,
    )

    storage_config = CloudStorageConfig.model_validate(config("baikal.cloud").to_dict())
    return CloudStorage(storage_config)


async def _pull(storage: CloudStorage, path: Path) -> None:
    async with storage:
        await storage.pull(path)


@command(help="Pull data from cloud storage.")
@option(
    "--settings",
    default="settings.toml",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    help="TOML settings file",
)
@option(
    "--secrets",
    default=".secrets.toml",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    help="TOML secrets file",
)
@option("--path", type=click.Path(path_type=Path), required=True, help="Bucket path")
def pull(settings: Path, secrets: Path, path: Path) -> None:
    storage = _create_storage(settings, secrets)
    asyncio.run(_pull(storage, path))


async def _push(storage: CloudStorage, path: Path) -> None:
    async with storage:
        await storage.push(path)


@command(help="Push data to cloud storage.")
@option(
    "--settings",
    default="settings.toml",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    help="TOML configuration file",
)
@option(
    "--secrets",
    default=".secrets.toml",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    help="TOML secrets file",
)
@option("--path", type=click.Path(path_type=Path), required=True, help="Bucket path")
def push(settings: Path, secrets: Path, path: Path) -> None:
    storage = _create_storage(settings, secrets)
    asyncio.run(_push(storage, path))


@group(invoke_without_command=True)
def cloud() -> None:
    pass


cloud.add_command(pull)
cloud.add_command(push)

cloud()
