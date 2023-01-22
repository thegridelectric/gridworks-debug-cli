import asyncio
from pathlib import Path

import rich
import typer
from anyio import create_task_group
from anyio import run
from rich.console import Console

# from gwdcli.events.console import console_task
from gwdcli.events.mqtt import run_mqtt_client
from gwdcli.events.queue_loop import queue_loop
from gwdcli.events.settings import EventsSettings
from gwdcli.events.settings import Paths
from gwdcli.events.show_dir import show_dir
from gwdcli.events.sync import sync


app = typer.Typer(no_args_is_help=True)
app.command("dir")(show_dir)


@app.command()
def show(config_path: Path = Paths().config_path):  # noqa: B008
    settings = EventsSettings.load(config_path)
    run(show_main, settings, Console())


@app.command()
def info(config_path: Path = Paths().config_path):  # noqa: B008
    rich.print("")
    rich.print(f"Config path: {config_path}")
    rich.print(f"Config path exists: {config_path.exists()}")
    rich.print("Current settings:")
    settings = EventsSettings.load(config_path)
    rich.print(settings)


@app.command()
def mkconfig(
    config_path: Path = Paths().config_path, force: bool = False  # noqa: B008
):
    rich.print()
    if config_path.exists() and not force:
        rich.print(
            f":warning-emoji:    [orange_red1]Config path [/]{config_path} [orange_red1]already exists."
        )
        rich.print(":warning-emoji:    [orange_red1][bold]Doing nothing.")
        rich.print()
        rich.print("Use [bold] --force [/bold] to overwrite existing config file.")
    else:
        if config_path.exists():
            rich.print(
                f":warning-emoji:    [orange_red1][bold]Overwritting config file [/][/]{config_path}."
            )
        else:
            rich.print(f"Creating default config at {config_path}")
        Paths(config_path=config_path).mkdirs()
        with config_path.open("w") as f:
            f.write(EventsSettings().json(sort_keys=True, indent=2) + "\n")
        rich.print("Created:")
        settings = EventsSettings.load(config_path)
        rich.print(settings)
    rich.print()


# noinspection PyUnusedLocal
async def show_main(settings: EventsSettings, console: Console):
    settings.paths.mkdirs()
    queue = asyncio.Queue()
    async with create_task_group() as tg:
        tg.start_soon(run_mqtt_client, settings.mqtt, queue)
        tg.start_soon(sync, settings, queue)
        tg.start_soon(queue_loop, settings, queue)
        # tg.start_soon(console_task, settings, console, queue)


if __name__ == "__main__":
    app()
