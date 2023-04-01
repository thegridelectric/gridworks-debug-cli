from pathlib import Path

import rich
import typer

from gwdcli.csv.egauge_download import egauge_download
from gwdcli.csv.settings import CSVSettings
from gwdcli.csv.settings import Paths
from gwdcli.csv.settings import ScadaConfig


app = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    rich_markup_mode="rich",
    help="""
    Commands for downloading GridWorks data in csv format.

    To generate default configuration, run: [green]
        gwd csv mkconfig
    [/green]
    """,
)
app.command("egd")(egauge_download)

# For sphinx:
typer_click_object = typer.main.get_command(app)


@app.command()
def info(config_path: Path = Paths().config_path):  # noqa: B008
    """Print current configuration."""

    rich.print("")
    rich.print(f"Config path: {config_path}")
    rich.print(f"Config path exists: {config_path.exists()}")
    rich.print("Current settings:")
    settings = CSVSettings.load(config_path)
    rich.print(settings)


@app.command()
def mkconfig(
    config_path: Path = Paths().config_path,
    data_dir: Path = typer.Option(
        Paths().data_dir, "-d", "--data-dir", help="Base output directory."
    ),
    force: bool = typer.Option(
        False, help="Overwrite existing config file, if it exists."
    ),
):
    """Create default config file for '[bold green]gwd csv[/bold green]' command."""

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
            f.write(
                CSVSettings(
                    paths=Paths(
                        config_path=config_path,
                        data_dir=Path(data_dir).absolute(),
                    ),
                    default_scada="apple",
                    scadas=dict(
                        apple=ScadaConfig(
                            atn="hw1.isone.me.freedom.apple",
                            egauge="4922",
                            bytes_per_row=240,
                        )
                    ),
                ).json(sort_keys=True, indent=2)
                + "\n"
            )
        rich.print("Created:")
        settings = CSVSettings.load(config_path)
        rich.print(settings)
    rich.print()


if __name__ == "__main__":
    app()
