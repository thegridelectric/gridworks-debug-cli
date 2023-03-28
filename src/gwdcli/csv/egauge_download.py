from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Tuple

import aiohttp
import anyio
import pendulum
import pytimeparse2
import typer
from rich import print
from rich.progress import BarColumn
from rich.progress import FileSizeColumn
from rich.progress import Progress
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TotalFileSizeColumn

from gwdcli.csv.settings import CSVSettings
from gwdcli.csv.settings import Paths


def egauge_download(  # noqa: C901
    scada: str = typer.Option(
        "",
        "-s",
        "--scada",
        help="Short name for scada whose eGauge download. Defaults to contents of [orange3]CSVSettings.default_scada",
    ),
    config_path: Path = typer.Option(Paths().config_path),
    start: datetime = typer.Option(
        None,
        "-s",
        "--start",
        help=(
            "Start date/time, in local timezone, e.g. [green]2023-03-01[/green]. "
            "If unspecified, calculated by moving --duration back from current time. "
            "If unspecified and --duration is an [bold]int[/bold] (a number of days), "
            "start will calculated from the upcoming midnight, not from the current time ("
            "start will be at a midnight)."
        ),
    ),
    duration: str = typer.Option(
        "1",
        "-d",
        "--duration",
        help=(
            "Duration of download, e.g. [green]1[/green] (1 day) or [green]8h[/green] (8 hours)"
        ),
    ),
    yesterday: bool = typer.Option(
        False,
        "-y",
        "--yesterday",
        help="Download data for mignight yesterday to midnight today.",
    ),
    period: int = typer.Option(60, "-p", "--period", help="Seconds per row."),
    dry_run: bool = typer.Option(False, help="Just print information. Do nothing."),
    thirsty_run: bool = typer.Option(False, help="Download data but do not save it."),
) -> None:
    """Download data directly from the eGauge server to a csv file.

    Output directory is next to the specified config_path, named for specified scada.
    By default downloads data from last midnight to now.

    Examples:

        Download today's data for default scada:

            [green]gwd csv egd[/green]

        Download yesterdays's data for default scada:

            [green]gwd csv egd -y[/green]

        Print info without downloading:

            [green]gwd csv egd --dry-run[/green]

        Download data until two midnights back (less than 48 hours):

            [green]gwd csv egd -d 2[green]

        Download data for last 48 hours from current time:

            [green]gwd csv egd -d 2d[/green]

        Or:

            [green]gwd csv egd -d 2.0[/green]

    """
    settings = CSVSettings.load(config_path)
    if not scada:
        scada = settings.default_scada
    if not scada:
        raise typer.BadParameter(
            "--egauge or --scada must be specified. Scada may be specified "
            "directly or by 'default_settings' in settings"
        )
    if scada not in settings.scadas:
        raise typer.BadParameter(
            f"Specified scada ({scada}) not founds in settings.scadas, which contains: "
            f"{str(list(settings.scadas.keys()))}"
        )
    egauge = settings.scadas[scada].egauge
    duration_seconds: int | float
    duration_is_int_days = False
    if yesterday:
        if start is not None or duration != "1":
            raise typer.BadParameter(
                "If --yesterday is specified, --start and --duration must not be specified"
            )
        duration_seconds = timedelta(days=1).total_seconds()
        start = pendulum.today().subtract(seconds=duration_seconds)
    else:
        try:
            try:
                duration_float = float(int(duration))
                duration_is_int_days = True
            except ValueError:
                duration_float = float(duration)
            duration_seconds = duration_float * timedelta(days=1).total_seconds()
        except ValueError:
            try:
                duration_seconds = pytimeparse2.parse(duration, raise_exception=True)
            except Exception as e:
                raise typer.BadParameter(
                    f"Could not parse duration ({duration}) as a duration"
                ) from e
        if start is None:
            if duration_is_int_days:
                start = pendulum.tomorrow().subtract(seconds=duration_seconds)
            else:
                start = pendulum.now().subtract(seconds=duration_seconds)
        else:
            start = pendulum.instance(start, tz="local")
    now = pendulum.now()
    if start.add(seconds=duration_seconds) > now:
        actual_duration_seconds = now.diff(start).total_seconds()
    else:
        actual_duration_seconds = duration_seconds
    start_utc = start.astimezone(tz=timezone.utc)
    rows = int(duration_seconds / period)
    url = settings.egauge.url(
        egauge_id=egauge,
        start_utc=start_utc.timestamp(),
        seconds_per_row=period,
        rows=rows,
    )
    egauge_csv_path = settings.paths.scada_csv_path(
        scada, start, start.add(seconds=actual_duration_seconds), "egauge"
    )
    print("\n[bold deep_sky_blue1]eGauge Download[/bold deep_sky_blue1]")
    print(f"  [green]specified duration[/green]: {timedelta(seconds=duration_seconds)}")
    print(
        f"  [green]actual duration[/green]:    {timedelta(seconds=actual_duration_seconds)}"
    )
    print(f"  [green]start[/green]:              {start.to_datetime_string()}")
    print(f"  [green]start utc[/green]:          {start_utc.to_datetime_string()}")
    print(f"  [green]seconds per row[/green]:    {period}")
    print(f"  [green]rows[/green]:               {rows}")
    print(f"  [green]url[/green]:                {url}")
    print(f"  [green]csv[/green]:                {egauge_csv_path}")

    async def download() -> Tuple[int, str]:
        print(
            "[green]Downloading csv from eGauge. [yellow][bold](Delays are from the eGauge server)[/bold][green] ..."
        )
        with Progress(
            BarColumn(complete_style="deep_sky_blue1"),
            FileSizeColumn(),
            TextColumn("/"),
            TotalFileSizeColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task(
                "",
                total=rows * settings.scadas[scada].bytes_per_row,
            )
            async with aiohttp.ClientSession() as session:
                progress.update(task, completed=0)
                async with session.get(url) as response:
                    chunks = b""
                    async for chunk, _ in response.content.iter_chunks():
                        chunks += chunk
                        progress.update(task, completed=len(chunks))
                text_ = chunks.decode(encoding=response.get_encoding())
                return response.status, text_

    if dry_run:
        print("\nDry run. Doing nothing.\n")
    else:
        code, text = anyio.run(download)
        if code == 200:
            csv_dir = egauge_csv_path.parent
            if thirsty_run:
                print("\nThirsty run. Not saving CSV file\n")
            else:
                if not csv_dir.exists():
                    csv_dir.mkdir(parents=True)
                with egauge_csv_path.open("w") as f:
                    f.write(text)
                print(f"Wrote {egauge_csv_path}")
        else:
            print(f"Download returned error {code}:\n{text}")
