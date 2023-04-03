import io
import typing
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from types import NoneType
from typing import Tuple

import aiohttp
import anyio
import pandas as pd
import pendulum
import pytimeparse2
import typer
from pandas._typing import ReadCsvBuffer  # noqa
from rich import print
from rich.progress import BarColumn
from rich.progress import FileSizeColumn
from rich.progress import Progress
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TotalFileSizeColumn
from yarl import URL

from gwdcli.csv.settings import CSVSettings
from gwdcli.csv.settings import Paths
from gwdcli.csv.settings import ScadaConfig


class EGDArgInfo:
    scada_name: str
    scada_config: ScadaConfig
    specified_duration_seconds: int
    actual_duration_seconds: int
    start: pendulum.DateTime
    end: pendulum.DateTime
    end_utc: pendulum.DateTime
    period_seconds: int
    rows: int
    url: URL
    egauge_csv_path: Path

    def __init__(  # noqa: C901
        self,
        settings: CSVSettings,
        scada_name: str,
        start: datetime | NoneType,
        duration: str,
        yesterday: bool,
        period_seconds: int,
    ):
        if not scada_name:
            scada_name = settings.default_scada
        if not scada_name:
            raise typer.BadParameter(
                "--egauge or --scada must be specified. Scada may be specified "
                "directly or by 'default_settings' in settings"
            )
        if scada_name not in settings.scadas:
            raise typer.BadParameter(
                f"Specified scada ({scada_name}) not founds in settings.scadas, which contains: "
                f"{str(list(settings.scadas.keys()))}"
            )
        self.scada_name = scada_name
        self.scada_config = settings.scadas[self.scada_name]
        duration_is_int_days = False
        if yesterday:
            if start is not None or duration != "1":
                raise typer.BadParameter(
                    "If --yesterday is specified, --start and --duration must not be specified"
                )
            self.specified_duration_seconds = int(timedelta(days=1).total_seconds())
            self.start = pendulum.today().subtract(
                seconds=self.specified_duration_seconds
            )
        else:
            try:
                try:
                    # First try to parse duration as an int, indicating days.
                    duration_days_float = float(int(duration))
                    duration_is_int_days = True
                except ValueError:
                    # Next try to parse duration as a float, also indicating days
                    duration_days_float = float(duration)
                self.specified_duration_seconds = int(
                    duration_days_float * timedelta(days=1).total_seconds()
                )
            except ValueError:
                try:
                    # Finally try to parse duration using pytimeparse2
                    self.specified_duration_seconds = int(
                        pytimeparse2.parse(duration, raise_exception=True)
                    )
                except Exception as e:
                    raise typer.BadParameter(
                        f"Could not parse duration ({duration}) as a duration"
                    ) from e
            if start is None:
                if duration_is_int_days:
                    self.start = pendulum.tomorrow().subtract(
                        seconds=self.specified_duration_seconds
                    )
                else:
                    self.start = pendulum.now().subtract(
                        seconds=self.specified_duration_seconds
                    )
            else:
                self.start = pendulum.instance(start, tz="local")
        now = pendulum.now()
        if self.start.add(seconds=self.specified_duration_seconds) > now:
            self.actual_duration_seconds = int(now.diff(self.start).total_seconds())
        else:
            self.actual_duration_seconds = self.specified_duration_seconds
        self.end = self.start.add(seconds=self.actual_duration_seconds)
        self.end_utc = self.end.astimezone(tz=timezone.utc)
        self.period_seconds = period_seconds
        self.rows = int(self.actual_duration_seconds / self.period_seconds)
        self.url = settings.egauge.url(
            egauge_id=self.scada_config.egauge,
            end_utc=self.end_utc.timestamp(),
            seconds_per_row=period_seconds,
            rows=self.rows,
        )
        self.egauge_csv_path = settings.paths.scada_csv_path(
            self.scada_name, self.start, self.end, "egauge"
        )

    def __str__(self) -> str:
        return (
            "[bold deep_sky_blue1]eGauge Download[/bold deep_sky_blue1]\n"
            f"  [green]Specified duration[/green]: {timedelta(seconds=self.specified_duration_seconds)}\n"
            f"  [green]Actual duration[/green]:    {timedelta(seconds=self.actual_duration_seconds)}\n"
            f"  [green]Start[/green]:              {self.start.to_datetime_string()}\n"
            f"  [green]End[/green]:                {self.end.to_datetime_string()}\n"
            f"  [green]End utc[/green]:            {self.end_utc.to_datetime_string()}\n"
            f"  [green]Seconds per row[/green]:    {self.period_seconds}\n"
            f"  [green]Rows[/green]:               {self.rows}\n"
            f"  [green]URL[/green]:                {self.url}\n"
            f"  [green]CSV path[/green]:           {self.egauge_csv_path}\n"
        )

    async def download(self) -> Tuple[int, str]:
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
                total=self.rows * self.scada_config.bytes_per_row,
            )
            async with aiohttp.ClientSession() as session:
                progress.update(task, completed=0)
                async with session.get(self.url) as response:
                    chunks = b""
                    async for chunk, _ in response.content.iter_chunks():
                        chunks += chunk
                        progress.update(task, completed=len(chunks))
                text_ = chunks.decode(encoding=response.get_encoding())
                return response.status, text_


def egauge_download(
    scada: str = typer.Option(
        "",
        "-g",
        "--gnode",
        help="Short name for atn/scada whose eGauge download. Defaults to contents of [orange3]CSVSettings.default_scada",
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
    local_time: bool = typer.Option(
        False,
        "-l",
        "--local-time",
        help="Convert downloaded times from UTC to local times before writing CSV",
    ),
    write_tz: bool = typer.Option(
        False, "-z", "--write-tz", help="Write timezone offset from UTC in the CSV"
    ),
    raw: bool = typer.Option(
        False,
        "--raw",
        help="In addition usual CSV file, output the raw CSV from eGauge, prior to date sorting timzone application",
    ),
    dry_run: bool = typer.Option(False, help="Just print information. Do nothing."),
    thirsty_run: bool = typer.Option(False, help="Download data but do not save it."),
) -> int:
    """Download data directly from the eGauge server to a csv file.

    Output directory is next to the specified config_path, named for specified scada.
    By default, downloads data from last midnight to now.

    Examples:

        Download today's data for default scada:

            [green]gwd csv egd[/green]

        Download yesterday's data for default scada:

            [green]gwd csv egd -y[/green]

        Print info without downloading:

            [green]gwd csv egd --dry-run[/green]

        Download data until two midnights back (less than 48 hours):

            [green]gwd csv egd -d 2[/green]

        Download data for last 48 hours from current time:

            [green]gwd csv egd -d 2d[/green]

        Or, equivalently:

            [green]gwd csv egd -d 2.0[/green]

    """
    settings = CSVSettings.load(config_path)
    info = EGDArgInfo(
        settings=settings,
        scada_name=scada,
        start=start,
        duration=duration,
        yesterday=yesterday,
        period_seconds=period,
    )
    print()
    print(str(info))
    ret = 0
    if dry_run:
        print("\nDry run. Doing nothing.\n")
    else:
        code, text = anyio.run(info.download)
        sio = io.StringIO(text)
        df = pd.read_csv(
            typing.cast(ReadCsvBuffer[str], sio),
            index_col="Date & Time",
            parse_dates=True,
        )
        df.index = df.index.sort_values()
        df.index = df.index.tz_localize("UTC")
        if local_time:
            df.index = df.index.tz_convert(pendulum.now().timezone.name)
        if not write_tz:
            df.index = df.index.tz_localize(None)
        if code == 200:
            if thirsty_run:
                print("\nThirsty run. Not saving CSV file\n")
            else:
                csv_dir = info.egauge_csv_path.parent
                if not csv_dir.exists():
                    csv_dir.mkdir(parents=True)
                df.to_csv(info.egauge_csv_path)
                print(f"Wrote {info.egauge_csv_path}")
                if raw:
                    raw_path = info.egauge_csv_path.parent / (
                        info.egauge_csv_path.name + ".raw.csv"
                    )
                    with raw_path.open("w") as f:
                        f.write(text)
                    print(f"Wrote {raw_path}")
                print("\nOpen with command:\n")
                print(f"open  {info.egauge_csv_path} &\n")
        else:
            print(f"Download returned error {code}:\n{text}")
            ret = -code
    return ret
