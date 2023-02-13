from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from rich.console import Console

from gwdcli.events.models import AnyEvent
from gwdcli.events.settings import Paths


console = Console()


def show_dir(directory: str = str(Paths().data_dir), src: str = "", n: int = 0):
    path = Path(directory)
    console.print(rf"Searching for event json files in \[{path}\]")
    parsed_events = AnyEvent.from_directories(
        [path], sort=True, ignore_validation_errors=True
    )
    console.print(f"Found {len(parsed_events)} files parseable as events.")
    df = AnyEvent.to_dataframe(
        parsed_events, columns=["TimeNS", "TypeName", "Src", "other_fields"]
    )
    if src:
        df = df.loc[df["Src"] == src]
        console.print(f"Found {len(df)} events for Src {src}.")
        df.drop(columns="Src", inplace=True)
    if 0 < n < len(df):
        console.print(f"Trimming to first {n} results")
        df = df[:n]

    from rich.table import Table

    table = Table(*(["Time"] + list(df.columns)))
    table.columns[0].header_style = "green"
    table.columns[0].style = "green"
    table.columns[1].header_style = "cyan"
    table.columns[1].style = "cyan"
    if "Src" in df.columns:
        table.columns[2].header_style = "dark_orange"
        table.columns[2].style = "dark_orange"

    local_tz = datetime.now(timezone(timedelta(0))).astimezone().tzinfo
    for time in df.index:
        row = df.loc[time]
        row_vals = [
            time.tz_convert(local_tz).strftime("%Y-%m-%d %X"),
            row.TypeName.removeprefix("gridworks.event.").removeprefix("comm."),
        ]
        if "Src" in df.columns:
            row_vals.append(row.Src)
        row_vals.append(row.other_fields)
        table.add_row(*row_vals)
    console.print(table)
