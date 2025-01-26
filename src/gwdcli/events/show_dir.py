from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from rich.console import Console

from gwdcli.events.models import AnyEvent
from gwdcli.events.settings import Paths
from gwdcli.events.tui import UNDISPLAYED_EVENTS


console = Console()


def show_dir(  # noqa:C901
    directory: str = "",
    src: str = "",
    n: int = 0,
    all: bool = False,  # noqa
    excludes: Optional[str] = None,
):
    """Display events in a directory."""
    summary = ""
    data_dir = Paths().data_dir
    if not directory:
        directory = data_dir
    path = Path(directory)
    if not path.exists():
        subdir_path = data_dir / directory
        if not subdir_path.exists():
            raise FileNotFoundError(
                f"Directory does not exist as either {directory} " f"or {subdir_path}"
            )
        path = subdir_path
    summary += rf"Searched for event json files in \[{path}\]"
    parsed_events = AnyEvent.from_directories(
        [path],
        sort=True,
        ignore_validation_errors=True,
    )
    summary += f"\nFound {len(parsed_events)} files parseable as events."
    df = AnyEvent.to_dataframe(
        parsed_events, columns=["TimeCreatedMs", "TypeName", "Src", "other_fields"]
    )
    if src:
        df = df.loc[df["Src"].str.contains(src)]
        summary += f"\nFound {len(df)} events for Src {src}."
        df.drop(columns="Src", inplace=True)
    if not all or excludes is not None:
        excluded = []
        if excludes is not None:
            excluded.extend(excludes.split(","))
        if not all:
            excluded.extend(UNDISPLAYED_EVENTS)
        df = df.loc[~df["TypeName"].isin(excluded)]
        summary += f"\nFound {len(df)} events after exclusions of {excluded}."

    if 0 < n < len(df):
        summary += f"\nTrimmed to first {n} results"
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
    time: pd.Timestamp
    for time, row in df.iterrows():
        row_vals = [
            time.tz_convert(local_tz).strftime("%Y-%m-%d %X"),
            row.TypeName.removeprefix("gridworks.event.").removeprefix("comm."),
        ]
        if "Src" in df.columns:
            row_vals.append(row.Src)
        row_vals.append(row.other_fields)
        table.add_row(*row_vals)
    console.print(table)
    console.print(summary)
