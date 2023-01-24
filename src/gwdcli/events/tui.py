import functools
import queue
import time
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pandas as pd
from anyio import to_thread
from gwproto.messages import EventBase
from rich.emoji import Emoji
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

from gwdcli.events.models import AnyEvent
from gwdcli.events.models import GWDEvent
from gwdcli.events.models import SyncCompleteEvent
from gwdcli.events.models import SyncStartEvent
from gwdcli.events.settings import EventsSettings


@dataclass
class SyncSpinnerData:
    name: str = ""
    spinner_name: str = "pong"
    style: str = "green"
    done_emoji: str = "white_check_mark"
    done: bool = False
    start_time: datetime = field(default_factory=datetime.now)
    elapsed: timedelta = timedelta(0)

    def stop(self):
        self.done = True
        self.elapsed = datetime.now() - self.start_time


class SyncSpinners:
    spinners: dict

    def __init__(self):
        self.spinners = dict()

    def add(self, spinner_data: SyncSpinnerData):
        self.spinners[spinner_data.name] = spinner_data

    def get(self, name: str) -> SyncSpinnerData:
        return self.spinners[name]

    def stop(self, name: str) -> None:
        self.get(name).stop()

    def panel(self, **kwargs) -> Panel:
        table = Table(style="cyan", **kwargs)
        table.add_column("Status")
        table.add_column("Info")
        for spinner_data in self.spinners.values():
            if spinner_data.done:
                renderables = (
                    Emoji(spinner_data.done_emoji),
                    Text(
                        f"{spinner_data.name}  {spinner_data.elapsed}",
                        style=spinner_data.style,
                    ),
                )
            else:
                renderables = (
                    Spinner(spinner_data.spinner_name, style=spinner_data.style),
                    Text(f"{spinner_data.name}  ", style=spinner_data.style),
                )
            table.add_row(*renderables)
        return Panel(table, title="[b]Sync", border_style="blue")


class TUI:
    settings: EventsSettings
    df: pd.DataFrame
    live_history_df: pd.DataFrame
    display_df: pd.DataFrame
    layout: Layout
    event_table: Table
    sync_table: Table
    sync_spinners: SyncSpinners
    queue: queue.Queue
    gwd_text: Text
    local_tz: timezone

    def __init__(self, settings: EventsSettings):
        self.settings = settings
        if self.settings.paths.csv_path.exists():
            self.df = pd.read_csv(
                self.settings.paths.csv_path,
                index_col="TimeNS",
                parse_dates=True,
                date_parser=functools.partial(pd.to_datetime, utc=True),
            )
        else:
            self.df = pd.DataFrame(
                index=pd.DatetimeIndex([], name="TimeNS"),
                columns=["MessageId", "Src", "TypeName", "other_fields"],
            )
        self.df.drop_duplicates("MessageId", inplace=True)
        self.live_history_df = self.df.head(0)
        self.display_df = self.df.tail(self.settings.tui.displayed_events)
        self.queue = queue.Queue()
        self.gwd_text = Text()
        self.sync_spinners = SyncSpinners()
        # noinspection PyTypeChecker
        self.local_tz = datetime.now(timezone(timedelta(0))).astimezone().tzinfo
        self.event_table = self.make_event_table()
        self.make_layout()

    def reload_dfs(self):
        self.df = pd.read_csv(
            self.settings.paths.csv_path,
            index_col="TimeNS",
            parse_dates=True,
            date_parser=functools.partial(pd.to_datetime, utc=True),
        )
        self.df = pd.concat([self.df, self.live_history_df]).sort_index()
        self.df.drop_duplicates("MessageId", inplace=True)
        self.display_df = self.df.tail(self.settings.tui.displayed_events)

    def make_layout(self):
        self.layout = Layout(name="root")
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="events", ratio=1),
            Layout(name="footer", size=10),
        )
        self.layout["footer"].split_row(
            # Layout(name="tree"),
            Layout(name="GWDEvents", minimum_size=100, ratio=2),
            Layout(name="sync"),
        )
        self.layout["header"].update(Header())
        self.layout["events"].update(self.event_table)
        self.layout["GWDEvents"].update(
            Panel(self.gwd_text, title="[b]GWDEvents", border_style="green")
        )
        self.layout["sync"].update(self.sync_spinners.panel())

    def handle_gwd_event(self, event: GWDEvent) -> None:
        event = event.event
        if isinstance(event, (SyncStartEvent, SyncCompleteEvent)):
            name = Path(event.synced_key).name
            if isinstance(event, SyncStartEvent):
                self.sync_spinners.add(SyncSpinnerData(name=name))
            else:
                self.sync_spinners.stop(name)
                self.reload_dfs()
                self.layout["events"].update(self.make_event_table())
            self.layout["sync"].update(self.sync_spinners.panel())
            text = name
        else:
            text = str(event)
            # with (self.settings.paths.data_dir / "log.txt").open("a") as f:
            #     f.write(text + "\n")
            if len(text) > 100:
                text = text[:97] + "..."
        self.gwd_text.append(
            f"{datetime.now().isoformat()}  "
            f"{event.TypeName[len('gridworks.event.debug_cli.'):]:24s}  "
            f"{text}\n"
        )

    def add_row(self, row: pd.Series) -> None:
        # noinspection PyUnresolvedReferences
        local_ts = row.name.tz_convert(self.local_tz)
        row_vals = [
            local_ts.strftime("%Y-%m-%d %X"),
            row.TypeName.removeprefix("gridworks.event.").removeprefix("comm."),
        ]
        if "Src" in row.index:
            row_vals.append(row.Src)
        row_vals.append(row.other_fields)
        self.event_table.add_row(*row_vals)

    def make_event_table(self) -> Table:
        self.event_table = Table(*(["Time", "TypeName", "Src", "other_fields"]))
        self.event_table.columns[0].header_style = "green"
        self.event_table.columns[0].style = "green"
        self.event_table.columns[1].header_style = "cyan"
        self.event_table.columns[1].style = "cyan"
        self.event_table.columns[2].header_style = "dark_orange"
        self.event_table.columns[2].style = "dark_orange"
        if self.settings.tui.max_other_fields_width > 0:
            self.event_table.columns[3].no_wrap = True
            self.event_table.columns[
                3
            ].max_width = self.settings.tui.max_other_fields_width
        for _, row in self.display_df.tail(
            self.settings.tui.displayed_events
        ).iterrows():
            self.add_row(row)
        return self.event_table

    def handle_event(self, event: EventBase) -> None:
        row_df = AnyEvent(**event.dict()).as_dataframe(columns=self.df.columns.values)
        display_not_full = len(self.display_df) < self.settings.tui.displayed_events
        if not (self.display_df["MessageId"] == event.MessageId).any() and (
            display_not_full or row_df.index[0] >= self.display_df.index[0]
        ):
            self.display_df = pd.concat([self.display_df, row_df]).sort_index()[1:]
            self.layout["events"].update(self.make_event_table())
        if (
            not (self.live_history_df["MessageId"] == event.MessageId).any()
            and not (self.df["MessageId"] == event.MessageId).any()
        ):
            self.live_history_df = pd.concat(
                [self.live_history_df, row_df]
            ).sort_index()
            if len(self.live_history_df) > 100:
                concatdf = pd.concat([self.df, self.live_history_df]).sort_index()
                droppeddf = concatdf.drop_duplicates("MessageId")
                droppeddf.to_csv(self.settings.paths.csv_path)
                self.df = droppeddf
                self.live_history_df = self.df.head(0)

    def handle_other(self, item: Any) -> None:
        pass

    def check_sync_queue(self):
        try:
            match item := self.queue.get(block=False):
                case GWDEvent():
                    self.handle_gwd_event(item)
                case EventBase():
                    self.handle_event(item)
                case _:
                    self.handle_other(item)
        except queue.Empty:
            pass

    def loop(self):
        with Live(self.layout, refresh_per_second=10, screen=False):
            while True:
                time.sleep(0.1)
                self.check_sync_queue()

    async def tui_task(self):
        await to_thread.run_sync(self.loop)


class Header:
    """Display header with clock."""

    # noinspection PyMethodMayBeStatic
    def __rich__(self) -> Panel:
        grid = Table.grid(expand=True)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="right")
        grid.add_row(
            "[b]Gridworks Events",
            datetime.now().ctime().replace(":", "[blink]:[/]"),
        )
        return Panel(grid, style="white on blue")
