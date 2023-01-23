import time
import queue
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from pathlib import Path

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
    start_time: datetime = field(default_factory = datetime.now)
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
        table = Table(title="S3 Syncs", style="cyan", **kwargs)
        table.add_column("Status")
        table.add_column("Info")
        for spinner_data in self.spinners.values():
            if spinner_data.done:
                renderables = (
                    Emoji(spinner_data.done_emoji),
                    Text(f"{spinner_data.name}  {spinner_data.elapsed}", style=spinner_data.style)
                )
            else:
                renderables = (
                    Spinner(spinner_data.spinner_name, style=spinner_data.style),
                    Text(f"{spinner_data.name}  ", style=spinner_data.style)
                )
            table.add_row(*renderables)
        return Panel(table, title="[b]Sync", border_style="blue")

class TUI:
    settings: EventsSettings
    df: pd.DataFrame
    layout: Layout
    event_table: Table
    sync_table: Table
    sync_spinners: SyncSpinners
    queue: queue.Queue
    gwd_text: Text


    def __init__(self, settings: EventsSettings):
        self.settings = settings
        self.df = pd.read_csv(self.settings.paths.csv_path, index_col="TimeNS", parse_dates=True)
        self.queue = queue.Queue()
        self.gwd_text = Text()
        self.sync_spinners = SyncSpinners()
        # self.make_progress()
        self.event_table = self.make_event_table()
        self.make_layout()

    def make_layout(self):
        self.layout = Layout(name="root")
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="events", ratio=1),
            Layout(name="footer", size=20),
        )
        self.layout["footer"].split_row(
            Layout(name="tree"),
            Layout(name="GWDEvents"),
            Layout(name="sync"),
        )
        self.layout["header"].update(Header())
        self.layout["events"].update(self.event_table)
        self.layout["GWDEvents"].update(Panel(self.gwd_text, title="[b]GWDEvents", border_style="green"))
        self.layout["tree"].update(Panel(self.layout.tree, border_style="red"))
        self.layout["sync"].update(self.sync_spinners.panel())

    def handle_gwd_event(self, event: GWDEvent) -> None:
        event = event.event
        if isinstance(event, (SyncStartEvent, SyncCompleteEvent)):
            name = Path(event.synced_key).name
            if isinstance(event, SyncStartEvent):
                self.sync_spinners.add(SyncSpinnerData(name=name))
            else:
                self.sync_spinners.stop(name)
            self.layout["sync"].update(self.sync_spinners.panel())
            text = name
        else:
            text = str(event)
        self.gwd_text.append(
            f"{datetime.now().isoformat()}  "
            f"{event.TypeName[len('gridworks.event.debug_cli.'):]:20s}  "
            f"{text}\n"
        )

    def add_row(self, timestamp: pd.Timestamp) -> None:
        row = self.df.loc[timestamp]
        row_vals = [
            timestamp.isoformat(timespec="milliseconds"),
            row.TypeName.removeprefix("gridworks.event.").removeprefix("comm."),
        ]
        if "Src" in self.df.columns:
            row_vals.append(row.Src)
        row_vals.append(row.other_fields)
        self.event_table.add_row(*row_vals)

    def make_event_table(self) -> Table:
        self.event_table = Table(*(["Time"] + list(self.df.columns)))
        self.event_table.columns[0].header_style = "green"
        self.event_table.columns[0].style = "green"
        self.event_table.columns[1].header_style = "cyan"
        self.event_table.columns[1].style = "cyan"
        if "Src" in self.df.columns:
            self.event_table.columns[2].header_style = "dark_orange"
            self.event_table.columns[2].style = "dark_orange"
        for timestamp in self.df.index:
            self.add_row(timestamp)
        return self.event_table

    def handle_event(self, event: EventBase) -> None:
        any_event = AnyEvent(**event.dict())
        row_dict = any_event.for_pandas()
        row_list = [row_dict[col] for col in self.df.columns.values]
        self.df.loc[row_dict["TimeNS"]] = row_list
        self.add_row(row_dict["TimeNS"])
        self.layout["events"].update(self.event_table)
        # def for_pandas(
        #         self,
        #         collapse_other_fields=True,
        #         other_field_name="other_fields",
        # ) -> dict:
        #     d = self.dict(include=EventBase.__fields__.keys())
        #     d["TimeNS"] = pd.Timestamp(self.TimeNS, unit="ns")
        #     other_fields = self.other_fields()
        #     if collapse_other_fields:
        #         d[other_field_name] = json.dumps(other_fields)
        #     else:
        #         d.update(other_fields)
        #     return d
        #
        pass

    def handle_other(self, Any) -> None:
        pass

    def check_sync_queue(self):
        try:
            match item:= self.queue.get(block=False):
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

