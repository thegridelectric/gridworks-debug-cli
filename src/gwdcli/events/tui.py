import functools
import json
import logging
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
from gwproto import Message
from gwproto.enums import TelemetryName
from gwproto.gs import GsPwr
from gwproto.gt.gt_sh_status import GtShStatus
from gwproto.gt.gt_sh_status import GtShStatus_Maker
from gwproto.gt.snapshot_spaceheat import SnapshotSpaceheat
from gwproto.gt.snapshot_spaceheat import SnapshotSpaceheat_Maker
from gwproto.messages import EventBase
from rich.console import RenderableType
from rich.emoji import Emoji
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Column
from rich.table import Table
from rich.text import Text

from gwdcli.events.models import AnyEvent
from gwdcli.events.models import GWDEvent
from gwdcli.events.models import SyncCompleteEvent
from gwdcli.events.models import SyncStartEvent
from gwdcli.events.settings import EventsSettings


logger = logging.getLogger("gwd.events")


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
    snaps: dict[str, SnapshotSpaceheat]
    statuses: dict[str, GtShStatus]
    scadas_to_snap: list[str]

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
        self.load_snaps()
        self.select_scadas_for_snaps()
        self.load_statuses()
        self.make_layout()

    def _load_latest(self, suffix: str, member_name: str, maker: Any) -> None:
        setattr(self, member_name, dict())
        member = getattr(self, member_name)
        latest_dir = getattr(self.settings.paths, f"{suffix}_dir")
        path_suffix = f".{suffix}.json"
        for path in latest_dir.glob(f"**/*{path_suffix}"):
            with path.open() as f:
                latest_str = f.read()
            member[path.name[: -len(path_suffix)]] = maker.type_to_tuple(latest_str)

    def select_scadas_for_snaps(self):
        self.scadas_to_snap = []
        for requested in self.settings.snaps:
            for scada in self.snaps:
                if requested in scada:
                    self.scadas_to_snap.append(scada)
        snap_names = list(self.snaps.keys())
        while len(self.scadas_to_snap) < 2:
            if len(snap_names):
                snap_name = snap_names.pop()
                if snap_name not in self.scadas_to_snap:
                    self.scadas_to_snap.append(snap_name)
                else:
                    self.scadas_to_snap.append("")
            else:
                self.scadas_to_snap.append("")

    def load_statuses(self):
        self._load_latest("status", "statuses", GtShStatus_Maker)

    def load_snaps(self):
        self._load_latest("snap", "snaps", SnapshotSpaceheat_Maker)

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
            Layout(name="main", ratio=1),
            Layout(name="footer", size=10),
        )
        self.layout["main"].split_row(
            Layout(name="latest"),
            Layout(name="events", ratio=3),
        )
        self.layout["latest"].split(
            Layout(name="snap0"),
            Layout(name="snap1"),
        )
        self.layout["footer"].split_row(
            Layout(name="GWDEvents", minimum_size=100, ratio=2),
            Layout(name="sync"),
        )
        self.layout["header"].update(Header())
        self.layout["events"].update(self.event_table)
        self.layout["GWDEvents"].update(
            Panel(self.gwd_text, title="[b]GWDEvents", border_style="green")
        )
        self.layout["snap0"].update(self.make_snapshot(self.scadas_to_snap[0]))
        self.layout["snap1"].update(self.make_snapshot(self.scadas_to_snap[1]))
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
            logger.debug(text)
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
        self.event_table.columns[0].min_width = 20
        self.event_table.columns[1].header_style = "cyan"
        self.event_table.columns[1].style = "cyan"
        self.event_table.columns[1].min_width = 25
        self.event_table.columns[2].header_style = "dark_orange"
        self.event_table.columns[2].style = "dark_orange"
        if self.settings.tui.max_other_fields_width > 0:
            self.event_table.columns[2].no_wrap = True
            self.event_table.columns[2].max_width = 40
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
        logger.debug("++handle_event")
        path_dbg = 0
        if event.TypeName in ["gridworks.event.problem", "gridworks.event.shutdown"]:
            logger.info(event.json(sort_keys=True, indent=2))
        row_df = AnyEvent(**event.dict()).as_dataframe(
            columns=self.df.columns.values, interpolate_summary=True
        )
        display_not_full = len(self.display_df) < self.settings.tui.displayed_events
        if not (self.display_df["MessageId"] == event.MessageId).any() and (
            display_not_full or row_df.index[0] >= self.display_df.index[0]
        ):
            path_dbg |= 0x00000001
            self.display_df = pd.concat([self.display_df, row_df]).sort_index()[1:]
            self.layout["events"].update(self.make_event_table())
        if (
            not (self.live_history_df["MessageId"] == event.MessageId).any()
            and not (self.df["MessageId"] == event.MessageId).any()
        ):
            path_dbg |= 0x00000002
            self.live_history_df = pd.concat(
                [self.live_history_df, row_df]
            ).sort_index()
            if len(self.live_history_df) > 100:
                path_dbg |= 0x00000004
                concatdf = pd.concat([self.df, self.live_history_df]).sort_index()
                droppeddf = concatdf.drop_duplicates("MessageId")
                droppeddf.to_csv(self.settings.paths.csv_path)
                self.df = droppeddf
                self.live_history_df = self.df.head(0)
        logger.debug(f"--handle_event: 0x{path_dbg:08X}")

    def handle_pwr(self, pwr: GsPwr):
        pass

    def handle_snapshot(self, snap: SnapshotSpaceheat):
        logger.debug("++handle_snapshot")
        path_dbg = 0
        try:
            snapshot_path = self.settings.paths.snap_path(snap.FromGNodeAlias)
            if not snapshot_path.exists():
                path_dbg |= 0x00000001
                newer = True
            else:
                path_dbg |= 0x00000002
                with snapshot_path.open() as f:
                    snap_dict = json.loads(f.read())
                    stored_time = snap_dict.get("Snapshot", dict()).get(
                        "ReportTimeUnixMs", 0
                    )
                    newer = snap.Snapshot.ReportTimeUnixMs > stored_time
            if newer:
                path_dbg |= 0x00000004
                snap_str = json.dumps(snap.asdict(), sort_keys=True, indent=2)
                with snapshot_path.open("w") as f:
                    f.write(snap_str)
                self.snaps[snap.FromGNodeAlias] = snap
                self.select_scadas_for_snaps()
                for idx in [0, 1]:
                    path_dbg |= 0x00000008
                    if snap.FromGNodeAlias == self.scadas_to_snap[idx]:
                        path_dbg |= 0x00000010
                        self.layout[f"snap{idx}"].update(
                            self.make_snapshot(snap.FromGNodeAlias)
                        )
                logger.info(f"Snapshot from {snap.FromGNodeAlias}:")
                logger.info(snap_str)
        except Exception as e:
            path_dbg |= 0x00000020
            logger.exception(f"ERROR handling snapshot: {e}")
        logger.debug("--handle_snapshot  path:0x{path_dbg:08X}")

    def make_snapshot(self, name: str) -> RenderableType:
        if name not in self.snaps:
            return Panel("", border_style="blue")
        snap = self.snaps[name]
        report_time = (
            pd.Timestamp(snap.Snapshot.ReportTimeUnixMs, unit="ms", tz="UTC")
            .tz_convert(self.local_tz)
            .strftime("%Y-%m-%d %X")
        )
        table = Table(
            Column("Node", header_style="dark_orange", style="dark_orange"),
            Column(
                "Value",
                header_style="bold cyan",
                style="bold cyan",
                justify="right",
            ),
            Column("Unit", header_style="orchid1", style="orchid1"),
            title=f"\nSnapshot at [green]{report_time}",
        )
        for i in range(len(snap.Snapshot.AboutNodeAliasList)):
            telemetry_name = snap.Snapshot.TelemetryNameList[i]
            if (
                telemetry_name == TelemetryName.WATER_TEMP_C_TIMES1000
                or telemetry_name == TelemetryName.WATER_TEMP_C_TIMES1000.value
            ):
                value_str = f"{snap.Snapshot.ValueList[i]/1000:5.2f}"
                unit = "C"
            else:
                value_str = f"{snap.Snapshot.ValueList[i]}"
                unit = snap.Snapshot.TelemetryNameList[i].value
            table.add_row(snap.Snapshot.AboutNodeAliasList[i], value_str, unit)

        return Panel(table, title=f"[b]{snap.FromGNodeAlias}", border_style="blue")

    def handle_status(self, status: GtShStatus):
        try:
            status_path = self.settings.paths.snap_path(status.FromGNodeAlias)
            with status_path.open("w") as f:
                f.write(json.dumps(status.asdict(), sort_keys=True, indent=2))
            self.statuses[status.FromGNodeAlias] = status
        except Exception as e:
            logger.exception(f"ERROR handling status: {e}")

    def handle_message(self, message: Message):
        logger.debug("++handle_message")
        path_dbg = 0
        match message.Payload:
            case GsPwr():
                path_dbg |= 0x00000001
                self.handle_pwr(message.Payload)
            case SnapshotSpaceheat():
                path_dbg |= 0x00000002
                self.handle_snapshot(message.Payload)
            case GtShStatus():
                path_dbg |= 0x00000004
                self.handle_status(message.Payload)
            case _:
                path_dbg |= 0x00000008
                pass
        logger.debug(f"--handle_message: 0x{path_dbg:08X}")

    def handle_other(self, item: Any) -> None:
        pass

    def check_sync_queue(self):
        try:
            while True:
                path_dbg = 0
                match item := self.queue.get(block=False):
                    case GWDEvent():
                        path_dbg |= 0x00000001
                        self.handle_gwd_event(item)
                    case EventBase():
                        path_dbg |= 0x00000002
                        self.handle_event(item)
                    case Message():
                        path_dbg |= 0x00000004
                        self.handle_message(item)
                    case _:
                        path_dbg |= 0x00000008
                        self.handle_other(item)
                logger.debug(f"--check_sync_queue: 0x{path_dbg:08X}")
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
