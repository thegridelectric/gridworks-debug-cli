import asyncio
import time
from datetime import datetime

from rich import box
from rich.align import Align
from rich.console import Group
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.progress import TaskID
from rich.syntax import Syntax
from rich.table import Table

from anyio import to_thread

from gwdcli.events.settings import EventsSettings


# noinspection PyUnusedLocal
async def console_task(settings: EventsSettings, console: Console, queue: asyncio.Queue):
    await to_thread.run_sync(LiveLayout.loop)

class LiveLayout:
    layout: Layout
    job_progress: Progress
    overall_progress: Progress
    overall_task: TaskID
    progress_table: Table

    def __init__(self):
        self.make_progress()
        self.make_layout()

    def make_layout(self):
        self.layout = make_layout()
        self.layout["header"].update(Header())
        self.layout["body"].update(make_sponsor_message())
        self.layout["box2"].update(Panel(make_syntax(), border_style="green"))
        self.layout["box1"].update(Panel(self.layout.tree, border_style="red"))
        self.layout["footer"].update(self.progress_table)

    def make_progress(self):
        self.job_progress = Progress(
            "{task.description}",
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        )
        self.job_progress.add_task("[green]Cooking")
        self.job_progress.add_task("[magenta]Baking", total=200)
        self.job_progress.add_task("[cyan]Mixing", total=400)
        self.overall_progress = Progress()
        self.overall_task = self.overall_progress.add_task(
            "All Jobs",
            total=int(sum(task.total for task in self.job_progress.tasks))
        )
        self.progress_table = Table.grid(expand=True)
        self.progress_table.add_row(
            Panel(
                self.overall_progress,
                title="Overall Progress",
                border_style="green",
                padding=(2, 2),
            ),
            Panel(self.job_progress, title="[b]Jobs", border_style="red", padding=(1, 2)),
        )

    @classmethod
    def loop(cls):
        helper = LiveLayout()
        with Live(helper.layout, refresh_per_second=10, screen=False):
            while not helper.overall_progress.finished:
                time.sleep(0.1)
                for job in helper.job_progress.tasks:
                    if not job.finished:
                        helper.job_progress.advance(job.id)
                completed = sum(task.completed for task in helper.job_progress.tasks)
                helper.overall_progress.update(helper.overall_task, completed=completed)

def make_layout() -> Layout:
    layout = Layout(name="root")

    layout.split(
        Layout(name="header", size=3),
        Layout(name="main", ratio=1),
        Layout(name="footer", size=7),
    )
    layout["main"].split_row(
        Layout(name="side"),
        Layout(name="body", ratio=2, minimum_size=60),
    )
    layout["side"].split(Layout(name="box1"), Layout(name="box2"))
    return layout


def make_sponsor_message() -> Panel:
    """Some example content."""
    sponsor_message = Table.grid(padding=1)
    sponsor_message.add_column(style="green", justify="right")
    sponsor_message.add_column(no_wrap=True)
    sponsor_message.add_row(
        "Twitter",
        "[u blue link=https://twitter.com/textualize]https://twitter.com/textualize",
    )
    sponsor_message.add_row(
        "CEO",
        "[u blue link=https://twitter.com/willmcgugan]https://twitter.com/willmcgugan",
    )
    sponsor_message.add_row(
        "Textualize", "[u blue link=https://www.textualize.io]https://www.textualize.io"
    )

    message = Table.grid(padding=1)
    message.add_column()
    message.add_column(no_wrap=True)
    message.add_row(sponsor_message)

    message_panel = Panel(
        Align.center(
            Group("\n", Align.center(sponsor_message)),
            vertical="middle",
        ),
        box=box.ROUNDED,
        padding=(1, 2),
        title="[b red]Thanks for trying out Rich!",
        border_style="bright_blue",
    )
    return message_panel


class Header:
    """Display header with clock."""

    # noinspection PyMethodMayBeStatic
    def __rich__(self) -> Panel:
        grid = Table.grid(expand=True)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="right")
        grid.add_row(
            "[b]Rich[/b] Layout application",
            datetime.now().ctime().replace(":", "[blink]:[/]"),
        )
        return Panel(grid, style="white on blue")


def make_syntax() -> Syntax:
    code = """\
def ratio_resolve(total: int, edges: List[Edge]) -> List[int]:
    sizes = [(edge.size or None) for edge in edges]

    # While any edges haven't been calculated
    while any(size is None for size in sizes):
        # Get flexible edges and index to map these back on to sizes list
        flexible_edges = [
            (index, edge)
            for index, (size, edge) in enumerate(zip(sizes, edges))
            if size is None
        ]
        # Remaining space in total
        remaining = total - sum(size or 0 for size in sizes)
        if remaining <= 0:
            # No room for flexible edges
            sizes[:] = [(size or 0) for size in sizes]
            break
        # Calculate number of characters in a ratio portion
        portion = remaining / sum((edge.ratio or 1) for _, edge in flexible_edges)

        # If any edges will be less than their minimum, replace size with the minimum
        for index, edge in flexible_edges:
            if portion * edge.ratio <= edge.minimum_size:
                sizes[index] = edge.minimum_size
                break
        else:
            # Distribute flexible space and compensate for rounding error
            # Since edge sizes can only be integers we need to add the remainder
            # to the following line
            _modf = modf
            remainder = 0.0
            for index, edge in flexible_edges:
                remainder, size = _modf(portion * edge.ratio + remainder)
                sizes[index] = int(size)
            break
    # Sizes now contains integers only
    return cast(List[int], sizes)
    """
    syntax = Syntax(code, "python", line_numbers=True)
    return syntax


