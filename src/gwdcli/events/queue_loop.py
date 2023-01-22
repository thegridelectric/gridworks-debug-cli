import asyncio
from typing import Any

import rich
from gwproto.messages import EventBase
from gwproto.messages import ProblemEvent

from gwdcli.events.models import GWDEvent
from gwdcli.events.models import SyncCompleteEvent
from gwdcli.events.models import SyncStartEvent
from gwdcli.events.settings import EventsSettings


# noinspection PyUnusedLocal
async def queue_loop(settings: EventsSettings, queue: asyncio.Queue):
    while True:
        item = await queue.get()
        await handle_queue_item(item)
        queue.task_done()


async def handle_gwdevent(gwdevent: GWDEvent):
    rich.print(gwdevent)
    match gwdevent.event:
        case SyncStartEvent():
            pass
        case SyncCompleteEvent():
            pass
        case ProblemEvent():
            pass


async def handle_queue_item(item: Any):
    match item:
        case GWDEvent():
            await handle_gwdevent(item)
        case EventBase():
            rich.print(item)
        case _:
            rich.print(item)
