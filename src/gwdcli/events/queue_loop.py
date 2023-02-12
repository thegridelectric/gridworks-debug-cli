"""Generic message processor inside asyncio event loop. Probably unnecessary. Should probably be removed and messages
should instead be directly fed to TUI's sync queue. """

import asyncio
import queue
from typing import Any

# import rich
# from gwproto.messages import EventBase
# from gwproto.messages import ProblemEvent
#
# from gwdcli.events.models import GWDEvent
# from gwdcli.events.models import SyncCompleteEvent
# from gwdcli.events.models import SyncStartEvent
from gwdcli.events.settings import EventsSettings


class AsyncQueueLooper:
    settings: EventsSettings
    async_queue: asyncio.Queue
    sync_queue: queue.Queue

    def __init__(
        self,
        settings: EventsSettings,
        async_queue: asyncio.Queue,
        sync_queue: queue.Queue,
    ):
        self.settings = settings
        self.async_queue = async_queue
        self.sync_queue = sync_queue

    @classmethod
    async def loop_task(
        cls,
        settings: EventsSettings,
        async_queue: asyncio.Queue,
        sync_queue: queue.Queue,
    ):
        looper = AsyncQueueLooper(settings, async_queue, sync_queue)
        await looper.loop()

    async def loop(self):
        while True:
            item = await self.async_queue.get()
            await self.handle_queue_item(item)
            self.async_queue.task_done()

    # async def handle_gwdevent(self, gwdevent: GWDEvent):
    #     rich.print(gwdevent)
    #     match gwdevent.event:
    #         case SyncStartEvent():
    #             self.sync_queue.put_nowait(gwdevent.event)
    #         case SyncCompleteEvent():
    #             self.sync_queue.put_nowait(gwdevent.event)
    #         case ProblemEvent():
    #             pass

    async def handle_queue_item(self, item: Any):
        self.sync_queue.put_nowait(item)
        # match item:
        #     case GWDEvent():
        #         await self.handle_gwdevent(item)
        #     case EventBase():
        #         rich.print(item)
        #     case _:
        #         rich.print(item)
