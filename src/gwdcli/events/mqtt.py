import asyncio

import asyncio_mqtt as aiomqtt
from gwproto.messages import Problems

from gwdcli.events.models import AnyEvent
from gwdcli.events.models import GWDEvent
from gwdcli.events.models import MQTTException
from gwdcli.events.models import MQTTFullySubscribedEvent
from gwdcli.events.models import MQTTParseException
from gwdcli.events.settings import MQTTClient


async def run_mqtt_client(
    settings: MQTTClient,
    queue: asyncio.Queue,
):
    delay = settings.reconnect_min_delay
    try:
        while True:
            connected = False
            try:
                async with aiomqtt.Client(**settings.constructor_dict()) as client:
                    connected = True
                    delay = settings.reconnect_min_delay
                    async with client.messages() as messages:
                        await client.subscribe("gw/#")
                        queue.put_nowait(
                            GWDEvent(
                                event=MQTTFullySubscribedEvent(
                                    PeerName=settings.hostname
                                )
                            )
                        )
                        async for message in messages:
                            message_str = message.payload.decode("utf-8")
                            result = AnyEvent.from_str(message_str)
                            if result.is_ok():
                                if result.value is not None:
                                    queue.put_nowait(result.value)
                            else:
                                queue.put_nowait(
                                    GWDEvent(
                                        event=MQTTParseException(
                                            ProblemType=Problems.warning,
                                            Summary=f"ERROR parsing on topic {message.topic}: [{result.value}]",
                                            Details=f"message:\n{message_str}",
                                            topic=message.topic,
                                        )
                                    )
                                )
            except aiomqtt.MqttError as mqtt_error:
                queue.put_nowait(
                    GWDEvent(
                        event=MQTTException(
                            PeerName=settings.hostname,
                            was_connected=connected,
                            exception=mqtt_error,
                            next_reconnect_delay=delay,
                            will_reconnect=True,
                        )
                    )
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, settings.reconnect_max_delay)
    except Exception as e:
        queue.put_nowait(
            GWDEvent(
                event=MQTTException(
                    PeerName=settings.hostname,
                    was_connected=connected,
                    exception=e,
                    next_reconnect_delay=-1,
                    will_reconnect=False,
                )
            )
        )
