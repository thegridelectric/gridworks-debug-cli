import asyncio
import logging
from typing import Any

import aiomqtt
from gwproto import Message
from gwproto import MQTTCodec
from gwproto import create_message_model
from gwproto.messages import Problems
from result import Err
from result import Ok
from result import Result

from gwdcli.events.models import GWDEvent
from gwdcli.events.models import MQTTException
from gwdcli.events.models import MQTTFullySubscribedEvent
from gwdcli.events.models import MQTTParseException
from gwdcli.events.settings import MQTTClient


logger = logging.getLogger("gwd.events")


async def run_mqtt_client(
    settings: MQTTClient,
    queue: asyncio.Queue,
):
    delay = settings.reconnect_min_delay
    decoder = GwdMQTTCodec()
    try:
        while True:
            connected = False
            try:
                async with aiomqtt.Client(**settings.constructor_dict()) as client:
                    connected = True
                    delay = settings.reconnect_min_delay
                    await client.subscribe("gw/#")
                    queue.put_nowait(
                        GWDEvent(
                            event=MQTTFullySubscribedEvent(PeerName=settings.hostname)
                        )
                    )
                    async for message in client.messages:
                        handle_message(message, queue, decoder)

            except (aiomqtt.MqttError, TimeoutError) as mqtt_error:
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
        await asyncio.sleep(1)
        raise e


GWDMessageDecoder = create_message_model(
    model_name="GWDMessageDecoder",
    module_names=["gwproto.messages"],
)


class GwdMQTTCodec(MQTTCodec):

    def validate_source_and_destination(self, src: str, dst: str) -> None:
        """No implementation"""

    def __init__(self):
        super().__init__(message_model=GWDMessageDecoder)

    def decode_mqtt_message(
        self, topic: str, payload: bytes
    ) -> Result[Message[Any], BaseException]:
        result: Result[Message[Any], BaseException]
        try:
            result = Ok(self.decode(topic, payload))
        except Exception as e:
            result = Err(e)
        return result


def handle_message(
    message: aiomqtt.Message, queue: asyncio.Queue, decoder: GwdMQTTCodec
) -> None:
    try:
        try:
            queue.put_nowait(decoder.decode(str(message.topic), message.payload))
        except Exception as e:
            queue.put_nowait(
                GWDEvent(
                    event=MQTTParseException(
                        Src=str(message.topic),
                        ProblemType=Problems.warning,
                        Summary=f"ERROR parsing on topic {message.topic}: [{e}]",
                        Details=f"message:\n{message.payload}",
                        topic=str(message.topic),
                    )
                )
            )
    except Exception as e:
        logger.exception(e)
        raise e
