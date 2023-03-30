import asyncio
import logging
from typing import Any

import asyncio_mqtt as aiomqtt
from gwproto import CallableDecoder
from gwproto import Decoders
from gwproto import Message
from gwproto import MQTTCodec
from gwproto import create_message_payload_discriminator
from gwproto.gs import GsPwr_Maker
from gwproto.messages import Problems
from gwproto.types import GtShStatus_Maker
from gwproto.types import PowerWatts_Maker
from gwproto.types import SnapshotSpaceheat_Maker
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


GWDMessageDecoder = create_message_payload_discriminator(
    model_name="GWDMessageDecoder",
    module_names=["gwproto.messages"],
)


class GwdMQTTCodec(MQTTCodec):
    def __init__(self):
        super().__init__(
            Decoders.from_objects(
                [
                    GtShStatus_Maker,
                    SnapshotSpaceheat_Maker,
                    PowerWatts_Maker,
                ],
                message_payload_discriminator=GWDMessageDecoder,
            ).add_decoder(
                "p", CallableDecoder(lambda decoded: GsPwr_Maker(decoded[0]).tuple)
            )
        )

    def validate_source_alias(self, source_alias: str):
        ...

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
