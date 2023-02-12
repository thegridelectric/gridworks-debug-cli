import json
from pathlib import Path
from typing import Literal
from typing import Optional
from typing import Sequence

import pandas as pd
from gwproto import Message
from gwproto.messages import CommEvent
from gwproto.messages import EventBase
from gwproto.messages import ProblemEvent
from pydantic import BaseModel
from pydantic import Extra
from pydantic import ValidationError
from result import Err
from result import Ok
from result import Result


class AnyEvent(EventBase, extra=Extra.allow):
    TypeName: str

    def other_fields(self) -> dict:
        return self.dict(exclude=EventBase.__fields__.keys())

    def as_pandas_record(
        self,
        collapse_other_fields: bool = True,
        other_field_name: str = "other_fields",
        explicit_summary: str = "",
        interpolate_summary: bool = False,
    ) -> dict:
        d = self.dict(include=EventBase.__fields__.keys())
        d["TimeNS"] = pd.Timestamp(self.TimeNS, unit="ns", tz="UTC")
        if explicit_summary:
            d[other_field_name] = explicit_summary
        else:
            if interpolate_summary and self.TypeName == "gridworks.event.shutdown":
                reason = getattr(self, "Reason")
                newline_idx = reason.find("\n")
                if newline_idx >= 0:
                    reason = reason[:newline_idx].rstrip(":")
                d[other_field_name] = reason
            elif interpolate_summary and self.TypeName == "gridworks.event.problem":
                d[other_field_name] = getattr(self, "Summary").replace("\n", "\\n")
            else:
                other_fields = self.other_fields()
                if collapse_other_fields:
                    d[other_field_name] = json.dumps(other_fields)
                else:
                    d.update(other_fields)
        return d

    def as_dataframe(
        self,
        columns: Optional[list[str]],
        explicit_summary: str = "",
        interpolate_summary: bool = False,
    ) -> pd.DataFrame:
        if columns is None:
            columns = ["TimeNS", "TypeName", "Src", "other_fields"]
        row_dict = self.as_pandas_record(
            explicit_summary=explicit_summary,
            interpolate_summary=interpolate_summary,
        )
        time_ns = row_dict.pop("TimeNS")
        return pd.DataFrame(
            {col: [val] for col, val in row_dict.items()},
            columns=columns,
            index=pd.DatetimeIndex([time_ns], name="TimeNS"),
        )

    @classmethod
    def from_event_dict(cls, d: dict) -> Result["AnyEvent", ValidationError]:
        """
        Parse d to AnyEvent, return ValidationError as Err(ValidationError)

        Args:
            d: Dictionary of data (possibly) representing a Gridworks Event.

        Returns:
            Ok(AnyEvent) or Err(ValidationError)
        """
        try:
            return Ok(AnyEvent.parse_obj(d))
        except ValidationError as e:
            return Err(e)

    @classmethod
    def from_message_dict(cls, d: dict) -> Result[Optional["AnyEvent"], BaseException]:
        """
        Extract AnyEvent from d, assuming d contains information representing a Gridworks Message.

        Args:
            d: Dictionary of data (possibly) representing a Gridworks Message with a Gridworks Event.

        Returns:
            - Ok(AnyEvent), if d is parseable as Message[AnyEvent] or
            - Ok(None), if d is parseable as a Message, but Message.Header.TypeName does not begin with "gridworks.event" or
            - Err(ValidationError), if d is not parseable as a Message.
        """
        try:
            m = Message.parse_obj(d)
            if m.Header.MessageType.startswith("gridworks.event"):
                return cls.from_event_dict(m.Payload)
            else:
                return Ok(None)
        except ValidationError as e:
            return Err(e)

    @classmethod
    def from_dict(cls, d: dict) -> Result[Optional["AnyEvent"], ValidationError]:
        if d.get("TypeName", "") == Message.type_name():
            return cls.from_message_dict(d)
        return cls.from_event_dict(d)

    @classmethod
    def from_str(cls, s: str | bytes) -> Result[Optional["AnyEvent"], BaseException]:
        try:
            if not isinstance(s, str):
                s = s.decode("utf-8")
            d = json.loads(s)
        except Exception as e:
            return Err(e)
        return cls.from_dict(d)

    @classmethod
    def from_path(cls, path: Path) -> Result[Optional["AnyEvent"], BaseException]:
        try:
            with path.open() as f:
                return cls.from_str(f.read())
        except Exception as e:
            return Err(e)

    @classmethod
    def from_directories(
        cls,
        directories: Sequence[Path],
        sort: bool = False,
        ignore_validation_errors: bool = False,
        keep_duplicates: bool = False,
    ) -> Sequence["AnyEvent"]:
        json_paths = []
        for directory in directories:
            json_paths += list(directory.glob("**/*.json"))
        events: list[AnyEvent] = []
        seen = set()
        for path in json_paths:
            result = cls.from_path(path)
            if result.is_ok():
                if result.value is not None:
                    if keep_duplicates or result.value.MessageId not in seen:
                        seen.add(result.value.MessageId)
                        events.append(result.value)
            else:
                error = result.value
                if not ignore_validation_errors or not isinstance(
                    error, ValidationError
                ):
                    raise error
        if sort:
            events = sorted(events, key=lambda event: event.TimeNS)
        return events

    @classmethod
    def to_dataframe(
        cls,
        events: Sequence["AnyEvent"],
        sort_index: bool = True,
        interpolate_summary: bool = False,
        **kwargs
    ) -> pd.DataFrame:
        df = pd.DataFrame.from_records(
            [
                e.as_pandas_record(interpolate_summary=interpolate_summary)
                for e in events
            ],
            index="TimeNS",
            **kwargs
        )
        if sort_index:
            df.sort_index(inplace=True)
        return df


class GWDEvent(BaseModel):
    """This class allows the a message processor to determine that this is an event _internal_ to the gwd client itself,
    not an externally generated event being reported on."""

    event: EventBase


class SyncStartEvent(EventBase):
    synced_key: str
    TypeName: Literal[
        "gridworks.event.debug_cli.sync.start"
    ] = "gridworks.event.debug_cli.sync.start"


class SyncCompleteEvent(EventBase):
    synced_key: str
    csv_path: Path
    TypeName: Literal[
        "gridworks.event.debug_cli.sync.complete"
    ] = "gridworks.event.debug_cli.sync.complete"


class MQTTParseException(ProblemEvent):
    topic: str
    TypeName: Literal[
        "gridworks.event.debug_cli.mqtt_parse_exception"
    ] = "gridworks.event.debug_cli.mqtt_parse_exception"


class MQTTFullySubscribedEvent(CommEvent):
    TypeName: Literal[
        "gridworks.event.debug_cli.mqtt_fully_subscribed"
    ] = "gridworks.event.debug_cli.mqtt_fully_subscribed"


class MQTTException(CommEvent):
    was_connected: bool
    exception: BaseException
    next_reconnect_delay: float
    will_reconnect: bool
    TypeName: Literal[
        "gridworks.event.debug_cli.mqtt_exception"
    ] = "gridworks.event.debug_cli.mqtt_exception"

    class Config:
        arbitrary_types_allowed: bool = True
