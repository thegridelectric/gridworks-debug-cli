import asyncio
import functools
import logging
import traceback
from pathlib import Path
from subprocess import CalledProcessError  # noqa: S404
from typing import Optional

import pandas as pd
from aiobotocore.session import AioSession
from anyio import create_task_group
from anyio import run_process
from anyio import to_process
from gwproto.messages import ProblemEvent
from gwproto.messages import Problems
from gwproto.messages import ReportEvent
from gwproto.named_types import SnapshotSpaceheat
from pandas import DataFrame
from result import Err
from result import Ok
from result import Result

from gwdcli.events.models import AnyEvent
from gwdcli.events.models import GWDEvent
from gwdcli.events.models import SyncCompleteEvent
from gwdcli.events.models import SyncStartEvent
from gwdcli.events.settings import EventsSettings
from gwdcli.events.settings import S3Settings


logger = logging.getLogger("gwd.events")


async def get_eventstore_subdirs(settings: S3Settings, **s3_client_args) -> list[str]:
    dirs = []
    session = AioSession(profile=settings.profile)
    async with session.create_client("s3", **s3_client_args) as client:
        more = True
        continuation_token = ""
        list_args = dict(
            Bucket=settings.bucket,
            Prefix=settings.prefix,
            Delimiter="/",
        )
        while more:
            one_list_args = dict(list_args)
            if continuation_token:
                one_list_args["ContinuationToken"] = continuation_token
            result = await client.list_objects_v2(**one_list_args)
            dirs.extend(
                [
                    Path(entry["Prefix"]).name
                    for entry in result.get("CommonPrefixes", [])  # noqa
                ]
            )
            continuation_token = result.get("NextContinuationToken", "")
            more = result.get("IsTruncated", False)
    return sorted(dirs)


def make_sync_command(
    bucket: str,
    prefix: str,
    profile: str,
    dest_base_path: str | Path = Path("."),  # noqa: B008
    region: str = "",
) -> list[str]:
    cmd = [
        "aws",
        "s3",
        "sync",
        f"s3://{bucket}/{prefix}",
        f"{Path(dest_base_path) / Path(prefix).name}",
        "--quiet",
        "--profile",
        profile,
        "--exclude",
        "*",
        "--include",
        "*scada-gridworks.event*",
    ]
    if region:
        cmd.extend(["--region", region])
    return cmd


def generate_directory_csv(
    src_directory_path: Path,
    dst_directory_csv_path: Path,
    main_csv_path: Path,
) -> Result[Optional[DataFrame], Exception]:
    try:
        parsed_events = AnyEvent.from_directories(
            [src_directory_path],
            sort=True,
            ignore_validation_errors=True,
            excludes=[
                ReportEvent.model_fields["TypeName"].default,
                SnapshotSpaceheat.model_fields["TypeName"].default,
                "remaining.elec.event",
                "gt.sh.status",
            ],
        )
        if parsed_events:
            df = AnyEvent.to_dataframe(parsed_events, interpolate_summary=True)
            df.to_csv(dst_directory_csv_path)
            if not main_csv_path.exists():
                df.to_csv(main_csv_path)
            else:
                main_df = pd.read_csv(
                    main_csv_path,
                    index_col="TimeCreatedMs",
                    parse_dates=True,
                    date_parser=functools.partial(pd.to_datetime, utc=True),
                )
                main_ids = set(main_df["MessageId"].index)
                directory_ids = set(df["MessageId"].index)
                if not directory_ids.issubset(main_ids):
                    main_df = pd.concat([main_df, df]).sort_index()
                    main_df.drop_duplicates("MessageId", inplace=True)
                    main_df.to_csv(main_csv_path)
        else:
            df = None
    except Exception as e:
        return Err(
            ValueError(
                f"generate_directory_csv({src_directory_path}) "
                f"caught exception {e}:\n"
                f"{traceback.format_exc()}"
            )
        )
    return Ok(df)


async def sync_dir_and_generate_csv(
    settings: EventsSettings, subdir: str, queue: asyncio.Queue
):
    logger.info(f"++sync_dir_and_generate_csv: <{subdir}>")
    path_dbg = 0
    s3 = settings.sync.s3
    synced_key = s3.synced_key(subdir)
    queue.put_nowait(GWDEvent(event=SyncStartEvent(synced_key=synced_key)))
    sync_cmd = make_sync_command(
        bucket=s3.bucket,
        prefix=s3.subprefix(subdir),
        profile=s3.profile,
        dest_base_path=settings.paths.data_dir,
        region=s3.region,
    )
    logger.info(f"Running: aws sync command:\n  {' '.join(sync_cmd)}")
    path_dbg |= 0x00000001
    try:
        await run_process(sync_cmd)
    except CalledProcessError as e:
        path_dbg |= 0x00000002
        queue.put_nowait(
            GWDEvent(
                event=ProblemEvent(
                    ProblemType=Problems.warning,
                    Summary=f"ERROR sync failure {e} for {synced_key}",
                    Details=f"stdout: {str(e.stdout)}\n" f"stderr: {str(e.stderr)}",
                )
            )
        )
    else:
        path_dbg |= 0x00000004
        csv_path = settings.paths.subdir_csv_path(subdir)
        try:
            result = await to_process.run_sync(
                generate_directory_csv,
                settings.paths.data_subdir(subdir),
                csv_path,
                settings.paths.csv_path,
            )
            logger.info(f"result generate_directory_csv <{subdir}>: {result.is_ok()}")
            if result.is_ok():
                path_dbg |= 0x00000008
                queue.put_nowait(
                    GWDEvent(
                        event=SyncCompleteEvent(
                            synced_key=synced_key, csv_path=csv_path
                        )
                    )
                )
            else:
                path_dbg |= 0x00000010
                logger.info(
                    f"result generate_directory_csv <{subdir}> error value: {result.err()}"
                )
                logger.exception(result.err())
                queue.put_nowait(
                    GWDEvent(
                        event=ProblemEvent(
                            ProblemType=Problems.error,
                            Summary=f"ERROR in generate_directory_csv: {result.value}",
                        )
                    )
                )
        except Exception as e:
            path_dbg |= 0x00000020
            logger.info(f"Caught exception from generate_directory_csv <{subdir}>: {e}")
            logger.exception(e)
    logger.info(f"--sync_dir_and_generate_csv: <{subdir}>  path:0x{path_dbg:08X}")


async def sync(settings: EventsSettings, queue: asyncio.Queue) -> None:
    subdirs = await get_eventstore_subdirs(settings.sync.s3)
    if settings.sync.num_dirs_to_sync:
        subdirs = subdirs[-settings.sync.num_dirs_to_sync :]
    else:
        subdirs = []
    if subdirs:
        # allow first sync to run without competition
        await sync_dir_and_generate_csv(settings, subdirs[-1], queue)
        # now run the rest together
        async with create_task_group() as tg:
            for subdir in subdirs[:-1]:
                tg.start_soon(sync_dir_and_generate_csv, settings, subdir, queue)
