from pathlib import Path

import xdg
from pendulum import DateTime
from pydantic import BaseModel
from pydantic import BaseSettings
from pydantic import validator
from yarl import URL

from gwdcli.utils.settings import RELATIVE_DEBUG_CLI_PATH
from gwdcli.utils.settings import S3Settings


RELATIVE_APP_PATH = RELATIVE_DEBUG_CLI_PATH / "csv"
CONFIG_FILE = "gwd.csv.config.json"


class Paths(BaseModel):
    config_path: str | Path = ""
    data_dir: str | Path = ""

    @validator("config_path", always=True)
    def get_config_path(cls, v: str | Path) -> Path:
        return Path(v if v else xdg.xdg_config_home() / RELATIVE_APP_PATH / CONFIG_FILE)

    @validator("data_dir", always=True)
    def get_data_dir(cls, v: str | Path) -> Path:
        return Path(v if v else xdg.xdg_state_home() / RELATIVE_APP_PATH)

    @property
    def config_dir(self) -> Path:
        return self.config_path.parent

    def scada_data_dir(self, scada: str) -> Path:
        return self.data_dir / scada

    @classmethod
    def dt_for_filename(cls, dt: DateTime) -> str:
        return dt.to_datetime_string().replace(":", ".").replace(" ", "_")

    @classmethod
    def scada_csv_file(
        cls, scada: str, start: DateTime, end: DateTime, data_type: str
    ) -> str:
        return (
            f"{scada}__"
            f"{cls.dt_for_filename(start)}__to__"
            f"{cls.dt_for_filename(end)}__"
            f"{data_type}.csv"
        )

    def scada_csv_path(
        self, scada: str, start: DateTime, end: DateTime, data_type: str
    ) -> Path:
        return self.scada_data_dir(scada) / self.scada_csv_file(
            scada, start, end, data_type
        )

    def mkdirs(self, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        self.config_dir.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        self.data_dir.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)


class ScadaConfig(BaseModel):
    atn: str = ""
    egauge: str = ""
    bytes_per_row: int = 240


class eGaugeSettings(BaseModel):  # noqa: N801
    url_format: str = "http://egauge{egauge_id}.egaug.es/cgi-bin/egauge-show"  # noqa
    relative_to_epoch: bool = True
    delta_compressed: bool = True
    localtime: bool = True

    def url(
        self,
        egauge_id: str,
        seconds_per_row: int | float,
        rows: int,
        end_utc: int | float,
    ) -> URL:
        raw_query_string = "c&S"
        if self.relative_to_epoch:
            raw_query_string += "&E"
        if self.delta_compressed:
            raw_query_string += "&C"
        if self.localtime:
            raw_query_string += "&Z="
        return URL(self.url_format.format(egauge_id=egauge_id)).with_query(
            raw_query_string
            + "&"
            + URL()
            .with_query(
                s=int(seconds_per_row) - 1,
                n=rows + 1,
                f=int(end_utc),
            )
            .raw_query_string
        )


class CSVSettings(BaseSettings):
    paths: Paths = Paths()
    s3: S3Settings = S3Settings()
    egauge: eGaugeSettings = eGaugeSettings()
    scadas: dict[str, ScadaConfig] = dict()
    default_scada: str = ""

    @classmethod
    def load(
        cls, config_path: Path = Paths().config_path  # noqa: B008
    ) -> "CSVSettings":
        paths = Paths(config_path=config_path)
        if paths.config_path.exists():
            settings = CSVSettings.parse_file(paths.config_path)
            settings.paths.config_path = config_path
            return settings
        else:
            settings = CSVSettings(paths=paths)
        return settings

    def mkdirs(self, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        self.paths.mkdirs(mode=mode, parents=parents, exist_ok=exist_ok)
        for scada in self.scadas:
            self.paths.scada_data_dir(scada).mkdir(
                mode=mode, parents=parents, exist_ok=exist_ok
            )
