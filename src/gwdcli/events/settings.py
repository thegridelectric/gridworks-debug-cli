from pathlib import Path
from typing import Optional

import xdg
from pydantic import BaseModel
from pydantic import BaseSettings
from pydantic import SecretStr
from pydantic import validator

from gwdcli.utils.settings import RELATIVE_DEBUG_CLI_PATH
from gwdcli.utils.settings import S3Settings
from gwdcli.utils.settings import config_file_name


RELATIVE_APP_PATH = RELATIVE_DEBUG_CLI_PATH / "events"
CONFIG_FILE = config_file_name("events")
CSV_FILE = "events.csv"
LOG_FILE = "events.log"


class Paths(BaseModel):
    config_path: str | Path = ""
    csv_path: str | Path = ""

    @validator("config_path", always=True)
    def get_config_path(cls, v: str | Path) -> Path:
        return Path(v if v else xdg.xdg_config_home() / RELATIVE_APP_PATH / CONFIG_FILE)

    @validator("csv_path", always=True)
    def get_csv_path(cls, v: str | Path) -> Path:
        return Path(v if v else xdg.xdg_state_home() / RELATIVE_APP_PATH / CSV_FILE)

    @property
    def config_dir(self) -> Path:
        return self.config_path.parent

    @property
    def data_dir(self) -> Path:
        return self.csv_path.parent

    @property
    def status_dir(self) -> Path:
        return self.data_dir / "status"

    @property
    def snap_dir(self) -> Path:
        return self.data_dir / "snap"

    @property
    def log_path(self) -> Path:
        return self.data_dir / LOG_FILE

    def status_path(self, src_name: str) -> Path:
        return self.status_dir / f"{src_name}.status.json"

    def snap_path(self, src_name: str) -> Path:
        return self.snap_dir / f"{src_name}.snap.json"

    def data_subdir(self, subdir: str) -> Path:
        return self.data_dir / subdir

    def subdir_csv_path(self, subdir: str) -> Path:
        return self.data_dir / f"{subdir}.csv"

    def mkdirs(self, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        self.config_dir.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        self.data_dir.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        self.status_dir.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        self.snap_dir.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)


class MQTTClient(BaseModel):
    """Settings for connecting to an MQTT Broker"""

    hostname: str = "localhost"
    port: int = 1883
    keepalive: int = 60
    bind_address: str = ""
    bind_port: int = 0
    username: Optional[str] = None
    password: SecretStr = SecretStr("")
    reconnect_min_delay: float = 1.0
    reconnect_max_delay: float = 120.0

    def constructor_dict(self) -> dict:
        return dict(
            self.dict(
                exclude={"password", "reconnect_min_delay", "reconnect_max_delay"}
            ),
            password=self.password.get_secret_value(),
        )


class SyncSettings(BaseModel):
    s3: S3Settings = S3Settings()
    num_dirs_to_sync: int = 4


class TUISettings(BaseModel):
    displayed_events: int = 45
    max_other_fields_width: int = 90
    c_to_f: bool = True


class EventsSettings(BaseSettings):
    verbosity: int = 0
    snaps: list[str] = []
    scadas: list[str] = []
    paths: Paths = Paths()
    sync: SyncSettings = SyncSettings()
    mqtt: MQTTClient = MQTTClient()
    tui: TUISettings = TUISettings()

    @classmethod
    def load(
        cls, config_path: Path = Paths().config_path  # noqa: B008
    ) -> "EventsSettings":
        paths = Paths(config_path=config_path)
        if paths.config_path.exists():
            settings = EventsSettings.parse_file(paths.config_path)
            settings.paths.config_path = config_path
            return settings
        else:
            settings = EventsSettings(paths=paths)
        return settings
