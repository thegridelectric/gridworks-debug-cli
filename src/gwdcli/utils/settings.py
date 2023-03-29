from pathlib import Path

from pydantic import BaseModel


RELATIVE_DEBUG_CLI_PATH = Path("gridworks/debug-cli")


def config_file_name(app: str) -> str:
    return f"gwd.{app}.config.json"


class S3Settings(BaseModel):
    bucket: str = ""
    prefix: str = ""
    profile: str = ""
    region: str = ""

    def subprefix(self, subdir: str) -> str:
        return f"{self.prefix.rstrip('/')}/{subdir}"

    def synced_key(self, subdir: str) -> str:
        return f"{self.bucket}/{self.subprefix(subdir)}"
