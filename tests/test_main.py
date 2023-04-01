"""Test cases for the __main__ module."""
from typer.testing import CliRunner

from gwdcli import __main__


def test_gwd_succeeds(runner: CliRunner) -> None:
    """It exits with a status code of zero."""
    result = runner.invoke(__main__.app)
    assert result.exit_code == 0
