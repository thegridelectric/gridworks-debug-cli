"""Command-line interface."""

import typer

from gwdcli.csv import app as csv_app
from gwdcli.events import app as events_app


app = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    rich_markup_mode="rich",
    help="GridWords command-Line debug tools.",
)
app.add_typer(events_app, name="events")
app.add_typer(csv_app, name="csv")

# For sphinx:
typer_click_object = typer.main.get_command(app)

if __name__ == "__main__":
    app()
