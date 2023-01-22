"""Command-line interface."""
import typer

from gwdcli.events import app as events_app


app = typer.Typer(no_args_is_help=True)
app.add_typer(events_app, name="events")

if __name__ == "__main__":
    app()
