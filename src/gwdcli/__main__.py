"""Command-line interface."""
import atexit

import typer

from gwdcli.events import app as events_app


app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
app.add_typer(events_app, name="events")

typer_click_object = typer.main.get_command(app)

if __name__ == "__main__":
    atexit.register(lambda: print("\x1b[?25h"))
    app()
