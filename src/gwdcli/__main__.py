"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """Gridworks Debug Cli."""


if __name__ == "__main__":
    main(prog_name="gridworks-debug-cli")  # pragma: no cover
