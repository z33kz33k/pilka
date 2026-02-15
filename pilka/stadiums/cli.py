"""

    pilka.stadiums.cli
    ~~~~~~~~~~~~~~~~~~
    Command-line interface.

    @author: z33k

"""
import click

from pilka.stadiums import dump_stadiums
from pilka.constants import OUTPUT_DIR


@click.group()
def stadiums() -> None:
    """Scrape stadiums data from stadiumdb.com/stadiony.net.
    """


@stadiums.command()
@click.option("--prefix", "-p", help="prefix for a dumpfile's name")
@click.option(
    "--timestamp/--no-timestamp", default=True, show_default=True,
    help="append a timestamp to the dumpfile's name or not")
@click.option(
    "--output-dir", "-o", type=click.Path(), default=OUTPUT_DIR, show_default=True,
    help="output directory")
@click.option(
    "--filename", "-f",
    help="explicit filename for the dumpfile (renders moot other filename-concerned options)")
@click.option(
    "--excluded", "-e", multiple=True,
    help="multiple specifier for countries to be excluded from dump (name, ID, or confederation)")
@click.argument("countries", nargs=-1)
def dump(countries, excluded, filename, output_dir, timestamp, prefix) -> None:
    """Dump stadiums data for COUNTRIES (all if not specified).
    """
    dump_stadiums(
        *countries, excluded=excluded, filename=filename, output_dir=output_dir,
        use_timestamp=timestamp, prefix=prefix)
