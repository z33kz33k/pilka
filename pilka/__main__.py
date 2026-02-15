"""

    pilka
    ~~~~~
    CLI's entry point.

    @author: z33k

"""
import click

from pilka.stadiums.cli import stadiums


@click.group()
def main() -> None:
    """Scrape football data.
    """


if __name__ == '__main__':
    main.add_command(stadiums)
    main()
