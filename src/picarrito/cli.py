import click
from .core import func


@click.command()
def main():
    click.echo(func())
