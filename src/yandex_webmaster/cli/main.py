"""CLI interface for Yandex Webmaster pipeline."""
import click

@click.group()
def cli():
    """Yandex Webmaster Data Pipeline CLI."""
    pass

@cli.command()
def hello():
    """Test command."""
    click.echo("Hello from Yandex Webmaster CLI!")

def main():
    """Entry point for CLI."""
    cli()

if __name__ == "__main__":
    main()
