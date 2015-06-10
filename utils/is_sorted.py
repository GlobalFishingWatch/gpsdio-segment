#!/usr/bin/env python


import click
import gpsdio


@click.command()
@click.argument('infile', type=click.File('r'), required=True)
def cli(infile):

    """
    Is the data sorted?
    """

    prev_msg = None
    with gpsdio.open(infile) as src:
        for msg in src:
            if prev_msg is None:
                prev_msg = msg
                continue
            else:
                assert msg['timestamp'] >= prev_msg['timestamp']

    print("True")


if __name__ == '__main__':
    cli()
