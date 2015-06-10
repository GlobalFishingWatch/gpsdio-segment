#!/usr/bin/env python


from glob import glob

import click


@click.command()
@click.argument('indir')
def cli(indir):

    """
    Get line counts.
    """

    counts = {}
    for infile in glob(indir + '*.json'):
        with open(infile) as src:
            counts[infile] = len(src.readlines())

    for key in sorted(counts.keys()):
        click.echo("%s %s" % (key, counts[key]))


if __name__ == '__main__':
    cli()
