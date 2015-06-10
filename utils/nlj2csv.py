#!/usr/bin/env python


import csv

import click
import newlinejson as nlj


@click.command()
@click.argument('infile', type=click.File('r'), required=True)
@click.argument('outfile', type=click.File('w'), required=True)
def cli(infile, outfile):

    """
    Convert to CSV.
    """

    writer = csv.DictWriter(outfile, ['timestamp', 'lat', 'lon', 'mmsi'])
    writer.writeheader()
    with nlj.open(infile) as src:
        for line in src:
            writer.writerow(line)



if __name__ == '__main__':
    cli()
