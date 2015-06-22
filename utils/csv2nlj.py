#!/usr/bin/env python


"""
Convert a CSV to GPSD newline JSON.
"""


import csv

import click
import newlinejson as nlj


@click.command()
@click.argument('infile', type=click.File('r'), required=True)
@click.argument('outfile', type=click.File('w'), required=True)
def cli(infile, outfile):

    """
    Convert a CSV to newline GPSD.  Only writes lat, lon, and timestamp.

    Auto-transforms longitude/latitude -> lon/lat.
    """

    with nlj.open(outfile, 'w') as dst:
        for row in csv.DictReader(infile):

            if 'longitude' in row:
                row['lon'] = float(row['longitude'])
                del row['longitude']
            else:
                row['lon'] = float(row['lon'])

            if 'latitude' in row:
                row['lat'] = float(row['latitude'])
                del row['latitude']
            else:
                row['lat'] = float(row['lat'])

            row['mmsi'] = int(row['mmsi'])

            dst.write(row)


if __name__ == '__main__':
    cli()
