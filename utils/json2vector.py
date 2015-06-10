#!/usr/bin/env python


import click
import fiona as fio
import gpsdio
from gpsdio.schema import DATETIME_FORMAT


@click.command()
@click.argument('infile', type=click.File('r'), required=True)
@click.argument('outfile', required=True)
def cli(infile, outfile):

    """
    Convert to a vector format.
    """

    meta = {
        'crs': 'EPSG:4326',
        'driver': 'ESRI Shapefile',
        'schema': {
            'geometry': 'Point',
            'properties': {
                'mmsi': 'int',
                'series': 'int',
                'timestamp': 'str',
            }
        }
    }

    fields = list(meta['schema']['properties'].keys())
    with gpsdio.open(infile) as src, fio.open(outfile, 'w', **meta) as dst:
        for msg in src:

            msg['timestamp'] = msg['timestamp'].strftime(DATETIME_FORMAT)
            dst.write({
                'type': 'Feature',
                'properties': {f: msg.get(f) for f in fields},
                'geometry': {
                    'type': 'Point',
                    'coordinates': [msg['lon'], msg['lat']]
                }
            })


if __name__ == '__main__':
    cli()
