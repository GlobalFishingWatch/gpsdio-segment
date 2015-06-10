#!/usr/bin/env python


import click
import fiona as fio
import gpsdio


@click.command()
@click.argument('infile', type=click.File('r'), required=True)
@click.argument('outfile', required=True)
def cli(infile, outfile):

    """
    Convert positional messages to a single vector line.
    """

    meta = {
        'crs': 'EPSG:4326',
        'driver': 'ESRI Shapefile',
        'schema': {
            'properties': {},
            'geometry': 'LineString'
        }
    }

    coords = []
    with gpsdio.open(infile) as src:
        for msg in src:
            coords.append((msg['lon'], msg['lat']))

    with fio.open(outfile, 'w', **meta) as dst:
        dst.write({
            'type': 'Feature',
            'properties': {},
            'geometry': {
                'type': 'LineString',
                'coordinates': coords
            }
        })


if __name__ == '__main__':
    cli()
