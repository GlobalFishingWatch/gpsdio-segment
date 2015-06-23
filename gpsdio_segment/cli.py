"""
Commandline interface for gpsdio-segment
"""


import logging

import click
import gpsdio
import gpsdio.drivers

import gpsdio_segment
from gpsdio_segment.core import Segmentizer
from gpsdio_segment.core import DEFAULT_MAX_SPEED
from gpsdio_segment.core import DEFAULT_MAX_HOURS
from gpsdio_segment.core import DEFAULT_NOISE_DIST


logging.basicConfig()


@click.command()
@click.version_option(version=gpsdio_segment.__version__)
@click.argument('infile', required=True)
@click.argument('outfile', required=True)
@click.option(
    '--mmsi', type=click.INT,
    help="Only segment this MMSI.  If not given the first MMSI found will be used."
)
@click.option(
    '--max-hours', type=click.FLOAT, default=DEFAULT_MAX_HOURS,
    help="Points with a time delta larger than N hours are forced to be discontinuous. "
         "(default: {})".format(DEFAULT_MAX_HOURS)
)
@click.option(
    '--max-speed', type=click.FLOAT, default=DEFAULT_MAX_SPEED,
    help="Units are knots.  Points with a speed above this value are always considered "
         "discontinuous. (default: {})".format(DEFAULT_MAX_SPEED)
)
@click.option(
    '--noise-dist', type=click.FLOAT, default=DEFAULT_NOISE_DIST,
    help="Units are nautical miles.  Points within this distance are always considered "
         "continuous.  Used to allow a certain amount of GPS noise. "
         "(default: {})".format(DEFAULT_NOISE_DIST)
)
@click.option(
    '--series-field', default='series',
    help="Add the segment ID to this field when writing messages. (default: series)"
)
@click.pass_context
def segment(ctx, infile, outfile, mmsi, max_hours, max_speed, noise_dist, series_field):

    """
    Segment AIS data into continuous segments.
    """

    logger = logging.getLogger('gpsdio-segment-cli')
    logger.setLevel(ctx.obj.get('verbosity', 1))

    with gpsdio.open(infile, driver=ctx.obj.get('i_drv'),
                     compression=ctx.obj.get('i_cmp')) as src, \
            gpsdio.open(outfile, 'a',
                        driver=ctx.obj.get('o_drv'), compression=ctx.obj.get('o_cmp')) as dst:

        logger.debug("Beginning to segment")
        for t_idx, seg in enumerate(Segmentizer(
                src, mmsi=mmsi, max_hours=max_hours,
                max_speed=max_speed, noise_dist=noise_dist)):

            logger.debug("Writing segment %s with %s messages and %s points",
                         (seg.id, len(seg), len(seg.coords)))
            for msg in seg:
                msg[series_field] = seg.id
                dst.write(msg)
