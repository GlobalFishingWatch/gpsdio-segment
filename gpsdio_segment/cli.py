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
@click.pass_context
def segment(ctx, infile, outfile, mmsi, max_hours, max_speed, noise_dist):

    """
    Segment AIS data into continuous segments.
    """

    logger = logging.getLogger('gpsdio-segment-cli')
    logger.setLevel(ctx.obj.get('verbosity', 1))

    o_drv = ctx.obj.get('o_drv')
    o_cmp = ctx.obj.get('o_cmp')
    out_ext = ''
    if o_drv is not None:
        out_ext += '.' + gpsdio.drivers.get_driver(o_drv).extensions[0]
    if o_cmp is not None:
        out_ext += '.' + gpsdio.drivers.get_compression(o_cmp).extensions[0]

    with gpsdio.open(infile, driver=ctx.obj.get('i_drv'),
                     compression=ctx.obj.get('i_cmp')) as src, \
            gpsdio.open(outfile, 'a',
                        driver=ctx.obj.get('o_drv'), compression=ctx.obj.get('o_cmp')) as dst:

        logger.debug("Begining to segment")
        longest_id = None
        longest_count = None
        for t_idx, segment in enumerate(Segmentizer(
                src, mmsi=mmsi, max_hours=max_hours,
                max_speed=max_speed, noise_dist=noise_dist)):

            if longest_id is None or len(segment) > longest_count:
                longest_id = segment.id
                longest_count = len(segment)

            # if len(segment) > 1:
            print("Writing segment %s with %s messages and %s points" % (segment.id, len(segment), len(segment.coords)))
            for msg in segment:
                dst.write(msg)
        print("Longest is %s with %s" % (longest_id, longest_count))
        print("Wrote %s segments" % (t_idx + 1))
