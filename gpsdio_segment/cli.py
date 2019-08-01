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
from gpsdio_segment.core import DEFAULT_SHORT_SEG_THRESHOLD
from gpsdio_segment.core import DEFAULT_SHORT_SEG_WEIGHT
from gpsdio_segment.core import DEFAULT_SEG_LENGTH_WEIGHT


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
    help="Maximum allowable speed over a long distance. Units are knots.  (default: {})".format(DEFAULT_MAX_SPEED)
)
@click.option(
    '--short-seg-threshold', type=click.FLOAT, default=DEFAULT_SHORT_SEG_THRESHOLD,
    help="Segments shorter than this are less likely to be matched. (default: {})".format(DEFAULT_MAX_SPEED)
)
@click.option(
    '--short-seg-weight', type=click.FLOAT, default=DEFAULT_SHORT_SEG_THRESHOLD,
    help="Max amount to down weight very short segments. (default: {})".format(DEFAULT_MAX_SPEED)
)
@click.option(
    '--seg-length-weight', type=click.FLOAT, default=DEFAULT_SEG_LENGTH_WEIGHT,
    help=("Max amount to down weight segments shorter than longest."
          "active segment (default: {})").format(DEFAULT_MAX_SPEED)
)
@click.option(
    '--segment-field', default='segment',
    help="Add the segment ID to this field when writing messages. (default: segment)"
)
@click.pass_context
def segment(ctx, infile, outfile, mmsi, max_hours, max_speed,
            short_seg_threshold, short_seg_weight, seg_length_weight,
            segment_field):

    """
    Group AIS data into continuous segments.
    """

    logger = logging.getLogger(__file__)

    with gpsdio.open(infile, driver=ctx.obj.get('i_drv'),
                     compression=ctx.obj.get('i_cmp')) as src, \
            gpsdio.open(outfile, 'w',
                        driver=ctx.obj.get('o_drv'), compression=ctx.obj.get('o_cmp')) as dst:

        logger.debug("Beginning to segment")
        for t_idx, seg in enumerate(Segmentizer(
                src, mmsi=mmsi, max_hours=max_hours,
                max_speed=max_speed)):

            logger.debug("Writing segment %s with %s messages and %s points",
                         seg.id, len(seg), len(seg.coords))
            for msg in seg:
                msg[segment_field] = seg.id
                dst.write(msg)
