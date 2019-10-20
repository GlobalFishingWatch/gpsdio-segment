"""
Commandline interface for gpsdio-segment
"""


import logging

import click
import gpsdio
import gpsdio.drivers

import gpsdio_segment
from gpsdio_segment.core import Segmentizer
from gpsdio_segment.core import DEFAULT_MAX_KNOTS
from gpsdio_segment.core import DEFAULT_MAX_HOURS
from gpsdio_segment.core import DEFAULT_BUFFER_HOURS
from gpsdio_segment.core import DEFAULT_LOOKBACK
from gpsdio_segment.core import DEFAULT_SHORT_SEG_THRESHOLD

@click.command()
@click.version_option(version=gpsdio_segment.__version__)
@click.argument('infile', required=True)
@click.argument('outfile', required=True)
@click.option(
    '--ssvid', type=click.INT,
    help="Only segment this SSVID.  If not given the first SSVID found will be used."
)
@click.option(
    '--max-hours', type=click.FLOAT, default=DEFAULT_MAX_HOURS,
    help="Points with a time delta larger than N hours are forced to be discontinuous. "
         "(default: {})".format(DEFAULT_MAX_HOURS)
)
@click.option(
    '--max-speed', type=click.FLOAT, default=DEFAULT_MAX_KNOTS,
    help="Maximum allowable speed over a long distance. Units are knots.  (default: {})".format(DEFAULT_MAX_KNOTS)
)
@click.option(
    '--buffer-hours', type=click.FLOAT, default=DEFAULT_BUFFER_HOURS,
    help="Number of hours to pad dt with when computing match metrics.  (default: {})".format(DEFAULT_BUFFER_HOURS)
)
@click.option(
    '--lookback', type=click.FLOAT, default=DEFAULT_LOOKBACK,
    help="Number of points and end of segment to look at when matching. (default: {})".format(DEFAULT_LOOKBACK)
)
@click.option(
    '--short-seg-threshold', type=click.FLOAT, default=DEFAULT_SHORT_SEG_THRESHOLD,
    help="Segments with lengths below this threshold, are less likely to match (default: {})".format(DEFAULT_SHORT_SEG_THRESHOLD)
)
@click.option(
    '--segment-field', default='segment',
    help="Add the segment ID to this field when writing messages. (default: segment)"
)
@click.pass_context
def segment(ctx, infile, outfile, ssvid, 
            max_hours, buffer_hours, max_speed, lookback,
            short_seg_threshold, 
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
                src, ssvid=ssvid, 
                max_hours=max_hours,
                buffer_hours=buffer_hours,
                max_speed=max_speed,
                lookback=lookback,
                short_seg_threshold=short_seg_threshold,
                )):

            logger.debug("Writing segment %s with %s messages and %s points",
                         seg.id, len(seg))
            for msg in seg:
                msg[segment_field] = seg.id
                dst.write(msg)
