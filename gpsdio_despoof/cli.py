"""
Commandline interface for gpsdio-sort
"""


import logging
import os

import click
import gpsdio
import gpsdio.drivers

from gpsdio_despoof.core import Despoofer
from gpsdio_despoof.core import DEFAULT_MAX_SPEED
from gpsdio_despoof.core import DEFAULT_MAX_HOURS
from gpsdio_despoof.core import DEFAULT_NOISE_DIST



logging.basicConfig()


@click.command()
@click.argument('infile', type=click.File('r'), required=True)
@click.argument('outdir', type=click.Path(resolve_path=True), required=True)
@click.option(
    '--mmsi', type=click.INT,
    help="Only despoof this MMSI.  If not given the first MMSI found will be used."
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
def despoof(ctx, infile, outdir, mmsi, max_hours, max_speed, noise_dist):

    """
    Despoof AIS data into multiple tracks.
    """

    logger = logging.getLogger('gpsdio-despoof-cli')
    logger.setLevel(ctx.obj.get('verbosity', 1))

    o_drv = ctx.obj.get('o_drv')
    o_cmp = ctx.obj.get('o_cmp')
    out_ext = ''
    if o_drv is not None:
        out_ext += '.' + gpsdio.drivers.get_driver(o_drv).extensions[0]
    if o_cmp is not None:
        out_ext += '.' + gpsdio.drivers.get_compression(o_cmp).extensions[0]

    with gpsdio.open(infile, driver=ctx.obj.get('i_drv'),
                     compression=ctx.obj.get('i_cmp')) as src:

        despoofer = Despoofer(
            src, mmsi=mmsi, max_hours=max_hours, max_speed=max_speed, noise_dist=noise_dist)

        logger.debug("Begining to despoof")
        longest_id = None
        longest_count = None
        for t_idx, track in enumerate(despoofer.despoof()):
            logger.debug("Got a track - writing")

            outpath = os.path.join(outdir, str(track.id) + out_ext)

            if longest_id is None or len(track) > longest_count:
                longest_id = track.id
                longest_count = len(track)

            with gpsdio.open(outpath, 'w', driver=ctx.obj.get('o_drv'),
                             compression=ctx.obj.get('o_cmp')) as dst:
                for msg in track:
                    dst.write(msg)
        print("Longest is %s with %s" % (longest_id, longest_count))
        print("Wrote %s tracks" % (t_idx + 1))
