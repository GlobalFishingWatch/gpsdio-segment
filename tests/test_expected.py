
import pytest
import glob
import gpsdio
import datetime

from gpsdio_segment.core import Segmentizer


def test_expected():
    for f in glob.glob('tests/data/expected/*.json'):
        with gpsdio.open(f) as src:
            segmentizer = Segmentizer(src)
            for seg in segmentizer:
                for msg in seg:
                    if msg['mmsi'] == 477320700 and msg['timestamp'] == datetime.datetime(2015,1,1,1,38,3):
                        print msg['expected'], seg.id

                    assert msg['expected'] == seg.id

