
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
                    assert msg['expected'] == seg.id

