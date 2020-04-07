
import pytest
import glob
import datetime

from gpsdio_segment.core import Segmentizer
from support import read_json


def test_expected():
    for f in glob.glob('tests/data/expected/*.json'):
        with open(f) as f:
            src = read_json(f)
            segmentizer = Segmentizer(src)
            for seg in segmentizer:
                for msg in seg:
                    assert msg['expected'] == seg.id

