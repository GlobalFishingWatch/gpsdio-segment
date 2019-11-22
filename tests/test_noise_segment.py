"""
Some debugging stuf.
"""

from datetime import datetime
from datetime import timedelta
import itertools as it
from collections import Counter

import pytest

from gpsdio_segment.core import Segmentizer
from gpsdio_segment.segment import NoiseSegment

from support import read_json

def test_noise_segment():

    with open('tests/data/338013000.json') as f:
        src = read_json(f)
        segmentizer = Segmentizer(src)
        segs = [seg for seg in segmentizer]
        assert len(segs) == 85
        assert Counter([seg.__class__.__name__ for seg in segs]) == {'ClosedSegment': 11, 
            'Segment': 8, 'InfoSegment': 60, 'DiscardedSegment': 6}


    with open('tests/data/338013000.json') as f:
        src = read_json(f)
        # now run it one day at a time and store the segment states in between
        seg_states = {}
        seg_types = {}
        for day, msgs in it.groupby(src, key=lambda x: x['timestamp'].day):
            prev_states = seg_states.get(day - 1)
            if prev_states:
                segmentizer = Segmentizer.from_seg_states(prev_states, list(msgs)[:1])
            else:
                segmentizer = Segmentizer(msgs)

            segs = [seg for seg in segmentizer]
            seg_types[day] = Counter([seg.__class__.__name__ for seg in segs])

            seg_states[day] = [seg.state for seg in segs]

        # some noise segments on the first day that does not get passed back in on the second day
        assert seg_types == {
                              18: {'InfoSegment': 14, 'Segment': 5, 'DiscardedSegment': 2},
                              19: {'Segment': 6},
                              20: {'Segment': 7}
                             }
