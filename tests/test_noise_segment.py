"""
Some debugging stuf.
"""

from datetime import datetime
from datetime import timedelta
import itertools as it
from collections import Counter

import pytest

import gpsdio
from gpsdio_segment.core import Segmentizer
from gpsdio_segment.segment import NoiseSegment


def test_noise_segment():

    with gpsdio.open('tests/data/338013000.json') as src:
        segmentizer = Segmentizer(src)
        segs = [seg for seg in segmentizer]
        assert len(segs) == 76
        assert Counter([seg.__class__.__name__ for seg in segs]) == {'ClosedSegment': 12, 
            'Segment': 2, 'InfoSegment': 60, 'DiscardedSegment': 2}


    with gpsdio.open('tests/data/338013000.json') as src:
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
                              18: {'InfoSegment': 14, 'Segment': 1, 'DiscardedSegment': 2},
                              19: {'Segment': 1},
                              20: {'Segment': 2}
                             }
