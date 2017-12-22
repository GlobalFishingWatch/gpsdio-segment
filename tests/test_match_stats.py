import itertools

from gpsdio_segment.core import SegmentState
from gpsdio_segment.core import Segment
from gpsdio_segment.core import Segmentizer
import gpsdio



def test_Segmentizer_collect_match_stats(tmpdir):
    outfile = str(tmpdir.mkdir('test_Segmentizer_collect_match_stats').join('segmented.json'))

    with gpsdio.open('tests/data/416000000.json') as src:
        segmentizer = Segmentizer(src, collect_match_stats=True)

        for seg in segmentizer:
            for msg in seg:
                assert 'segment_matches' in msg