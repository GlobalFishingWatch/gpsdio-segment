import itertools

from gpsdio_segment.core import SegmentState
from gpsdio_segment.core import Segment
from gpsdio_segment.core import Segmentizer
import gpsdio


def test_SegmentState():
    s = SegmentState()
    assert s.to_dict() == SegmentState.from_dict(s.to_dict()).to_dict()
    s.id = 'ABC'
    s.mmsi = '123456789'
    s.msgs = [{'mmsi': 123456789}, {'mmsi': 123456789}]
    s.msg_count = 1
    assert s.to_dict() == SegmentState.from_dict(s.to_dict()).to_dict()


def test_Segment_state_save_load(msg_generator):
    id = 1
    mmsi = 123456789
    seg1 = Segment(id, mmsi)
    state = seg1.state
    assert state.id == id
    assert state.mmsi == mmsi

    seg1.add_msg(msg_generator.next_msg())
    seg1.add_msg(msg_generator.next_posit_msg())
    seg1.add_msg(msg_generator.next_time_posit_msg())
    seg1.add_msg(msg_generator.next_time_posit_msg())
    state = seg1.state
    assert state.msg_count == 4
    assert len(state.msgs) == 1
    seg1.add_msg(msg_generator.next_msg())
    state = seg1.state

    seg2 = Segment.from_state(state)
    assert seg2.id == id
    assert seg2.mmsi == mmsi
    assert len(seg2) == 0
    assert seg2._prev_state

    assert seg2.last_msg == seg1.last_msg
    assert seg2.last_posit_msg == seg1.last_posit_msg
    assert seg2.last_time_posit_msg == seg1.last_time_posit_msg

    msg = msg_generator.next_msg()
    seg2.add_msg(msg)
    assert seg2.last_msg == msg
    assert seg2.last_posit_msg == seg1.last_posit_msg
    assert seg2.last_time_posit_msg == seg1.last_time_posit_msg

    msg = msg_generator.next_posit_msg()
    seg2.add_msg(msg)
    assert seg2.last_msg == msg
    assert seg2.last_posit_msg == msg
    assert seg2.last_time_posit_msg == seg1.last_time_posit_msg

    msg = msg_generator.next_time_posit_msg()
    seg2.add_msg(msg)
    assert seg2.last_msg == msg
    assert seg2.last_posit_msg == msg
    assert seg2.last_time_posit_msg == msg


    msg = msg_generator.next_msg()
    seg2.add_msg(msg)

    assert len(seg2) == 4
    state = seg2.state
    assert len(state.msgs) == 2
    assert state.msg_count == 9


def test_Segmentizer_state_save_load(tmpdir):
    outfile = str(tmpdir.mkdir('test_Segmentizer_state_save_load').join('segmented.json'))

    with gpsdio.open('tests/data/416000000.json') as src:
        segmentizer = Segmentizer(src)
        segs = [seg for seg in segmentizer]
        full_run_seg_states = [seg.state for seg in segs]
        full_run_msg_count = sum(len(seg) for seg in segs)

    with gpsdio.open('tests/data/416000000.json') as src:
        n = 800
        segmentizer = Segmentizer(itertools.islice(src, n))
        first_half_seg_states = [seg.state for seg in segmentizer]

        assert n == sum([st.msg_count for st in first_half_seg_states])

        segmentizer = Segmentizer.from_seg_states(first_half_seg_states, src)
        assert sum([seg._prev_state.msg_count for seg in segmentizer._segments.values()]) == n

        segs = [seg for seg in segmentizer]
        assert sum(len(seg) for seg in segs) == full_run_msg_count - n

        second_half_seg_states = [seg.state for seg in segs]

    assert sum([st.msg_count for st in full_run_seg_states]) == \
        sum([st.msg_count for st in second_half_seg_states])

    assert sorted([st.to_dict() for st in full_run_seg_states], key=lambda x:x['id']) == \
           sorted([st.to_dict() for st in second_half_seg_states], key=lambda x:x['id'])


