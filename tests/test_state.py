import pytest
import datetime

from gpsdio_segment.core import Segment

def test_segment_state_save_load(msg_generator):
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

