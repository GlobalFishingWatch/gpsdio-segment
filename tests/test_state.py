import pytest
import datetime

from gpsdio_segment.core import Segment

def test_segment_state_save_load():
    id = 1
    mmsi = 123456789
    seg1 = Segment(id, mmsi)
    state = seg1.state
    assert state.id == id
    assert state.mmsi == mmsi

    msgs = [
        {'mmsi': 123456789},
        {'mmsi': 123456789, 'lat': 2, 'lon': 3},
        {'mmsi': 123456789, 'lat': 2.1, 'lon': 3.1, 'timestamp': datetime.datetime.now()},
        {'mmsi': 123456789,  'timestamp': datetime.datetime.now() + datetime.timedelta(minutes=1)}
    ]
    for msg in msgs:
        seg1.add_msg(msg)
    state = seg1.state
    assert state.msg_count == 4
    assert len(state.msgs) == 2

    seg2 = Segment.from_state(state)
    assert seg2.id == id
    assert seg2.mmsi == mmsi
    assert len(seg2) == 0
    
    assert seg2.last_msg == seg1.last_msg
    assert seg2.last_posit_msg == seg1.last_posit_msg
    assert seg2.last_time_posit_msg == seg1.last_time_posit_msg