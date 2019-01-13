"""
Unittests for message type rules
"""

import pytest

from datetime import datetime
from datetime import timedelta

from itertools import groupby

from gpsdio_segment.core import Segmentizer



def generate_messages(message_stubs):
    t = datetime.now()
    lat = 0
    lon = 0

    for idx, stub in enumerate(message_stubs):
        msg = dict(
            idx=idx,
            mmsi=1,
            timestamp = t
        )
        msg.update(stub)

        if msg.get('type', 99) in (1, 18, 19):
            msg['lat'] = lat + (msg['seg'] * 2)
            msg['lon'] = lon + (msg['seg'] * 2)

        t += timedelta(hours=1)
        lat += 0.01
        lon += 0.01

        yield msg



@pytest.mark.parametrize("message_stubs", [
    ( [{'seg': 0, 'type': 1},                   # one segement, one name
       {'seg': 0, 'type': 5, 'shipname': 'A'}]
    ),
    ([{'seg': 0, 'type': 1},                    # only one position, so only one segment, so both names go to the
      {'seg': 0, 'type': 5, 'shipname': 'A'},   # sane segment
      {'seg': 0, 'type': 5, 'shipname': 'B'}]
     ),
    ([{'seg': 0, 'type': 1},                    # seg 0 starts
      {'seg': 0, 'type': 5, 'shipname': 'A'},   # seg 0 gets name A
      {'seg': 1, 'type': 1},                    # seg 1 starts
      {'seg': 1, 'type': 5, 'shipname': 'B'}]   # seg 1 gets name B
     ),
    ([{'seg': 0, 'type': 1},                    # seg 0 starts
      {'seg': 0, 'type': 5, 'shipname': 'A'},   # seg 0 gets name A
      {'seg': 1, 'type': 1},                    # seg 1 starts
      {'seg': 1, 'type': 5, 'shipname': 'B'},   # seg 1 gets name B
      {'seg': 0, 'type': 5, 'shipname': 'A'}]   # name A goes to seg 0 because the name matches, even though seg 1
     ),                                         # has the more recent postion

    ([{'seg': 0, 'type': 18},                   # seg 0 starts
      {'seg': 0, 'type': 24, 'shipname': 'A'},  # seg 0 gets name A
      {'seg': 1, 'type': 18},                   # seg 1 starts
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # seg 1 gets name B
      {'seg': 0, 'type': 24, 'shipname': 'A'},  # name A goes to seg 1 because the name matches
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # name B goes to seg 2
      {'seg': 1, 'type': 24, 'shipname': 'C'}]  # name C does not match either seg, so it goes to seg 1 because it has
    ),                                          # the most recent position

    ([{'seg': 0, 'type': 18},                   # seg 0 starts
      {'seg': 0, 'type': 24, 'shipname': 'A'},  # seg 0 name is now A
      {'seg': 1, 'type': 18},                   # seg 1 starts
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # seg 1 name is now B
      {'seg': 1, 'type': 24, 'shipname': 'C'},  # seg 1 name changes to C, because seg 1 has the most recent position
      {'seg': 0, 'type': 24, 'shipname': 'A'},  # seg 0 is still A
      {'seg': 0, 'type': 18},                   # seg 0 now has the most recent positon
      {'seg': 1, 'type': 24, 'shipname': 'C'},  # seg 1 is still C, even though seg 0 has the most recent position
      {'seg': 0, 'type': 24, 'shipname': 'B'}]  # seg 0 name changes to B because it does not match A or C and
                                                # seg 0 has the most recent position
    ),
    ([{'seg': 0, 'type': 19, 'shipname': 'A'},  # seg 0 starts, with name A
      {'seg': 1, 'type': 18},                   # seg 1 starts
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # seg 1 name is now B
      {'seg': 1, 'type': 24, 'shipname': 'C'},  # seg 1 name changes to C, because seg 1 has the most recent position
      {'seg': 0, 'type': 24, 'shipname': 'A'},  # seg 0 is still A
      {'seg': 0, 'type': 18},                   # seg 0 now has the most recent positon
      {'seg': 1, 'type': 24, 'shipname': 'C'},  # seg 1 is still C, even though seg 0 has the most recent position
      {'seg': 0, 'type': 24, 'shipname': 'B'},  # name B goes to seg 0 since it does not match A or C and seg 0 has
                                                # the most recent position, but the name A is anchored by the type 19 msg
      {'seg': 1, 'type': 18},                   # seg 1 now has the most recent positon
      {'seg': 1, 'type': 24, 'shipname': 'B'}]  # this time B goes to seg 1, because seg 0 still has name A
    ),
    ([{'seg': 0, 'type': 19, 'shipname': 'A', 'callsign': '1'},  # seg 0 starts, with name A, callsign 1
      {'seg': 1, 'type': 18},                   # seg 1 starts
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # seg 1 name is now B
      {'seg': 0, 'type': 24, 'callsign': '1'},  # goes to seg 0 because it matches
      {'seg': 1, 'type': 24, 'callsign': '2'}]  # goes to seg 1 because that is the most recent position
    ),
])
def test_seg_ident(message_stubs):
    messages = list(generate_messages(message_stubs))
    segments = list(Segmentizer(messages))

    # group the input messages into exected segment groups based on the 'seg' field
    sorted_messages = sorted(messages, key=lambda x: x['seg'])
    grouped_messages = groupby(sorted_messages, key=lambda x: x['seg'])
    expected_seg_messages = [set(m['idx'] for m in msgs) for _, msgs in grouped_messages]

    # put segments in time order and extract message indexes
    sorted_segments = sorted(segments, key=lambda x: x.temporal_extent)
    actual_seg_messages = [set(m['idx'] for m in seg) for seg in sorted_segments]

    # compare the sets of message indexes in the actual and expected segment groups
    assert len(actual_seg_messages) == len(expected_seg_messages)
    for actual, expected in zip(actual_seg_messages, expected_seg_messages):
        assert actual == expected

    # assert False

