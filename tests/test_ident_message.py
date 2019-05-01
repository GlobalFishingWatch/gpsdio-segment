"""
Unittests for message type rules
"""

import pytest

from datetime import datetime
from datetime import timedelta

from itertools import groupby

from gpsdio_segment.core import Segmentizer



@pytest.mark.parametrize("message_stubs", [
    ( [{'seg': 0, 'type': 1},                   # one segment, one name
       {'seg': 0, 'type': 5, 'shipname': 'A'}]
    ),
    ([{'seg': 0, 'type': 1},                    # only one position, so only one segment, so both names go to the
      {'seg': 0, 'type': 5, 'shipname': 'A'},   # same segment
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
      {'seg': 1, 'type': 5, 'shipname': 'A'}]   # name A also goes to seg 1 because its more recent position
    ),                                          
    ([{'seg': 0, 'type': 18},                   # seg 0 starts
      {'seg': 0, 'type': 24, 'shipname': 'A'},  # seg 0 gets name A
      {'seg': 1, 'type': 18},                   # seg 1 starts
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # seg 1 gets a bunch of names
      {'seg': 1, 'type': 24, 'shipname': 'A'},  # 
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # 
      {'seg': 1, 'type': 24, 'shipname': 'C'}]  # 
    ),                                          
    ([{'seg': 0, 'type': 18},                   # seg 0 starts
      {'seg': 0, 'type': 24, 'shipname': 'A'},  # seg 0 name is now A
      {'seg': 1, 'type': 18},                   # seg 1 starts
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # seg 1 name is now B
      {'seg': 1, 'type': 24, 'shipname': 'C'},  # seg 1 name changes to C, because seg 1 has the most recent position
      {'seg': 1, 'type': 24, 'shipname': 'A'},  # seg 1 is now A
      {'seg': 0, 'type': 18},                   # seg 0 now has the most recent positon
      {'seg': 0, 'type': 24, 'shipname': 'C'},  # seg 0 is now C
      {'seg': 0, 'type': 24, 'shipname': 'B'}]  # seg 0 name changes to B
    ),
    ([{'seg': 0, 'type': 19, 'shipname': 'A'},  # seg 0 starts, with name A
      {'seg': 1, 'type': 18},                   # seg 1 starts
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # seg 1 name is now B
      {'seg': 1, 'type': 24, 'shipname': 'C'},  # seg 1 name changes to C, because seg 1 has the most recent position
      {'seg': 1, 'type': 24, 'shipname': 'A'},  # seg 1 is now A
      {'seg': 0, 'type': 18},                   # seg 0 now has the most recent positon
      {'seg': 0, 'type': 24, 'shipname': 'C'},  # seg 0 is now C
      {'seg': 0, 'type': 24, 'shipname': 'B'},  # name B goes to seg 0
      {'seg': 1, 'type': 18},                   # seg 1 now has the most recent positon
      {'seg': 1, 'type': 24, 'shipname': 'B'}]  # this time B goes to seg 1
    ),
    ([{'seg': 0, 'type': 19, 'shipname': 'A', 'callsign': '1'},  # seg 0 starts, with name A, callsign 1
      {'seg': 1, 'type': 18},                   # seg 1 starts
      {'seg': 1, 'type': 24, 'shipname': 'B'},  # seg 1 name is now B
      {'seg': 1, 'type': 24, 'callsign': '1'},  # goes to seg 1 
      {'seg': 1, 'type': 24, 'callsign': '2'}]  # goes to seg 1 
    ),
    ([{'seg': 0, 'type': 18},
      {'seg': 0, 'type': 18},
      {'seg': 0, 'type': 18},
      {'seg': 0, 'type': 18},
      {'seg': 0, 'type': 18},
      {'seg': 0, 'type': 18},
      {'seg': 1, 'type': 18},
      {'seg': 1, 'type': 24, 'shipname': 'A'}, 
      ]
    ),

    # These tests are currently not applicable because we have suspended
    # Tx type matching

    #   ([{'seg': 0, 'type': 18},
    #     {'seg': 1, 'type': 1},
    #     {'seg': 0, 'type': 24, 'callsign' : 'A'}, # Goes to 0 because Tx type matches
    #     {'seg': 0, 'type': 18},
    #     {'seg': 1, 'type': 5, 'callsign' : 'B'}, # Goes to ` because Tx type matches
    #   ]
    # ),
    #   ([{'seg': 0, 'type': 18},
    #     {'seg': 1, 'type': 18}, # Seg 1 has multiple Tx types
    #     {'seg': 1, 'type': 1}, 
    #     {'seg': 0, 'type': 24, 'callsign' : 'A'}, # Goes to 1 because MOST recent Tx type for segment matches
    #   ]                                           # NOTE: this is not ideal behavior, ideally it goes to 1
    # ),                                            # here, but that's a more complicated fix.
])
def test_seg_ident(message_stubs, msg_generator):
    messages = list(msg_generator.generate_messages(message_stubs))
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
