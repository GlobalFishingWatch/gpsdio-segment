from __future__ import division

class SegmentState:

    """
    A simple container to hold the current state of a Segment.   Get one of
    these from `Segment.state` and pass it in when you create a new Segment
    with `Segment.from_state()`.

    The use case for this is when you a parsing a stream in chunks, perhaps
    one chunk per day of data, and you need to preserve the state of the
    `Segment()` from one processing run to the next  without keeping all the
    old messages that you no longer need.
    """

    def __init__(self):
        self.id = None
        self.mmsi = None
        self.msgs = []
        self.msg_count = 0
        self.noise = False
        self.closed = False

    def to_dict(self):
        return {
            'id': self.id,
            'mmsi': self.mmsi,
            'msgs': self.msgs,
            'msg_count': self.msg_count,
            'noise': self.noise,
            'closed': self.closed
        }

    @classmethod
    def from_dict(cls, d):
        s = cls()
        s.mmsi = d['mmsi']
        s.id = d['id']
        s.msgs = d['msgs']
        s.msg_count = d['msg_count']
        s.noise = d.get('noise', False)
        s.closed = d.get('closed', False)
        return s
