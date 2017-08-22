from __future__ import division

from itertools import chain
import logging
from gpsdio_segment.state import SegmentState 

logger = logging.getLogger(__file__)
# logger.setLevel(logging.DEBUG)


class Segment(object):

    """
    Contains all the messages that have been deemed by the `Segmentizer()` to
    be continuous.
    """

    def __init__(self, id, mmsi):

        """
        Parameters
        ----------
        id : str or int
            Unique identifier for this segment.  If not globally unique must
            be unique within a given `Segmentizer()` run.
        mmsi : int
            MMSI contained within the segment.
        """
        self._id = id
        self._mmsi = mmsi

        self._prev_state = None
        self._prev_segment = None

        self._msgs = []
        self._coords = []

        self._iter_idx = 0

        # logger.debug("Created an instance of %s() with ID: %s",
        #              self.__class__.__name__, self._id)

    @classmethod
    def from_state(cls, state):

        """
        Create a `Segment()` from a previously preserved segment state.  This
        allows continuity of segments across multiple independent processing
        runs.

        Returns
        -------
        Segment
        """

        if type(state) is dict:
            state = SegmentState.from_dict(state)

        seg = cls(state.id, state.mmsi)
        seg._prev_state = state
        seg._prev_segment = Segment(state.id, state.mmsi)
        for msg in state.msgs:
            seg._prev_segment.add_msg(msg)
        return seg

    def __repr__(self):
        return "<{cname}(id={id}, mmsi={mmsi}) with {msg_cnt} msgs at {hsh}>".format(
            cname=self.__class__.__name__, id=self.id, msg_cnt=len(self),
            mmsi=self.mmsi, hsh=hash(self))

    def __iter__(self):
        return iter(self.msgs)

    def __len__(self):
        return len(self.msgs)

    @property
    def has_prev_state(self):
        return self._prev_segment is not None

    @property
    def state(self):

        """
        Capture the current state of the Segment.  Preserves the state of the
        latest messages for creating a new `Segment()` object in a future
        processing run.

        Returns
        -------
        SegmentState
        """

        state = self._prev_state or SegmentState()
        state.id = self.id
        state.mmsi = self.mmsi
        state.msgs = []

        prev_msg = None
        for msg in [self.first_msg,
                    self.last_time_posit_msg,
                    self.last_posit_msg,
                    self.last_msg]:
            if msg is not None and msg is not prev_msg:
                state.msgs.append(msg)
                prev_msg = msg

        state.msg_count += len(self)

        return state

    @property
    def id(self):
        return self._id

    @property
    def mmsi(self):
        return self._mmsi


    @property
    def coords(self):
        """
        A list of tuples containing `(x, y)` coordinates.  Derived from all
        positional messages.
        """

        return self._coords

    @property
    def last_point(self):
        """
        The last `(x, y)` pair or `None` if the segment does not contain any
        positional messages.
        """

        try:
            return self.coords[-1]
        except IndexError:
            return None

    @property
    def msgs(self):
        return self._msgs

    @property
    def last_msg(self):
        try:
            return self.msgs[-1]
        except IndexError:
            # this segment has no messages, see if there are any in the saved state
            return self._prev_segment.last_msg if self._prev_segment else None

    @property
    def last_posit_msg(self):
        """
        Return the last message added to the segment with lat and lon values
        that are not `None`.
        """

        for msg in reversed(self.msgs):
            if msg.get('lat') is not None \
                    and msg.get('lon') is not None:
                return msg

        return self._prev_segment.last_posit_msg if self._prev_segment else None

    @property
    def last_time_posit_msg(self):
        """
        Return the last message added to the segment with `lat`, `lon`, and
        `timestamp` fields that are not `None`.
        """

        for msg in reversed(self.msgs):
            if msg.get('lat') is not None \
                    and msg.get('lon') is not None \
                    and msg.get('timestamp') is not None:
                return msg

        return self._prev_segment.last_time_posit_msg if self._prev_segment else None

    @property
    def first_msg (self):
        return self._prev_segment.first_msg if self.has_prev_state else self.msgs[0] if self.msgs else None

    @property
    def bounds(self):
        """
        Spatial bounds of the segment based on the positional messages.

        Returns
        -------
        tuple
            xmin, ymin, xmax, ymax
        """

        c = list(chain(*self.coords))
        return min(c[0::2]), min(c[1::2]), max(c[2::2]), max(c[3::2])

    @property
    def temporal_extent(self):
        """
        earliest and latest timestamp for messages in this segment

        Returns
        -------
        tuple
            tsmin, tsmax
        """

        return self.first_msg.get('timestamp', None), self.last_msg.get('timestamp', None)

    @property
    def total_seconds(self):
        """
        Total number of seconds from the first message to the last messsage in the segment
        """

        t1, t2 = self.temporal_extent
        return (t2 - t1).total_seconds()

    def add_msg(self, msg):
        if msg.get('mmsi') != self.mmsi:
            raise ValueError(
                'MMSI mismatch: {internal} != {new}'.format(
                    internal=self.mmsi, new=msg.get('mmsi')))
        self._msgs.append(msg)
        if msg.get('lat') is not None and msg.get('lon') is not None:
            self._coords.append((msg['lon'], msg['lat']))


class BadSegment(Segment):
    """
    Sometimes points cannot be segmented for some reason, like if their
    location falls outside the world bounds, so rather than throw the point
    away we stick it into a `BadSegment()` so the user can filter with an
    instance check.
    """