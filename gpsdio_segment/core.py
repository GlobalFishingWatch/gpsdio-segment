"""
Core components for segmenting data
"""


from __future__ import division

from itertools import chain
import logging
import datetime

from gpsdio.schema import datetime2str

import pyproj


logger = logging.getLogger(__file__)
# logger.setLevel(logging.DEBUG)


# See Segmentizer() for more info
DEFAULT_MAX_HOURS = 24  # hours
DEFAULT_MAX_SPEED = 40  # knots
DEFAULT_NOISE_DIST = round(500 / 1852, 3)  # nautical miles
INFINITE_SPEED = 1000000


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

    def __repr__(self):
        return "<{cname}(id={id}, mmsi={mmsi}) with {msg_cnt} msgs at {hsh}>".format(
            cname=self.__class__.__name__, id=self.id, msg_cnt=len(self),
            mmsi=self.mmsi, hsh=hash(self))

    def __iter__(self):

        """
        Iterate over all messages currently contained within the segment.
        """

        return iter(self.msgs)

    def __len__(self):

        """
        Number of messages within the segment
        """

        return len(self.msgs)

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

    @property
    def has_prev_state(self):

        """
        States whether this segment was instantiated as `Segment.from_state()`.

        Returns
        -------
        bool
        """

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
        if self.last_time_posit_msg:
            state.msgs.append(self.last_time_posit_msg)
        if self.last_posit_msg is not self.last_time_posit_msg:
            state.msgs.append(self.last_posit_msg)
        if self.last_msg is not self.last_posit_msg:
            state.msgs.append(self.last_msg)

        state.msg_count += len(self)

        return state

    @property
    def id(self):

        """
        Segment ID.  See `__init__()` documentation.
        """

        return self._id

    @property
    def coords(self):

        """
        A list of tuples containing `(x, y)` coordinates.  Derived from all
        positional messages.

        Returns
        -------
        list
            [(x, y), (x, y), ..]
        """

        return self._coords

    @property
    def msgs(self):

        """
        A list of all GPSd messages contained within this segment.

        Returns
        -------
        list
            [{'mmsi': 123...}, {'mmsi': 123...}, ...]
        """

        return self._msgs

    @property
    def mmsi(self):
        return self._mmsi

    @property
    def last_point(self):

        """
        The last `(x, y)` pair or `None` if the segment does not contain any
        positional messages.

        Returns
        -------
        tuple or None
        """

        try:
            return self.coords[-1]
        except IndexError:
            return None

    @property
    def last_msg(self):

        """
        Return the last message added to the segment.

        Returns
        -------
        dict
        """

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

        Returns
        -------
        dict
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

        Returns
        -------
        dict
        """

        for msg in reversed(self.msgs):
            if msg.get('lat') is not None \
                    and msg.get('lon') is not None \
                    and msg.get('timestamp') is not None:
                return msg

        return self._prev_segment.last_time_posit_msg if self._prev_segment else None

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

    def add_msg(self, msg):

        """
        Add a message to this segment.
        """

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


class Segmentizer(object):

    """
    Group positional messages into related segments based on speed and distance.
    """

    def __init__(self, instream, mmsi=None, max_hours=DEFAULT_MAX_HOURS,
                 max_speed=DEFAULT_MAX_SPEED, noise_dist=DEFAULT_NOISE_DIST):

        """
        Looks at a stream of messages and pull out segments of points that are
        related.  If an existing state is already known, instantiate from the
        `Segmentizer()`

            >>> import gpsdio
            >>> from gpsdio_segment import Segmentizer
            >>> with gpsdio.open(infile) as src, gpsdio.open(outfile) as dst:
            ...     for segment in Segmentizer(src):
            ...        for msg in segment:
            ...            dst.write(msg)

        Parameters
        ----------
        instream : iter
            Stream of GPSd messages.
        mmsi : int, optional
            MMSI to pull out of the stream and process.  If not given the first
            valid MMSI is used.  All messages with a different MMSI are thrown
            away.
        max_hours : int, optional
            Maximum number of hours to allow between points.
        max_speed : int, optional
            Maximum speed allowed between points in nautical miles.
        noise_dist : int, optional
            If a point is within this distance (nautical miles) then add it to
            the closes segment without looking at anything else.
        """

        self.max_hours = max_hours
        self.max_speed = max_speed
        self.noise_dist = noise_dist

        # Exposed via properties
        self._instream = instream

        # Internal objects
        self._geod = pyproj.Geod(ellps='WGS84')
        self._segments = {}
        self._mmsi = mmsi
        self._prev_msg = None
        self._last_segment = None

        # logger.debug("Created an instance of Segmentizer() with "
        #              "max_speed=%s, max_hours=%s, noise_dist=%s",
        #              max_speed, max_hours, noise_dist)

    def __iter__(self):

        """
        Produces completed, detached segments.

        Yields
        ------
        Segment
        """

        return self.process()

    def __repr__(self):
        return "<{cname}() max_speed={mspeed} max_hours={mhours} noise_dist={ndist} at {id_}>".format(
            cname=self.__class__.__name__, mspeed=self.max_speed,
            mhours=self.max_hours, ndist=self.noise_dist, id_=hash(self))

    @classmethod
    def from_seg_states(cls, seg_states, instream, **kwargs):

        """
        Create a Segmentizer and initialize its Segments from a stream of
        `SegmentStates()`, or a stream of dictionaries that can be converted
        via `SegmentState.fromdict()`.

        Returns
        -------
        Segmentizer
        """

        s = cls(instream, **kwargs)
        for state in seg_states:
            seg = Segment.from_state(state)
            s._segments[seg.id] = seg
        if s._segments:
            s._last_segment = max(
                s._segments.values(), key=lambda x: x.last_msg.get('timestamp'))
            s._prev_msg = s._last_segment.last_msg
            if s._mmsi:
                assert s._mmsi == s._last_segment.mmsi
            s._mmsi = s._last_segment.mmsi
        return s

    @property
    def instream(self):

        """
        Handle to the input data stream.
        """

        return self._instream

    @property
    def mmsi(self):

        """
        The MMSI being processed.
        """

        return self._mmsi

    def _segment_unique_id(self, msg):

        """
        Generate a unique ID for a segment from a message, ideally its first.

        Returns
        -------
        str
        """

        ts = msg['timestamp']
        while True:
            seg_id = '{}-{}'.format(msg['mmsi'], datetime2str(ts))
            if seg_id not in self._segments:
                return seg_id
            ts += datetime.timedelta(milliseconds=1)

    def _validate_position(self, x, y):
        """
        Test to see if x and y are in valid range for lat/lon
        """
        return x is None or y is None or (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0 )

    def _create_segment(self, msg):

        """
        Add a new segment to the segments container with its initial message.

        Parameters
        ----------
        msg : dict
            Initial message to place into the segment.
        """

        id_ = self._segment_unique_id(msg)
        t = Segment(id_, self.mmsi)
        t.add_msg(msg)

        self._segments[id_] = t
        self._last_segment = t

    def timedelta(self, msg1, msg2):

        """
        Compute the timedelta between two messages in common units.
        """

        ts1 = msg1['timestamp']
        ts2 = msg2['timestamp']
        if ts1 > ts2:
            return (ts1 - ts2).total_seconds() / 3600
        else:
            return (ts2 - ts1).total_seconds() / 3600

    def msg_diff_stats(self, msg1, msg2):

        """
        Compute the stats required to determine if two points are continuous.  Input
        messages must have a `lat`, `lon`, and `timestamp`, that are not `None` and
        `timestamp` must be an instance of `datetime.datetime()`.

        Parameters
        ----------
        msg1 : dict
            A GPSD message.
        msg2 : dict
            See `msg1`.

        Returns
        -------
        dict
            distance : float
                Distance in natucal miles between the points.
            timedelta : float
                Amount of time between the two points in hours.
            speed : float
                Required speed in knots to travel between the two points within the
                time allotted by `timedelta`.
        """

        x1 = msg1['lon']
        y1 = msg1['lat']

        x2 = msg2['lon']
        y2 = msg2['lat']

        distance = self._geod.inv(x1, y1, x2, y2)[2] / 1850
        timedelta = self.timedelta(msg1, msg2)

        try:
            speed = (distance / timedelta)
        except ZeroDivisionError:
            speed = INFINITE_SPEED

        return {
            'distance': distance,
            'timedelta': timedelta,
            'speed': speed
        }

    def _compute_best(self, msg):

        """
        Compute which segment is the best segment

        Returns
        -------
        int or None
            ID of best segment or `None` no segments exist or all are out of
            range.
        """

        # logger.debug("Computing best segment for %s", msg)

        # best_stats are the stats between the input message and the current best segment
        # segment_stats are the stats between the input message and the current segment
        best_stats = None
        best = None
        best_metric = None
        for segment in self._segments.values():
            if best is None and segment.last_time_posit_msg:
                best = segment
                best_stats = self.msg_diff_stats(msg, best.last_time_posit_msg)
                best_metric = best_stats['timedelta'] * best_stats['distance']
                # logger.debug("    No best - auto-assigned %s", best.id)

            elif segment.last_time_posit_msg:
                segment_stats = self.msg_diff_stats(msg, segment.last_time_posit_msg)
                segment_metric = segment_stats['timedelta'] * segment_stats['distance']

                if segment_metric < best_metric:
                    best = segment
                    best_metric = segment_metric
                    best_stats = segment_stats

        if best is None:
            best = self._segments[sorted(self._segments.keys())[0]]
            # logger.debug("Could not determine best, probably because none of the segments "
            #              "have any positional messages.  Defaulting to first: %s", best.id)
            return best.id

        # logger.debug("Best segment is %s", best.id)
        # logger.debug("    Num segments: %s", len(self._segments))

        # TODO: An explicit timedelta check should probably be added to the first part of if
        #       Currently a point within noise distance but is outside time will be added
        #       but ONLY if tracks are not closed when out of time range for some reason.
        #       A better check is one that also incorporates max_hours rather than relying
        #       on tracks that are outside the allowed time delta be closed.
        #       Need to finish some unittests before adding this.
        if best_stats['distance'] <= self.noise_dist or (
                        best_stats['timedelta'] <= self.max_hours and
                        best_stats['speed'] <= self.max_speed):
            return best.id
        else:
            # logger.debug("    Dropped best")
            return None

    def process(self):

        """
        The method that does all the work.  Creates a generator that spits out
        finished segments.  Rather than calling this directly the intended use
        of this class is:

            >>> import gpsdio
            >>> with gpsdio.open('infile.ext') as src:
            ...    for segment in Segmentizer(src):
            ...        # Do something with the segment

        Yields
        ------
        Segment
        """

        # logger.debug(
        #     "Starting to segment %s",
        #     self._mmsi if self._mmsi is not None else '- finding MMSI ...')

        for idx, msg in enumerate(self.instream):

            # Cache the MMSI and some other fields
            mmsi = msg.get('mmsi')
            y = msg.get('lat')
            x = msg.get('lon')
            timestamp = msg.get('timestamp')

            # Reject any message that has invalid position
            if not self._validate_position(x, y):
                bs = BadSegment(self._segment_unique_id(msg), mmsi=msg['mmsi'])
                bs.add_msg(msg)
                yield bs
                logger.debug("Rejected bad message  mmsi: {mmsi} lat: {lat}  lon: {lon} timestamp: {timestamp} ".format(**msg))
                continue

            # First check if there are any segments that are too far away
            # in time and yield them.  It's possible for messages to not
            # have a timestamp so only do this if the current point has a TS.
            _yielded = []
            for segment in self._segments.values():
                if timestamp and segment.last_msg.get('timestamp'):
                    td = self.timedelta(msg, segment.last_msg)
                    if td > self.max_hours:
                        _yielded.append(segment.id)
                        # logger.debug("Segment %s exceeds max time: %s", segment.id, td)
                        # logger.debug("    Current:  %s", msg['timestamp'])
                        # logger.debug("    Previous: %s", segment.last_msg['timestamp'])
                        # logger.debug("    Time D:   %s", td)
                        # logger.debug("    Max H:    %s", self.max_hours)
                        yield segment

            # TODO: Is there a way to integrate this into the above for loop?  Maybe with dict.pop()?
            for s_id in _yielded:
                del self._segments[s_id]

            # This is the first message with a valid MMSI
            # Make it the previous message and create a new segment
            if self.mmsi is None:
                # logger.debug("Found a valid MMSI - processing: %s", mmsi)

                # We have to make sure the first message isn't out of bounds, otherwise
                # any message compared to it will be
                # It is far more efficient to ensure that no bad segments end up in the
                # segments container rather than trying to yield them every iteration.
                if x is not None and y is not None:
                    try:
                        self._geod.inv(0, 0, x, y)  # Argument order matters
                    except ValueError:
                        logger.debug(
                            "    Could not compute a distance from the first point - "
                            "producing a bad segment")
                        bs = BadSegment(self._segment_unique_id(msg), mmsi=msg['mmsi'])
                        bs.add_msg(msg)
                        yield bs

                        # We still don't have a good initial segment so keep looking ...
                        logger.debug("Still looking for a good first message ...")
                        continue

                self._mmsi = mmsi
                self._prev_msg = msg
                self._create_segment(msg)
                continue

            # Found an MMSI that does not match - skip
            elif mmsi != self.mmsi:
                logger.debug("Found a non-matching MMSI %s - skipping", mmsi)
                continue

            # All segments have been closed - create a new one
            elif len(self._segments) is 0:
                self._create_segment(msg)

            # Non positional message or lacking timestamp.  Add to the most recent segment.
            elif x is None or y is None or timestamp is None:
                self._last_segment.add_msg(msg)

            # Everything is set up - process!
            elif timestamp < self._prev_msg['timestamp']:
                raise ValueError("Input data is unsorted")
            else:

                # Points that are off the map raise an exception in pyproj
                # Let them fall into their own segment for filtering later
                # TODO: This throws away continuous out of bounds segments
                #       which we may want to keep.
                # best_id = self._compute_best(msg)
                try:
                    best_id = self._compute_best(msg)
                except ValueError as e:
                    # logger.debug("    Could not compute best segment: %s", e)
                    # logger.debug("    Bad msg: %s", msg)
                    # logger.debug("    Yielding bad segment")
                    bs = BadSegment(self._segment_unique_id(msg), msg['mmsi'])
                    bs.add_msg(msg)
                    yield bs
                    continue

                if best_id is None:
                    self._create_segment(msg)
                else:
                    self._segments[best_id].add_msg(msg)
                    self._last_segment = self._segments[best_id]

            if x and y and timestamp:
                self._prev_msg = msg

        # No more points to process.  Yield all the remaining segments.
        for series, segment in self._segments.items():
            yield segment


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

    fields = {'id': None, 'mmsi': None, 'msgs': [], 'msg_count': 0}

    def __init__(self):
        self.id = None
        self.mmsi = None
        self.msgs = []
        self.msg_count = 0

    def to_dict(self):

        """
        Convert the state to a dictionary.

            {
                "id": Segment.id,
                "mmsi": Segment.mmsi,
                "msgs": Segment.msgs,
                "msg_count": Segment.msg_count
            }

        Returns
        -------
        dict
        """

        return {
            'id': self.id,
            'mmsi': self.mmsi,
            'msgs': self.msgs,
            'msg_count': self.msg_count
        }

    @classmethod
    def from_dict(cls, d):

        """
        Instantiate from a dictionary.  See `to_dict()` for expected format.

        Returns
        -------
        SegmentState
        """

        s = cls()
        s.mmsi = d['mmsi']
        s.id = d['id']
        s.msgs = d['msgs']
        s.msg_count = d['msg_count']
        return s
