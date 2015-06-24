"""
Core components for segmenting data
"""


from __future__ import division

from copy import deepcopy
from itertools import chain
import logging

import pyproj


logging.basicConfig()
logger = logging.getLogger('gpsdio-segment-core')


# See `Segmentizer()` for more info
DEFAULT_MAX_HOURS = 24  # hours
DEFAULT_MAX_SPEED = 40  # knots
DEFAULT_NOISE_DIST = round(500 / 1852, 3)  # nautical miles
INFINITE_SPEED = 1000000


class Segmentizer(object):

    def __init__(self, instream, mmsi=None, max_hours=DEFAULT_MAX_HOURS,
                 max_speed=DEFAULT_MAX_SPEED, noise_dist=DEFAULT_NOISE_DIST):

        self.max_hours = max_hours
        self.max_speed = max_speed
        self.noise_dist = noise_dist

        # Exposed via properties
        self._instream = instream

        # Internal objects
        self._geod = pyproj.Geod(ellps='WGS84')
        self._segments = {}
        self._last_id = -1
        self._mmsi = mmsi
        self._prev_msg = None
        self._last_segment = None

        logger.debug("Created an instance of `Segmentizer()` with max_speed=%s, "
                     "max_hours=%s, noise_dist=%s", max_speed, max_hours, noise_dist)

    def __iter__(self):

        """
        Produces completed segments.
        """

        return self.process()

    def __repr__(self):
        return "<{cname}() max_speed={mspeed} max_hours={mhours} noise_dist={ndist} at {id_}>"\
            .format(cname=self.__class__.__name__, mspeed=self.max_speed,
                    mhours=self.max_hours, ndist=self.noise_dist, id_=hash(self))

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

    def _create_segment(self, msg):

        """
        Add a new segments to the segments container.
        """

        self._last_id += 1

        t = Segment(self._last_id, self.mmsi)
        t.add_msg(msg)

        self._segments[self._last_id] = t
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

        try:
            x2 = msg2['lon']
            y2 = msg2['lat']
        except Exception:
            from pprint import pformat
            logger.debug("MSG ISSUE1: %s", msg1)
            logger.debug("MSG ISSUE2: %s", msg2)
            logger.debug("SEGMENTS: %s", self._segments)
            logger.debug(pformat(self._segments[0].msgs))
            raise

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

        Returns the ID or None
        """

        logger.debug("Computing best segment for %s", msg)

        # best_stats are the stats between the input message and the current best segment
        # segment_stats are the stats between the input message and the current segment
        best_stats = None
        best = None
        best_metric = None
        for segment in self._segments.values():
            if not best and segment.last_time_posit_msg:
                best = segment
                best_stats = self.msg_diff_stats(msg, best.last_time_posit_msg)
                best_metric = best_stats['timedelta'] * best_stats['distance']
                logger.debug("    No best - auto-assigned %s", best.id)

            elif segment.last_time_posit_msg:
                segment_stats = self.msg_diff_stats(msg, segment.last_time_posit_msg)
                segment_metric = segment_stats['timedelta'] * segment_stats['distance']

                if segment_metric < best_metric:
                    best = segment
                    best_metric = segment_metric
                    best_stats = segment_stats

        if best is None:
            best = self._segments[sorted(self._segments.keys())[0]]
            logger.debug("Could not determine best, probably because none of the segments "
                         "have any positional messages.  Defaulting to first: %s", best.id)
            return best.id

        logger.debug("Best segment is %s", best.id)
        logger.debug("    Num segments: %s", len(self._segments))

        # TODO: An explicit timedelta check should probably be added to the first part of if
        #       Currently a point within noise distance but is outside time will be added
        #       but ONLY if tracks are not closed when out of time range for some reason.
        #       A better check is one that also incorporates max_hours rather than relying
        #       on tracks that are outside the allowed time delta be closed.
        #       Need to finish some unittests before adding this.
        if best_stats['distance'] <= self.noise_dist or (best_stats['timedelta'] <= self.max_hours and best_stats['speed'] <= self.max_speed):
            return best.id
        else:
            logger.debug("    Dropped best")
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
        """

        logger.debug("Starting to segment %s",
                     ' %s' % self._mmsi if self._mmsi is not None else ' - finding MMSI ...')

        for idx, msg in enumerate(self.instream):

            # Cache the MMSI and some other fields
            mmsi = msg.get('mmsi')
            y = msg.get('lat')
            x = msg.get('lon')
            timestamp = msg.get('timestamp')

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
            for s_id in _yielded:
                del self._segments[s_id]

            # This is the first message with a valid MMSI
            # Make it the previous message and create a new segment
            if self.mmsi is None:
                logger.debug("Found a valid MMSI - processing: %s", mmsi)
                self._mmsi = mmsi
                self._prev_msg = msg
                self._create_segment(msg)
                continue

            # Found an MMSI that does not match - skip
            elif mmsi != self.mmsi:
                logger.debug("Found a non-matching MMSI %s - skipping", mmsi)
                continue

            # Non positional message or lacking timestamp.  Add to the most recent segment.
            elif not x or not y or not timestamp:
                self._last_segment.add_msg(msg)

            # All segments have been closed - create a new one
            elif len(self._segments) is 0:
                self._create_segment(msg)

            # Everything is set up - process!
            elif timestamp < self._prev_msg['timestamp']:
                raise ValueError("Input data is unsorted")
            else:
                best_id = self._compute_best(msg)
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


class Segment(object):

    def __init__(self, id, mmsi):
        self._id = id
        self._mmsi = mmsi

        self._msgs = []
        self._coords = []

        self._iter_idx = 0

        logger.debug("Created an in instance of `Segment()` with ID: %s", id)

    def __repr__(self):
        return "<{cname}(id={id}, mmsi={mmsi}) with {msg_cnt} msgs at {hsh}>".format(
            cname=self.__class__.__name__, id=self.id, msg_cnt=len(self),
            mmsi=self.mmsi, hsh=hash(self))

    def __iter__(self):
        return iter(self.msgs)

    def __len__(self):
        return len(self.msgs)

    # def next(self):
    #
    #     """
    #     Returns a message with an added series value.
    #     """
    #
    #     # Add the series value and return the message
    #     # Iterate the cursor
    #     try:
    #         return self.msgs[self._iter_idx]
    #     except IndexError:
    #         raise StopIteration
    #     # finally:
    #     #     if self._iter_idx >= len(self.msgs):
    #     #         self._iter_idx = 0
    #     #         raise StopIteration
    #     #     else:
    #     #         self._iter_idx += 1
    #
    # __next__ = next

    @property
    def id(self):
        return self._id

    @property
    def coords(self):
        return self._coords

    @property
    def msgs(self):
        return self._msgs

    @property
    def mmsi(self):
        return self._mmsi

    @property
    def last_point(self):
        try:
            return self.coords[-1]
        except IndexError:
            return None

    @property
    def last_msg(self):

        """
        Return the last message added to the segment.
        """

        try:
            return self.msgs[-1]
        except IndexError:
            return None

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

    @property
    def last_time_posit_msg(self):

        """
        Return the last message added to the segment with lat, lon, and timestamp
        values that are not `None`.
        """

        for msg in reversed(self.msgs):
            if msg.get('lat') is not None \
                    and msg.get('lon') is not None \
                    and msg.get('timestamp') is not None:
                return msg

    @property
    def bounds(self):
        c = list(chain(*self.coords))
        return min(c[0::2]), min(c[1::2]), max(c[2::2]), max(c[3::2])

    def add_msg(self, msg):

        if msg.get('mmsi') != self.mmsi:
            raise ValueError(
                'MMSI mismatch: {internal} != {new}'.format(
                    internal=self.mmsi, new=msg.get('mmsi')))
        msg = deepcopy(msg)
        self._msgs.append(msg)
        if msg.get('lat') is not None and msg.get('lon') is not None:
            self._coords.append((msg['lon'], msg['lat']))
