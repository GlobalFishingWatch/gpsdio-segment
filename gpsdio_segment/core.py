"""
Some ships using AIS are using the same ship identifiers, MMSI. This
means that it is not possible to directly distinguish points for one
ship from points from points for the other ship.

To do so, we use a spatial algorithm. It separates the tracks based on
a maximum possible speed between two consecutive points for a vessel.
If two points are impossible to get between in a low enough speed,
given their times and locations, they must belong to different tracks
(from different vessels).

We also consider any break longer than max_hours=24 hours as creating
two separate tracks, as it would be possible to travel around the
whole earth in that time, in a sufficiently low speed, making it
impossible to say if the two tracks belong to the same vessel or not.

The segmenter maintains a set of "open tracks". For each open tracks
it keeps the last point (latitude, longitude, timestamp). For each new
point, it considers which of the open tracks to add it to, or to
create a new track, and also if it should close any open tracks.

Points are added to the track with the lowest score. The score is
timedelta / max(1, seg_duration) where seg_duration is the length in
time of the segment. There is special handling for when timedelta=0 or
distance=0, see the code.

Points are not added to tracks where the timedelta is greater
than max_hours=24hours. In addition, it is neither added if the speed
implied by the distance and and time delta between the end of the
track and the new point is greater than a cutoff max speed dependant
on the distance, which grows to infinity at zero distance.

If none of the tracks fulfills these requirements, a new track is
opened for the point. If any track is ignored due to the
max_hours, that track is closed, as points are assumed to be
sorted by time, and no new point will ever be added to this track
again.

Points that do not have a timestamp or lat/lon are added to the track
last added to.
"""


from __future__ import division

from itertools import chain
import logging
import datetime
import math

from gpsdio.schema import datetime2str

import pyproj


from gpsdio_segment.segment import Segment, BadSegment
from gpsdio_segment.segment import Segment, NoiseSegment
from gpsdio_segment.state import SegmentState


logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


# See Segmentizer() for more info
DEFAULT_MAX_HOURS = 24  # hours
DEFAULT_MAX_SPEED = 10  # knots
INFINITE_SPEED = 1000000
DEFAULT_SHORT_SEG_THRESHOLD = 10
DEFAULT_SHORT_SEG_WEIGHT = 10
DEFAULT_SEG_LENGTH_WEIGHT = 10

# The values 52 and 102.3 are both almost always noise, and don't
# reflect the vessel's actual speed. They need to be commented out.
# The value 102.3 is reserved for "bad value." It looks like 51.2
# is also almost always noise. The value 63 means unavailable for
# type 27 messages so we exclude that as well. Because the values are floats,
# and not always exactly 102.3 or 51.2, we give a range.
REPORTED_SPEED_EXCLUSION_RANGES = [(51.15, 51.25), (62.95, 63.05), (102.25,102.35)]
AIS_CLASS = {
    1: 'A',
    2: 'A',
    3: 'A',
    5: 'A',
    18: 'B',
    19: 'B',
    24: 'B'
}

class Segmentizer(object):

    """
    Group positional messages into related segments based on speed and distance.
    """

    def __init__(self, instream, mmsi=None, max_hours=DEFAULT_MAX_HOURS,
                 max_speed=DEFAULT_MAX_SPEED, short_seg_threshold=DEFAULT_SHORT_SEG_THRESHOLD,
                 short_seg_weight=DEFAULT_SHORT_SEG_WEIGHT, seg_length_weight=DEFAULT_SEG_LENGTH_WEIGHT,
                 collect_match_stats=False):

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
        """

        self.max_hours = max_hours
        self.max_speed = max_speed
        self.short_seg_threshold = short_seg_threshold
        self.short_seg_weight = short_seg_weight
        self.seg_length_weight = seg_length_weight
        self.collect_match_stats = collect_match_stats

        # Exposed via properties
        self._instream = instream

        # Internal objects
        self._geod = pyproj.Geod(ellps='WGS84')
        self._segments = {}
        self._mmsi = mmsi
        self._prev_timestamp = None
        self._last_segment = None

    def __repr__(self):
        return "<{cname}() max_speed={mspeed} max_hours={mhours} at {id_}>".format(
            cname=self.__class__.__name__, mspeed=self.max_speed,
            mhours=self.max_hours, id_=hash(self))

    @classmethod
    def from_seg_states(cls, seg_states, instream, **kwargs):
        """
        Create a Segmentizer and initialize its Segments from a stream of
        `SegmentStates()`, or a stream of dictionaries that can be converted
        via `SegmentState.fromdict()`.
        """

        s = cls(instream, **kwargs)
        for seg in [Segment.from_state(state) for state in seg_states]:
            # ignore segments that contain only noise messages (bad lat,lon, timestamp etc.)
            if not seg.noise:
                s._segments[seg.id] = seg
        if s._segments:
            s._last_segment = max(
                s._segments.values(), key=lambda x: x.last_msg.get('timestamp'))
            s._prev_timestamp = s._last_segment.last_msg['timestamp']
            if s._mmsi:
                assert s._mmsi == s._last_segment.mmsi
            s._mmsi = s._last_segment.mmsi
        return s

    @property
    def instream(self):
        return self._instream

    @property
    def mmsi(self):
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

    def _validate_message(self, x, y, course, speed):
        return ((x is None and y is None) or # informational message
                (-180.0 <= x <= 180.0 and 
                 -90.0 <= y <= 90.0 and
                 course is not None and 
                 0.0 <= course < 360.0 and # 360 is invalid
                 not any([(x >= v[0] and x <= v[1]) for v in REPORTED_SPEED_EXCLUSION_RANGES])
                 )) 

    def _create_segment(self, msg, cls=Segment):
        id_ = self._segment_unique_id(msg)
        mmsi = self.mmsi if self.mmsi is not None else msg['mmsi']
        t = cls(id_, mmsi)
        t.add_msg(msg)
        return t

    def _add_segment(self, msg):
        t = self._create_segment(msg)

        self._segments[t.id] = t
        self._last_segment = t

    def timedelta(self, msg1, msg2):
        ts1 = msg1['timestamp']
        ts2 = msg2['timestamp']
        if ts1 > ts2:
            return (ts1 - ts2).total_seconds() / 3600
        else:
            return (ts2 - ts1).total_seconds() / 3600

    def delta_hours(self, msg1, msg2):
        ts1 = msg1['timestamp']
        ts2 = msg2['timestamp']
        return (ts2 - ts1).total_seconds() / 3600

    def reported_speed(self, msg):
        s = msg['speed']
        for r in REPORTED_SPEED_EXCLUSION_RANGES:
            if r[0] < s < r[1]:
                logger.warning('This message should have been excluded by _validate_message: {}'.
                    format(msg))
                s = 0
        return s


    @staticmethod
    def message_type(msg):
        return AIS_CLASS.get(msg.get('type'))

    @staticmethod
    def _compute_expected_position(msg, hours):
        # TODO: is it worth looking into PyProj for this?
        epsilon = 1e-3
        x = msg['lon']
        y = msg['lat']
        # Speed is in knots, so `dist` is in nautical miles (nm)
        dist = msg['speed'] * hours 
        course = math.radians(90.0 - msg['course'])
        deg_lat_per_nm = 1.0 / 60
        deg_lon_per_nm = deg_lat_per_nm / (math.cos(math.radians(y)) + epsilon)
        dx = math.cos(course) * dist * deg_lon_per_nm
        dy = math.sin(course) * dist * deg_lat_per_nm
        return x + dx, y + dy

    def msg_diff_stats(self, msg1, msg2):

        """
        Compute the stats required to determine if two points are continuous.  Input
        messages must have a `lat`, `lon`, `course`, `speed` and `timestamp`, 
        that are not `None` and `timestamp` must be an instance of `datetime.datetime()`.

        Returns
        -------
        dict
            distance : float
                Distance in natucal miles between the points.
            delta_hours : float
                Amount of time between the two points in hours (signed).
            speed : float
                Required speed in knots to travel between the two points within the
                time allotted by `timedelta`.
            discrepancy : float
                Difference in nautical miles between where the vessel is expected to 
                be basted on position course and speed versus where it is in a second
                message. Averaged between looking forward from msg1 and looking 
                backward from msg2.
        """

        hours = self.delta_hours(msg1, msg2)

        x1 = msg1['lon']
        y1 = msg1['lat']

        if (x1 is None or y1 is None):
            distance = None
            speed = None
            discrepancy = None
        else:
            x2 = msg2['lon']
            y2 = msg2['lat']

            x2p, y2p = self._compute_expected_position(msg1, hours)
            x1p, y1p = self._compute_expected_position(msg2, -hours)

            def wrap(x):
                return (x + 180) % 360 - 180

            deg_lat_per_nm = 1.0 / 60
            y = 0.5 * (y1 + y2)
            epsilon = 1e-3
            deg_lon_per_nm = deg_lat_per_nm / (math.cos(math.radians(y)) + epsilon)
            info = wrap(x1p - x1), wrap(x2p - x2), (y1p - y1), (y2p - y2)
            discrepancy = 0.5 * (
                math.hypot(1 / deg_lon_per_nm * wrap(x1p - x1) , 
                           1 / deg_lat_per_nm * (y1p - y1)) + 
                math.hypot(1 / deg_lon_per_nm * wrap(x2p - x2) , 
                           1 / deg_lat_per_nm * (y2p - y2)))
            
            distance = self._geod.inv(x1, y1, x2, y2)[2] / 1850     # 1850 meters = 1 nautical mile

            try:
                speed = (distance / abs(hours))
            except ZeroDivisionError:
                speed = INFINITE_SPEED

        return {
            'distance': distance,
            'delta_hours' : hours,
            'speed': speed,
            'discrepancy' : discrepancy,
            'info' : info
        }

    def _segment_match(self, segment, msg):
        match = {'seg_id': segment.id}

        if not segment.last_time_posit_msg:
            match['metric'] = self.max_hours * self.max_speed
            return match

        match.update(self.msg_diff_stats(segment.last_time_posit_msg, msg))

        if abs(match['delta_hours']) > self.max_hours: 
            # Too long has passed, we can't match this segment
            match['metric'] = None
        elif match['distance'] is None:
            # Informational message, match closest position message in time.
            match['metric'] = abs(match['delta_hours'])
        else:
            discrepancy = match['discrepancy']
            minute = 1.0 / 60
            effective_hours = 10 * minute + abs(match['delta_hours'])
            discrepancy_speed = discrepancy / effective_hours
            if discrepancy_speed <= self.max_speed:
                match['metric'] = discrepancy_speed
            else:
                match['metric'] = None

        return match

    def _compute_best(self, msg):
        # figure out which segment is the best match for the given message

        segs = list(self._segments.values())
        best_match = None
        matches = []

        if len(segs) == 1:
            # This is the most common case, so make it optimal
            # and avoid all the messing around with lists in the num_segs > 1 case

            match = self._segment_match(segs[0], msg)
            matches = [match]

            if match['metric'] is not None:
                best_match = match

        elif len(segs) > 1:
            # get match metrics for all candidate segments
            matches = [self._segment_match(seg, msg) for seg in segs]

            # If metric is none, then the segment is not a match candidate
            valid_segs = [s for s, m in zip(segs, matches) if m is not None]
            matches = [m for m in matches if m['metric'] is not None]

            # Fine the longest segment and compute scale factors
            longest = max(len(s.msgs) for s in segs)
            # lower the weight of short_segments, both absolute sense and relative sense
            treshhold = float(self.short_seg_threshold)

            MAX_WEIGHT = 10 
            scales_1 = [1 / (1 + (self.short_seg_weight - 1) * min(len(s.msgs) / treshhold, 1))
                            for s in valid_segs]
            scales_2 = [1 / (1 + (self.seg_length_weight - 1) * len(s.msgs) / float(longest)) 
                            for s in valid_segs]
            scales = [s1 * s2 for (s1, s2) in zip(scales_1, scales_2)]

            metric_match_pairs = [(m['metric'] * s, m) for (m, s) in zip(matches, scales)]

            if metric_match_pairs:
                # find the smallest metric value
                best_match = min(metric_match_pairs, key=lambda x: x[0])[1]

        if self.collect_match_stats:
            msg['segment_matches'] = matches

        return best_match

    def __iter__(self):
        return self.process()

    def process(self):
        for idx, msg in enumerate(self.instream):
            mmsi = msg.get('mmsi')
            timestamp = msg.get('timestamp')
            if self.collect_match_stats:
                msg['segment_matches'] = []

            y = msg.get('lat')
            x = msg.get('lon')
            course = msg.get('course')
            speed = msg.get('speed')

            # Reject any message that has invalid position, course or speed
            if not self._validate_message(x, y, course, speed):
                yield self._create_segment(msg, cls=BadSegment)
                # bs = BadSegment(self._segment_unique_id(msg), mmsi=msg['mmsi'])
                # bs.add_msg(msg)
                # yield bs
                logger.debug("Rejected bad message  mmsi: {mmsi} lat: {lat}  lon: {lon} timestamp: {timestamp} ".format(**msg))
                continue

            _yielded = []
            for segment in self._segments.values():
                last_msg = segment.last_time_posit_msg or segment.last_msg
                if timestamp and last_msg:
                    td = self.timedelta(msg, last_msg)
                    if td > self.max_hours:
                        if False:
                            logger.debug("Segment %s exceeds max time: %s", segment.id, td)
                            logger.debug("    Current:  %s", msg['timestamp'])
                            logger.debug("    Previous: %s", segment.last_msg['timestamp'])
                            logger.debug("    Time D:   %s", td)
                            logger.debug("    Max H:    %s", self.max_hours)
                        _yielded.append(segment.id)
                        yield segment

            # TODO: Is there a way to integrate this into the above for loop?  Maybe with dict.pop()?
            for s_id in _yielded:
                del self._segments[s_id]

            if self.mmsi is None:
                # logger.debug("Found a valid MMSI - processing: %s", mmsi)
                if x is not None and y is not None:
                    try:
                        # We have to make sure the first message isn't out of bounds
                        self._geod.inv(0, 0, x, y)  # Argument order matters
                    except ValueError:
                        logger.debug(
                            "    Could not compute a distance from the first point - "
                            "producing a bad segment")
                        yield self._create_segment(msg, cls=BadSegment)
                        # bs = BadSegment(self._segment_unique_id(msg), mmsi=msg['mmsi'])
                        # bs.add_msg(msg)
                        # yield bs

                        logger.debug("Still looking for a good first message ...")
                        continue

                self._mmsi = mmsi
                self._prev_timestamp = msg['timestamp']
                self._add_segment(msg)
                continue

            elif mmsi != self.mmsi:
                logger.debug("Found a non-matching MMSI %s - skipping", mmsi)
                continue

            elif len(self._segments) is 0:
                self._add_segment(msg)

            elif timestamp is None:
                raise ValueError("Message missing  timestamp")
                self._last_segment.add_msg(msg)

            elif timestamp < self._prev_timestamp:
                raise ValueError("Input data is unsorted")

            else:
                try:
                    best_match = self._compute_best(msg)
                except ValueError as e:
                    if False:
                        logger.debug("    Out of bound points, could not compute best segment: %s", e)
                        logger.debug("    Bad msg: %s", msg)
                        logger.debug("    Yielding bad segment")
                    yield self._create_segment(msg, cls=BadSegment)
                    continue

                if best_match is None:
                    self._add_segment(msg)
                else:
                    id = best_match['seg_id']
                    self._segments[id].add_msg(msg)
                    self._last_segment = self._segments[id]


            if x and y and timestamp:
                self._prev_timestamp = msg['timestamp']

        for series, segment in self._segments.items():
            yield segment

