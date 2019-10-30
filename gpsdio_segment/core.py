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


from __future__ import division, print_function

from itertools import chain
import logging
import datetime
import math

from gpsdio.schema import datetime2str

import pyproj


from gpsdio_segment.segment import Segment, BadSegment, ClosedSegment
from gpsdio_segment.segment import DiscardedSegment, InfoSegment


logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


# See Segmentizer() for more info
DEFAULT_MAX_HOURS = 2.5 * 24 
DEFAULT_PENALTY_HOURS = 1
DEFAULT_HOURS_EXP = 2.0
DEFAULT_BUFFER_HOURS = 5 / 60
DEFAULT_LOOKBACK = 5
DEFAULT_LOOKBACK_FACTOR = 1.2
DEFAULT_MAX_KNOTS = 25
DEFAULT_AMBIGUITY_FACTOR = 2.0
DEFAULT_SHORT_SEG_THRESHOLD = 10
DEFAULT_SHORT_SEG_EXP = 0.5



MAX_OPEN_SEGMENTS = 10

# TODO: move these into parameters
VERY_SLOW = 0.35
# LOOKBACK_SPEED = 0.5
SHAPE_FACTOR = 2
NEW_HOURS_EXP = 0.9
# BUFFER_NM = 1
ALPHA_0 = DEFAULT_MAX_KNOTS / 10

# The values 52 and 102.3 are both almost always noise, and don't
# reflect the vessel's actual speed. They need to be commented out.
# The value 102.3 is reserved for "bad value." It looks like 51.2
# is also almost always noise. The value 63 means unavailable for
# type 27 messages so we exclude that as well. Because the values are floats,
# and not always exactly 102.3 or 51.2, we give a range.
REPORTED_SPEED_EXCLUSION_RANGES = [(51.15, 51.25), (62.95, 63.05), (102.25,102.35)]
SAFE_SPEED = min([x for (x, y) in REPORTED_SPEED_EXCLUSION_RANGES])



POSITION_MESSAGE = object()
INFO_MESSAGE = object()
BAD_MESSAGE = object()


class Segmentizer(object):

    """
    Group positional messages into related segments based on speed and distance.
    """

    def __init__(self, instream, ssvid=None, 
                 max_hours=DEFAULT_MAX_HOURS,
                 penalty_hours=DEFAULT_PENALTY_HOURS, 
                 buffer_hours=DEFAULT_BUFFER_HOURS,
                 hours_exp=DEFAULT_HOURS_EXP,
                 max_speed=DEFAULT_MAX_KNOTS, 
                 lookback=DEFAULT_LOOKBACK,
                 lookback_factor=DEFAULT_LOOKBACK_FACTOR,
                 short_seg_threshold=DEFAULT_SHORT_SEG_THRESHOLD,
                 short_seg_exp=DEFAULT_SHORT_SEG_EXP,
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
        ssvid : int, optional
            MMSI or other Source Specific ID to pull out of the stream and process.  
            If not given, the first valid ssvid is used.  All messages with a 
            different ssvid are thrown away.
        max_hours : float, optional
            Maximum number of hours to allow between points in a segment.
        penalty_hours : float, optional
            The effective hours is reduced at around this point.
        hours_exp: float, optional
            Exponent used when computing the penalty hours correction.
        buffer_hours: float, optional
            Time between points is padded by this amount when computing metrics.
        max_speed : int, optional
            Maximum speed allowed between points in nautical miles.
        lookback : int, optional
            Number of points to look backwards when matching segments.
        lookback_factor : float, optional:
            How much better a match to a previous point has to be in order to use lookback.
        short_seg_threshold : int, optional
            Segments shorter than this are penalized when computing metrics
        short_seg_exp : float, optional
            Controls the scaling of short seg. 
        """

        self.max_hours = max_hours
        self.penalty_hours = penalty_hours
        self.hours_exp = hours_exp
        self.max_speed = max_speed
        self.buffer_hours = buffer_hours
        self.lookback = lookback
        self.lookback_factor = lookback_factor
        self.short_seg_threshold = short_seg_threshold
        self.short_seg_exp = short_seg_exp
        self.collect_match_stats = collect_match_stats

        # Exposed via properties
        self._instream = instream

        # Internal objects
        self._segments = {}
        self._ssvid = ssvid
        self._prev_timestamp = None

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
        for state in seg_states:
            if isinstance(state, dict):
                if state['closed']:
                    continue
            else:
                if state.closed:
                    continue
            seg = Segment.from_state(state)
            s._segments[seg.id] = seg
            if seg.last_msg:
                ts = seg.last_msg['timestamp']
                if s._prev_timestamp is None or ts > s._prev_timestamp:
                    s._prev_timestamp = ts
        return s

    @property
    def instream(self):
        return self._instream

    @property
    def ssvid(self):
        return self._ssvid

    def _segment_unique_id(self, msg):
        """
        Generate a unique ID for a segment from a message, ideally its first.

        Returns
        -------
        str
        """

        ts = msg['timestamp']
        while True:
            seg_id = '{}-{}'.format(msg['ssvid'], datetime2str(ts))
            if seg_id not in self._segments:
                return seg_id
            ts += datetime.timedelta(milliseconds=1)

    def _message_type(self, x, y, course, speed):
        def is_null(v):
            return (v is None) or math.isnan(v)
        if is_null(x) and is_null(y) and is_null(course) and is_null(speed):
            return INFO_MESSAGE
        if  (x is not None and y is not None and
             speed is not None and course is not None and 
             -180.0 <= x <= 180.0 and 
             -90.0 <= y <= 90.0 and
             course is not None and 
             speed is not None and
             ((speed <= VERY_SLOW and course > 359.95) or
             0.0 <= course <= 359.95) and # 360 is invalid unless speed is very low.
             (speed < SAFE_SPEED or
             not any(l < speed < h for (l, h) in REPORTED_SPEED_EXCLUSION_RANGES))):
            return POSITION_MESSAGE
        return BAD_MESSAGE

    def _create_segment(self, msg, cls=Segment):
        id_ = self._segment_unique_id(msg)
        seg = cls(id_, self.ssvid)
        seg.add_msg(msg)
        return seg

    def _remove_excess_segments(self):
        while len(self._segments) >= MAX_OPEN_SEGMENTS:
            # Remove oldest segment
            segs = list(self._segments.items())
            segs.sort(key=lambda x: x[1].last_msg['timestamp'])
            stalest_seg_id, _ = segs[0]
            logger.debug('Removing stale segment {}'.format(stalest_seg_id))
            for x in self.clean(self._segments.pop(stalest_seg_id), ClosedSegment):
                yield x

    def _add_segment(self, msg):
        for excess_seg in self._remove_excess_segments():
            yield excess_seg
        seg = self._create_segment(msg)
        self._segments[seg.id] = seg


    def delta_hours(self, msg1, msg2):
        ts1 = msg1['timestamp']
        ts2 = msg2['timestamp']
        return (ts1 - ts2).total_seconds() / 3600

    @staticmethod
    def _compute_expected_position(msg, hours):
        epsilon = 1e-3
        x = msg['lon']
        y = msg['lat']
        speed = msg['speed']
        course = msg['course']
        if course > 359.95:
            assert speed <= VERY_SLOW, (course, speed)
            speed = 0
        # Speed is in knots, so `dist` is in nautical miles (nm)
        dist = speed * hours 
        course = math.radians(90.0 - course)
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
        """

        hours = self.delta_hours(msg2, msg1)
        assert hours >= 0

        x1 = msg1['lon']
        y1 = msg1['lat']
        assert x1 is not None and y1 is not None
        x2 = msg2.get('lon')
        y2 = msg2.get('lat')

        if (x2 is None or y2 is None):
            distance = None
            speed = None
            discrepancy = None
        else:
            x2p, y2p = self._compute_expected_position(msg1, hours)
            x1p, y1p = self._compute_expected_position(msg2, -hours)

            def wrap(x):
                return (x + 180) % 360 - 180

            nm_per_deg_lat = 60.0
            y = 0.5 * (y1 + y2)
            epsilon = 1e-3
            nm_per_deg_lon = nm_per_deg_lat  * math.cos(math.radians(y))
            discrepancy1 = 0.5 * (
                math.hypot(nm_per_deg_lon * wrap(x1p - x1) , 
                           nm_per_deg_lat * (y1p - y1)) + 
                math.hypot(nm_per_deg_lon * wrap(x2p - x2) , 
                           nm_per_deg_lat * (y2p - y2)))

            # Vessel just stayed put
            dist = math.hypot(nm_per_deg_lat * (y2 - y1), 
                              nm_per_deg_lon * wrap(x2 - x1))
            discrepancy2 = dist * SHAPE_FACTOR

            # Distance perp to line
            rads21 = math.atan2(nm_per_deg_lat * (y2 - y1), 
                                nm_per_deg_lon * wrap(x2 - x1))
            delta21 = math.radians(90 - msg1['course']) - rads21
            tangential21 = math.cos(delta21) * msg1['speed'] * hours
            if 0 < tangential21 <= msg1['speed'] * hours:
                normal21 = abs(math.sin(delta21)) * dist
            else:
                normal21 = math.inf
            delta12 = math.radians(90 - msg2['course']) - rads21 
            tangential12 = math.cos(delta12) * msg2['speed'] * hours
            if 0 < tangential12 <= msg2['speed'] * hours:
                normal12 = abs(math.sin(delta12)) * dist
            else:
                normal12 = math.inf
            discrepancy3 = 0.5 * (normal12 + normal21) * SHAPE_FACTOR

            discrepancy = min(discrepancy1, discrepancy2, discrepancy3)

        return discrepancy, hours


    def _segment_match(self, segment, msg):
        match = {'seg_id': segment.id,
                 'msgs_to_drop' : [],
                 'metric' : None}

        # Get the stats for the last `lookback` positional messages
        candidates = []

        n = len(segment)
        msgs_to_drop = []
        metric = 0
        for cnd_msg in segment.get_all_reversed_msgs():
            n -= 1
            if cnd_msg.get('drop'):
                continue
            candidates.append((metric, msgs_to_drop[:], self.msg_diff_stats(cnd_msg, msg)))
            if len(candidates) >= self.lookback or n < 0:
                # This allows looking back 1 message into the previous batch of messages
                break
            msgs_to_drop.append(cnd_msg)
            metric = cnd_msg.get('metric', 0)

        assert len(candidates) > 0

        for lookback, (existing_metric, msgs_to_drop, (discrepancy, hours)) in enumerate(candidates):
            assert hours >= 0
            if hours > self.max_hours: 
                # Too long has passed, we can't match this segment
                break
            else:
                effective_hours = (hours + self.buffer_hours)
                if effective_hours > 1:
                    effective_hours **= NEW_HOURS_EXP
                max_allowed_discrepancy = effective_hours * self.max_speed
                if discrepancy <= max_allowed_discrepancy:
                    alpha = ALPHA_0 * discrepancy / max_allowed_discrepancy 
                    metric = math.exp(-alpha ** 2) / effective_hours ** 2
                    # Scale the metric using the lookback factor so that it only
                    # matches to points further in the past if they are noticeably better
                    # lookback_offset = LOOKBACK_SPEED * lookback
                    metric = metric * self.lookback_factor ** -lookback #+ lookback_offset
                    if metric < existing_metric:
                        # Don't make existing segment worse
                        continue
                    if match['metric'] is None or metric > match['metric']:
                        match['metric'] = metric
                        match['msgs_to_drop'] = msgs_to_drop

        return match

    def _compute_best(self, msg):
        # figure out which segment is the best match for the given message

        segs = list(self._segments.values())
        best_match = None

        # get match metrics for all candidate segments
        raw_matches = [self._segment_match(seg, msg) for seg in segs]
        # If metric is none, then the segment is not a match candidate
        matches = [x for x in raw_matches if x['metric'] is not None]

        if len(matches) == 1:
            # This is the most common case, so make it optimal
            # and avoid all the messing around with lists in the num_segs > 1 case
            [best_match] = matches
        elif len(matches) > 1:
            # Down-weight (decrease metric) for short segments
            valid_segs = [s for s, m in zip(segs, raw_matches) if m is not None]
            alphas = [min(s.msg_count / self.short_seg_threshold, 1) for s in valid_segs]
            metric_match_pairs = [(m['metric'] * a ** self.short_seg_exp, m) 
                                    for (m, a) in zip(matches, alphas)]
            metric_match_pairs.sort(key=lambda x: x[0], reverse=True)
            # Check if best match is close enough to an existing match to be ambiguous.
            best_metric, best_match = metric_match_pairs[0]
            close_matches = [best_match]
            for metric, match in metric_match_pairs[1:]:
                if metric * DEFAULT_AMBIGUITY_FACTOR >= best_metric:
                    close_matches.append(match)
            if len(close_matches) > 1:
                logger.debug('Ambiguous messages for id {}'.format(msg['ssvid']))
                best_match = close_matches


        if self.collect_match_stats:
            msg['segment_matches'] = matches

        return best_match

    def __iter__(self):
        return self.process()

    def clean(self, segment, cls):
        if segment.has_prev_state:
            new_segment = cls.from_state(segment.prev_state)
        else:
            new_segment = cls(segment.id, segment.ssvid)
        for msg in segment.msgs:
            msg.pop('metric', None)
            if msg.pop('drop', False):
                logger.debug(("Dropping message from ssvid: {ssvid!r} timestamp: {timestamp!r}").format(
                    **msg))
                yield self._create_segment(msg, cls=DiscardedSegment)
                continue
            else:
                new_segment.add_msg(msg)
        yield new_segment

    def process(self):
        for idx, msg in enumerate(self.instream):
            ssvid = msg.get('ssvid')

            if self.ssvid is None:
                self._ssvid = ssvid
            elif ssvid != self.ssvid:
                logger.warning("Skipping non-matching SSVID %r, expected %r", ssvid, self.ssvid)
                continue

            timestamp = msg.get('timestamp')
            if timestamp is None:
                raise ValueError("Message missing timestamp") 
            if self._prev_timestamp is not None and timestamp < self._prev_timestamp:
                raise ValueError("Input data is unsorted")
            self._prev_timestamp = msg['timestamp']

            y = msg.get('lat')
            x = msg.get('lon')
            course = msg.get('course')
            speed = msg.get('speed')

            msg_type = self._message_type(x, y, course, speed)

            if msg_type is BAD_MESSAGE:
                yield self._create_segment(msg, cls=BadSegment)
                logger.debug(("Rejected bad message from ssvid: {ssvid!r} lat: {y!r}  lon: {x!r} "
                              "timestamp: {timestamp!r} course: {course!r} speed: {speed!r}").format(**locals()))
                continue

            if msg_type is INFO_MESSAGE:
                yield self._create_segment(msg, cls=InfoSegment)
                logger.debug("Skipping info message form ssvid: %s", msg['ssvid'])
                continue

            assert msg_type is POSITION_MESSAGE

            if len(self._segments) == 0:
                for x in self._add_segment(msg):
                    yield x
            else:
                # Finalize and remove any segments that have not had a positional message in `max_hours`
                for segment in list(self._segments.values()):
                    if (self.delta_hours(msg, segment.last_msg) > self.max_hours):
                            for x in self.clean(self._segments.pop(segment.id), cls=ClosedSegment):
                                yield x

                best_match = self._compute_best(msg)
                if best_match is None:
                    for x in self._add_segment(msg):
                        yield x
                elif isinstance(best_match, list):
                    # This message could match multiple segments. 
                    # So finalize and remove ambiguous segments so we can start fresh
                    # TODO: once we are fully py3, this and similar can be cleaned up using `yield from`
                    for match in best_match:
                        for x in self.clean(self._segments.pop(match['seg_id']), cls=ClosedSegment):
                            yield x
                    # Then add as new segment.
                    for x in self._add_segment(msg):
                        yield x
                else:
                    id_ = best_match['seg_id']
                    for msg_to_drop in best_match['msgs_to_drop']:
                        msg_to_drop['drop'] = True
                    msg['metric'] = best_match['metric']
                    self._segments[id_].add_msg(msg)


        for series, segment in list(self._segments.items()):
            for x in self.clean(self._segments.pop(segment.id), Segment):
                yield x

