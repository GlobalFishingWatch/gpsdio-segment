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

The details of how this is performed is best explained by examining
the logic in the function `_compute_best`.

Points that do not have a timestamp or lat/lon are added to the track
last added to.
"""


from __future__ import division, print_function
import logging
import datetime
import math


from gpsdio_segment.segment import Segment, BadSegment, ClosedSegment
from gpsdio_segment.segment import DiscardedSegment, InfoSegment



logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

inf = float("inf")

POSITION_TYPES = {
    'AIS.1' : {'AIS-A'}, 
    'AIS.2' : {'AIS-A'},
    'AIS.3' : {'AIS-A'},
    'AIS.18' : {'AIS-B'}, 
    'AIS.19' : {'AIS-B'},
    'AIS.27' : {'AIS-A', 'AIS-B'}
    } 

INFO_TYPES = {
    'AIS.5' : 'AIS-A',
    'AIS.18' : 'AIS-B', 
    'AIS.19' : 'AIS-B'
    }



# See Segmentizer() for more info
DEFAULT_MAX_HOURS = 2.5 * 24 
DEFAULT_PENALTY_HOURS = 1
DEFAULT_HOURS_EXP = 0.5
DEFAULT_BUFFER_HOURS = 5 / 60
DEFAULT_LOOKBACK = 5
DEFAULT_LOOKBACK_FACTOR = 1.2
DEFAULT_MAX_KNOTS = 25
DEFAULT_AMBIGUITY_FACTOR = 10.0
DEFAULT_SHORT_SEG_THRESHOLD = 10
DEFAULT_SHORT_SEG_EXP = 0.5
DEFAULT_SHAPE_FACTOR = 4.0
DEFAULT_BUFFER_NM = 5.0
DEFAULT_TRANSPONDER_MISMATCH_WEIGHT = 0.1
DEFAULT_PENALTY_SPEED = 5.0
DEFAULT_MAX_OPEN_SEGMENTS = 20
DEFAULT_VERY_SLOW = 0.35

INFO_PING_INTERVAL_MINS = 6

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
                 prev_msgids=None, 
                 prev_locations=None,
                 prev_info=None,
                 max_hours=DEFAULT_MAX_HOURS,
                 penalty_hours=DEFAULT_PENALTY_HOURS, 
                 buffer_hours=DEFAULT_BUFFER_HOURS,
                 hours_exp=DEFAULT_HOURS_EXP,
                 max_speed=DEFAULT_MAX_KNOTS, 
                 lookback=DEFAULT_LOOKBACK,
                 lookback_factor=DEFAULT_LOOKBACK_FACTOR,
                 short_seg_threshold=DEFAULT_SHORT_SEG_THRESHOLD,
                 short_seg_exp=DEFAULT_SHORT_SEG_EXP,
                 shape_factor=DEFAULT_SHAPE_FACTOR,
                 buffer_nm=DEFAULT_BUFFER_NM,
                 transponder_mismatch_weight=DEFAULT_TRANSPONDER_MISMATCH_WEIGHT,
                 penalty_speed=DEFAULT_PENALTY_SPEED,
                 max_open_segments=DEFAULT_MAX_OPEN_SEGMENTS,
                 very_slow=DEFAULT_VERY_SLOW
                 ):

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
        prev_msgids : set, optional
            Messages with msgids in this set are skipped as duplicates
        prev_locations : set, optional
            Location messages that match values in this set are skipped as duplicates.
        prev_info : set, optional
            Set of info data from previous run that may be relevant to current run.
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
        shape_factor : float, optional
            Controls how close we insist vessels to be along the path between their start
            and the their extrapolated destination if not near there destination. Large
            shape factor means very close.
        buffer_nm : float, optional
            Distances closer than this are considered "very close" and speed is not enforced
            as rigorously.
        transponder_mismatch_weight : float, optional
            Weight to multiply messages by that have a different transponder type than the
            segment we want to match to. Should be between 0 and 1.
        penalty_speed : float, optional
            Speeds (relative to where we expect the boat to be) greater than this are strongly
            discouraged.
        max_open_segments : int, optional
            Maximum number of segments to keep open at one time. This is limited for performance
            reasons.
        very_slow : float, optional
            Speeds at or below this are considered slow enough that we allow courses over 360
            (meaning not-available)

        """
        self.prev_msgids = prev_msgids if prev_msgids else set()
        self.cur_msgids = {}
        self.prev_locations = prev_locations if prev_locations else set()
        self.cur_locations = {}
        self.cur_info = prev_info.copy() if prev_info else {}
        self.max_hours = max_hours
        self.penalty_hours = penalty_hours
        self.hours_exp = hours_exp
        self.max_speed = max_speed
        self.buffer_hours = buffer_hours
        self.lookback = lookback
        self.lookback_factor = lookback_factor
        self.short_seg_threshold = short_seg_threshold
        self.short_seg_exp = short_seg_exp
        self.shape_factor = shape_factor
        self.buffer_nm = buffer_nm
        self.transponder_mismatch_weight = transponder_mismatch_weight
        self.penalty_speed = penalty_speed
        self.max_open_segments = max_open_segments
        self.very_slow = very_slow

        # Exposed via properties
        self._instream = instream

        # Internal objects
        self._segments = {}
        self._ssvid = ssvid
        self._prev_timestamp = None
        self._discrepancy_alpha_0 = self.max_speed / self.penalty_speed

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

    @staticmethod
    def transponder_types(msg):
        return POSITION_TYPES.get(msg.get('type'), set())


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
            seg_id = '{}-{:%Y-%m-%dT%H:%M:%S.%fZ}'.format(msg['ssvid'], ts)
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
             ((speed <= self.very_slow and course > 359.95) or
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
        while len(self._segments) >= self.max_open_segments:
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

    def _compute_expected_position(self, msg, hours):
        epsilon = 1e-3
        x = msg['lon']
        y = msg['lat']
        speed = msg['speed']
        course = msg['course']
        if course > 359.95:
            assert speed <= self.very_slow, (course, speed)
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
            discrepancy2 = dist * self.shape_factor

            # Distance perp to line
            rads21 = math.atan2(nm_per_deg_lat * (y2 - y1), 
                                nm_per_deg_lon * wrap(x2 - x1))
            delta21 = math.radians(90 - msg1['course']) - rads21
            tangential21 = math.cos(delta21) * dist
            if 0 < tangential21 <= msg1['speed'] * hours:
                normal21 = abs(math.sin(delta21)) * dist
            else:
                normal21 = inf
            delta12 = math.radians(90 - msg2['course']) - rads21 
            tangential12 = math.cos(delta12) * dist
            if 0 < tangential12 <= msg2['speed'] * hours:
                normal12 = abs(math.sin(delta12)) * dist
            else:
                normal12 = inf
            discrepancy3 = 0.5 * (normal12 + normal21) * self.shape_factor

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
        transponder_types = set()
        for cnd_msg in segment.get_all_reversed_msgs():
            n -= 1
            if cnd_msg.get('drop'):
                continue
            transponder_types |= self.transponder_types(cnd_msg)
            candidates.append((metric, msgs_to_drop[:], self.msg_diff_stats(cnd_msg, msg)))
            if len(candidates) >= self.lookback or n < 0:
                # This allows looking back 1 message into the previous batch of messages
                break
            msgs_to_drop.append(cnd_msg)
            metric = cnd_msg.get('metric', 0)

        # Consider transponders matched if the transponder shows up in any of lookback items
        transponder_match = bool(transponder_types & self.transponder_types(msg))

        assert len(candidates) > 0

        for lookback, match_info in enumerate(candidates):
            existing_metric, msgs_to_drop, (discrepancy, hours) = match_info
            assert hours >= 0
            if hours > self.max_hours: 
                # Too long has passed, we can't match this segment
                break
            else:
                effective_hours = (math.hypot(hours, self.buffer_hours) / 
                                    (1 + (hours / self.penalty_hours) ** (1 - self.hours_exp)))
                discrepancy = math.hypot(self.buffer_nm, discrepancy) - self.buffer_nm
                max_allowed_discrepancy = effective_hours * self.max_speed
                if discrepancy <= max_allowed_discrepancy:
                    alpha = self._discrepancy_alpha_0 * discrepancy / max_allowed_discrepancy 
                    metric = math.exp(-alpha ** 2) / effective_hours ** 2
                    # Scale the metric using the lookback factor so that it only
                    # matches to points further in the past if they are noticeably better
                    metric = metric * self.lookback_factor ** -lookback 
                    # Down weight cases where transceiver types don't match.
                    if not transponder_match:
                        metric *= self.transponder_mismatch_weight
                    if metric <= existing_metric:
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

    @staticmethod
    def extract_location(msg):
        return (msg.get('lon'),
                msg.get('lat'),
                msg.get('course'),
                msg.get('speed'),
                msg.get('heading'))

    @staticmethod
    def normalize_location(lat, lon, course, speed, heading):
        return (round(lat * 60000),
                round(lon * 60000),
                round(course * 10),
                round(speed * 10),
                None if (heading is None) else round(heading))

    def _prune_info(self, latest_time):
        stale = set()
        last_valid_ts = latest_time - datetime.timedelta(minutes=INFO_PING_INTERVAL_MINS)
        for ts in self.cur_info:
            if ts < last_valid_ts:
                stale.add(ts)
        for ts in stale:
            self.cur_info.pop(ts)

    def store_info(self, msg):
        self._prune_info(msg['timestamp'])
        shipname = msg.get('shipname')
        callsign = msg.get('callsign')
        imo = msg.get('imo')
        if shipname is None and callsign is None and imo is None:
            return
        transponder_type = INFO_TYPES.get(msg.get('type'))
        if not transponder_type:
            return
        receiver_type = msg.get('receiver_type')
        ts = msg['timestamp']
        # Using tzinfo as below is only stricly valid for UTC and naive time due to
        # issues with DST (see http://pytz.sourceforge.net).
        assert ts.tzinfo is None or ts.tzinfo.zone == 'UTC'
        rounded_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute,
                                        tzinfo=ts.tzinfo)
        k2 = (transponder_type, receiver_type)
        for offset in range(-INFO_PING_INTERVAL_MINS, INFO_PING_INTERVAL_MINS + 1):
            k1 = rounded_ts + datetime.timedelta(minutes=offset)
            if k1 not in self.cur_info:
                self.cur_info[k1] = {k2 : ({}, {}, {})}
            elif k2 not in self.cur_info[k1]:
                self.cur_info[k1][k2] = ({}, {}, {})
            shipnames, callsigns, imos = self.cur_info[k1][k2]
            if shipname is not None:
                shipnames[shipname] = shipnames.get(shipname, 0) + 1
            if callsign is not None:
                callsigns[callsign] = callsigns.get(callsign, 0) + 1
            if imos is not None:
                imos[imo] = imos.get(imo, 0) + 1

    def add_info(self, msg):
        ts = msg['timestamp']
        k1 = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute)
        msg['shipnames'] = shipnames = {}
        msg['callsigns'] = callsigns = {}
        msg['imos'] = imos = {}
        def updatesum(orig, new):
            for k, v in new.items():
                orig[k] = orig.get(k, 0) + v
        if k1 in self.cur_info:
            for transponder_type in POSITION_TYPES.get(msg.get('type'), ()):
                receiver_type = msg.get('receiver_type')
                k2 = (transponder_type, receiver_type)
                if k2 in self.cur_info[k1]:
                    names, signs, nums = self.cur_info[k1][k2]
                    updatesum(shipnames, names)
                    updatesum(callsigns, signs)
                    updatesum(imos, nums)

    def process(self):
        for msg in self.instream:
            timestamp = msg.get('timestamp')
            if timestamp is None:
                raise ValueError("Message missing timestamp") 
            if self._prev_timestamp is not None and timestamp < self._prev_timestamp:
                raise ValueError("Input data is unsorted")
            self._prev_timestamp = msg['timestamp']

            msgid = msg.get('msgid')
            if msgid in self.prev_msgids or msgid in self.cur_msgids:
                continue
            self.cur_msgids[msgid] = timestamp

            ssvid = msg.get('ssvid')
            if self.ssvid is None:
                self._ssvid = ssvid
            elif ssvid != self.ssvid:
                logger.warning("Skipping non-matching SSVID %r, expected %r", ssvid, self.ssvid)
                continue


            x, y, course, speed, heading = self.extract_location(msg)

            msg_type = self._message_type(x, y, course, speed)

            if msg_type is BAD_MESSAGE:
                yield self._create_segment(msg, cls=BadSegment)
                logger.debug(("Rejected bad message from ssvid: {ssvid!r} lat: {y!r}  lon: {x!r} "
                              "timestamp: {timestamp!r} course: {course!r} speed: {speed!r}").format(**locals()))
                continue

            if msg_type is INFO_MESSAGE:
                self.store_info(msg)
                yield self._create_segment(msg, cls=InfoSegment)
                logger.debug("Skipping info message form ssvid: %s", msg['ssvid'])
                continue

            assert msg_type is POSITION_MESSAGE

            self.add_info(msg)

            loc = self.normalize_location(x, y, course, speed, heading)
            if speed > 0 and (loc in self.prev_locations or loc in self.cur_locations):
                # Multiple identical locations with non-zero speed almost certainly bogus
                continue
            self.cur_locations[loc] = timestamp

            if len(self._segments) == 0:
                logger.debug("adding new segment because no current segments")
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
                    logger.debug("adding new segment because no match")
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
                    logger.debug("adding new segment because of ambiguity with {} segments".format(len(best_match)))
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

