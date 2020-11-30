"""
Some ships using AIS are using the same ship identifiers, MMSI. This
means that it is not possible to directly distinguish points for one
ship from points from points for the other ship. In addition timing 
noise in satellite clocks and bit noise in the received signals can
result in points or whole portions of tracks displaced in either 
time or space.

To combat this we look at the projected position based on the reported
course and speed and consider whether it is plausible for the message
to correspond to one of the existing tracks or to start a new track.

The segmenter maintains a set of "open tracks". For each open tracks
it keeps the last point (latitude, longitude, course, speed and timestamp). 
For each new point, it considers which of the open tracks to add 
it to, or to create a new track, and also if it should close any 
open tracks.

The details of how this is performed is best explained by examining
the logic in the function `_compute_best`.
"""


from __future__ import division, print_function
import logging
import datetime
import math

from gpsdio_segment.discrepancy import DiscrepancyCalculator
from gpsdio_segment.segment import Segment, BadSegment, ClosedSegment
from gpsdio_segment.segment import DiscardedSegment, InfoSegment


logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

log = logger.info

inf = float("inf")

POSITION_TYPES = {
    'AIS.1' : {'AIS-A'}, 
    'AIS.2' : {'AIS-A'},
    'AIS.3' : {'AIS-A'},
    'AIS.18' : {'AIS-B'}, 
    'AIS.19' : {'AIS-B'},
    'AIS.27' : {'AIS-A', 'AIS-B'},
    'VMS' : {'VMS'}
    } 

INFO_TYPES = {
    'AIS.5' : 'AIS-A',
    'AIS.19' : 'AIS-B', 
    'AIS.24' : 'AIS-B',
    'VMS' : 'VMS'
    }


INFO_PING_INTERVAL_MINS = 15

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

NO_MATCH = object()
IS_NOISE = object()


class Segmentizer(DiscrepancyCalculator):

    """
    Group positional messages into related segments based on speed and distance.
    """

    # These default values are a result of generating segments 
    # and assembling them into vessel tracks for a large number
    # of vessels.  There are enough knobs here that these are
    # likely still not optimal and more experimentation would likely
    # be helpful.
    max_hours = 12
    penalty_hours = 4
    hours_exp = 0.5
    buffer_hours = 0.25
    lookback = 5
    lookback_factor = 2
    max_knots = 25
    ambiguity_factor = 10.0
    short_seg_threshold = 10
    transponder_mismatch_weight = 0.1
    penalty_speed = 5.0
    max_open_segments = 20
    min_type_27_hours = 1.0


    def __init__(self, instream, 
                 ssvid=None, 
                 prev_msgids=None, 
                 prev_locations=None,
                 prev_info=None,
                 **kwargs
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
        max_knots : int, optional
            Maximum speed allowed between points in nautical miles.
        lookback : int, optional
            Number of points to look backwards when matching segments.
        lookback_factor : float, optional:
            How much better a match to a previous point has to be in order to use lookback.
        short_seg_threshold : int, optional
            Segments shorter than this are penalized when computing metrics
        shape_factor : float, optional
            Controls how close we insist vessels to be along the path between their start
            and the their extrapolated destination if not near their destination. Large
            shape factor means very close.
        transponder_mismatch_weight : float, optional
            Weight to multiply messages by that have a different transponder type than the
            segment we want to match to. Should be between 0 and 1.
        penalty_speed : float, optional
            Speeds (relative to where we expect the boat to be) greater than this are strongly
            discouraged.
        max_open_segments : int, optional
            Maximum number of segments to keep open at one time. This is limited for performance
            reasons.
        min_type_27_hours : float, optional
            If a type 27 message occurs closer than this time to a non-type 27 message, it is 
            dropped. This is because the low resolution type 27 messages can result in strange
            tracks, particularly when a vessel is in port.

        """
        self.prev_msgids = prev_msgids if prev_msgids else set()
        self.cur_msgids = {}
        self.prev_locations = prev_locations if prev_locations else set()
        self.cur_locations = {}
        self.cur_info = prev_info.copy() if prev_info else {}

        for k in ['max_hours', 'penalty_hours', 'hours_exp', 'buffer_hours',
                  'max_knots', 'lookback', 'lookback_factor', 
                  'short_seg_threshold', 'shape_factor',
                  'transponder_mismatch_weight', 'penalty_speed',
                  'max_open_segments']:
            self._update(k, kwargs)

        # Exposed via properties
        self._instream = instream

        # Internal objects
        self._segments = {}
        self._used_seg_ids = set()
        self._ssvid = ssvid
        self._prev_timestamp = None
        self._discrepancy_alpha_0 = self.max_knots / self.penalty_speed

    def __repr__(self):
        return "<{cname}() max_knots={mspeed} max_hours={mhours} at {id_}>".format(
            cname=self.__class__.__name__, mspeed=self.max_knots,
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
            s._used_seg_ids.add(seg.id)
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
            if seg_id not in self._used_seg_ids:
                self._used_seg_ids.add(seg_id)
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
            log('Removing stale segment {}'.format(stalest_seg_id))
            for x in self.clean(self._segments.pop(stalest_seg_id), ClosedSegment):
                yield x

    def _add_segment(self, msg):
        for excess_seg in self._remove_excess_segments():
            yield excess_seg
        seg = self._create_segment(msg)
        self._segments[seg.id] = seg


    def _segment_match(self, segment, msg):
        match = {'seg_id': segment.id,
                 'msgs_to_drop' : [],
                 'hours' : None,
                 'metric' : None}

        # Get the stats for the last `lookback` positional messages
        candidates = []

        n = len(segment)
        msgs_to_drop = []
        metric = 0
        transponder_types = set()
        for prev_msg in segment.get_all_reversed_msgs():
            n -= 1
            if prev_msg.get('drop'):
                continue
            transponder_types |= self.transponder_types(prev_msg)
            hours = self.compute_msg_delta_hours(prev_msg, msg)
            penalized_hours = hours / (1 + (hours / self.penalty_hours) ** (1 - self.hours_exp))
            discrepancy = self.compute_discrepancy(prev_msg, msg, penalized_hours)
            candidates.append((metric, msgs_to_drop[:], discrepancy, hours, penalized_hours))
            if len(candidates) >= self.lookback or n < 0:
                # This allows looking back 1 message into the previous batch of messages
                break
            msgs_to_drop.append(prev_msg)
            metric = prev_msg.get('metric', 0)

        # Consider transponders matched if the transponder shows up in any of lookback items
        transponder_match = bool(transponder_types & self.transponder_types(msg))

        assert len(candidates) > 0

        best_metric_lb = 0
        for lookback, match_info in enumerate(candidates):
            existing_metric, msgs_to_drop, discrepancy, hours, penalized_hours = match_info
            assert hours >= 0
            if hours > self.max_hours: 
                log("can't match due to max_hours")
                # Too long has passed, we can't match this segment
                break
            else:
                padded_hours = math.hypot(hours, self.buffer_hours)
                max_allowed_discrepancy = padded_hours * self.max_knots
                if discrepancy <= max_allowed_discrepancy:
                    alpha = self._discrepancy_alpha_0 * discrepancy / max_allowed_discrepancy 
                    metric = math.exp(-alpha ** 2) / padded_hours #** 2
                    # Down weight cases where transceiver types don't match.
                    if not transponder_match:
                        metric *= self.transponder_mismatch_weight
                    # For lookback use the weight reduced by the lookback factor,
                    # But don't store this weight, use base metric instead.
                    metric_lb = metric / max(1, lookback * self.lookback_factor)
                    # Scale the existing metric using the lookback factor so that we only
                    # matches to points further in the past if they are noticeably better
                    if metric_lb <= existing_metric:
                        log("can't make metric worse: %s vs %s (%s) at lb %s", 
                            metric_lb, existing_metric, metric, lookback)
                        # Don't make existing segment worse
                        continue
                    if metric_lb > best_metric_lb:
                        log('updating metric %s (%s)', metric_lb, metric)
                        best_metric_lb = metric_lb
                        match['metric'] = metric
                        match['hours'] = hours
                        match['msgs_to_drop'] = msgs_to_drop
                else:
                    log("can't match due to discrepancy: %s / %s = %s", 
                            discrepancy, padded_hours, discrepancy / padded_hours)


        return match

    def _compute_best(self, msg):
        # figure out which segment is the best match for the given message

        segs = list(self._segments.values())
        best_match = NO_MATCH

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
            alphas = [s.msg_count / self.short_seg_threshold for s in valid_segs]
            metric_match_pairs = [(m['metric'] * a / math.sqrt(1 + a**2), m) 
                                    for (m, a) in zip(matches, alphas)]
            metric_match_pairs.sort(key=lambda x: x[0], reverse=True)
            # Check if best match is close enough to an existing match to be ambiguous.
            best_metric, best_match = metric_match_pairs[0]
            close_matches = [best_match]
            for metric, match in metric_match_pairs[1:]:
                if metric * self.ambiguity_factor >= best_metric:
                    close_matches.append(match)
            if len(close_matches) > 1:
                log('Ambiguous messages for id {}'.format(msg['ssvid']))
                best_match = close_matches

        if best_match is not NO_MATCH:
            hours = (min([x['hours'] for x in best_match]) 
                        if isinstance(best_match, list) else best_match['hours'])
            if  msg.get('type') == 'AIS.27' and hours < self.min_type_27_hours:
                # Type 27 messages have low resolution, so only include them where there likely to 
                # not mess up the tracks
                return IS_NOISE

        return best_match

    def __iter__(self):
        return self.process()

    def clean(self, segment, cls):
        if segment.has_prev_state:
            new_segment = cls.from_state(segment.prev_state)
        else:
            new_segment = cls(segment.id, segment.ssvid)
        for msg in segment.msgs:
            self.add_info(msg)
            msg.pop('metric', None)
            if msg.pop('drop', False):
                log(("Dropping message from ssvid: {ssvid!r} timestamp: {timestamp!r}").format(
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
                None if course is None else round(course * 10),
                round(speed * 10),
                None if (heading is None or math.isnan(heading)) else round(heading))

    @classmethod
    def store_info(cls, info, msg):
        shipname = msg.get('shipname')
        callsign = msg.get('callsign')
        imo = msg.get('imo')
        destination = msg.get('destination')
        length = msg.get('length')
        width = msg.get('width')
        n_shipname = msg.get('n_shipname')
        n_callsign = msg.get('n_callsign')
        n_imo = msg.get('n_imo')
        if shipname is None and callsign is None and imo is None and destination is None:
            return
        transponder_type = INFO_TYPES.get(msg.get('type'))
        if not transponder_type:
            return
        receiver_type = msg.get('receiver_type')
        source = msg.get('source')
        ts = msg['timestamp']
        # Using tzinfo as below is only stricly valid for UTC and naive time due to
        # issues with DST (see http://pytz.sourceforge.net).
        assert ts.tzinfo.zone == 'UTC'
        rounded_ts = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute,
                                        tzinfo=ts.tzinfo)
        k2 = (transponder_type, receiver_type, source)
        for offset in range(-INFO_PING_INTERVAL_MINS, INFO_PING_INTERVAL_MINS + 1):
            k1 = rounded_ts + datetime.timedelta(minutes=offset)
            if k1 not in info:
                info[k1] = {k2 : ({}, {}, {}, {}, {}, {}, {}, {}, {})}
            elif k2 not in info[k1]:
                info[k1][k2] = ({}, {}, {}, {}, {}, {}, {}, {}, {})
            (shipnames, callsigns, imos, destinations, lengths, widths, 
                                    n_shipnames, n_callsigns, n_imos) = info[k1][k2]
            if shipname is not None:
                shipnames[shipname] = shipnames.get(shipname, 0) + 1
                n_shipnames[n_shipname] = n_shipnames.get(n_shipname, 0) + 1
            if callsign is not None:
                callsigns[callsign] = callsigns.get(callsign, 0) + 1
                n_callsigns[n_callsign] = callsigns.get(n_callsign, 0) + 1
            if imo is not None:
                imos[imo] = imos.get(imo, 0) + 1
                n_imos[n_imo] = imos.get(n_imo, 0) + 1
            if destination is not None:
                destinations[destination] = destinations.get(destination, 0) + 1
            if length is not None:
                lengths[length] = lengths.get(length, 0) + 1
            if width is not None:
                widths[width] = lengths.get(width, 0) + 1

    def add_info(self, msg):
        ts = msg['timestamp']
        # Using tzinfo as below is only stricly valid for UTC and naive time due to
        # issues with DST (see http://pytz.sourceforge.net).
        assert ts.tzinfo.zone == 'UTC'
        k1 = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute,
                                        tzinfo=ts.tzinfo)
        msg['shipnames'] = shipnames = {}
        msg['callsigns'] = callsigns = {}
        msg['imos'] = imos = {}
        msg['destinations'] = destinations = {}
        msg['lengths'] = lengths = {}
        msg['widths'] = widths = {}
        msg['n_shipnames'] = n_shipnames = {}
        msg['n_callsigns'] = n_callsigns = {}
        msg['n_imos'] = n_imos = {}
        def updatesum(orig, new):
            for k, v in new.items():
                orig[k] = orig.get(k, 0) + v
        if k1 in self.cur_info:
            for transponder_type in POSITION_TYPES.get(msg.get('type'), ()):
                receiver_type = msg.get('receiver_type')
                source = msg.get('source')
                k2 = (transponder_type, receiver_type, source)
                if k2 in self.cur_info[k1]:
                    (names, signs, nums, dests, lens, wdths, 
                                    n_names, n_signs, n_nums) = self.cur_info[k1][k2]
                    updatesum(shipnames, names)
                    updatesum(callsigns, signs)
                    updatesum(imos, nums)
                    updatesum(destinations, dests)
                    updatesum(lengths, lens)
                    updatesum(widths, wdths)
                    updatesum(n_shipnames, n_names)
                    updatesum(n_callsigns, n_signs)
                    updatesum(n_imos, n_nums)


    def process(self):
        for msg in self.instream:
            if 'type' not in msg:
                raise ValueError("`msg` is missing required field `type`")

            # Add empty info fields so they are always preset
            msg['shipnames'] = {}
            msg['callsigns'] = {}
            msg['imos'] = {}

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

            # Type 19 messages, although rare, have both position and info, so 
            # store any info in POSITION or INFO messages
            self.store_info(self.cur_info, msg)

            if msg_type is INFO_MESSAGE:
                yield self._create_segment(msg, cls=InfoSegment)
                logger.debug("Skipping info message form ssvid: %s", msg['ssvid'])
                continue

            assert msg_type is POSITION_MESSAGE

            loc = self.normalize_location(x, y, course, speed, heading)
            if speed > 0 and (loc in self.prev_locations or loc in self.cur_locations):
                # Multiple identical locations with non-zero speed almost certainly bogus
                continue
            self.cur_locations[loc] = timestamp

            if len(self._segments) == 0:
                log("adding new segment because no current segments")
                for x in self._add_segment(msg):
                    yield x
            else:
                # Finalize and remove any segments that have not had a positional message in `max_hours`
                for segment in list(self._segments.values()):
                    if (self.compute_msg_delta_hours(segment.last_msg, msg) > self.max_hours):
                            for x in self.clean(self._segments.pop(segment.id), cls=ClosedSegment):
                                yield x

                best_match = self._compute_best(msg)
                if best_match is NO_MATCH:
                    log("adding new segment because no match")
                    for x in self._add_segment(msg):
                        yield x
                elif best_match is IS_NOISE:
                    yield self._create_segment(msg, cls=BadSegment)
                elif isinstance(best_match, list):
                    # This message could match multiple segments. 
                    # So finalize and remove ambiguous segments so we can start fresh
                    # TODO: once we are fully py3, this and similar can be cleaned up using `yield from`
                    for match in best_match:
                        for x in self.clean(self._segments.pop(match['seg_id']), cls=ClosedSegment):
                            yield x
                    # Then add as new segment.
                    log("adding new segment because of ambiguity with {} segments".format(len(best_match)))
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

