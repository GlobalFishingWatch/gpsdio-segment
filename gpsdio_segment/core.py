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
the logic in the function `matcher.compute_best`.
"""


from __future__ import division, print_function

import datetime
import logging
import math

from gpsdio_segment.discrepancy import DiscrepancyCalculator
from gpsdio_segment.matcher import Matcher, NO_MATCH, IS_NOISE, POSITION_TYPES
from gpsdio_segment.segment import (
    BadSegment,
    ClosedSegment,
    DiscardedSegment,
    InfoSegment,
    Segment,
)

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

log = logger.info

inf = float("inf")


INFO_TYPES = {"AIS.5": "AIS-A", "AIS.19": "AIS-B", "AIS.24": "AIS-B", "VMS": "VMS"}


INFO_PING_INTERVAL_MINS = 15

# The values 52 and 102.3 are both almost always noise, and don't
# reflect the vessel's actual speed. They need to be commented out.
# The value 102.3 is reserved for "bad value." It looks like 51.2
# is also almost always noise. The value 63 means unavailable for
# type 27 messages so we exclude that as well. Because the values are floats,
# and not always exactly 102.3 or 51.2, we give a range.
REPORTED_SPEED_EXCLUSION_RANGES = [(51.15, 51.25), (62.95, 63.05), (102.25, 102.35)]
SAFE_SPEED = min([x for (x, y) in REPORTED_SPEED_EXCLUSION_RANGES])


POSITION_MESSAGE = object()
INFO_ONLY_MESSAGE = object()
BAD_MESSAGE = object()


class Segmentizer(DiscrepancyCalculator):

    """
    Group positional messages into related segments based on speed and distance.
    """

    def __init__(
        self,
        instream,
        ssvid=None,
        prev_msgids=None,
        prev_locations=None,
        prev_info=None,
        max_hours=Matcher.max_hours,
        max_open_segments=20,
        **kwargs,
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
        max_open_segments : int, optional
            Maximum number of segments to keep open at one time. This is limited for performance
            reasons.

        """
        self.max_hours = max_hours
        self.max_open_segments = max_open_segments
        self._matcher = Matcher(max_hours=max_hours, **kwargs)
        self.prev_msgids = prev_msgids if prev_msgids else set()
        self.cur_msgids = {}
        self.prev_locations = prev_locations if prev_locations else set()
        self.cur_locations = {}
        self.cur_info = prev_info.copy() if prev_info else {}

        # Exposed via properties
        self._instream = instream

        # Internal objects
        self._segments = {}
        self._ssvid = ssvid
        self._prev_timestamp = None

    def __repr__(self):
        return "<{cname}() max_knots={mspeed} max_hours={mhours} at {id_}>".format(
            cname=self.__class__.__name__,
            mspeed=self.max_knots,
            mhours=self.max_hours,
            id_=hash(self),
        )

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
                if state["closed"]:
                    continue
            else:
                if state.closed:
                    continue
            seg = Segment.from_state(state)
            s._segments[seg.id] = seg
            if seg.last_msg:
                ts = seg.last_msg["timestamp"]
                if s._prev_timestamp is None or ts > s._prev_timestamp:
                    s._prev_timestamp = ts
        return s

    @property
    def instream(self):
        return self._instream

    @property
    def ssvid(self):
        return self._ssvid

    @property
    def max_knots(self):
        # TODO: This is to preserve behavior during refactor, probably can be removed later
        return self._matcher.max_knots

    def _segment_unique_id(self, msg):
        """
        Generate a unique ID for a segment from a message, ideally its first.

        Returns
        -------
        str
        """

        ts = msg["timestamp"]
        while True:
            seg_id = "{}-{:%Y-%m-%dT%H:%M:%S.%fZ}".format(msg["ssvid"], ts)
            if seg_id not in self._segments:
                return seg_id
            ts += datetime.timedelta(milliseconds=1)

    def _message_type(self, msg):
        x, y, course, speed, heading = self.extract_location(msg)

        def is_null(v):
            return (v is None) or math.isnan(v)

        if is_null(x) and is_null(y) and is_null(course) and is_null(speed):
            return INFO_ONLY_MESSAGE
        if (
            x is not None
            and y is not None
            and speed is not None
            and course is not None
            and -180.0 <= x <= 180.0
            and -90.0 <= y <= 90.0
            and course is not None
            and speed is not None
            and (
                (speed <= self.very_slow and course > 359.95) or 0.0 <= course <= 359.95
            )
            and (  # 360 is invalid unless speed is very low.
                speed < SAFE_SPEED
                or not any(l < speed < h for (l, h) in REPORTED_SPEED_EXCLUSION_RANGES)
            )
        ):
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
            segs.sort(key=lambda x: x[1].last_msg["timestamp"])
            stalest_seg_id, _ = segs[0]
            log("Removing stale segment {}".format(stalest_seg_id))
            for x in self.clean(self._segments.pop(stalest_seg_id), ClosedSegment):
                yield x

    def _add_segment(self, msg, why=None):
        if why is not None:
            log(f"adding new segment because {why}")
        for excess_seg in self._remove_excess_segments():
            yield excess_seg
        seg = self._create_segment(msg)
        self._segments[seg.id] = seg

    def __iter__(self):
        return self.process()

    def clean(self, segment, cls):
        if segment.has_prev_state:
            new_segment = cls.from_state(segment.prev_state)
        else:
            new_segment = cls(segment.id, segment.ssvid)
        for msg in segment.msgs:
            self.add_info(msg)
            msg.pop("metric", None)
            if msg.pop("drop", False):
                log(
                    (
                        "Dropping message from ssvid: {ssvid!r} timestamp: {timestamp!r}"
                    ).format(**msg)
                )
                yield self._create_segment(msg, cls=DiscardedSegment)
                continue
            else:
                new_segment.add_msg(msg)
        yield new_segment

    @staticmethod
    def extract_location(msg):
        return (
            msg.get("lon"),
            msg.get("lat"),
            msg.get("course"),
            msg.get("speed"),
            msg.get("heading"),
        )

    @staticmethod
    def normalize_location(lat, lon, course, speed, heading):
        # TODO: this can probably be removed since @andres is cleaning this up.
        return (
            round(lat * 60000),
            round(lon * 60000),
            None if course is None else round(course * 10),
            round(speed * 10),
            None if (heading is None or math.isnan(heading)) else round(heading),
        )

    @classmethod
    def _store_info(cls, info, msg):
        shipname = msg.get("shipname")
        callsign = msg.get("callsign")
        imo = msg.get("imo")
        n_shipname = msg.get("n_shipname")
        n_callsign = msg.get("n_callsign")
        n_imo = msg.get("n_imo")
        if shipname is None and callsign is None and imo is None:
            return
        transponder_type = INFO_TYPES.get(msg.get("type"))
        if not transponder_type:
            return
        receiver_type = msg.get("receiver_type")
        source = msg.get("source")
        ts = msg["timestamp"]
        # Using tzinfo as below is only stricly valid for UTC and naive time due to
        # issues with DST (see http://pytz.sourceforge.net).
        assert ts.tzinfo.zone == "UTC"
        rounded_ts = datetime.datetime(
            ts.year, ts.month, ts.day, ts.hour, ts.minute, tzinfo=ts.tzinfo
        )
        k2 = (transponder_type, receiver_type, source)
        for offset in range(-INFO_PING_INTERVAL_MINS, INFO_PING_INTERVAL_MINS + 1):
            k1 = rounded_ts + datetime.timedelta(minutes=offset)
            if k1 not in info:
                info[k1] = {k2: ({}, {}, {}, {}, {}, {})}
            elif k2 not in info[k1]:
                info[k1][k2] = ({}, {}, {}, {}, {}, {})
            shipnames, callsigns, imos, n_shipnames, n_callsigns, n_imos = info[k1][k2]
            if shipname is not None:
                shipnames[shipname] = shipnames.get(shipname, 0) + 1
                n_shipnames[n_shipname] = n_shipnames.get(n_shipname, 0) + 1
            if callsign is not None:
                callsigns[callsign] = callsigns.get(callsign, 0) + 1
                n_callsigns[n_callsign] = callsigns.get(n_callsign, 0) + 1
            if imo is not None:
                imos[imo] = imos.get(imo, 0) + 1
                n_imos[n_imo] = imos.get(n_imo, 0) + 1

    def add_info(self, msg):
        ts = msg["timestamp"]
        # Using tzinfo as below is only stricly valid for UTC and naive time due to
        # issues with DST (see http://pytz.sourceforge.net).
        assert ts.tzinfo.zone == "UTC"
        k1 = datetime.datetime(
            ts.year, ts.month, ts.day, ts.hour, ts.minute, tzinfo=ts.tzinfo
        )
        msg["shipnames"] = shipnames = {}
        msg["callsigns"] = callsigns = {}
        msg["imos"] = imos = {}
        msg["n_shipnames"] = n_shipnames = {}
        msg["n_callsigns"] = n_callsigns = {}
        msg["n_imos"] = n_imos = {}

        def updatesum(orig, new):
            for k, v in new.items():
                orig[k] = orig.get(k, 0) + v

        if k1 in self.cur_info:
            for transponder_type in POSITION_TYPES.get(msg.get("type"), ()):
                receiver_type = msg.get("receiver_type")
                source = msg.get("source")
                k2 = (transponder_type, receiver_type, source)
                if k2 in self.cur_info[k1]:
                    names, signs, nums, n_names, n_signs, n_nums = self.cur_info[k1][k2]
                    updatesum(shipnames, names)
                    updatesum(callsigns, signs)
                    updatesum(imos, nums)
                    updatesum(n_shipnames, n_names)
                    updatesum(n_callsigns, n_signs)
                    updatesum(n_imos, n_nums)

    def _checked_stream(self, stream):
        for msg in stream:
            if "type" not in msg:
                raise ValueError("`msg` is missing required field `type`")

            # Add empty info fields so they are always preset
            msg["shipnames"] = {}
            msg["callsigns"] = {}
            msg["imos"] = {}

            # Check that message is valid (in order, correct ssvid, not already seen) REFACTOR
            timestamp = msg.get("timestamp")
            if timestamp is None:
                raise ValueError("Message missing timestamp")
            if self._prev_timestamp is not None and timestamp < self._prev_timestamp:
                raise ValueError("Input data is unsorted")
            # TODO: shouldn't this come after the duplicate message check
            self._prev_timestamp = msg["timestamp"]

            msgid = msg.get("msgid")
            if msgid in self.prev_msgids or msgid in self.cur_msgids:
                continue
            self.cur_msgids[msgid] = timestamp

            ssvid = msg.get("ssvid")
            if self.ssvid is None:
                self._ssvid = ssvid
            elif ssvid != self.ssvid:
                logger.warning(
                    "Skipping non-matching SSVID %r, expected %r", ssvid, self.ssvid
                )
                continue

            yield msg

    def _process_bad_msg(self, msg):
        yield self._create_segment(msg, cls=BadSegment)
        logger.debug(
            (
                f"Rejected bad message from ssvid: {msg['ssvid']!r} lat: {msg['lat']!r}  lon: {msg['lon']!r} "
                f"timestamp: {msg['timestamp']!r} course: {msg['course']!r} speed: {msg['speed']!r}"
            )
        )

    def _process_info_only_msg(self, msg):
        yield self._create_segment(msg, cls=InfoSegment)
        logger.debug("Skipping info message from ssvid: %s", msg["ssvid"])

    def _already_seen_loc(self, loc):
        # Multiple identical locations with non-zero speed are almost certainly bogus
        x, y, course, speed, heading = loc
        return speed > 0 and (loc in self.prev_locations or loc in self.cur_locations)

    def _process_ambiguous_match(self, msg, best_match):
        # This message could match multiple segments.
        # So finalize and remove ambiguous segments so we can start fresh
        for match in best_match:
            yield from self.clean(
                self._segments.pop(match["seg_id"]), cls=ClosedSegment
            )
        # Then add as new segment.
        log(
            "adding new segment because of ambiguity with {} segments".format(
                len(best_match)
            )
        )
        yield from self._add_segment(msg)

    def _process_normal_match(self, msg, best_match):
        id_ = best_match["seg_id"]
        for msg_to_drop in best_match["msgs_to_drop"]:
            msg_to_drop["drop"] = True
        msg["metric"] = best_match["metric"]
        self._segments[id_].add_msg(msg)
        return
        # Force this to be an iterator so it matches _process_ambiguous_match
        yield None

    def _finalize_old_msgs(self, msg):
        # Finalize and remove any segments that have not had a positional message in `max_hours`
        for segment in list(self._segments.values()):
            if self.compute_msg_delta_hours(segment.last_msg, msg) > self.max_hours:
                yield from self.clean(self._segments.pop(segment.id), cls=ClosedSegment)

    def _process_position_msg(self, msg):
        timestamp = msg.get("timestamp")
        x, y, course, speed, heading = self.extract_location(msg)
        loc = self.normalize_location(x, y, course, speed, heading)

        if self._already_seen_loc(loc):
            return

        self.cur_locations[loc] = timestamp
        if len(self._segments) == 0:
            for x in self._add_segment(msg, why="there are no current segments"):
                yield x
        else:
            yield from self._finalize_old_msgs(msg)
            best_match = self._matcher.compute_best_match(msg, self._segments)

            if best_match is NO_MATCH:
                yield from self._add_segment(msg, why="no match")
            elif best_match is IS_NOISE:
                yield self._create_segment(msg, cls=BadSegment)
            elif isinstance(best_match, list):
                yield from self._process_ambiguous_match(msg, best_match)
            else:
                yield from self._process_normal_match(msg, best_match)

    def process(self):
        for msg in self._checked_stream(self.instream):

            msg_type = self._message_type(msg)

            if msg_type is BAD_MESSAGE:
                yield from self._process_bad_msg(msg)
            elif msg_type is INFO_ONLY_MESSAGE:
                self._store_info(self.cur_info, msg)
                yield from self._process_info_only_msg(msg)
            elif msg_type is POSITION_MESSAGE:
                # Type 19 messages, although rare, have both position and info, so
                # store any info found in POSITION messages.
                self._store_info(self.cur_info, msg)
                yield from self._process_position_msg(msg)
            else:
                raise ValueError(f"unknown msg type {msg_type}")

        # Yield all pending segments now that processing is completed
        for series, segment in list(self._segments.items()):
            for x in self.clean(self._segments.pop(segment.id), Segment):
                yield x
