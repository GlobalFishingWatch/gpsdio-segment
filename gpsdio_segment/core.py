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

from gpsdio_segment.discrepancy import DiscrepancyCalculator
from gpsdio_segment.matcher import IS_NOISE, NO_MATCH, Matcher
from gpsdio_segment.msg_processor import (
    BAD_MESSAGE,
    INFO_ONLY_MESSAGE,
    POSITION_MESSAGE,
    MsgProcessor,
)
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
        self._msg_processor = MsgProcessor(
            self._matcher.very_slow, ssvid, prev_msgids, prev_locations, prev_info
        )

        # Exposed via properties
        self._instream = instream
        self._ssvid = ssvid

        # Internal objects
        self._segments = {}

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
                # TODO: clean up
                if s._msg_processor._prev_timestamp is None or ts > s._prev_timestamp:
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

    @property
    def cur_locations(self):
        # TODO: This is to preserve behavior during refactor, probably can be removed later
        return self._msg_processor.cur_locations

    @property
    def cur_msgids(self):
        # TODO: This is to preserve behavior during refactor, probably can be removed later
        return self._msg_processor.cur_msgids

    @property
    def cur_info(self):
        # TODO: This is to preserve behavior during refactor, probably can be removed later
        return self._msg_processor.info

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
            for x in self._clean(self._segments.pop(stalest_seg_id), ClosedSegment):
                yield x

    def _add_segment(self, msg, why=None):
        if why is not None:
            log(f"adding new segment because {why}")
        for excess_seg in self._remove_excess_segments():
            yield excess_seg
        seg = self._create_segment(msg)
        self._segments[seg.id] = seg

    def _clean(self, segment, cls):
        if segment.has_prev_state:
            new_segment = cls.from_state(segment.prev_state)
        else:
            new_segment = cls(segment.id, segment.ssvid)
        for msg in segment.msgs:
            self._msg_processor.add_info_to_msg(msg)
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

    def _process_ambiguous_match(self, msg, best_match):
        # This message could match multiple segments.
        # So finalize and remove ambiguous segments so we can start fresh
        for match in best_match:
            yield from self._clean(
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

    def _finalize_old_segments(self, msg):
        # Finalize and remove any segments that have not had a positional message in `max_hours`
        for segment in list(self._segments.values()):
            if self.compute_msg_delta_hours(segment.last_msg, msg) > self.max_hours:
                yield from self._clean(
                    self._segments.pop(segment.id), cls=ClosedSegment
                )

    def _process_position_msg(self, msg):

        if len(self._segments) == 0:
            for x in self._add_segment(msg, why="there are no current segments"):
                yield x
        else:
            yield from self._finalize_old_segments(msg)
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
        for msg_type, msg in self._msg_processor(self.instream):

            if msg_type is BAD_MESSAGE:
                yield from self._process_bad_msg(msg)
            elif msg_type is INFO_ONLY_MESSAGE:
                yield from self._process_info_only_msg(msg)
            elif msg_type is POSITION_MESSAGE:
                yield from self._process_position_msg(msg)
            else:
                raise ValueError(f"unknown msg type {msg_type}")

        # Yield all pending segments now that processing is completed
        for series, segment in list(self._segments.items()):
            for x in self._clean(self._segments.pop(segment.id), Segment):
                yield x

    def __iter__(self):
        return self.process()
