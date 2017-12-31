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

from gpsdio.schema import datetime2str

import pyproj


from gpsdio_segment.segment import Segment, BadSegment
from gpsdio_segment.segment import Segment, NoiseSegment
from gpsdio_segment.state import SegmentState

logger = logging.getLogger(__file__)
# logger.setLevel(logging.DEBUG)


# See Segmentizer() for more info
DEFAULT_MAX_HOURS = 24  # hours
DEFAULT_MAX_SPEED = 25  # knots
DEFAULT_NOISE_DIST = 100  # nautical miles
INFINITE_SPEED = 1000000
REPORTED_SPEED_MULTIPLIER = 1.1 # multiply this by reported speed in max speed calculation
MAX_SPEED_MULTIPLIER = 1600     # magic number used to compute max_speed_at_distance
MAX_SPEED_EXPONENT = 2.0     # magic number used to compute max_speed_at_distance

# The values 52 and 102.3 are both almost always noise, and don't
# reflect the vessel's actual speed. They need to be commented out.
# The value 102.3 is reserved for "bad value." It looks like 51.2
# is also almost always noise. Because the values are floats,
# and not always exactly 102.3 or 51.2, we give a range.
REPORTED_SPEED_EXCLUSION_RANGES = [(51.0, 51.3),(102.2,103.0)]


class Segmentizer(object):

    """
    Group positional messages into related segments based on speed and distance.
    """

    def __init__(self, instream, mmsi=None, max_hours=DEFAULT_MAX_HOURS,
                 max_speed=DEFAULT_MAX_SPEED, noise_dist=DEFAULT_NOISE_DIST,
                 reported_speed_multiplier=REPORTED_SPEED_MULTIPLIER,
                 max_speed_multiplier = MAX_SPEED_MULTIPLIER,
                 max_speed_exponent=MAX_SPEED_EXPONENT,
                 collect_match_stats=False
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
        mmsi : int, optional
            MMSI to pull out of the stream and process.  If not given the first
            valid MMSI is used.  All messages with a different MMSI are thrown
            away.
        max_hours : int, optional
            Maximum number of hours to allow between points.
        max_speed : int, optional
            Maximum speed allowed between points in nautical miles.
        noise_dist : float, optional
            If a point is within this distance (nautical miles) to an existing segment
            but does not match with any segment, then consider it to be noise and
            emit it in a singleton segment. Set to 0 to disable this. Default: 0
        reported_speed_multiplier: float
            multiplier applied to reported speed to determine the max allowable speed
        max_speed_multiplier: float
            multiplier used to compute the max allowable speed
        max_speed_exponent: float
            exponent used to compute max allowable speed (see usage in _segment_match_metric())
        """

        self.max_hours = max_hours
        self.max_speed = max_speed
        self.noise_dist = noise_dist
        self.reported_speed_multiplier = reported_speed_multiplier
        self.max_speed_multiplier = max_speed_multiplier
        self.max_speed_exponent = max_speed_exponent
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
        return "<{cname}() max_speed={mspeed} max_hours={mhours} noise_dist={ndist} at {id_}>".format(
            cname=self.__class__.__name__, mspeed=self.max_speed,
            mhours=self.max_hours, ndist=self.noise_dist, id_=hash(self))

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

    def _validate_position(self, x, y):
        return x is None or y is None or (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0 )

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

    def reported_speed(self, msg):
        s = msg.get('speed', 0) or 0
        for r in REPORTED_SPEED_EXCLUSION_RANGES:
            if r[0] < s < r[1]:
                s = 0
        return s

    def msg_diff_stats(self, msg1, msg2):

        """
        Compute the stats required to determine if two points are continuous.  Input
        messages must have a `lat`, `lon`, and `timestamp`, that are not `None` and
        `timestamp` must be an instance of `datetime.datetime()`.

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

        distance = self._geod.inv(x1, y1, x2, y2)[2] / 1850     # 1850 meters = 1 nautical mile
        timedelta = self.timedelta(msg1, msg2)
        reported_speed = max(self.reported_speed(msg1), self.reported_speed(msg2))

        try:
            speed = (distance / timedelta)
        except ZeroDivisionError:
            speed = INFINITE_SPEED

        return {
            'distance': distance,
            'timedelta': timedelta,
            'speed': speed,
            'reported_speed': reported_speed,
            'noise_factor': self.noise_dist / distance if distance > 0 else 0
        }


    def _segment_match(self, segment, msg):
        match = {'seg_id': segment.id,
                 'noise_factor': 0,
                 'metric': None}

        if not segment.last_time_posit_msg:
            match['metric'] = self.max_hours * self.max_speed
            return match

        match.update(self.msg_diff_stats(msg, segment.last_time_posit_msg))

        seg_duration = max(1.0, segment.total_seconds) / 3600

        if match['timedelta'] > self.max_hours:
            match['metric'] = None
        elif match['timedelta'] == 0:
            # only keep identical timestamps if the distance is small
            # allow for the distance you can go at max speed for one minute
            if match['distance'] < (self.max_speed / 60):  # max_speed is nautical miles per hour, so divide by 60 for minutes
                match['metric'] = match['distance'] / seg_duration
        elif match['distance'] == 0:
            match['metric'] = match['timedelta'] / seg_duration
        else:
            # allow a higher max computed speed for vessels that report a high speed
            max_speed_at_inf = max(self.max_speed, match['reported_speed'] * self.reported_speed_multiplier)
            max_speed_at_distance = max_speed_at_inf * \
                                    (1 + self.max_speed_multiplier / (match['distance'])**self.max_speed_exponent)
            # This previously gave an unrealistic speed. This new version, with max speed 
            # of 30, and thus max_speed_at_inf of 60, allows a vessel to travel 1nm 20 seconds,
            # 1.5 nautical miles in a minute. After 20 minutes, the allowed speed drops
            # to about 30 knots. In other words, below gaps between points of under 
            # 20 minutes, faster speeds are allowed. 
            if match['speed'] <= max_speed_at_distance:
                match['metric'] = match['timedelta'] / seg_duration

        return match

    def _compute_best(self, msg):
        # figure out which segment is the best match for the given message

        segs = self._segments.values()
        best_metric = None
        max_noise_factor = 0
        matches = []

        if len(segs) == 1:
            # This is the most common case, so make it optimal
            # and avoid all the messing around with lists in the num_segs > 1 case

            match = self._segment_match(segs[0], msg)
            matches = [match]
            max_noise_factor = match['noise_factor']

            if match['metric'] is not None:
                best_metric = match

        elif len(segs) > 1:
            # get match metrics for all candidate segments
            matches = [self._segment_match(seg, msg) for seg in segs]

            # max noise distance includes all segments, even if they are not match candidates
            max_noise_factor = max(match['noise_factor'] for match in matches)

            # If metric is none, then the segment is not a match candidate
            metrics = list((match['metric'], match) for match in matches if match['metric'] is not None)

            if metrics:
                # find the smallest metric value
                best_metric = min(metrics, key=lambda x: x[0])[1]

        if self.collect_match_stats:
            msg['segment_matches'] = matches

        return (best_metric, max_noise_factor)

    def __iter__(self):
        return self.process()

    def process(self):
        for idx, msg in enumerate(self.instream):
            mmsi = msg.get('mmsi')
            y = msg.get('lat')
            x = msg.get('lon')
            timestamp = msg.get('timestamp')
            if self.collect_match_stats:
                msg['segment_matches'] = []

            # Reject any message that has invalid position
            if not self._validate_position(x, y):
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

            elif x is None or y is None or timestamp is None:
                self._last_segment.add_msg(msg)

            elif timestamp < self._prev_timestamp:
                raise ValueError("Input data is unsorted")

            else:
                try:
                    best_match, noise_factor = self._compute_best(msg)
                except ValueError as e:
                    if False:
                        logger.debug("    Out of bound points, could not compute best segment: %s", e)
                        logger.debug("    Bad msg: %s", msg)
                        logger.debug("    Yielding bad segment")
                    yield self._create_segment(msg, cls=BadSegment)
                    # bs = BadSegment(self._segment_unique_id(msg), msg['mmsi'])
                    # bs.add_msg(msg)
                    # yield bs
                    continue

                if best_match is None:
                    if noise_factor > 1.0:
                        yield self._create_segment(msg, cls=NoiseSegment)
                        # s = NoiseSegment(self._segment_unique_id(msg), msg['mmsi'])
                        # s.add_msg(msg)
                        # yield s
                    else:
                        self._add_segment(msg)
                else:
                    id = best_match['seg_id']
                    self._segments[id].add_msg(msg)
                    self._last_segment = self._segments[id]

            if x and y and timestamp:
                self._prev_timestamp = msg['timestamp']

        for series, segment in self._segments.items():
            yield segment

