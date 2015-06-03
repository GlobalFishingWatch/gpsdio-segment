"""
Core components for despoofing data.
"""


from __future__ import division

import logging

import pyproj


logger = logging.getLogger('gpsdio-despoof-core')


# See `Despoof()` for more info
DEFAULT_MAX_HOURS = 24  # hours
DEFAULT_MAX_SPEED = 100  # knots
DEFAULT_NOISE_DIST = round(50 / 1852, 3)  # nautical miles


def msg_diff_stats(msg1, msg2, geod):

    """
    Compute the stats required to determine if two points are continuous.  Input
    messages must have a `lat`, `lon`, and `timestamp`, that are not `None` and
    `timestamp` must be an instance of `datetime.datetime`.

    Parameters
    ----------
    msg1 : dict
        A GPSD message.
    msg2 : dict
        See `msg1`.
    geod : pyproj.Geod
        Used to compute distance.

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
    ts1 = msg1['timestamp']

    x2 = msg2['lon']
    y2 = msg2['lat']
    ts2 = msg2['timestamp']

    distance = geod.inv(x1, y1, x2, y2)[2] / 1850
    if ts1 > ts2:
        timedelta = (ts1 - ts2).total_seconds() / 3600
    else:
        timedelta = (ts2 - ts1).total_seconds() / 3600
    speed = (distance / timedelta)

    return {
        'distance': distance,
        'timedelta': timedelta,
        'speed': speed
    }



class Despoofer(object):

    def __init__(self, instream, mmsi=None, max_hours=DEFAULT_MAX_HOURS,
                 max_speed=DEFAULT_MAX_SPEED, noise_dist=DEFAULT_NOISE_DIST):

        self.max_hours = max_hours
        self.max_speed = max_speed
        self.noise_dist = noise_dist

        # Exposed via properties
        self._instream = instream

        self._geod = pyproj.Geod(ellps='WGS84')
        self._tracks = {}
        self._last_id = -1
        self._mmsi = mmsi
        self._prev_msg = None
        self._last_track = None

        logger.debug("Created an instance of `Despoofer()` with ...")
        logger.debug("max_speed = %s", max_speed)
        logger.debug("max_hours = %s", max_hours)
        logger.debug("noise_dist = %s", noise_dist)
        logger.debug("noise_dist = %s", noise_dist)

    def __iter__(self):

        """
        Produces completed tracks.
        """

        return next(self.despoof())

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

    @property
    def last_track(self):

        return self._last_track

    def _add_track(self, msg):

        """
        Add a new track to the track container.
        """

        self._last_id += 1

        t = Track(self._last_id, self.mmsi)
        t.add_msg(msg)

        self._tracks[self._last_id] = t
        self._last_track = t

    def _add_to_best(self, msg):

        """
        Add a message to the 'best' track.

        Parameters
        ----------
        msg : dict
            A GPSD message.
        """

        # Only one track so no need to figure out which is the best
        if len(self._tracks) is 1:
            t = list(self._tracks.values())[0]
            t.add_msg(msg)
            self._last_track = t

        else:

            # best_stats are the stats between the input message and the current best track
            # track_stats are the stats between the input message and the current track
            best_stats = None
            best = None
            for track in self._tracks.values():
                if best is None:
                    best = track
                    best_stats = msg_diff_stats(msg, best.last_msg, self._geod)
                else:
                    track_stats = msg_diff_stats(msg, best.last_msg, self._geod)
                    track_distance = track_stats['timedelta'] * track_stats['distance']
                    best_distance = best_stats['timedelta'] * best_stats['distance']

                    if track_distance <= best_distance:
                        best = track
                    else:
                        continue

            self._tracks[best.id].add_msg(msg)

    def despoof(self):

        """
        The method that does all the work.  Creates a generator that spits out
        finished tracks.  Rather than calling this directly the intended use of
        this class is:

            >>> import gpsdio
            >>> with gpsdio.open('infile.ext') as src:
            ...    for track in Despoofer(src):
            ...        # Do something with the track
        """

        logger.debug("Starting to despoof%s",
                     ' %s' % self._mmsi if self._mmsi is not None else ' - finding MMSI ...')

        for idx, msg in enumerate(self.instream):

            # Cache the MMSI and some other fields
            mmsi = msg.get('mmsi')
            y = msg.get('lat')
            x = msg.get('lon')
            timestamp = msg.get('timestamp')

            # This is the first message with a valid MMSI
            # Make it the previous message and create a new track
            if self.mmsi is None:
                logger.debug("Found a valid MMSI - processing: %s", mmsi)
                self._mmsi = mmsi
                self._add_track(msg)
                continue

            # Non positional message or lacking timestamp.  Add to the most recent track.
            elif x is None or y is None or timestamp is None:
                self.last_track.add_msg(msg)

            # Everything is set up - process!
            else:

                # Dump all tracks that are dicontinuous in time before doing anything
                # with the current point
                for track in self._tracks.values():
                    if (track.last_msg['timestamp'] - timestamp).total_seconds() / 3600 > self.max_hours:
                        try:
                            yield track
                        finally:
                            del self._tracks[track.id]

                prev_x = self._prev_msg.get('lon')
                prev_y = self._prev_msg.get('lat')
                distance = self._geod.inv(prev_x, prev_y, x, y)[2] / 1850
                timedelta = (self._prev_msg['timestamp'] - timestamp).total_seconds() / 3600
                speed = distance / timedelta

                # This point exceeds the maximum time delta - create a new track.
                if self.last_track is None or timedelta > self.max_hours:
                    self._add_track(msg)
                    continue
                
                # This point is traveling way too fast - create a new track
                elif speed > self.max_speed:
                    self._add_track(msg)
                    continue

                # This point is so close to the previous point that we can consider it noise
                # Add it to the last track
                elif distance <= self.noise_dist:
                    self.last_track.add_msg(msg)

                # This point passed all the other checks so add it to the best track
                else:
                    self._add_to_best(msg)

            self._prev_msg = msg

        # No more points to process.  Yield all the remaining tracks.
        for track in self._tracks.values():
            yield track



class Track(object):

    def __init__(self, id, mmsi, id_field='series'):
        self._id = id
        self._mmsi = mmsi

        self._msgs = []
        self._coords = []

        self._id_field = id_field
        self._iter_idx = 0

        logger.debug("Created an in instance of `Track()` with ID: %s", id)

    def __iter__(self):
        return self

    def __len__(self):
        return len(self.msgs)

    def next(self):

        """
        Returns a message with an added series value.
        """

        # Already returned all the messages - be sure to update the cursor
        if self._iter_idx >= len(self.msgs):
            try:
                raise StopIteration
            finally:
                self._iter_idx = 0

        # Add the series value and return the message
        # Iterate the cursor
        try:
            msg = self.msgs[self._iter_idx]
            msg[self._id_field] = self.id
            return msg
        finally:
            self._iter_idx += 1

    __next__ = next

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
        return self.coords[-1]

    @property
    def last_msg(self):
        return self.msgs[-1]

    def add_msg(self, msg):

        if msg.get('mmsi') is not self.mmsi:
            raise ValueError(
                'MMSI mismatch: {internal} != {new}'.format(self.mmsi, msg.get('mmsi')))

        self._msgs.append(msg)
        if msg.get('lat') is not None and msg.get('lon') is not None:
            self._coords += (msg.get('lon'), msg.get('lat'))
