from collections import namedtuple
import datetime
import logging
import math

from gpsdio_segment.matcher import POSITION_TYPES

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.WARNING)

log = logger.info

# Note that, the values may now be or soon will be exact because
# we are fixing the numerical issues; also invalid values will
# soon be translated to null, so we can just check for that and skip this.
# The values 52 and 102.3 are both almost always noise, and don't
# reflect the vessel's actual speed. They need to be commented out.
# The value 102.3 is reserved for "bad value." It looks like 51.2
# is also almost always noise. The value 63 means unavailable for
# type 27 messages so we exclude that as well. Because the values are floats,
# and not always exactly 102.3 or 51.2, we give a range.
REPORTED_SPEED_EXCLUSION_RANGES = [(51.15, 51.25), (62.95, 63.05), (102.25, 102.35)]

POSITION_MESSAGE = object()
INFO_ONLY_MESSAGE = object()
BAD_MESSAGE = object()

INFO_TYPES = {"AIS.5": "AIS-A", "AIS.19": "AIS-B", "AIS.24": "AIS-B", "VMS": "VMS"}
INFO_PING_INTERVAL_MINS = 15

Identity = namedtuple(
    "Identity", ["shipname", "callsign", "imo", "transponder_type", "length", "width"]
)

Destination = namedtuple("Destination", ["destination"])


def is_null(v):
    return (v is None) or math.isnan(v)


class MsgProcessor:
    def __init__(self, very_slow, ssvid):
        """
        Manages information from information only messages and processes messages
        to determine message type.

        Parameters
        ----------
        very_slow : float
            See DiscrepancyCalculator for more information.
        ssvid : int, optional
            MMSI or other Source Specific ID to pull out of the stream and process.
            If not given, the first valid ssvid is used.  All messages with a
            different ssvid are thrown away.
        """
        self.very_slow = very_slow
        self.ssvid = ssvid
        self.cur_msgids = {}
        self.cur_locations = {}
        self._prev_timestamp = None
        self.identities = {}
        self.destinations = {}

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
    def extract_normalized_location(msg):
        # TODO: this can probably be removed since @andres is cleaning this up.
        lat, lon, course, speed, heading = MsgProcessor.extract_location(msg)
        return (
            round(lat * 60000),
            round(lon * 60000),
            None if course is None else round(course * 10),
            round(speed * 10),
            None if (heading is None or math.isnan(heading)) else round(heading),
        )

    def _checked_stream(self, stream):
        """
        Check messages in the message stream for proper timestamps,
        duplicates, and matching SSVIDs.

        Yields
        -------
        dict
        """
        for msg in stream:
            if "type" not in msg:
                raise ValueError("`msg` is missing required field `type`")

            # Add empty info fields so they are always present
            msg["identities"] = {}
            msg["destinations"] = {}

            timestamp = msg.get("timestamp")
            if timestamp is None:
                raise ValueError("Message missing timestamp")
            if self._prev_timestamp is not None and timestamp < self._prev_timestamp:
                raise ValueError("Input data is unsorted")
            # TODO: shouldn't this come after the duplicate message check
            self._prev_timestamp = msg["timestamp"]

            msgid = msg.get("msgid")
            if msgid in self.cur_msgids:
                logger.debug(
                    f"Skipping duplicate msgid {msgid}",
                )
                continue
            self.cur_msgids[msgid] = timestamp

            ssvid = msg.get("ssvid")
            if self.ssvid is None:
                self.ssvid = ssvid
            elif ssvid != self.ssvid:
                logger.warning(
                    "Skipping non-matching SSVID %r, expected %r", ssvid, self.ssvid
                )
                continue

            yield msg

    def _message_type(self, msg):
        """
        Determine message type based on position, course, and speed.

        Yields
        -------
        object
            One of the following: POSITION_MESSAGE, INFO_ONLY_MESSAGE, or BAD_MESSAGE
        """
        x, y, course, speed, _ = self.extract_location(msg)

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
            and (  # 360 is invalid unless speed is very low.
                (speed <= self.very_slow and course > 359.95) or 0.0 <= course <= 359.95
            )
            and (not any(l < speed < h for (l, h) in REPORTED_SPEED_EXCLUSION_RANGES))
        ):
            return POSITION_MESSAGE
        return BAD_MESSAGE

    def _already_seen(self, loc):
        """
        Return True if this location has non-zero speed and had previously been seen
        as it does not make sense that the vessel has not changed location.

        Yields
        -------
        boolean
        """
        x, y, course, speed, heading = loc
        return speed > 0 and loc in self.cur_locations

    def _store_info(self, msg):
        """
        Links information from this message to timestamps within a certain range
        before and after it's own timestamp, specified by `INFO_PING_INTERVAL_MINS`.
        Timestamps are all rounded down to the minute.

        This information will later be used to link position messages to identity
        information that was received in close proximity.
        """
        transponder_type = INFO_TYPES.get(msg.get("type"))
        identity = Identity(
            msg.get("shipname"),
            msg.get("callsign"),
            msg.get("imo"),
            transponder_type,
            msg["length"],
            msg["width"],
        )
        destination = Destination(msg.get("destination"))

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
            if k1 not in self.identities:
                self.identities[k1] = {k2: {}}
            elif k2 not in self.identities[k1]:
                self.identities[k1][k2] = {}
            idents = self.identities[k1][k2]
            idents[identity] = idents.get(identity, 0) + 1
            #
            if k1 not in self.destinations:
                self.destinations[k1] = {k2: {}}
            elif k2 not in self.destinations[k1]:
                self.destinations[k1][k2] = {}
            dests = self.destinations[k1][k2]
            dests[destination] = dests.get(destination, 0) + 1

    def add_info_to_msg(self, msg):
        """
        Gets the identity information associated with the timestamp of the message,
        rounded down to the minute, and adds it to the message.
        """
        ts = msg["timestamp"]
        # Using tzinfo as below is only stricly valid for UTC and naive time due to
        # issues with DST (see http://pytz.sourceforge.net).
        assert ts.tzinfo == datetime.timezone.utc or ts.tzinfo.zone == "UTC"
        k1 = datetime.datetime(
            ts.year, ts.month, ts.day, ts.hour, ts.minute, tzinfo=ts.tzinfo
        )
        receiver_type = msg.get("receiver_type")
        source = msg.get("source")

        msg["identities"] = msg_idents = {}
        msg["destinations"] = msg_dests = {}

        def updatesum(orig, new):
            for k, v in new.items():
                orig[k] = orig.get(k, 0) + v

        if k1 in self.identities:
            for transponder_type in POSITION_TYPES.get(msg.get("type"), ()):
                k2 = (transponder_type, receiver_type, source)
                if k2 in self.identities[k1]:
                    idents = self.identities[k1][k2]
                    for k, v in idents.items():
                        msg_idents[k] = msg_idents.get(k, 0) + v

        if k1 in self.destinations:
            for transponder_type in POSITION_TYPES.get(msg.get("type"), ()):
                k2 = (transponder_type, receiver_type, source)
                if k2 in self.destinations[k1]:
                    dests = self.destinations[k1][k2]
                    for k, v in dests.items():
                        msg_dests[k] = msg_dests.get(k, 0) + v

    def __call__(self, stream):
        for msg in self._checked_stream(stream):
            msg_type = self._message_type(msg)
            if msg_type is not BAD_MESSAGE:
                self._store_info(msg)
            if msg_type is POSITION_MESSAGE:
                timestamp = msg.get("timestamp")
                loc = self.extract_normalized_location(msg)
                if self._already_seen(loc):
                    logger.debug(f"Skipping already seen location {loc}")
                    continue
                self.cur_locations[loc] = timestamp
            yield msg_type, msg
