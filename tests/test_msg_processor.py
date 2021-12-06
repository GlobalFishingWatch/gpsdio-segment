from datetime import datetime

import pytest
from support import utcify

from gpsdio_segment.matcher import Matcher
from gpsdio_segment.msg_processor import (
    BAD_MESSAGE,
    INFO_ONLY_MESSAGE,
    POSITION_MESSAGE,
    REPORTED_SPEED_EXCLUSION_RANGES,
    MsgProcessor,
)


# Checks for MsgProcessor._message_type()
def test_info_only_message():
    messages = [
        {
            "ssvid": 10,
            "msgid": 1,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": None,
            "lon": None,
            "course": None,
            "speed": None,
        },
    ]
    messages = [utcify(x) for x in messages]
    msg_processor = MsgProcessor(
        Matcher.very_slow, ssvid=10, prev_msgids=None, prev_locations=None
    )
    processed_messages = list(msg_processor(messages))
    assert len(processed_messages) == 1
    msg_type, msg = processed_messages[0]
    assert msg_type == INFO_ONLY_MESSAGE


def test_position_message():
    messages = [
        {
            "ssvid": 20,
            "msgid": 1,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
    ]
    messages = [utcify(x) for x in messages]
    msg_processor = MsgProcessor(
        Matcher.very_slow, ssvid=20, prev_msgids=None, prev_locations=None
    )
    processed_messages = list(msg_processor(messages))
    assert len(processed_messages) == 1
    msg_type, msg = processed_messages[0]
    assert msg_type == POSITION_MESSAGE


def test_bad_messages():
    """
    These all represent a condition that fails to match to INFO_ONLY_MESSAGE
    or POSITION_MESSAGE in MsgProcessor._message_type().
    """
    messages = [
        # lon is missing
        {
            "ssvid": 30,
            "msgid": 1,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "course": 0,
            "speed": 1,
        },
        # lat is missing
        {
            "ssvid": 30,
            "msgid": 2,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
        # speed is missing
        {
            "ssvid": 30,
            "msgid": 3,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": 0,
        },
        # course is missing
        {
            "ssvid": 30,
            "msgid": 4,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "speed": 1,
        },
        # lon is < -180
        {
            "ssvid": 30,
            "msgid": 5,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": -181,
            "course": 0,
            "speed": 1,
        },
        # lon is > 180
        {
            "ssvid": 30,
            "msgid": 6,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 181,
            "course": 0,
            "speed": 1,
        },
        # lat is < -90
        {
            "ssvid": 30,
            "msgid": 7,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": -91,
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
        # lat is > 90
        {
            "ssvid": 30,
            "msgid": 8,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 91,
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
        # course is < 0
        {
            "ssvid": 30,
            "msgid": 9,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": -1,
            "speed": 1,
        },
        # course is > 359.95 and speed > self.very_slow
        {
            "ssvid": 30,
            "msgid": 10,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": 360,
            "speed": Matcher.very_slow + 1,
        },
    ]

    # speed is in one of the REPORTED_SPEED_EXCLUSION_RANGES
    # Doing one check for each of the current ranges
    for i, (l, h) in enumerate(REPORTED_SPEED_EXCLUSION_RANGES):
        messages.append(
            {
                "ssvid": 30,
                "msgid": 11 + i,
                "timestamp": datetime.now(),
                "type": "UNKNOWN",
                "lat": 90,
                "lon": 90,
                "course": 0,
                "speed": (l + h) / 2,
            }
        )
    messages = [utcify(x) for x in messages]
    msg_processor = MsgProcessor(
        Matcher.very_slow, ssvid=30, prev_msgids=None, prev_locations=None
    )
    processed_messages = list(msg_processor(messages))
    assert len(processed_messages) == len(messages)
    for msg_type, msg in processed_messages:
        assert msg_type == BAD_MESSAGE


# Checks for MsgProcessor._checked_stream()
def test_missing_type():
    messages = [
        {
            "ssvid": 10,
            "msgid": 1,
            "timestamp": datetime.now(),
            "lat": 90,
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
    ]
    messages = [utcify(x) for x in messages]
    msg_processor = MsgProcessor(
        Matcher.very_slow, ssvid=10, prev_msgids=None, prev_locations=None
    )
    with pytest.raises(ValueError) as excinfo:
        list(msg_processor(messages))
    assert "missing required field `type`" in str(excinfo.value)


def test_missing_timestamp():
    messages = [
        {
            "ssvid": 10,
            "msgid": 1,
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
    ]
    messages = [utcify(x) for x in messages]
    msg_processor = MsgProcessor(
        Matcher.very_slow, ssvid=10, prev_msgids=None, prev_locations=None
    )
    with pytest.raises(ValueError) as excinfo:
        list(msg_processor(messages))
    assert "missing timestamp" in str(excinfo.value)


def test_unsorted():
    before = {
        "ssvid": 10,
        "msgid": 1,
        "timestamp": datetime.now(),
        "type": "UNKNOWN",
        "lat": 90,
        "lon": 90,
        "course": 0,
        "speed": 1,
    }
    after = {
        "ssvid": 10,
        "msgid": 2,
        "timestamp": datetime.now(),
        "type": "UNKNOWN",
        "lat": 90,
        "lon": 90,
        "course": 0,
        "speed": 1,
    }
    messages = [after, before]
    messages = [utcify(x) for x in messages]
    msg_processor = MsgProcessor(
        Matcher.very_slow, ssvid=10, prev_msgids=None, prev_locations=None
    )
    with pytest.raises(ValueError) as excinfo:
        list(msg_processor(messages))
    assert "unsorted" in str(excinfo.value)


def test_duplicate_msgid():
    messages = [
        {
            "ssvid": 10,
            "msgid": 1,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
        {
            "ssvid": 10,
            "msgid": 1,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
    ]
    messages = [utcify(x) for x in messages]
    msg_processor = MsgProcessor(
        Matcher.very_slow, ssvid=10, prev_msgids=None, prev_locations=None
    )
    processed_messages = list(msg_processor(messages))
    assert len(processed_messages) == 1


def test_set_ssvid_if_none():
    messages = [
        {
            "ssvid": 10,
            "msgid": 1,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": 0,
            "speed": 1,
        }
    ]
    messages = [utcify(x) for x in messages]
    msg_processor = MsgProcessor(
        Matcher.very_slow, ssvid=None, prev_msgids=None, prev_locations=None
    )
    processed_messages = list(
        msg_processor(messages)
    )  # Run this so MsgProcessor knows what to set SSVID to
    assert len(processed_messages) == 1
    assert msg_processor.ssvid == 10


def test_skip_incorrect_ssvid():
    messages = [
        {
            "ssvid": 10,
            "msgid": 1,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
        {
            "ssvid": 20,
            "msgid": 2,
            "timestamp": datetime.now(),
            "type": "UNKNOWN",
            "lat": 90,
            "lon": 90,
            "course": 0,
            "speed": 1,
        },
    ]
    messages = [utcify(x) for x in messages]
    msg_processor = MsgProcessor(
        Matcher.very_slow, ssvid=10, prev_msgids=None, prev_locations=None
    )
    # Logs a warning but does not throw an exception.
    # Simply ignores the message with the incorrect SSVID.
    processed_messages = list(msg_processor(messages))
    assert len(processed_messages) == 1
