"""Test application of identities"""
from datetime import datetime, timedelta
from gpsdio_segment.core import Segmentizer
import pytz

class _MsgGenerator(object):
    def __init__(self, interval=timedelta(minutes=4)):
        self._timestamp = datetime.utcnow().replace(tzinfo=pytz.UTC)
        self._msgid = 1
        self._interval = interval

    def make_position_message(self):
        msg = {'msgid' : self._msgid, 'ssvid': 1, 'lat': 0, 'lon': 0, 'type' : 'AIS.1',
               'timestamp': self._timestamp, 'course' : 0, 'speed': 0}
        self._timestamp += self._interval
        self._msgid += 1
        return msg

    def make_identity_message(self, shipname='boatymcboatface'):
        msg = {'msgid' : self._msgid, 'ssvid': 1, 'type' : 'AIS.5',
               'timestamp': self._timestamp, 'shipname' : shipname}
        self._timestamp += self._interval
        self._msgid += 1
        return msg



def test_identity_before():
    gen = _MsgGenerator()
    messages = []
    messages.append(gen.make_identity_message())
    for i in range(20):
        messages.append(gen.make_position_message())

    segments = [x for x in Segmentizer(messages) if not x.noise]

    assert len(segments) == 1
    names = [x['shipnames'] for x in segments[0].msgs]
    for i in range(3):
        assert names[i] == {'boatymcboatface' : 1}
    for i in range(3, 20):
        assert names[i] == {}   


def test_identity_after():
    gen = _MsgGenerator()
    messages = []
    for i in range(20):
        messages.append(gen.make_position_message())
    messages.append(gen.make_identity_message())

    segments = [x for x in Segmentizer(messages) if not x.noise]

    assert len(segments) == 1
    names = [x['shipnames'] for x in segments[0].msgs]
    for i in range(17):
        assert names[i] == {}
    for i in range(17, 20):
        assert names[i] == {'boatymcboatface' : 1}


def test_multiple_identities():
    gen = _MsgGenerator()
    messages = []
    for i in range(6):
        messages.append(gen.make_position_message())
    messages.append(gen.make_identity_message())
    messages.append(gen.make_position_message())
    messages.append(gen.make_position_message())
    messages.append(gen.make_position_message())
    messages.append(gen.make_identity_message())
    messages.append(gen.make_position_message())
    messages.append(gen.make_position_message())
    messages.append(gen.make_position_message())
    messages.append(gen.make_identity_message('samiam'))
    for i in range(6):
        messages.append(gen.make_position_message())

    segments = [x for x in Segmentizer(messages) if not x.noise]

    assert len(segments) == 1
    names = [x['shipnames'] for x in segments[0].msgs]

    assert names == [
        {}, {}, {}, 
        {'boatymcboatface': 1}, {'boatymcboatface': 1}, {'boatymcboatface': 1}, 
        {'boatymcboatface': 2}, {'boatymcboatface': 2}, {'boatymcboatface': 2}, 
        {'boatymcboatface': 1, 'samiam': 1}, {'boatymcboatface': 1, 'samiam': 1}, 
            {'boatymcboatface': 1, 'samiam': 1}, 
        {'samiam': 1}, {'samiam': 1}, {'samiam': 1}, 
        {}, {}, {}
    ]

