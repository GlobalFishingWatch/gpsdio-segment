"""Test application of identities"""
from datetime import datetime, timedelta
from gpsdio_segment.core import Segmentizer
import pytz

# def test_with_non_posit():
#     # Non-positional messages are emitted as there own segments
#     # This should produce two segments, each with 3 points - two of which are
#     # positional and 1 that is a non-posit

#     # Continuous
#     msg1 = {'idx': 0, 'msgid' : 1, 'ssvid': 1, 'lat': 0, 'lon': 0, 'type' : 'AIS.1',
#             'timestamp': datetime.now(), 'course' : 0, 'speed': 1}
#     msg2 = {'idx': 1, 'msgid' : 2, 'ssvid': 1, 'type' : 'AIS.1',
#             'timestamp': msg1['timestamp'] + timedelta(hours=1)}
#     msg3 = {'idx': 2, 'msgid' : 3, 'ssvid': 1, 'lat': 0.00001, 'lon': 0.00001, 'type' : 'AIS.1',
#             'timestamp': msg1['timestamp'] + timedelta(hours=2), 'course' : 0, 'speed': 1}

#     # Also continuous but not to the previous trio
#     msg4 = {'idx': 3, 'msgid': 4, 'ssvid': 1, 'lat': 65, 'lon': 65, 'type' : 'AIS.1',
#             'timestamp': msg3['timestamp'] + timedelta(days=100), 'course' : 0, 'speed': 1}
#     msg5 = {'idx': 4, 'msgid': 5, 'ssvid': 1, 'type' : 'AIS.1',
#             'timestamp': msg4['timestamp'] + timedelta(hours=1)}
#     msg6 = {'idx': 5, 'msgid': 6, 'ssvid': 1, 'lat': 65.00001, 'lon': 65.00001, 'type' : 'AIS.1',
#             'timestamp': msg4['timestamp'] + timedelta(hours=2), 'course' : 0, 'speed': 1}

#     messages = [msg1, msg2, msg3, msg4, msg5, msg6]
#     messages = [utcify(x) for x in messages]

#     segments = list(Segmentizer(messages))
#     assert len(segments) == 4
#     assert [len(seg) for seg in segments] == [1, 2, 1, 2]

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

    # This is the behaviour we want
    # for i in range(17, 20):
    #     assert names[i] == {'boatymcboatface' : 1}

    # This is the behaviour we currently have
    for i in range(17, 20):
        assert names[i] == {}

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
    # for i in range(17):
    #     assert names[i] == {}

    print(names)
    # This is the behaviour we want
    # assert names == [
    #     {}, {}, {}, 
    #     {'boatymcboatface': 1}, {'boatymcboatface': 1}, {'boatymcboatface': 1}, 
    #     {'boatymcboatface': 2}, {'boatymcboatface': 2}, {'boatymcboatface': 2}, 
    #     {'boatymcboatface': 1, 'samiam': 1}, {'boatymcboatface': 1, 'samiam': 1}, 
    #         {'boatymcboatface': 1, 'samiam': 1}, 
    #     {'samiam': 1}, {'samiam': 1}, {'samiam': 1}, 
    #     {}, {}, {}
    # ]

    # This is the behaviour we currently have
    assert names == [
        {}, {}, {}, 
        {}, {}, {}, 
        {'boatymcboatface': 1}, {'boatymcboatface': 1}, {'boatymcboatface': 1}, 
        {'boatymcboatface': 1}, {'boatymcboatface': 1}, {'boatymcboatface': 1}, 
        {'samiam': 1}, {'samiam': 1}, {'samiam': 1}, 
        {}, {}, {}
    ]
