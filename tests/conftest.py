import pytest
import datetime


class MessageGenerator(object):
    def __init__(self, mmsi=None):
        self.mmsi = mmsi if mmsi else 123456789
        self.reset()

    def reset(self):
        self.timestamp = datetime.datetime.now()
        self.lat = 0
        self.lon = 0
        self.index = 0

    def increment(self):
        self.timestamp += datetime.timedelta(hours=1)
        self.lat += 0.01
        self.lon += 0.01
        self.index += 1

    def next_msg(self):
        self.increment()
        return {'mmsi': self.mmsi, 'idx': self.index}

    def next_posit_msg(self):
        self.increment()
        return {'mmsi': self.mmsi, 'lat': self.lat, 'lon': self.lon}

    def next_time_posit_msg(self):
        self.increment()
        return {
            'mmsi': self.mmsi, 'lat': self.lat, 'lon': self.lon, 'timestamp': self.timestamp}

    def next_msg_from_stub(self, stub):
        self.increment()
        msg = dict(
            idx=self.index,
            mmsi=self.mmsi,
            timestamp=self.timestamp
        )
        msg.update(stub)
        seg = msg.get('seg', 0)
        if msg.get('type', 99) in (1, 3, 18, 19):
            msg['lat'] = self.lat + (seg * 2)
            msg['lon'] = self.lon + (seg * 2)

        return msg

    def generate_messages (self, message_stubs):
        self.reset()
        for stub in message_stubs:
            yield self.next_msg_from_stub(stub)


@pytest.fixture(scope='function')
def msg_generator():
    return MessageGenerator()
