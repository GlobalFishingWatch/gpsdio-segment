import pytest
import datetime


class MessageGenerator(object):
    def __init__(self, mmsi=None):
        self.mmsi = mmsi if mmsi else 123456789
        self.timestamp = datetime.datetime.now()
        self.lat = 0
        self.lon = 0
        self.field = 0

    def increment(self):
        self.timestamp += datetime.timedelta(minutes=1)
        self.lat += 0.01
        self.lon += 0.01
        self.field += 1

    def next_msg(self):
        self.increment()
        return {'mmsi': self.mmsi, 'field': self.field}

    def next_posit_msg(self):
        self.increment()
        return {'mmsi': self.mmsi, 'lat': self.lat, 'lon': self.lon}

    def next_time_posit_msg(self):
        self.increment()
        return {'mmsi': self.mmsi, 'lat': self.lat, 'lon': self.lon, 'timestamp': self.timestamp}


@pytest.fixture(scope='function')
def msg_generator():
    return MessageGenerator()
