"""
The pair_defs define two points that need to be compared.  The is_same key
denotes whether these two points should be in the same segment.  For the most
point the pairs are the first and last points in a segment.
"""


from datetime import datetime

from click.testing import CliRunner

import gpsdio
import gpsdio.cli.main


def prep_pair_def(pair_def):

    """
    Make sure lat/lon has been rounded to 7 places and timestamp fields are
    converted to an instance of datetime.
    """

    for k, v in pair_def.copy().items():
        if 'lat' in k or 'lon' in k:
            v = round(v, 7)
        elif 'ts' in k and not isinstance(v, datetime):
            v = datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
        pair_def[k] = v

    return pair_def


def process(infile, tmpdir):

    """
    Run segmentation on a file and return the output file path.
    """

    try:
        outfile = str(tmpdir.mkdir('out').join('outfile.msg.gz'))
    except:
        outfile = str(tmpdir.join('outfile.msg.gz'))

    result = CliRunner().invoke(gpsdio.cli.main.main_group, [
        'segment',
        '--segment-field', 'segment',
        infile,
        outfile
    ])
    assert result.exit_code == 0
    return outfile


def compare(pair_def, stream):

    """
    Parse a stream to extract the necessary messages and then check to see if
    the messages have the correct segment values.

    `pair_def` is something like:

        {
            'is_same': True,
            'mmsi': 431704490,

            'ts1': datetime(year=2015, month=1, day=2, hour=16, minute=38, second=31),
            'lon1': -33.0719985962,
            'lat1': 9.6271667481,

            'ts2': datetime(year=2015, month=1, day=4, hour=7, minute=45, second=0)
            'lon2': -32.3031654358,
            'lat2': 9.9744997025,
        }

    The `is_same` key dictates whether or not the segment ID's should match.
    """

    pair_def = prep_pair_def(pair_def)

    msg1 = msg2 = None
    for msg in stream:

        x = round(msg.get('lon', -9999), 7)
        y = round(msg.get('lat', -9999), 7)
        ts = msg['timestamp']
        msg_mmsi = msg['mmsi']

        if msg_mmsi == pair_def['mmsi'] and None not in (x, y, ts):

            if msg1 is None and y == round(pair_def['lat1'], 7) \
                    and x == round(pair_def['lon1'], 7) \
                    and ts == pair_def['ts1']:
                msg1 = msg
            elif msg2 is None and y == round(pair_def['lat2'], 7) \
                    and x == round(pair_def['lon2'], 7) \
                    and ts == pair_def['ts2']:
                msg2 = msg

        elif msg1 and msg2:
            break

    assert msg1['segment'] is not None
    assert msg2['segment'] is not None
    return (msg1['segment'] == msg2['segment']) == pair_def['is_same']


def test_224051350(tmpdir):
    pair_def = {
        'mmsi': 224051350,
        'is_same': True,

        'lon1': round(-96.2392425537, 7),
        'lat1': round(-21.3278064728, 7),
        'ts1': datetime(year=2015, month=1, day=1, hour=1, minute=53, second=24),

        'lon2': round(-95.8099060059, 7),
        'lat2': round(-20.7386302948, 7),
        'ts2': datetime(year=2015, month=1, day=31, hour=23, minute=31, second=54)
    }

    with gpsdio.open(process('tests/data/point-pair/224051350.msg.gz', tmpdir)) as src:
        assert compare(pair_def, src)


def test_431704490(tmpdir):
    pair_tests = [
        {
            'is_same': True,
            'mmsi': 431704490,

            'ts1': datetime(year=2015, month=1, day=2, hour=16, minute=38, second=31),
            'lon1': -33.0719985962,
            'lat1': 9.6271667481,

            'ts2': datetime(year=2015, month=1, day=4, hour=7, minute=45, second=0),
            'lon2': -32.3031654358,
            'lat2': 9.9744997025,
        },
        {
            'is_same': False,
            'mmsi': 431704490,

            'ts1': datetime(year=2015, month=1, day=2, hour=16, minute=38, second=31),
            'lon1': -33.0719985962,
            'lat1': 9.6271667481,

            'ts2': datetime(year=2015, month=1, day=2, hour=16, minute=41, second=18),
            'lon2': -81.2001647949,
            'lat2': -90,

        },
        {
            'is_same': False,
            'mmsi': 431704490,

            'ts1': datetime(year=2015, month=1, day=12, hour=12, minute=14, second=20),
            'lon1': -33.4316673279,
            'lat1': 8.0314998627,

            'ts2': datetime(year=2015, month=1, day=12, hour=12, minute=16, second=54),
            'lon2': -33.4386672974,
            'lat2': 9.7333335877
        }
    ]

    for pair_def in pair_tests:
        with gpsdio.open(process('tests/data/point-pair/431704490.msg.gz', tmpdir)) as src:
            assert compare(pair_def, src)


def test_240000000(tmpdir):

    """
    mmsi	    timestamp1	        lon1	        lat1	    timestamp2              lon2	    lat2
    240000000   2015-01-03 04:52:25	-74.73930359	10.87963486 2015-01-03  05:04:50	83.27050781	17.7059803
    """

    pair_def = {
        'is_same': False,
        'mmsi': 240000000,

        'ts1': datetime(year=2015, month=1, day=3, hour=4, minute=52, second=25),
        'lon1': -74.73930359,
        'lat1': 10.87963486,

        'ts2': datetime(year=2015, month=1, day=3, hour=5, minute=4, second=50),
        'lon2': 83.27050781,
        'lat2': 17.7059803
    }

    with gpsdio.open(process('tests/data/point-pair/240000000.msg.gz', tmpdir)) as src:
        assert compare(pair_def, src)


def test_204814000(tmpdir):
    pair_def = {
        'is_same': False,
        'mmsi': 204814000,

        'ts1': '2014-08-02 00:20:10',
        'lon1': -9.8415250778,
        'lat1': 41.0997772217,

        'ts2': '2014-08-02 00:20:15',
        'lon2': -9.8284282684,
        'lat2': 41.1031188965
    }

    with gpsdio.open(process('tests/data/point-pair/204814000.msg.gz', tmpdir)) as src:
        assert compare(pair_def, src)


def test_224108190(tmpdir):
    pair_def = {
        'is_same': False,
        'mmsi': 224108190,

        'ts1': '2014-08-24 21:24:01',
        'lon1': 3.1034851074,
        'lat1': 41.7655258179,

        'ts2': '2014-08-24 21:37:48',
        'lon2': 3.1313149929,
        'lat2': 41.0869140625
    }

    with gpsdio.open(process('tests/data/point-pair/224108190.msg.gz', tmpdir)) as src:
        assert compare(pair_def, src)


def test_600011817(tmpdir):
    pair_def = {
        'is_same': False,
        'mmsi': 600011817,

        'ts1': '2014-08-19 02:14:59',
        'lon1': 120.225990295,
        'lat1': 33.3282089233,

        'ts2': '2014-08-19 02:26:59',
        'lon2': 120.141143799,
        'lat2': 33.5084609985
    }

    with gpsdio.open(process('tests/data/point-pair/600011817.msg.gz', tmpdir)) as src:
        assert compare(pair_def, src)


def test_538004505(tmpdir):
    pair_def = {
        'is_same': False,
        'mmsi': 538004505,

        'ts1': '2014-08-29 19:08:16',
        'lon1': -10.73543358,
        'lat1': 37.88721848,

        'ts2': '2014-08-29 19:09:06',
        'lon2': -10.72798347,
        'lat2': 37.9012184
    }

    with gpsdio.open(process('tests/data/point-pair/538004505.msg.gz', tmpdir)) as src:
        assert compare(pair_def, src)


def test_205316000(tmpdir):
    # This test was originally designed to show a vessel with a gap between points
    # that was > 100 NM but that criteria has been removed.
    # No reason to remove the test so we flip is_same to True
    pair_def = {
        'is_same': True,
        'mmsi': 205316000,

        'ts1': '2014-08-06 12:59:09',
        'lon1': 3.1858482361,
        'lat1': 51.635723114,

        'ts2': '2014-08-07 03:09:27',
        'lon2': 5.1019883156,
        'lat2': 53.8683776855,
    }

    with gpsdio.open(process('tests/data/point-pair/205316000.msg.gz', tmpdir)) as src:
        assert compare(pair_def, src)


def test_227317360(tmpdir):
    pair_tests = [
        {
            'is_same': False,
            'mmsi': 227317360,

            'ts1': '2014-08-06 01:21:47',
            'lon1': 1.5701249838,
            'lat1': 50.7410850525,

            'ts2': '2014-08-07 01:38:35',
            'lat2': 50.7266807556,
            'lon2': 1.5989949703,
        },
        {
            'is_same': False,
            'mmsi': 227317360,

            'ts1': '2014-08-07 01:38:35',
            'lon1': 1.5989949703,
            'lat1': 50.7266807556,

            'ts2': '2014-08-19 00:37:46',
            'lon2': 1.5986549854,
            'lat2': 50.7268600464,
        }
    ]

    for pair_def in pair_tests:
        with gpsdio.open(process('tests/data/point-pair/227317360.msg.gz', tmpdir)) as src:
            assert compare(pair_def, src)


def test_230942350(tmpdir):
    pair_def = {
        'is_same': False,
        'mmsi': 230942350,

        'ts1': '2014-08-22 05:35:51',
        'lon1': 21.21557426,
        'lat1': 62.37387848,

        'ts2': '2014-08-24 21:46:28',
        'lon2': 21.21780968,
        'lat2': 62.37731934,
    }

    with gpsdio.open(process('tests/data/point-pair/230942350.msg.gz', tmpdir)) as src:
        assert compare(pair_def, src)
