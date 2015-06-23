"""
[
    # Done
    # {
    #     'lat1': '-21.3278064728',
    #     'lat2': '-20.7386302948',
    #     'lon1': '-96.2392425537',
    #     'lon2': '-95.8099060059',
    #     'mmsi': '224051350',
    #     'notes': '',
    #     'requirement': 'Continuous track not split',
    #     'should be': 'same track',
    #     'timestamp1': '2015-01-01 01:53:24',
    #     'timestamp2': '2015-01-31 23:31:54'
    # },
    {
        'lat1': '9.6271667481',
        'lat2': '9.9744997025',
        'lon1': '-33.0719985962',
        'lon2': '-32.3031654358',
        'mmsi': '431704490',
        'notes': '',
        'requirement': '',
        'should be': 'same track',
        'timestamp1': '2015-01-02 16:38:31',
        'timestamp2': '2015-01-04 07:45:00'
    },
     {
        'lat1': '9.6271667481',
        'lat2': '-90',
        'lon1': '-33.0719985962',
        'lon2': '-81.2001647949',
        'mmsi': '431704490',
        'notes': '',
        'requirement': 'Outliers seperated out',
        'should be': 'different tracks',
        'timestamp1': '2015-01-02 16:38:31',
        'timestamp2': '2015-01-02 16:41:18'
    },
    {
        'lat1': '8.0314998627',
        'lat2': '9.7333335877',
        'lon1': '-33.4316673279',
        'lon2': '-33.4386672974',
        'mmsi': '431704490',
        'notes': '',
        'requirement': '',
        'should be': 'different tracks',
        'timestamp1': '2015-01-12 12:14:20',
        'timestamp2': '2015-01-12 12:16:54'
    },
    {
        'lat1': '10.87963486',
        'lat2': '17.7059803',
        'lon1': '-74.73930359',
        'lon2': '83.27050781',
        'mmsi': '240000000',
        'notes': '',
        'requirement': 'Vessels far apart are separate',
        'should be': 'different tracks',
        'timestamp1': '2015-01-03 04:52:25',
        'timestamp2': '2015-01-03 05:04:50'
    },
    {
        'lat1': '41.0997772217',
        'lat2': '41.1031188965',
        'lon1': '-9.8415250778',
        'lon2': '-9.8284282684',
        'mmsi': '204814000',
        'notes': 'seems to be due to timestamp error',
        'requirement': 'Ave. speed too great',
        'should be': 'different tracks',
        'timestamp1': '2014-08-02 00:20:10',
        'timestamp2': '2014-08-02 00:20:15'
    },
    {
        'lat1': '41.7655258179',
        'lat2': '41.0869140625',
        'lon1': '3.1034851074',
        'lon2': '3.1313149929',
        'mmsi': '224108190',
        'notes': '',
        'requirement': '',
        'should be': 'different tracks',
        'timestamp1': '2014-08-24 21:24:01',
        'timestamp2': '2014-08-24 21:37:48'
    },
    {
        'lat1': '33.3282089233',
        'lat2': '33.5084609985',
        'lon1': '120.225990295',
        'lon2': '120.141143799',
        'mmsi': '600011817',
        'notes': '',
        'requirement': '',
        'should be': 'different tracks',
        'timestamp1': '2014-08-19 02:14:59',
        'timestamp2': '2014-08-19 02:26:59'
    },
    {
        'lat1': '37.88721848',
        'lat2': '37.90121841',
        'lon1': '-10.73543358',
        'lon2': '-10.72798347',
        'mmsi': '538004505',
        'notes': 'likely timestamp error',
        'requirement': '',
        'should be': 'different tracks',
        'timestamp1': '2014-08-29 19:08:16',
        'timestamp2': '2014-08-29 19:09:06'
    },
    {
        'lat1': '51.635723114',
        'lat2': '53.8683776855',
        'lon1': '3.1858482361',
        'lon2': '5.1019883156',
        'mmsi': '205316000',
        'notes': 'lets consider if we really want to break a track at this distance threshold',
        'requirement': 'distance greater than 100 nm',
        'should be': 'different tracks',
        'timestamp1': '2014-08-06 12:59:09',
        'timestamp2': '2014-08-07 03:09:27'
    },
    {
        'lat1': '50.7410850525',
        'lat2': '50.7266807556',
        'lon1': '1.5701249838',
        'lon2': '1.5989949703',
        'mmsi': '227317360',
        'notes': '',
        'requirement': 'time gap over 24 hr',
        'should be': 'different tracks',
        'timestamp1': '2014-08-06 01:21:47',
        'timestamp2': '2014-08-07 01:38:35'
    },
    {
        'lat1': '50.7266807556',
        'lat2': '50.7268600464',
        'lon1': '1.5989949703',
        'lon2': '1.5986549854',
        'mmsi': '227317360',
        'notes': 'vessel reappears in same position after several days, track should break',
        'requirement': '',
        'should be': 'different tracks',
        'timestamp1': '2014-08-07 01:38:35',
        'timestamp2': '2014-08-19 00:37:46'
    },
    {
        'lat1': '62.37387848',
        'lat2': '62.37731934',
        'lon1': '21.21557426',
        'lon2': '21.21780968',
        'mmsi': '230942350',
        'notes': '',
        'requirement': '',
        'should be': 'different tracks',
        'timestamp1': '2014-08-22 05:35:51',
        'timestamp2': '2014-08-24 21:46:28'
    }
]
"""


from datetime import datetime
import os

from click.testing import CliRunner
import py._error

import gpsdio
import gpsdio.cli.main



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

    assert msg1['series'] is not None
    assert msg2['series'] is not None
    return (msg1['series'] == msg2['series']) == pair_def['is_same']


def test_224051350(tmpdir):
    # These two points are the start and endpoint for a segment so they should
    # share the same ID.

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


# def test_240000000(tmpdir):
#
#     """
#     mmsi	    timestamp1	        lon1	        lat1	    timestamp2              lon2	    lat2
#     240000000   2015-01-03 04:52:25	-74.73930359	10.87963486 2015-01-03  05:04:50	83.27050781	17.7059803
#     """
#
#     pair_def = {
#         'is_same': False,
#         'mmsi': 240000000,
#
#         'ts1': datetime(year=2015, month=1, day=3, hour=4, minute=52, second=25),
#         'lon1': -74.73930359,
#         'lat1': 10.87963486,
#
#         'ts2': datetime(year=2015, month=1, day=3, hour=5, minute=4, second=50),
#         'lon2': 83.27050781,
#         'lat2': 17.7059803
#     }
#
#     with gpsdio.open(process('tests/data/point-pair/240000000.msg.gz', tmpdir)) as src:
#         pass
#         # assert compare(pair_def, src)
