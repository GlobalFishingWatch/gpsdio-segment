==============
gpsdio-segment
==============

.. image:: https://magnum.travis-ci.com/SkyTruth/gpsdio-segment.svg?token=tu7qmzYG3ruJYdnto4aT
    :target: https://magnum.travis-ci.com/SkyTruth/gpsdio-segment


Segment a stream of messages into continuous tracks.


Usage
-----

.. code-block:: console

    Usage: gpsdio segment [OPTIONS] INFILE OUTFILE

      Segment AIS data into continuous segments.

    Options:
      --version            Show the version and exit.
      --mmsi INTEGER       Only segment this MMSI.  If not given the first MMSI
                           found will be used.
      --max-hours FLOAT    Points with a time delta larger than N hours are forced
                           to be discontinuous. (default: 24)
      --max-speed FLOAT    Units are knots.  Points with a speed above this value
                           are always considered discontinuous. (default: 30)
      --noise-dist FLOAT   DEPRECATED. Units are nautical miles.  Points within this distance
                           are always considered continuous.  Used to allow a
                           certain amount of GPS noise. (default: 0.27)
      --segment-field TEXT Add the segment ID to this field when writing messages.
                           (default: segment)
      --help               Show this message and exit.


Installing
----------

.. code-block:: console

    $ git clone https://github.com/SkyTruth/gpsdio-segment
    $ pip install gpsdio-segment/


Developing
----------

.. code-block:: console

    $ git clone https://github.com/SkyTruth/gpsdio-segment
    $ cd gpsdio-segment
    $ virtualenv venv
    $ source venv/bin/activate
    $ pip install -e .\[dev\]
    $ py.test tests --cov gpsdio_segment --cov-report term-missing


License
-------

See ``LICENSE.txt``
