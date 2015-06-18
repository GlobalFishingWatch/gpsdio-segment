==============
gpsdio-segment
==============

.. image:: https://codeship.com/projects/640e1460-f82b-0132-6b38-021f9e2ec51a/status?branch=master
    :target: https://codeship.com/projects/86547

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
                           are always considered discontinuous. (default: 40)
      --noise-dist FLOAT   Units are nautical miles.  Points within this distance
                           are always considered continuous.  Used to allow a
                           certain amount of GPS noise. (default: 0.27)
      --series-field TEXT  Add the segment ID to this field when writing messages.
                           (default: series)
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
    $ pip install -e .[test]
    $ py.test tests --cov gpsdio_segment --cov-report term-missing


License
-------

See ``LICENSE.txt``.
