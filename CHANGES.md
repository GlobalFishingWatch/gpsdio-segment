Changelog
=========

DEV
-------------------
* 

0.12
-------------------
* [#60](https://github.com/SkyTruth/gpsdio-segment/pull/60) 
  Prefer segments with the same message type


0.11 - (2017-12-31)
-------------------

* ['#54'](https://github.com/SkyTruth/gpsdio-segment/pull/54)
  Ignore noise segments in Segmentizer.from_seg_states()

* ['#56'](https://github.com/SkyTruth/gpsdio-segment/pull/56)
  Performance improvement for the special case where all messages added to a segment are 
  non-positional (have no lat/lon as is the case with type 5 AIS messages)

0.10 - (2017-12-22)
-------------------

* ['#50'](https://github.com/SkyTruth/gpsdio-segment/pull/50)
  Emit noise messages in a new segment class `NoiseSegment` that works like 
  `BadSegment`

* ['#49'](https://github.com/SkyTruth/gpsdio-segment/pull/49)
  New option `collect_match_stats` for Segmentizer that captures all the stats used to 
  determine which segment a message is added to.  The stats are added to the message in 
  a field called `segment_matches`

  
0.9 - (2017-10-18)
------------------

- New parameters that reduce the allowable speed at distance and handle noise better #42


0.8 - (2017-08-22)
------------------

- Documentation and reorganization


0.7 - (2017-01-26)
------------------

- Bugfix for no reported speed.


0.6 - (2016-08-30)
------------------

- Improved segmenting algorithm to handle high speed better .  #32, #34


0.5 - (2016-06-10)
------------------

- Better handling bad locations to prevent subsequent positions from being added to `BadSegment()`.  #29


0.4 - (2016-05-23)
------------------

- Handle an edge case where a non-posit is the first message encountered after clearing out _all_ segments.  #24


0.3 - (2016-05-17)
------------------

- Better handling for out-of-bounds locations.  #17
- Shush logging.  #19


0.2 - (2015-10-06)
------------------

- Added states for Segmentizer and Segments.  #13


0.1 - (2015-06-24)
------------------

- Initial release.
