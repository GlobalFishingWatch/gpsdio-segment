# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## 3.0.0 - 2022-07-15

### Added

  add even more additional sorting to stabilize remove stale segments

  add additional sorting to stabilize remove stale segments

  simplify checks for bad message

  fix tests now that we are relying on input being sanitized of bad values

  better null checks

  check using is_null not is None

  switch checks to take into acount Andres's fixes to the segmenter that convert invalid values to None

  now that bad courses become nans correctly handle them

  [PR#85](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/85): Improve seg id generation so we don't get occasional duplicate seg ids.
  Changes the segmenter identity matching logic [PR#73](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/73)
    This changes the identity matching window to 15 minutes, since 6 minutes
    is too low for the data quality we have on 2012. It also removes the
    receiver from the identity cache keys, because that field is causing a
    lot of problems due to hard inconsistencies.
  Updates the version in the __init__.py
  add basic tests for current identity assignment [PR#74](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/74)
  move identity  addition so it works forward and backwards; adjust tests [PR#74](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/74)
  Adds version update information
  Adds changelog information for the last 2 releases
  run through black, isort, and flak8
  start adding comments about where we should refactor
  first pass of refactor; break up process in small functions; py.test passes and flake8 happy
  break out matching code to separate class
  break out msg processing code into separate class
  fix imports with isort
  Removed class inheritance on Segmentizer as it no longer needs DiscrepancyCalculator functions. The only one is uses is a static method so it is now called directly from the class rather than from self. [PR#77](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/77)
  Updated a few loop yields to use  instead for cleaner code
  Changed one last yield for loop to a
  Added docstrings to all internal functions. Renamed `_clean` to `_clean_segment` for more clarity.
  Added docstrings to DiscrepancyCalculator class. Change one function to be denoted internal with underscore.
  Some flake8 cleanup
  Finished docstrings
  Changed Segmentizer to Segmenter and placed in segmenter.py to maintain consistency. core.py is kept with pass-through import and Segmentizer class.
  Ran pre-commit on all files for linting/styling
  add new regression tests
  Updated the path to the repo in the README.rst
  Changed query that pulls expected tracks for regression testing to be deterministic by adding msgid to sort criteria. Update .pre-commit-config.yaml because there were so many conflicts to reorder checks because isort and flake8 were colliding.
  Modified the gitignore for .ipynb_checkpoints/
  Fixed a bug in test_edge_cases where test messages were not given types and pytest was passing on the wrong type of ValueError. Modified to add type to messages and explicitly check that ValueError message mentioned 'unsorted'
  Added new unit tests aimed at msg_processor.py. Fixed a bug and duplicate code in msg_processor.py. Moved a test from test_edge_cases.py to test_msg_processor.py since it was more relevant there.
  Added a readme to gpsdio_segment/ with diagram for how a message moves through the segmenter.
  Added unit test for last BAD_MESSAGE test.
  Removed SAFE_SPEED check and updated unit tests to reflect
  Added link between the two READMEs
  Update README.rst
  allow newish builting timezone objects
  Atomic identities ([PR#78](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/78))
  switch to atomic identities
  fix identity storage
  another identity fix
  still trying to get something for identities that will work well pipe-segment
  add destintions; clean out unused code
  make destination have tuple type similar to identity
  add some debug logging to track down where all my data went
  add more debug logging to track down where all my data went
  turn down logging
  refactor to allow pipe-segment to use metrics
  bump default hours to 24
  protect against negative hours
  fix all fast tests; comment out identity tests since they need to be redone
  Updated identity unit tests to reflect new atomic identities. ([PR#81](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/81))
  Co-authored-by: Jenn Van Osdel <jenn@globalfishingwatch.org>
  Updated regression tests to reflect atomic identities [PR#82](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/82). Some segmenter changes made segments span over more days, changing the expected seg_id for many messages. Some SSVID regression tests were automatically deleted as they did not have data in 2021010* as at some point these files had been created for the larger time range of 202101*. Biggest change is converting new namedtuples for Identity and Destination into JSON serializable objects in test_expected.add_expected().
  fix for [Issue#83](https://github.com/GlobalFishingWatch/gpsdio-segment/issues/83)-[PR#84](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/84) test_unsorted transient failure
  untested implementation of new way to generate ids
  fix spelling of itertools
    Co-authored-by: Andres Arana <andres@globalfishingwatch.org>
    Co-authored-by: Andrés Arana <and2arana@gmail.com>
    Co-authored-by: Matias Piano <matias@globalfishingwatch.org>
    Co-authored-by: Jenn Van Osdel <jenn@globalfishingwatch.org>
    Co-authored-by: jennvanosdel <jennifervo07@gmail.com>
    Co-authored-by: pwoods25443 <pwoods25443@gmail.com>

  [PR#72](https://github.com/GlobalFishingWatch/gpsdio-segment/pull/72) Pipe 3 Segmenter Improvements / Add Stitcher
  make sticher work on day by day identity instead of cumjutive identity info
  updated sticher version that uses lookahead
  filter empty segments before sort to avoid comparison issues
  bump max hours back up to 8
  tweak stitcher
  update tests
  fix tests so work under py27 and py37
  use identity count to improve longterm stability
  improve stitcher performance
  start implementing framework for looking at active tracks
  trim number of active tracks to try to eleiminate hot spots
  trim number of active tracks further
  fix bug that resulted in make_tracks hanging
  loosen tolerances, but make dependent on track number
  bulletproof index function
  bug fixes to improve performance, tweaks to improve results
  add index to tracks; clean up code somewhat
  Support VMS and require type field in messages.
  switch back to dev tag
  bump version to make it clearer that we're working on tracks
  emit active status of track
  tweak metrics to improve stitching
  minor tuning tweaks / cleanup of metrics
  switch to primarily using namedtuples in track generation, begin working on support items for next prototype
  commit still working version before tearing the rest of the way apart to try MHT
  cleaned up MHT version
  allow placeholder segments
  allow placeholder segments - fix
  add more fields to Track so we keep track of last message
  skip sig checks for now when first seg is missing
  Keep track of signature in tracks
  clean up costs a bit
  remove unused track_sigs parameter
  Protect signature cost function from overflow
  another try at preventing overflow in signature code
  protect against small magnitude vectors
  catch errors in sig calculator and log warning instead
  add notebook showing examples of track based voyages
  update TrackBasedVoyages notebook
  update to use daily message counts
  clarify logic when pruning tracks
  fix bug in warning for large decay
  fix bug in assembling identity data for tracks
  add ommitted sig_cost
  lower weight of signature, as it was breaking causing trouble
  reduce weight for signature
  add logging of costs; reduce signature weight
  Fix typo in setting debug level
  debug signatures, tweak values
  use all of signature for matching
  fix signatures so intermediate dates get correct values
  use last message of day rather than first because it's always available
  store whole previous tracks instead of just historical signatures
  switch from storing tracks_by_date to parent_track
  bump warning up to debug so logging doesn't get swamped
  only use last_msg from track to avoid reconstituting fake segments
  use last_message for cost key rather than segment
  remove unnecessary test for seg_ids / prefix
  tweak parameters to fix up some of the examples after recent changes
  sort by last message of day to be consistent with way we reconstitute tracks
  cleanup and try to eliminate difference between running daily and batch
  more changes to make less sensitive to restarts
  bug fix
  reinstate length cost; more cleanup
  try fix duplicate segment emission; updates to trackidexample
  add destination to recorded fields; tweak stitcher params
  fix bug where n_destinations left in add_info
  add daily message counts to segments
  support destination as a signature value
  merge changes from develop
  add destinations, lengths and widths to signature
  bug fix; missing lengths
  add missing argumnents when creating segment
  port idenity changes from 2.5

  undo changes meant for other branch

  Add changes; bump version to 3.0

  ignore notebooks in ipynb format



## 0.20.2 - 2020-10-13

### Fixes

* [PIPELINE-144](https://globalfishingwatch.atlassian.net/browse/PIPELINE-144):
  Fixes the identity assignment logic so that identity messages appearing after
  a batch of positional messages are not discarded.

## 0.20.1 - 2020-10-06

### Changes

* [PIPELINE-139](https://globalfishingwatch.atlassian.net/browse/PIPELINE-139):
  Removes the receiver from the identity cache because that field is completely
  broken.

## 0.20 - 2020-04-07

### Added

* [GlobalFishingWatch/gfw-eng-tasks#47](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/47): Adds
    guard code over `course` field when has None value when normalize message.

## 0.19

### Changes

* Change to a metric based primarily on "discrepancy" which is how far the vessel is from where we expect it to be based on its course and speed.
* What we mean by expect is informed by the experiments in extracting probability clouds for AIS matching.
* Many heuristics are applied based on extensive experimentation, but more experimentation would still be helpful.

## 0.18

### Changes

* [GlobalFishingWatch/GFW-Tasks#1015](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1015)
  * Remove checks for name/callsign when assigning segments.
  * Never check for transceiver type.
  * Add type 3 messages to class A message types.

## 0.17

### Changes
* Fix version of pyproj to avoid going to 2.X

## 0.16

### ADDED
* [#61](https://github.com/SkyTruth/gpsdio-segment/issues/61)
  Prefer segments with matching shipname and/or callsign
* [#63](https://github.com/SkyTruth/gpsdio-segment/issues/63)
  Treat a duplicate timestamp that is within noise distance from an existing segment as noise
* [#66](https://github.com/SkyTruth/gpsdio-segment/issues/66)
  When no shipname or callsign matches, prefer the segment with the most recent position

## 0.12
* [#60](https://github.com/SkyTruth/gpsdio-segment/pull/60) 
  Prefer segments with the same message type


## 0.11 - (2017-12-31)

* ['#54'](https://github.com/SkyTruth/gpsdio-segment/pull/54)
  Ignore noise segments in Segmentizer.from_seg_states()

* ['#56'](https://github.com/SkyTruth/gpsdio-segment/pull/56)
  Performance improvement for the special case where all messages added to a segment are 
  non-positional (have no lat/lon as is the case with type 5 AIS messages)

## 0.10 - (2017-12-22)

* ['#50'](https://github.com/SkyTruth/gpsdio-segment/pull/50)
  Emit noise messages in a new segment class `NoiseSegment` that works like 
  `BadSegment`

* ['#49'](https://github.com/SkyTruth/gpsdio-segment/pull/49)
  New option `collect_match_stats` for Segmentizer that captures all the stats used to 
  determine which segment a message is added to.  The stats are added to the message in 
  a field called `segment_matches`

  
## 0.9 - (2017-10-18)

- New parameters that reduce the allowable speed at distance and handle noise better #42


## 0.8 - (2017-08-22)

- Documentation and reorganization


## 0.7 - (2017-01-26)

- Bugfix for no reported speed.


## 0.6 - (2016-08-30)

- Improved segmenting algorithm to handle high speed better .  #32, #34


## 0.5 - (2016-06-10)

- Better handling bad locations to prevent subsequent positions from being added to `BadSegment()`.  #29


## 0.4 - (2016-05-23)

- Handle an edge case where a non-posit is the first message encountered after clearing out _all_ segments.  #24


## 0.3 - (2016-05-17)

- Better handling for out-of-bounds locations.  #17
- Shush logging.  #19


## 0.2 - (2015-10-06)

- Added states for Segmentizer and Segments.  #13


## 0.1 - (2015-06-24)

- Initial release.
