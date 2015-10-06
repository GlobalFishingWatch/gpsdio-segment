"""
Tools for segmenting positional AIS messages into continuous tracks.

Includes a CLI plugin for `gpsdio` to run the algorithm.
"""


import logging

from gpsdio_segment.core import Segment
from gpsdio_segment.core import Segmentizer


logger = logging.getLogger('gpsdio-segment')


__version__ = '0.2'
__author__ = 'Kevin Wurster'
__email__ = 'kevin@skytruth.org'
__source__ = 'https://github.com/SkyTruth/gpsdio-segment'
__license__ = """
Copyright 2015 SkyTruth

Authors: Kevin Wurster <kevin@skytruth.org>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
