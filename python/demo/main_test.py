#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Functional tests for the demo application."""

import logging
import sys
import unittest

# Fix up paths for running tests.
sys.path.insert(0, '../src/')

from appengine_pipeline.test import testutil

from . import main


class CountReportTest(unittest.TestCase):

  def setUp(self):
    testutil.setup_for_testing()

  def testSimple(self):
    main.GuestbookPost(color='red').put()
    main.GuestbookPost(color='red').put()
    main.GuestbookPost(color='blue').put()
    main.GuestbookPost(color='green').put()
    job = main.CountReport(
        'foo@example.com',
        main.GuestbookPost._get_kind(),
        'color',
        'red', 'green', 'blue')
    job.start_test()
    self.assertEqual(4, job.outputs.default.value)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
