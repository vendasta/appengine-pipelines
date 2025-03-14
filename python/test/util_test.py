#!/usr/bin/env python
"""Tests for util.py."""

import datetime
import logging
import os
import sys
import unittest

# Fix up paths for running tests.
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from pipeline import util

from google.appengine.api import taskqueue


class JsonSerializationTest(unittest.TestCase):
  """Test custom json encoder and decoder."""

  def testE2e(self):
    now = datetime.datetime.now()
    obj = {"a": 1, "b": [{"c": "d"}], "e": now}
    new_obj = util.json.loads(util.json.dumps(
        obj, cls=util.JsonEncoder), cls=util.JsonDecoder)
    self.assertEqual(obj, new_obj)


class GetTaskTargetTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    os.environ["GAE_VERSION"] = "v7.1"
    os.environ["GAE_SERVICE"] = "foo-module"

  def testGetTaskTarget(self):
    self.assertEqual("v7.foo-module", util._get_task_target())
    task = taskqueue.Task(url="/relative_url",
                          target=util._get_task_target())
    self.assertEqual("v7.foo-module", task.target)

  def testGetTaskTargetDefaultModule(self):
    os.environ["GAE_SERVICE"] = "default"
    self.assertEqual("v7.default", util._get_task_target())
    task = taskqueue.Task(url="/relative_url",
                          target=util._get_task_target())
    self.assertEqual("v7.default", task.target)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
