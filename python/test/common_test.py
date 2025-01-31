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

"""Tests for common Pipelines."""

import logging
import os
import sys
import unittest

# Fix up paths for running tests.
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from pipeline import common
from pipeline import pipeline, testing as test_shared
import testutil


class CommonTest(test_shared.TaskRunningMixin, testutil.TestSetupMixin, unittest.TestCase):

  def testReturn(self):
    self.assertEqual(
        1234, self.run_pipeline(common.Return(1234)).default.value)
    self.assertEqual(
        'hi there',
        self.run_pipeline(common.Return('hi there')).default.value)
    self.assertTrue(self.run_pipeline(common.Return()).default.value is None)

  def testIgnore(self):
    self.assertTrue(
        self.run_pipeline(common.Ignore(1, 2, 3, 4)).default.value is None)

  def testDict(self):
    self.assertEqual(
      {
        'one': 'red',
        'two': 12345,
        'three': [5, 'hello', 6.7],
      },
      self.run_pipeline(
          common.Dict(one='red',
                      two=12345,
                      three=[5, 'hello', 6.7])).default.value)

  def testList(self):
    self.assertEqual(
        [5, 'hello', 6.7],
        self.run_pipeline(common.List(5, 'hello', 6.7)).default.value)

  def testAbortIfTrue_Abort(self):
    try:
      self.run_pipeline(common.AbortIfTrue(True, message='Forced an abort'))
      self.fail('Should have raised')
    except pipeline.Abort as e:
      self.assertEqual('Forced an abort', str(e))

  def testAbortIfTrue_DoNotAbort(self):
    self.run_pipeline(common.AbortIfTrue(False, message='Should not abort'))

  def testAll(self):
    self.assertFalse(self.run_pipeline(common.All()).default.value)
    self.assertFalse(self.run_pipeline(common.All(False, False)).default.value)
    self.assertFalse(self.run_pipeline(common.All(False, True)).default.value)
    self.assertTrue(self.run_pipeline(common.All(True, True)).default.value)

  def testAny(self):
    self.assertFalse(self.run_pipeline(common.Any()).default.value)
    self.assertFalse(self.run_pipeline(common.Any(False, False)).default.value)
    self.assertTrue(self.run_pipeline(common.Any(False, True)).default.value)
    self.assertTrue(self.run_pipeline(common.Any(True, True)).default.value)

  def testComplement(self):
    self.assertEqual(True, self.run_pipeline(
        common.Complement(False)).default.value)
    self.assertEqual(False, self.run_pipeline(
        common.Complement(True)).default.value)
    self.assertEqual([False, True], self.run_pipeline(
        common.Complement(True, False)).default.value)

  def testMax(self):
    self.assertEqual(10, self.run_pipeline(common.Max(1, 10, 5)).default.value)
    self.assertEqual(22, self.run_pipeline(common.Max(22)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Max)

  def testMin(self):
    self.assertEqual(1, self.run_pipeline(common.Min(1, 10, 5)).default.value)
    self.assertEqual(22, self.run_pipeline(common.Min(22)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Min)

  def testSum(self):
    self.assertEqual(16, self.run_pipeline(common.Sum(1, 10, 5)).default.value)
    self.assertEqual(22, self.run_pipeline(common.Sum(22)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Sum)

  def testMultiply(self):
    self.assertEqual(50, self.run_pipeline(
        common.Multiply(1, 10, 5)).default.value)
    self.assertEqual(22, self.run_pipeline(
        common.Multiply(22)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Multiply)

  def testNegate(self):
    self.assertEqual(-20, self.run_pipeline(
        common.Negate(20)).default.value)
    self.assertEqual(20, self.run_pipeline(
        common.Negate(-20)).default.value)
    self.assertEqual([-20, 15, -2], self.run_pipeline(
        common.Negate(20, -15, 2)).default.value)
    self.assertRaises(TypeError, self.run_pipeline, common.Negate)

  def testExtend(self):
    self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8], self.run_pipeline(
        common.Extend([1, 2, 3], (4, 5, 6), [7], (8,))).default.value)
    self.assertEqual([], self.run_pipeline(
        common.Extend([], (), [], ())).default.value)
    self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8], self.run_pipeline(
        common.Extend([1, 2, 3], [], (4, 5, 6), (), [7], (8,))).default.value)
    self.assertEqual([[1, 2, 3], [4, 5, 6], [7], [8]], self.run_pipeline(
        common.Extend([[1, 2, 3], [4, 5, 6], [7], [8]])).default.value)
    self.assertEqual([], self.run_pipeline(common.Extend()).default.value)

  def testAppend(self):
    self.assertEqual([[1, 2, 3], [4, 5, 6], [7], [8]], self.run_pipeline(
        common.Append([1, 2, 3], [4, 5, 6], [7], [8])).default.value)
    self.assertEqual([[], [], [], []], self.run_pipeline(
        common.Append([], [], [], [])).default.value)
    self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8], self.run_pipeline(
        common.Append(1, 2, 3, 4, 5, 6, 7, 8)).default.value)
    self.assertEqual([], self.run_pipeline(common.Append()).default.value)

  def testConcat(self):
    self.assertEqual('somestringshere', self.run_pipeline(
        common.Concat('some', 'strings', 'here')).default.value)
    self.assertEqual('some|strings|here', self.run_pipeline(
        common.Concat('some', 'strings', 'here', separator='|')).default.value)
    self.assertEqual('', self.run_pipeline(common.Concat()).default.value)

  def testUnion(self):
    self.assertEqual(list({1, 2, 3, 4}), self.run_pipeline(
        common.Union([1], [], [2, 3], [], [4])).default.value)
    self.assertEqual([], self.run_pipeline(
        common.Union([], [], [], [])).default.value)
    self.assertEqual([], self.run_pipeline(common.Union()).default.value)

  def testIntersection(self):
    self.assertEqual(list({1, 3}), self.run_pipeline(
        common.Intersection([1, 2, 3], [1, 3, 7], [0, 3, 1])).default.value)
    self.assertEqual([], self.run_pipeline(
        common.Intersection([1, 2, 3], [4, 5, 6], [7, 8, 9])).default.value)
    self.assertEqual([], self.run_pipeline(
        common.Intersection([], [], [])).default.value)
    self.assertEqual(
        [], self.run_pipeline(common.Intersection()).default.value)

  def testUniquify(self):
    self.assertEqual({3, 2, 1}, set(self.run_pipeline(
        common.Uniquify(1, 2, 3, 3, 2, 1)).default.value))
    self.assertEqual([], self.run_pipeline(common.Uniquify()).default.value)

  def testFormat(self):
    self.assertEqual('this red 14 message', self.run_pipeline(
        common.Format.tuple('this %s %d message', 'red', 14)).default.value)
    self.assertEqual('this red 14 message', self.run_pipeline(
        common.Format.dict(
            'this %(mystring)s %(mynumber)d message',
            mystring='red', mynumber=14)
        ).default.value)
    self.assertEqual('a string here', self.run_pipeline(
        common.Format.tuple('a string here')).default.value)
    self.assertRaises(pipeline.Abort, self.run_pipeline,
        common.Format('blah', 'silly message'))

  def testLog(self):
    saved = []
    def SaveArgs(*args, **kwargs):
      saved.append((args, kwargs))

    self.assertEqual(None, self.run_pipeline(
        common.Log.log(logging.INFO, 'log then %s %d', 'hi', 44)).default.value)

    old_log = common.Log._log_method
    common.Log._log_method = SaveArgs
    try:
      self.run_pipeline(common.Log.log(-333, 'log then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.debug('debug then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.info('info then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.warning('warning then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.error('error then %s %d', 'hi', 44))
      self.run_pipeline(common.Log.critical('critical then %s %d', 'hi', 44))
    finally:
      common.Log._log_method = old_log

    self.assertEqual(saved,
        [
            ((-333, 'log then %s %d', 'hi', 44), {}),
            ((10, 'debug then %s %d', 'hi', 44), {}),
            ((20, 'info then %s %d', 'hi', 44), {}),
            ((30, 'warning then %s %d', 'hi', 44), {}),
            ((40, 'error then %s %d', 'hi', 44), {}),
            ((50, 'critical then %s %d', 'hi', 44), {})
        ])


class CommonTestModeTest(test_shared.TestModeMixin, CommonTest):
  """Runs all the common library tests in test mode.

  To ensure they can be reused by users in their own functional tests.
  """

  DO_NOT_DELETE = "Seriously... We only need the class declaration."


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
