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

"""Tests for the Pipeline API."""

import base64
import datetime
import functools
import json
import logging
import os
import pickle
import sys
import unittest
import urllib.error
import urllib.parse
import urllib.request

# Fix up paths for running tests.
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

import testutil
from flask import Flask
from google.appengine.ext import ndb, testbed
from google.appengine.api.datastore_errors import BadRequestError

from pipeline import common, pipeline, storage, testing as test_shared

# For convenience.
_BarrierIndex = pipeline.models._BarrierIndex
_BarrierRecord = pipeline.models._BarrierRecord
_PipelineRecord = pipeline.models._PipelineRecord
_SlotRecord = pipeline.models._SlotRecord
_StatusRecord = pipeline.models._StatusRecord


class TestBase(testutil.TestSetupMixin, unittest.TestCase):
  """Base class for all tests in this module."""

  def setUp(self):
    super(TestBase, self).setUp()
    self.maxDiff = 10**10
    # First, create an instance of the Testbed class.
    self.testbed = testbed.Testbed()
    # Then activate the testbed, which prepares the service stubs for use.
    self.testbed.activate()
    # Next, declare which service stubs you want to use.
    self.testbed.init_urlfetch_stub()
    self.testbed.init_app_identity_stub()
    self.testbed.init_datastore_v3_stub()
    ndb.get_context().clear_cache()

    self.storageData = {}
    def _write_json_gcs(encoded_value, pipeline_id=None):
      key = str(len(self.storageData))
      self.storageData.update({key: encoded_value})
      return key

    pipeline.write_json_gcs = _write_json_gcs
    pipeline.read_blob_gcs = lambda x: self.storageData.get(x)
    storage.write_json_gcs = _write_json_gcs
    storage.read_blob_gcs = lambda x: self.storageData.get(x)

  def tearDown(self):
    self.testbed.deactivate()

  def assertIn(self, the_thing, what_thing_should_be_in):
    """Asserts that something is contained in something else."""
    if the_thing not in what_thing_should_be_in:
      raise AssertionError('Could not find %r in %r' % (
                           the_thing, what_thing_should_be_in))


class SlotTest(TestBase):
  """Tests for the Slot class."""

  def testCreate(self):
    """Tests creating Slots with names and keys."""
    slot = pipeline.Slot(name='stuff')
    self.assertEqual('stuff', slot.name)
    self.assertTrue(slot.key)
    self.assertFalse(slot.filled)
    self.assertFalse(slot._exists)
    self.assertRaises(pipeline.SlotNotFilledError, lambda: slot.value)
    self.assertRaises(pipeline.SlotNotFilledError, lambda: slot.filler)
    self.assertRaises(pipeline.SlotNotFilledError, lambda: slot.fill_datetime)

    slot_key = ndb.Key('mykind', 'mykey')
    slot = pipeline.Slot(name='stuff', slot_key=slot_key)
    self.assertEqual('stuff', slot.name)
    self.assertEqual(slot_key, slot.key)
    self.assertFalse(slot.filled)
    self.assertTrue(slot._exists)

    self.assertRaises(pipeline.UnexpectedPipelineError, pipeline.Slot)

  def testSlotRecord(self):
    """Tests filling Slot attributes with a _SlotRecord."""
    slot_key = ndb.Key(_SlotRecord, 'myslot',)
    filler_key = ndb.Key(_PipelineRecord, 'myfiller')
    now = datetime.datetime.utcnow()
    slot_record = _SlotRecord(
        filler=filler_key,
        value_text=json.dumps('my value'),
        status=_SlotRecord.FILLED,
        fill_time=now)

    slot = pipeline.Slot(name='stuff', slot_key=slot_key)
    slot._set_value(slot_record)
    self.assertTrue(slot._exists)
    self.assertTrue(slot.filled)
    self.assertEqual('my value', slot.value)
    self.assertEqual(filler_key.string_id(), slot.filler)
    self.assertEqual(now, slot.fill_datetime)

  def testValueTestMode(self):
    """Tests filling Slot attributes for test mode."""
    slot_key = ndb.Key('myslot', 'mykey')
    filler_key = ndb.Key('myfiller', 'mykey')
    now = datetime.datetime.utcnow()
    value = 'my value'

    slot = pipeline.Slot(name='stuff', slot_key=slot_key)
    slot._set_value_test(filler_key, value)
    self.assertTrue(slot._exists)
    self.assertTrue(slot.filled)
    self.assertEqual('my value', slot.value)
    self.assertEqual(filler_key.string_id(), slot.filler)
    self.assertTrue(isinstance(slot.fill_datetime, datetime.datetime))


class PipelineFutureTest(TestBase):
  """Tests for the PipelineFuture class."""

  def testNormal(self):
    """Tests using a PipelineFuture in normal mode."""
    future = pipeline.PipelineFuture([])
    self.assertTrue('default' in future._output_dict)
    default = future.default
    self.assertTrue(isinstance(default, pipeline.Slot))
    self.assertFalse(default.filled)

    self.assertFalse('stuff' in future._output_dict)
    stuff = future.stuff
    self.assertTrue('stuff' in future._output_dict)
    self.assertNotEqual(stuff.key, default.key)
    self.assertTrue(isinstance(stuff, pipeline.Slot))
    self.assertFalse(stuff.filled)

  def testStrictMode(self):
    """Tests using a PipelineFuture that's in strict mode."""
    future = pipeline.PipelineFuture(['one', 'two'])
    self.assertTrue(future._strict)
    self.assertTrue('default' in future._output_dict)
    self.assertTrue('one' in future._output_dict)
    self.assertTrue('two' in future._output_dict)

    default = future.default
    self.assertTrue(isinstance(default, pipeline.Slot))
    self.assertFalse(default.filled)

    one = future.one
    self.assertTrue(isinstance(one, pipeline.Slot))
    self.assertFalse(one.filled)
    self.assertNotEqual(one.key, default.key)

    two = future.two
    self.assertTrue(isinstance(two, pipeline.Slot))
    self.assertFalse(two.filled)
    self.assertNotEqual(two.key, default.key)
    self.assertNotEqual(two.key, one.key)

    self.assertRaises(pipeline.SlotNotDeclaredError, lambda: future.three)

  def testReservedOutputs(self):
    """Tests reserved output slot names."""
    self.assertRaises(pipeline.UnexpectedPipelineError,
                      pipeline.PipelineFuture, ['default'])

  def testInheritOutputs(self):
    """Tests _inherit_outputs without resolving their values."""
    future = pipeline.PipelineFuture([])
    already_defined = {
        'one': ndb.Key(_SlotRecord, 'does not exist1').urlsafe().decode(),
        'two': ndb.Key(_SlotRecord, 'does not exist2').urlsafe().decode(),
        'three': ndb.Key(_SlotRecord, 'does not exist3').urlsafe().decode(),
        'default': ndb.Key(_SlotRecord, 'does not exist4').urlsafe().decode(),
    }
    future = pipeline.PipelineFuture([])
    self.assertFalse(future.default._exists)

    future._inherit_outputs('mypipeline', already_defined)

    self.assertEqual(already_defined['one'], future.one.key.urlsafe().decode())
    self.assertEqual(already_defined['two'], future.two.key.urlsafe().decode())
    self.assertEqual(already_defined['three'], future.three.key.urlsafe().decode())
    self.assertEqual(already_defined['default'], future.default.key.urlsafe().decode())

    self.assertTrue(future.one._exists)
    self.assertTrue(future.two._exists)
    self.assertTrue(future.three._exists)
    self.assertTrue(future.default._exists)

  def testInheritOutputsStrictMode(self):
    """Tests _inherit_outputs without resolving their values in strict mode."""
    already_defined = {
        'one': ndb.Key(_SlotRecord, 'does not exist1').urlsafe().decode(),
        'two': ndb.Key(_SlotRecord, 'does not exist2').urlsafe().decode(),
        'three': ndb.Key(_SlotRecord, 'does not exist3').urlsafe().decode(),
        'default': ndb.Key(_SlotRecord, 'does not exist4').urlsafe().decode(),
    }
    future = pipeline.PipelineFuture(['one', 'two', 'three'])

    self.assertFalse(future.one._exists)
    self.assertFalse(future.two._exists)
    self.assertFalse(future.three._exists)
    self.assertFalse(future.default._exists)

    future._inherit_outputs('mypipeline', already_defined)

    self.assertEqual(already_defined['one'], future.one.key.urlsafe().decode())
    self.assertEqual(already_defined['two'], future.two.key.urlsafe().decode())
    self.assertEqual(already_defined['three'], future.three.key.urlsafe().decode())
    self.assertEqual(already_defined['default'], future.default.key.urlsafe().decode())

    self.assertTrue(future.one._exists)
    self.assertTrue(future.two._exists)
    self.assertTrue(future.three._exists)
    self.assertTrue(future.default._exists)

  def testInheritOutputsStrictModeUndeclared(self):
    """Tests _inherit_outputs when an inherited output has not been declared."""
    already_defined = {
        'one': ndb.Key(_SlotRecord, 'does not exist1').urlsafe().decode(),
        'two': ndb.Key(_SlotRecord, 'does not exist2').urlsafe().decode(),
        'three': ndb.Key(_SlotRecord, 'does not exist3').urlsafe().decode(),
        'default': ndb.Key(_SlotRecord, 'does not exist4').urlsafe().decode(),
        'five': ndb.Key(_SlotRecord, 'does not exist5').urlsafe().decode(),
    }
    future = pipeline.PipelineFuture(['one', 'two', 'three'])
    self.assertRaises(pipeline.UnexpectedPipelineError, future._inherit_outputs,
                      'mypipeline', already_defined)

  def testInheritOutputsResolveValues(self):
    """Tests _inherit_outputs with resolving their current values."""
    one = _SlotRecord(
        value_text=json.dumps('hi one'),
        status=_SlotRecord.FILLED,
        fill_time=datetime.datetime.utcnow(),
        filler=ndb.Key(_PipelineRecord, 'mykey1'))
    one.put()

    two = _SlotRecord(
        value_text=json.dumps('hi two'),
        status=_SlotRecord.FILLED,
        fill_time=datetime.datetime.utcnow(),
        filler=ndb.Key(_PipelineRecord, 'mykey2'))
    two.put()

    three = _SlotRecord()
    three.put()

    default = _SlotRecord()
    default.put()

    already_defined = {
        'one': one.key.urlsafe().decode(),
        'two': two.key.urlsafe().decode(),
        'three': three.key.urlsafe().decode(),
        'default': default.key.urlsafe().decode(),
    }
    future = pipeline.PipelineFuture([])
    future._inherit_outputs('mypipeline', already_defined, resolve_outputs=True)

    self.assertEqual('hi one', future.one.value)
    self.assertEqual('hi two', future.two.value)
    self.assertFalse(future.three.filled)

  def testInheritOutputsResolveValuesMissing(self):
    """Tests when output _SlotRecords are missing for inherited outputs."""
    already_defined = {
        'four': ndb.Key(_SlotRecord, 'does not exist').urlsafe().decode(),
    }
    future = pipeline.PipelineFuture([])
    self.assertRaises(pipeline.UnexpectedPipelineError, future._inherit_outputs,
                      'mypipeline', already_defined, resolve_outputs=True)


class NothingPipeline(pipeline.Pipeline):
  """Pipeline that does nothing."""

  output_names = ['one', 'two']

  def run(self):
    self.fill('one', 1)
    self.fill('two', 1)


class OutputlessPipeline(pipeline.Pipeline):
  """Pipeline that outputs nothing."""

  def run(self):
    pass


class AsyncOutputlessPipeline(pipeline.Pipeline):
  """Pipeline that outputs nothing."""

  async_ = True

  def run(self):
    self.complete()


class AsyncCancellable(pipeline.Pipeline):
  """Pipeline that can be cancelled."""

  async_ = True

  def run(self):
    self.complete()

  def try_cancel(self):
    return True


class PipelineTest(TestBase):
  """Tests for the Pipeline class."""

  def testClassPath(self):
    """Tests the class path resolution class method."""
    module_dict = {}
    self.assertEqual(None, pipeline.Pipeline._class_path)
    pipeline.Pipeline._set_class_path(module_dict)
    self.assertEqual(None, pipeline.Pipeline._class_path)

    class MyModule(object):
      pass

    mymodule = MyModule()
    setattr(mymodule, 'NothingPipeline', NothingPipeline)

    # Does not require __main__.
    module_dict['other'] = mymodule
    NothingPipeline._set_class_path(module_dict=module_dict)
    self.assertEqual(f'{NothingPipeline.__module__}.NothingPipeline', NothingPipeline._class_path)

    # Will ignore __main__.
    NothingPipeline._class_path = None
    module_dict['__main__'] = mymodule
    NothingPipeline._set_class_path(module_dict=module_dict)
    self.assertEqual(f'{NothingPipeline.__module__}.NothingPipeline', NothingPipeline._class_path)

    # Will use __main__ as a last resort.
    NothingPipeline._class_path = None
    del module_dict['other']
    NothingPipeline._set_class_path(module_dict=module_dict)
    self.assertEqual(f'{NothingPipeline.__module__}.NothingPipeline', NothingPipeline._class_path)

  def testStart(self):
    """Tests starting a Pipeline."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    self.assertEqual(('one', 'two'), stage.args)
    self.assertEqual({'three': 'red', 'four': 1234}, stage.kwargs)

    self.assertTrue(stage.start() is None)
    self.assertEqual('default', stage.queue_name)
    self.assertEqual('/_ah/pipeline', stage.base_path)
    self.assertEqual(stage.pipeline_id, stage.root_pipeline_id)
    self.assertTrue(stage.is_root)

    pipeline_record = _PipelineRecord.get_by_id(stage.pipeline_id)
    self.assertTrue(pipeline_record is not None)
    self.assertEqual(f'{NothingPipeline.__module__}.NothingPipeline', pipeline_record.class_path)
    self.assertEqual(_PipelineRecord.WAITING, pipeline_record.status)

    params = pipeline_record.params
    self.assertEqual(params['args'],
        [{'type': 'value', 'value': 'one'}, {'type': 'value', 'value': 'two'}])
    self.assertEqual(params['kwargs'],
        {'four': {'type': 'value', 'value': 1234},
         'three': {'type': 'value', 'value': 'red'}})
    self.assertEqual([], params['after_all'])
    self.assertEqual('default', params['queue_name'])
    self.assertEqual('/_ah/pipeline', params['base_path'])
    self.assertEqual(set(NothingPipeline.output_names + ['default']),
                      set(params['output_slots'].keys()))
    self.assertTrue(pipeline_record.is_root_pipeline)
    self.assertTrue(isinstance(pipeline_record.start_time, datetime.datetime))

    # Verify that all output slots are present.
    slot_records = list(_SlotRecord.query(
      _SlotRecord.root_pipeline == ndb.Key(_PipelineRecord, stage.pipeline_id)))
    slot_dict = dict((s.key, s) for s in slot_records)
    self.assertEqual(3, len(slot_dict))

    for outputs in list(params['output_slots'].values()):
      slot_record = slot_dict[ndb.Key(urlsafe=outputs)]
      self.assertEqual(_SlotRecord.WAITING, slot_record.status)

    # Verify that trying to add another output slot will fail.
    self.assertRaises(pipeline.SlotNotDeclaredError,
                      lambda: stage.outputs.does_not_exist)

    # Verify that the slot existence has been set to true.
    for slot in list(stage.outputs._output_dict.values()):
      self.assertTrue(slot._exists)

    # Verify the enqueued task.
    task_list = test_shared.get_tasks()
    self.assertEqual(1, len(task_list))
    task = task_list[0]
    self.assertEqual(
        {'pipeline_key': [ndb.Key(_PipelineRecord, stage.pipeline_id).urlsafe().decode()]},
        task['params'])
    self.assertEqual('/_ah/pipeline/run', task['url'])

  def testStartIdempotenceKey(self):
    """Tests starting a pipeline with an idempotence key."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    self.assertTrue(stage.start(idempotence_key='banana') is None)
    self.assertEqual('banana', stage.pipeline_id)

  def testStartReturnTask(self):
    """Tests starting a pipeline and returning the kick-off task."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    task = stage.start(return_task=True, idempotence_key='banana')
    self.assertEqual(0, len(test_shared.get_tasks()))
    self.assertEqual('/_ah/pipeline/run', task.url)
    self.assertEqual(
        'pipeline_key=%s' % ndb.Key(_PipelineRecord, 'banana').urlsafe().decode(),
        task.payload)
    self.assertTrue(task.name is None)

  def testStartQueueName(self):
    """Tests that the start queue name will be preserved."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    self.assertTrue(stage.start(queue_name='other') is None)
    self.assertEqual(0, len(test_shared.get_tasks('default')))
    self.assertEqual(1, len(test_shared.get_tasks('other')))

  def testStartCountdown(self):
    """Tests starting a pipeline with a countdown."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    eta = datetime.datetime.utcnow() + datetime.timedelta(seconds=30)
    task = stage.start(return_task=True, countdown=30)
    self.assertEqual(0, len(test_shared.get_tasks()))
    self.assertTrue(eta <= task.eta.replace(tzinfo=None))

  def testStartEta(self):
    """Tests starting a pipeline with an eta."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    eta = datetime.datetime.now() + datetime.timedelta(seconds=30)
    task = stage.start(return_task=True, eta=eta)
    self.assertEqual(0, len(test_shared.get_tasks()))
    self.assertEqual(eta, test_shared.utc_to_local(task.eta))

  def testStartCountdownAndEta(self):
    """Tests starting a pipeline with both a countdown and eta."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    eta = datetime.datetime.utcnow() + datetime.timedelta(seconds=30)
    self.assertRaises(pipeline.PipelineSetupError,
                      stage.start, countdown=30, eta=eta)

  def testStartUndeclaredOutputs(self):
    """Tests that accessing undeclared outputs on a root pipeline will err.

    Only applies to root pipelines that have no named outputs and only have
    the default output slot.
    """
    stage = OutputlessPipeline()
    stage.start()
    self.assertFalse(stage.outputs.default.filled)
    self.assertRaises(pipeline.SlotNotDeclaredError, lambda: stage.outputs.blah)

  def testStartIdempotenceKeyExists(self):
    """Tests when the idempotence key is a dupe."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    other_stage = OutputlessPipeline()
    self.assertRaises(pipeline.PipelineExistsError,
                      other_stage.start, idempotence_key='banana')

  def testStartIdempotenceKeyIsRandomGarbage(self):
    """Tests when the idempotence key binary garbage."""
    idempotence_key = b'\xfb\xcaOu\t72\xa2\x08\xc9\xb9\x82\xa1\xf4>\xba>SwL'
    self.assertRaises(UnicodeDecodeError, idempotence_key.decode, 'utf-8')

    stage = OutputlessPipeline()
    stage.start(idempotence_key=idempotence_key)

    other_stage = OutputlessPipeline()
    self.assertRaises(pipeline.PipelineExistsError,
                      other_stage.start, idempotence_key=idempotence_key)

    result = OutputlessPipeline.from_id(idempotence_key)
    self.assertTrue(result is not None)

  def testStartRetryParameters(self):
    """Tests setting retry backoff parameters before calling start()."""
    stage = OutputlessPipeline()
    stage.max_attempts = 15
    stage.backoff_seconds = 1234.56
    stage.backoff_factor = 2.718
    stage.start(idempotence_key='banana')
    pipeline_record = _PipelineRecord.get_by_id(stage.pipeline_id)
    self.assertTrue(pipeline_record is not None)
    self.assertEqual(15, pipeline_record.params['max_attempts'])
    self.assertEqual(1234.56, pipeline_record.params['backoff_seconds'])
    self.assertEqual(2.718, pipeline_record.params['backoff_factor'])

  def testStartException(self):
    """Tests when a dependent method from start raises an exception."""
    def mock_raise(*args, **kwargs):
      raise Exception('Doh! Fake error')

    stage = OutputlessPipeline()
    stage._set_values_internal = mock_raise
    try:
      stage.start(idempotence_key='banana')
      self.fail('Did not raise')
    except pipeline.PipelineSetupError as e:
      self.assertEqual(
          f'Error starting {OutputlessPipeline.__module__}.OutputlessPipeline(*(), **{{}})#banana: Doh! Fake error',
          str(e))

  def testFromId(self):
    """Tests retrieving a Pipeline instance by ID."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.max_attempts = 15
    stage.backoff_seconds = 1234.56
    stage.backoff_factor = 2.718
    stage.target = 'my-other-target'
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertEqual(('one', 'two'), other.args)
    self.assertEqual({'three': 'red', 'four': 1234}, other.kwargs)
    self.assertEqual('other', other.queue_name)
    self.assertEqual('/other', other.base_path)
    self.assertEqual('meep', other.pipeline_id)
    self.assertEqual('meep', other.root_pipeline_id)
    self.assertTrue(other.is_root)
    self.assertEqual(15, other.max_attempts)
    self.assertEqual(1234.56, other.backoff_seconds)
    self.assertEqual(2.718, other.backoff_factor)
    self.assertEqual('my-other-target', other.target)
    self.assertEqual(1, other.current_attempt)

    self.assertFalse(other.outputs.one.filled)
    self.assertEqual(stage.outputs.one.key, other.outputs.one.key)
    self.assertFalse(other.outputs.two.filled)
    self.assertEqual(stage.outputs.two.key, other.outputs.two.key)

  def testFromIdResolveOutputs(self):
    """Tests retrieving a Pipeline instance by ID and resolving its outputs."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')
    stage.fill('one', 'red')
    stage.fill('two', 'blue')

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertTrue(other.outputs.one.filled)
    self.assertEqual(stage.outputs.one.key, other.outputs.one.key)
    self.assertEqual('red', other.outputs.one.value)
    self.assertTrue(other.outputs.two.filled)
    self.assertEqual(stage.outputs.two.key, other.outputs.two.key)
    self.assertEqual('blue', other.outputs.two.value)

  def testFromIdReturnsOriginalClass(self):
    """Tests that from_id() will always return the original class."""
    stage = AsyncOutputlessPipeline()
    stage.start()

    other = pipeline.Pipeline.from_id(stage.pipeline_id)
    self.assertTrue(isinstance(other, AsyncOutputlessPipeline))
    self.assertTrue(type(other) is not pipeline.Pipeline)
    self.assertTrue(other.async_)  # Class variables preserved

  def testFromIdCannotFindOriginalClass(self):
    """Tests when from_id() cannot find the original class."""
    stage = NothingPipeline()
    stage.start()

    pipeline_record = _PipelineRecord.get_by_id(stage.pipeline_id)
    pipeline_record.class_path = 'does_not_exist.or_something'
    pipeline_record.put()

    other = pipeline.Pipeline.from_id(stage.pipeline_id)
    self.assertTrue(type(other) is pipeline.Pipeline)

  def testFillString(self):
    """Tests filling a slot by name."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')
    stage.fill('one', 'red')
    stage.fill('two', 'blue')

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertEqual('red', other.outputs.one.value)
    self.assertEqual('blue', other.outputs.two.value)

  def testFillSlot(self):
    """Tests filling a slot with a Slot instance."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')
    stage.fill(stage.outputs.one, 'red')
    stage.fill(stage.outputs.two, 'blue')

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertEqual('red', other.outputs.one.value)
    self.assertEqual('blue', other.outputs.two.value)

  def testFillSlot_Huge(self):
    """Tests filling a slot with over 1MB of data."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')

    big_data = 'red' * 1000000
    self.assertTrue(len(big_data) > 1000000)
    small_data = 'blue' * 500
    self.assertTrue(len(small_data) < 1000000)

    stage.fill(stage.outputs.one, big_data)
    stage.fill(stage.outputs.two, small_data)

    other = NothingPipeline.from_id(stage.pipeline_id)
    self.assertEqual(big_data, other.outputs.one.value)
    self.assertEqual(small_data, other.outputs.two.value)

  def testFillSlotErrors(self):
    """Tests errors that happen when filling slots."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(queue_name='other', base_path='/other', idempotence_key='meep')
    with self.assertRaises(pipeline.UnexpectedPipelineError):
      stage.fill(object(), 'red')

    slot = pipeline.Slot(name='one')
    with self.assertRaises(pipeline.SlotNotDeclaredError):
      stage.fill(slot, 'red')

    stage.outputs.one.key.delete()
    with self.assertRaises(pipeline.UnexpectedPipelineError):
      stage.fill(stage.outputs.one, 'red')

  def testComplete(self):
    """Tests asynchronous completion of the pipeline."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    stage.complete(1234)

    other = AsyncOutputlessPipeline.from_id(stage.pipeline_id)
    self.assertEqual(1234, other.outputs.default.value)

  def testCompleteDisallowed(self):
    """Tests completion of the pipeline when it's not asynchronous."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start()
    self.assertRaises(pipeline.UnexpectedPipelineError, stage.complete)

  def testGetCallbackUrl(self):
    """Tests the get_callback_url method."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    result = stage.get_callback_url(one='red', two='blue', three=12345)
    self.assertEqual(
        '/_ah/pipeline/callback'
        '?one=red&pipeline_id=banana&three=12345&two=blue',
        result)

  def testGetCallbackTask(self):
    """Tests the get_callback_task method."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    now = datetime.datetime.now()
    task = stage.get_callback_task(
        params=dict(one='red', two='blue', three=12345),
        method='overridden',
        name='my-name',
        eta=now)
    self.assertEqual('/_ah/pipeline/callback', task.url)
    self.assertEqual(
        {'two': ['blue'],
         'one': ['red'],
         'pipeline_id': ['banana'],
         'three': ['12345']},
        urllib.parse.parse_qs(task.payload))
    self.assertEqual('POST', task.method)
    self.assertEqual('my-name', task.name)
    self.assertEqual(now, test_shared.utc_to_local(task.eta))

  def testAccesorsUnknown(self):
    """Tests using accessors when they have unknown values."""
    stage = OutputlessPipeline()
    self.assertTrue(stage.pipeline_id is None)
    self.assertTrue(stage.root_pipeline_id is None)
    self.assertTrue(stage.queue_name is None)
    self.assertTrue(stage.base_path is None)
    self.assertFalse(stage.has_finalized)
    self.assertFalse(stage.was_aborted)
    self.assertFalse(stage.has_finalized)

  def testHasFinalized(self):
    """Tests the has_finalized method."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertFalse(stage.has_finalized)

    other = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertFalse(other.has_finalized)

    other._context.transition_complete(other._pipeline_key)

    another = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertTrue(another.has_finalized)

  def testWasAborted(self):
    """Tests the was_aborted method."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertFalse(stage.was_aborted)

    other = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertFalse(other.was_aborted)
    other.abort()

    # Even after sending the abort signal, it won't show up as aborted.
    another = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertFalse(another.was_aborted)

    # Now transition to the aborted state.
    another._context.transition_aborted(stage._pipeline_key)
    yet_another = OutputlessPipeline.from_id(stage.pipeline_id)
    self.assertTrue(yet_another.was_aborted)

  def testRetryPossible(self):
    """Tests calling retry when it is possible."""
    stage = AsyncCancellable()
    stage.start(idempotence_key='banana')
    self.assertEqual(1, stage.current_attempt)
    self.assertTrue(stage.retry('My message 1'))

    other = AsyncCancellable.from_id(stage.pipeline_id)
    self.assertEqual(2, other.current_attempt)

    self.assertTrue(stage.retry())
    other = AsyncCancellable.from_id(stage.pipeline_id)
    self.assertEqual(3, other.current_attempt)

  def testRetryNotPossible(self):
    """Tests calling retry when the pipeline says it's not possible."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertEqual(1, stage.current_attempt)
    self.assertFalse(stage.retry())

    other = AsyncCancellable.from_id(stage.pipeline_id)
    self.assertEqual(1, other.current_attempt)

  def testRetryDisallowed(self):
    """Tests retry of the pipeline when it's not asynchronous."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertEqual(1, stage.current_attempt)
    self.assertRaises(pipeline.UnexpectedPipelineError, stage.retry)

  def testAbortRootSync(self):
    """Tests aborting a non-async, root pipeline."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.abort('gotta bail!'))
    # Does not effect the current instance; it's just a signal.
    self.assertFalse(stage.was_aborted)

  def testAbortRootAsync(self):
    """Tests when the root pipeline is async and try_cancel is True."""
    stage = AsyncCancellable()
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.abort('gotta bail!'))
    # Does not effect the current instance; it's just a signal.
    self.assertFalse(stage.was_aborted)

  def testAbortRootAsyncNotPossible(self):
    """Tests when the root pipeline is async and cannot be canceled."""
    stage = AsyncOutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertFalse(stage.abort('gotta bail!'))
    # Does not effect the current instance; it's just a signal.
    self.assertFalse(stage.was_aborted)

  def testAbortRootSyncAlreadyAborted(self):
    """Tests aborting when the sync pipeline has already been aborted."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.abort('gotta bail!'))
    self.assertFalse(stage.abort('gotta bail 2!'))

  def testAbortRootAsyncAlreadyAborted(self):
    """Tests aborting when the async pipeline has already been aborted."""
    stage = AsyncCancellable()
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.abort('gotta bail!'))
    self.assertFalse(stage.abort('gotta bail 2!'))

  def testSetStatus(self):
    """Tests for the set_status method."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    stage.set_status(
        message='This is my message',
        console_url='/path/to/the/console',
        status_links=dict(first='/one', second='/two', third='/three'))
    record_list = list(_StatusRecord.query().fetch())
    self.assertEqual(1, len(record_list))
    status_record = record_list[0]

    self.assertEqual('This is my message', status_record.message)
    self.assertEqual('/path/to/the/console', status_record.console_url)
    self.assertEqual(['first', 'second', 'third'], status_record.link_names)
    self.assertEqual(['/one', '/two', '/three'], status_record.link_urls)
    self.assertTrue(isinstance(status_record.status_time, datetime.datetime))

    # Now resetting it will overwrite all fields.
    stage.set_status(console_url='/another_console')
    after_status_record = status_record.key.get()

    self.assertEqual(None, after_status_record.message)
    self.assertEqual('/another_console', after_status_record.console_url)
    self.assertEqual([], after_status_record.link_names)
    self.assertEqual([], after_status_record.link_urls)
    self.assertNotEqual(after_status_record.status_time,
                         status_record.status_time)

  def testSetStatusError(self):
    """Tests when set_status hits a Datastore error."""
    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    try:
      stage.set_status(message=object())
    except pipeline.PipelineRuntimeError as e:
      self.assertIn('Expected string, got <object object', str(e))
      self.assertIn(
        f'Could not set status for {OutputlessPipeline.__module__}.OutputlessPipeline(*(), **{{}})',
        str(e)
      )

  def testTestMode(self):
    """Tests the test_mode property of Pipelines."""
    from pipeline import pipeline as local_pipeline
    stage = OutputlessPipeline()
    self.assertFalse(stage.test_mode)
    local_pipeline._TEST_MODE = True
    try:
      self.assertTrue(stage.test_mode)
    finally:
      local_pipeline._TEST_MODE = False

  def testCleanup(self):
    """Tests the cleanup method of Pipelines."""
    stage = OutputlessPipeline()
    self.assertRaises(pipeline.UnexpectedPipelineError, stage.cleanup)
    stage.start(idempotence_key='banana')
    self.assertTrue(stage.is_root)
    stage.cleanup()
    task_list = test_shared.get_tasks('default')
    self.assertEqual(2, len(task_list))
    start_task, cleanup_task = task_list
    self.assertEqual('/_ah/pipeline/run', start_task['url'])

    self.assertEqual('/_ah/pipeline/cleanup', cleanup_task['url'])
    self.assertEqual(
        'aglteS1hcHAtaWRyHwsSE19BRV9QaXBlbGluZV9SZWNvcmQiBmJhbmFuYQw',
        dict(cleanup_task['headers'])['X-Ae-Pipeline-Key'])
    self.assertEqual(
        ['aglteS1hcHAtaWRyHwsSE19BRV9QaXBlbGluZV9SZWNvcmQiBmJhbmFuYQw'],
        cleanup_task['params']['root_pipeline_key'])

    # If the stage is actually a child stage, then cleanup does nothing.
    stage._root_pipeline_key = ndb.Key(_PipelineRecord, 'other')
    self.assertFalse(stage.is_root)
    stage.cleanup()
    task_list = test_shared.get_tasks('default')
    self.assertEqual(2, len(task_list))

  def testInheritTarget(self):
    """Tests pipeline inherits task target if none is specified."""
    stage = OutputlessPipeline()
    self.assertEqual('my-version.foo-module', stage.target)
    stage.start(idempotence_key='banana')

    task_list = test_shared.get_tasks('default')
    self.assertEqual(1, len(task_list))
    start_task = task_list[0]
    self.assertEqual('/_ah/pipeline/run', start_task['url'])
    self.assertEqual(
        'my-version.foo-module.my-app-id.appspot.com',
        dict(start_task['headers'])['Host'])

  def testWithParams(self):
    """Tests the with_params helper method."""
    stage = OutputlessPipeline().with_params(target='my-cool-target')
    self.assertEqual('my-cool-target', stage.target)
    stage.start(idempotence_key='banana')

    task_list = test_shared.get_tasks('default')
    self.assertEqual(1, len(task_list))
    start_task = task_list[0]
    self.assertEqual('/_ah/pipeline/run', start_task['url'])
    self.assertEqual(
        'my-cool-target.my-app-id.appspot.com',
        dict(start_task['headers'])['Host'])

  def testWithParams_Errors(self):
    """Tests misuse of the with_params helper method."""
    stage = OutputlessPipeline()

    # Bad argument
    self.assertRaises(
        TypeError, stage.with_params, unknown_arg='blah')

    # If it's already active then you can't change the parameters.
    stage.start(idempotence_key='banana')
    self.assertRaises(
        pipeline.UnexpectedPipelineError, stage.with_params)


class OrderingTest(TestBase):
  """Tests for the Ordering classes."""

  def testAfterEmpty(self):
    """Tests when no futures are passed to the After() constructor."""
    pipeline.After._local._after_all_futures = []
    futures = []
    after = pipeline.After(*futures)
    self.assertEqual([], pipeline.After._local._after_all_futures)
    after.__enter__()
    self.assertEqual([], pipeline.After._local._after_all_futures)
    self.assertFalse(after.__exit__(None, None, None))
    self.assertEqual([], pipeline.After._local._after_all_futures)

  def testAfterParameterNotFuture(self):
    """Tests when some other object is passed to the After() constructor."""
    futures = [object(), object()]
    self.assertRaises(TypeError, pipeline.After, *futures)

  def testAfter(self):
    """Tests the After class."""
    pipeline.After._local._after_all_futures = []
    futures = [pipeline.PipelineFuture([]), pipeline.PipelineFuture([])]
    after = pipeline.After(*futures)
    self.assertEqual([], pipeline.After._local._after_all_futures)
    after.__enter__()
    self.assertCountEqual(futures,
                      pipeline.After._local._after_all_futures)
    self.assertFalse(after.__exit__(None, None, None))
    self.assertEqual([], pipeline.After._local._after_all_futures)

  def testAfterNested(self):
    """Tests nested behavior of the After class."""
    pipeline.After._local._after_all_futures = []
    futures = [pipeline.PipelineFuture([]), pipeline.PipelineFuture([])]

    after = pipeline.After(*futures)
    self.assertEqual([], pipeline.After._local._after_all_futures)
    after.__enter__()
    self.assertCountEqual(futures,
                      pipeline.After._local._after_all_futures)

    after2 = pipeline.After(*futures)
    self.assertCountEqual(futures,
                      pipeline.After._local._after_all_futures)
    after2.__enter__()
    self.assertCountEqual(futures + futures,
                      pipeline.After._local._after_all_futures)

    self.assertFalse(after.__exit__(None, None, None))
    self.assertCountEqual(futures,
                      pipeline.After._local._after_all_futures)
    self.assertFalse(after.__exit__(None, None, None))
    self.assertEqual([], pipeline.After._local._after_all_futures)

  def testInOrder(self):
    """Tests the InOrder class."""
    pipeline.InOrder._local._in_order_futures = set()
    pipeline.InOrder._local._activated = False
    inorder = pipeline.InOrder()
    self.assertFalse(pipeline.InOrder._local._activated)
    self.assertEqual(set(), pipeline.InOrder._local._in_order_futures)
    pipeline.InOrder._add_future(object())
    self.assertEqual(set(), pipeline.InOrder._local._in_order_futures)

    inorder.__enter__()
    self.assertTrue(pipeline.InOrder._local._activated)
    one, two, three = object(), object(), object()
    pipeline.InOrder._add_future(one)
    pipeline.InOrder._add_future(two)
    pipeline.InOrder._add_future(three)
    pipeline.InOrder._add_future(three)
    self.assertEqual(set([one, two, three]),
                      pipeline.InOrder._local._in_order_futures)

    inorder.__exit__(None, None, None)
    self.assertFalse(pipeline.InOrder._local._activated)
    self.assertEqual(set(), pipeline.InOrder._local._in_order_futures)

  def testInOrderNested(self):
    """Tests nested behavior of the InOrder class."""
    pipeline.InOrder._local._in_order_futures = set()
    pipeline.InOrder._local._activated = False
    inorder = pipeline.InOrder()
    self.assertFalse(pipeline.InOrder._local._activated)
    inorder.__enter__()
    self.assertTrue(pipeline.InOrder._local._activated)

    inorder2 = pipeline.InOrder()
    self.assertRaises(pipeline.UnexpectedPipelineError, inorder2.__enter__)
    inorder.__exit__(None, None, None)


class GenerateArgs(pipeline.Pipeline):
  """Pipeline to test the _generate_args helper function."""

  output_names = ['three', 'four']

  def run(self, *args, **kwargs):
    pass


class UtilitiesTest(TestBase):
  """Tests for module-level utilities."""

  def testDereferenceArgsNotFilled(self):
    """Tests when an argument was not filled."""
    slot_key = ndb.Key(_SlotRecord, 'myslot')
    args = [{'type': 'slot', 'slot_key': slot_key.urlsafe().decode()}]
    self.assertRaises(pipeline.SlotNotFilledError,
        pipeline._dereference_args, 'foo', args, {})

  def testDereferenceArgsBadType(self):
    """Tests when a positional argument has a bad type."""
    self.assertRaises(pipeline.UnexpectedPipelineError,
        pipeline._dereference_args, 'foo', [{'type': 'bad'}], {})

  def testDereferenceKwargsBadType(self):
    """Tests when a keyword argument has a bad type."""
    self.assertRaises(pipeline.UnexpectedPipelineError,
        pipeline._dereference_args, 'foo', [], {'one': {'type': 'bad'}})

  def testGenerateArgs(self):
    """Tests generating a parameter dictionary from arguments."""
    future = pipeline.PipelineFuture(['one', 'two', 'unused'])
    other_future = pipeline.PipelineFuture(['three', 'four'])

    future.one.key = ndb.Key(_SlotRecord, 'one')
    future.two.key = ndb.Key(_SlotRecord, 'two')
    future.default.key = ndb.Key(_SlotRecord, 'three')
    future.unused.key = ndb.Key(_SlotRecord, 'unused')

    other_future.three.key = ndb.Key(_SlotRecord, 'three')
    other_future.four.key = ndb.Key(_SlotRecord, 'four')
    other_future.default.key = ndb.Key(_SlotRecord, 'four')

    other_future._after_all_pipelines.add(future)

    # When the parameters are small.
    stage = GenerateArgs(future.one, 'some value', future,
                         red=1234, blue=future.two)
    (dependent_slots, output_slot_keys,
     params_text, params_gcs) = pipeline._generate_args(
        stage,
        other_future,
        'my-queue',
        '/base-path')

    self.assertEqual(
        set([future.one.key, future.default.key, future.two.key]),
        dependent_slots)
    self.assertEqual(
        set([other_future.three.key, other_future.four.key,
             other_future.default.key]),
        output_slot_keys)

    self.assertEqual(None, params_gcs)
    params = json.loads(params_text)
    self.assertEqual(
        {
            'queue_name': 'my-queue',
            'after_all': [future.default.key.urlsafe().decode()],
            'class_path': '{}.GenerateArgs'.format(__name__),
            'args': [
                {'slot_key': future.one.key.urlsafe().decode(),
                 'type': 'slot'},
                {'type': 'value', 'value': 'some value'},
                {'slot_key': future.default.key.urlsafe().decode(),
                 'type': 'slot'}
            ],
            'base_path': '/base-path',
            'kwargs': {
                'blue': {'slot_key': future.two.key.urlsafe().decode(),
                         'type': 'slot'},
                'red': {'type': 'value', 'value': 1234}
            },
            'output_slots': {
                'default': other_future.default.key.urlsafe().decode(),
                'four': other_future.four.key.urlsafe().decode(),
                'three': other_future.three.key.urlsafe().decode()
            },
            'max_attempts': 3,
            'backoff_factor': 2,
            'backoff_seconds': 15,
            'task_retry': False,
            'target': 'my-version.foo-module',
        }, params)

    # When the parameters are big enough we need an external blob.
    stage = GenerateArgs(future.one, 'some value' * 1000000, future,
                         red=1234, blue=future.two)

    (dependent_slots, output_slot_keys,
     params_text, params_gcs) = pipeline._generate_args(
        stage,
        other_future,
        'my-queue',
        '/base-path')

    self.assertEqual(
        set([future.one.key, future.default.key, future.two.key]),
        dependent_slots)
    self.assertEqual(
        set([other_future.three.key, other_future.four.key,
             other_future.default.key]),
        output_slot_keys)

    self.assertEqual(None, params_text)

    blob = self.storageData.get(params_gcs)
    params = json.loads(blob)

    self.assertEqual('some value' * 1000000, params['args'][1]['value'])

  def testShortRepr(self):
    """Tests for the _short_repr function."""
    my_dict = {
      'red': 1,
      'two': ['hi'] * 100
    }
    self.assertEqual(
        "{'red': 1, 'two': ['hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi',"
        " 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi',"
        " 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi', 'hi',"
        " '... (619 bytes)",
        pipeline._short_repr(my_dict))


class PipelineContextTest(TestBase):
  """Tests for the internal _PipelineContext class."""

  def setUp(self):
    """Sets up the test harness."""
    TestBase.setUp(self)
    self.pipeline1_key = ndb.Key(_PipelineRecord, '1')
    self.pipeline2_key = ndb.Key(_PipelineRecord, '2')
    self.pipeline3_key = ndb.Key(_PipelineRecord, '3')
    self.pipeline4_key = ndb.Key(_PipelineRecord, '4')
    self.pipeline5_key = ndb.Key(_PipelineRecord, '5')

    self.slot1_key = ndb.Key(_SlotRecord, 'one')
    self.slot2_key = ndb.Key(_SlotRecord, 'missing')
    self.slot3_key = ndb.Key(_SlotRecord, 'three')
    self.slot4_key = ndb.Key(_SlotRecord, 'four')

    self.slot1 = _SlotRecord(
        key=self.slot1_key,
        status=_SlotRecord.FILLED)
    self.slot3 = _SlotRecord(
        key=self.slot3_key,
        status=_SlotRecord.WAITING)
    self.slot4 = _SlotRecord(
        key=self.slot4_key,
        status=_SlotRecord.FILLED)

    self.barrier1, self.barrier1_index1 = (
        pipeline._PipelineContext._create_barrier_entities(
            self.pipeline1_key,
            self.pipeline1_key,
            _BarrierRecord.FINALIZE,
            [self.slot1_key]))

    self.barrier2, self.barrier2_index1, self.barrier2_index3 = (
        pipeline._PipelineContext._create_barrier_entities(
            self.pipeline2_key,
            self.pipeline2_key,
            _BarrierRecord.START,
            [self.slot1_key, self.slot3_key]))

    self.barrier3, self.barrier3_index1, self.barrier3_index4 = (
        pipeline._PipelineContext._create_barrier_entities(
            self.pipeline3_key,
            self.pipeline3_key,
            _BarrierRecord.START,
            [self.slot1_key, self.slot4_key]))
    self.barrier3.status = _BarrierRecord.FIRED

    self.barrier4 = _BarrierRecord(
        parent=self.pipeline4_key,
        id=_BarrierRecord.START,
        root_pipeline=self.pipeline4_key,
        target=self.pipeline4_key,
        blocking_slots=[self.slot1_key, self.slot2_key],
        status=_BarrierRecord.FIRED)

    self.barrier4, self.barrier4_index1, self.barrier4_index2 = (
        pipeline._PipelineContext._create_barrier_entities(
            self.pipeline4_key,
            self.pipeline4_key,
            _BarrierRecord.START,
            [self.slot1_key, self.slot2_key]))
    self.barrier4.status = _BarrierRecord.FIRED

    self.barrier5, self.barrier5_index1 = (
        pipeline._PipelineContext._create_barrier_entities(
            self.pipeline5_key,
            self.pipeline5_key,
            _BarrierRecord.START,
            [self.slot1_key]))

    self.context = pipeline._PipelineContext(
        'my-task1', 'default', '/base-path')

  def testNotifyBarrierFire_WithBarrierIndexes(self):
    """Tests barrier firing behavior."""
    self.assertEqual(_BarrierRecord.WAITING, self.barrier1.status)
    self.assertEqual(_BarrierRecord.WAITING, self.barrier2.status)
    self.assertEqual(_BarrierRecord.FIRED, self.barrier3.status)
    self.assertTrue(self.barrier3.trigger_time is None)
    self.assertEqual(_BarrierRecord.FIRED, self.barrier4.status)
    self.assertEqual(_BarrierRecord.WAITING, self.barrier5.status)

    ndb.put_multi([self.barrier1, self.barrier2, self.barrier3, self.barrier4,
            self.barrier5, self.slot1, self.slot3, self.slot4,
            self.barrier1_index1, self.barrier2_index1, self.barrier2_index3,
            self.barrier3_index1, self.barrier3_index4, self.barrier4_index1,
            self.barrier4_index2, self.barrier5_index1])
    self.context.notify_barriers(
        self.slot1_key,
        None,
        use_barrier_indexes=True,
        max_to_notify=3)
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(3, len(task_list))
    task_list.sort(key=lambda x: x['name'])  # For deterministic tests.
    first_task, second_task, continuation_task = task_list

    self.assertEqual(
        {'pipeline_key': [self.pipeline1_key.urlsafe().decode()],
         'purpose': [_BarrierRecord.FINALIZE]},
        first_task['params'])
    self.assertEqual('/base-path/finalized', first_task['url'])

    self.assertEqual(
        {'pipeline_key': [self.pipeline3_key.urlsafe().decode()],
         'purpose': [_BarrierRecord.START]},
        second_task['params'])
    self.assertEqual('/base-path/run', second_task['url'])

    self.assertEqual('/base-path/output', continuation_task['url'])
    self.assertEqual(
        [self.slot1_key.urlsafe().decode()], continuation_task['params']['slot_key'])
    self.assertEqual(
        'my-task1-ae-barrier-notify-0',
        continuation_task['name'])

    barrier1, barrier2, barrier3 = ndb.get_multi(
        [self.barrier1.key, self.barrier2.key, self.barrier3.key])

    self.assertEqual(_BarrierRecord.FIRED, barrier1.status)
    self.assertTrue(barrier1.trigger_time is not None)

    self.assertEqual(_BarrierRecord.WAITING, barrier2.status)
    self.assertTrue(barrier2.trigger_time is None)

    # NOTE: This barrier relies on slots 1 and 4, to force the "blocking slots"
    # inner loop to be excerised. By putting slot4 last on the last barrier
    # tested in the loop, we ensure that any inner-loop variables do not pollute
    # the outer function context.
    self.assertEqual(_BarrierRecord.FIRED, barrier3.status)
    # Show that if the _BarrierRecord was already in the FIRED state that it
    # will not be overwritten again and have its trigger_time changed.
    self.assertTrue(barrier3.trigger_time is None)

    # Run the first continuation task. It should raise an error because slot2
    # does not exist.
    self.context.task_name = 'my-task1-ae-barrier-notify-0'
    self.assertRaises(
        pipeline.UnexpectedPipelineError,
        functools.partial(
            self.context.notify_barriers,
            self.slot1_key,
            continuation_task['params']['cursor'][0],
            use_barrier_indexes=True,
            max_to_notify=2))

    # No tasks should be added because the exception was raised.
    task_list = test_shared.get_tasks()
    self.assertEqual([], task_list)

    # Adding slot2 should allow forward progress.
    slot2 = _SlotRecord(
        key=self.slot2_key,
        status=_SlotRecord.WAITING)
    slot2.put()
    self.context.notify_barriers(
        self.slot1_key,
        continuation_task['params']['cursor'][0],
        use_barrier_indexes=True,
        max_to_notify=2)

    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(2, len(task_list))
    third_task, continuation2_task = task_list

    self.assertEqual(
        {'pipeline_key': [self.pipeline5_key.urlsafe().decode()],
         'purpose': [_BarrierRecord.START]},
        third_task['params'])
    self.assertEqual('/base-path/run', third_task['url'])

    self.assertEqual('/base-path/output', continuation2_task['url'])
    self.assertEqual(
        [self.slot1_key.urlsafe().decode()], continuation2_task['params']['slot_key'])
    self.assertEqual(
        'my-task1-ae-barrier-notify-1',
        continuation2_task['name'])

    barrier4, barrier5 = ndb.get_multi([self.barrier4.key, self.barrier5.key])
    self.assertEqual(_BarrierRecord.FIRED, barrier4.status)
    # Shows that the _BarrierRecord entity was not overwritten.
    self.assertTrue(barrier4.trigger_time is None)

    self.assertEqual(_BarrierRecord.FIRED, barrier5.status)
    self.assertTrue(barrier5.trigger_time is not None)

    # Running the continuation task again will re-tigger the barriers,
    # but no tasks will be inserted because they're already tombstoned.
    self.context.task_name = 'my-task1-ae-barrier-notify-0'
    self.context.notify_barriers(
        self.slot1_key,
        continuation_task['params']['cursor'][0],
        use_barrier_indexes=True,
        max_to_notify=2)
    self.assertEqual(0, len(test_shared.get_tasks()))

    # Running the last continuation task will do nothing.
    self.context.task_name = 'my-task1-ae-barrier-notify-1'
    self.context.notify_barriers(
        self.slot1_key,
        continuation2_task['params']['cursor'][0],
        use_barrier_indexes=True,
        max_to_notify=2)
    self.assertEqual(0, len(test_shared.get_tasks()))

  def testNotifyBarrierFire_WithBarrierIndexes_BarrierMissing(self):
      """Tests _BarrierIndex firing when a _BarrierRecord is missing."""
      self.assertEqual(_BarrierRecord.WAITING, self.barrier1.status)
      ndb.put_multi([self.slot1, self.barrier1_index1])

      # The _BarrierRecord corresponding to barrier1_index1 is never put, which
      # will cause notify_barriers to fail with a missing barrier error.
      self.assertNotEqual(None, self.barrier1_index1.key.get())
      self.assertEqual(None, self.barrier1.key.get())

      # This doesn't raise an exception.
      self.context.notify_barriers(
          self.slot1_key,
          None,
          use_barrier_indexes=True,
          max_to_notify=3)

  def testNotifyBarrierFire_NoBarrierIndexes(self):
    """Tests barrier firing behavior without using _BarrierIndexes."""
    self.assertEqual(_BarrierRecord.WAITING, self.barrier1.status)
    self.assertEqual(_BarrierRecord.WAITING, self.barrier2.status)
    self.assertEqual(_BarrierRecord.FIRED, self.barrier3.status)
    self.assertTrue(self.barrier3.trigger_time is None)
    self.assertEqual(_BarrierRecord.FIRED, self.barrier4.status)
    self.assertEqual(_BarrierRecord.WAITING, self.barrier5.status)

    ndb.put_multi([self.barrier1, self.barrier2, self.barrier3, self.barrier4,
            self.barrier5, self.slot1, self.slot3, self.slot4])
    self.context.notify_barriers(
        self.slot1_key,
        None,
        use_barrier_indexes=False,
        max_to_notify=3)
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(3, len(task_list))
    task_list.sort(key=lambda x: x['name'])  # For deterministic tests.
    first_task, second_task, continuation_task = task_list

    self.assertEqual(
        {'pipeline_key': [self.pipeline1_key.urlsafe().decode()],
         'purpose': [_BarrierRecord.FINALIZE]},
        first_task['params'])
    self.assertEqual('/base-path/finalized', first_task['url'])

    self.assertEqual(
        {'pipeline_key': [self.pipeline3_key.urlsafe().decode()],
         'purpose': [_BarrierRecord.START]},
        second_task['params'])
    self.assertEqual('/base-path/run', second_task['url'])

    self.assertEqual('/base-path/output', continuation_task['url'])
    self.assertEqual(
        [self.slot1_key.urlsafe().decode()], continuation_task['params']['slot_key'])
    self.assertEqual(
        'my-task1-ae-barrier-notify-0',
        continuation_task['name'])

    barrier1, barrier2, barrier3 = ndb.get_multi(
        [self.barrier1.key, self.barrier2.key, self.barrier3.key])

    self.assertEqual(_BarrierRecord.FIRED, barrier1.status)
    self.assertTrue(barrier1.trigger_time is not None)

    self.assertEqual(_BarrierRecord.WAITING, barrier2.status)
    self.assertTrue(barrier2.trigger_time is None)

    # NOTE: This barrier relies on slots 1 and 4, to force the "blocking slots"
    # inner loop to be excerised. By putting slot4 last on the last barrier
    # tested in the loop, we ensure that any inner-loop variables do not pollute
    # the outer function context.
    self.assertEqual(_BarrierRecord.FIRED, barrier3.status)
    # Show that if the _BarrierRecord was already in the FIRED state that it
    # will not be overwritten again and have its trigger_time changed.
    self.assertTrue(barrier3.trigger_time is None)

    # Run the first continuation task. It should raise an error because slot2
    # does not exist.
    self.context.task_name = 'my-task1-ae-barrier-notify-0'
    self.assertRaises(
        pipeline.UnexpectedPipelineError,
        functools.partial(
            self.context.notify_barriers,
            self.slot1_key,
            continuation_task['params']['cursor'][0],
            use_barrier_indexes=False,
            max_to_notify=2))

    # No tasks should be added because the exception was raised.
    task_list = test_shared.get_tasks()
    self.assertEqual([], task_list)

    # Adding slot2 should allow forward progress.
    slot2 = _SlotRecord(
        key=self.slot2_key,
        status=_SlotRecord.WAITING)
    slot2.put()
    self.context.notify_barriers(
        self.slot1_key,
        continuation_task['params']['cursor'][0],
        use_barrier_indexes=False,
        max_to_notify=2)

    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(2, len(task_list))
    third_task, continuation2_task = task_list

    self.assertEqual(
        {'pipeline_key': [self.pipeline5_key.urlsafe().decode()],
         'purpose': [_BarrierRecord.START]},
        third_task['params'])
    self.assertEqual('/base-path/run', third_task['url'])

    self.assertEqual('/base-path/output', continuation2_task['url'])
    self.assertEqual(
        [self.slot1_key.urlsafe().decode()], continuation2_task['params']['slot_key'])
    self.assertEqual(
        'my-task1-ae-barrier-notify-1',
        continuation2_task['name'])

    barrier4, barrier5 = ndb.get_multi([self.barrier4.key, self.barrier5.key])
    self.assertEqual(_BarrierRecord.FIRED, barrier4.status)
    # Shows that the _BarrierRecord entity was not overwritten.
    self.assertTrue(barrier4.trigger_time is None)

    self.assertEqual(_BarrierRecord.FIRED, barrier5.status)
    self.assertTrue(barrier5.trigger_time is not None)

    # Running the continuation task again will re-tigger the barriers,
    # but no tasks will be inserted because they're already tombstoned.
    self.context.task_name = 'my-task1-ae-barrier-notify-0'
    self.context.notify_barriers(
        self.slot1_key,
        continuation_task['params']['cursor'][0],
        use_barrier_indexes=False,
        max_to_notify=2)
    self.assertEqual(0, len(test_shared.get_tasks()))

    # Running the last continuation task will do nothing.
    self.context.task_name = 'my-task1-ae-barrier-notify-1'
    self.context.notify_barriers(
        self.slot1_key,
        continuation2_task['params']['cursor'][0],
        use_barrier_indexes=False,
        max_to_notify=2)
    self.assertEqual(0, len(test_shared.get_tasks()))

  def testTransitionRunMissing(self):
    """Tests transition_run when the _PipelineRecord is missing."""
    self.assertTrue(self.pipeline1_key.get() is None)
    self.context.transition_run(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionRunBadStatus(self):
    """Tests transition_run when the _PipelineRecord.status is bad."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline1_key)
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)
    self.context.transition_run(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionRunMissingBarrier(self):
    """Tests transition_run when the finalization _BarrierRecord is missing."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        key=self.pipeline1_key)
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)
    self.assertRaises(pipeline.UnexpectedPipelineError,
        self.context.transition_run,
        self.pipeline1_key,
        blocking_slot_keys=[self.slot1_key])

  def testTransitionCompleteMissing(self):
    """Tests transition_complete when the _PipelineRecord is missing."""
    self.assertTrue(self.pipeline1_key.get() is None)
    self.context.transition_complete(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionCompleteBadStatus(self):
    """Tests transition_complete when the _PipelineRecord.status is bad."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline1_key)
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)
    self.context.transition_complete(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionRetryMissing(self):
    """Tests transition_retry when the _PipelineRecord is missing."""
    self.assertTrue(self.pipeline1_key.get() is None)
    self.assertFalse(
        self.context.transition_retry(self.pipeline1_key, 'my message'))
    # No exception raised.
    self.assertEqual(0, len(test_shared.get_tasks()))

  def testTransitionRetryBadStatus(self):
    """Tests transition_retry when the _PipelineRecord.status is bad."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline1_key)
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)
    self.assertFalse(
        self.context.transition_retry(self.pipeline1_key, 'my message'))
    # No exception raised.
    self.assertEqual(0, len(test_shared.get_tasks()))

  def testTransitionRetryMaxFailures(self):
    """Tests transition_retry when _PipelineRecord.max_attempts is exceeded."""
    params = {
        'backoff_seconds': 10,
        'backoff_factor': 1.5,
        'max_attempts': 15,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        key=self.pipeline1_key,
        max_attempts=5,
        current_attempt=4,
        params_text=json.dumps(params),
        root_pipeline=self.pipeline5_key)
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)
    self.assertFalse(
        self.context.transition_retry(self.pipeline1_key, 'my message'))

    # A finalize task should be enqueued.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(1, len(task_list))

    self.assertEqual('/base-path/fanout_abort', task_list[0]['url'])
    self.assertEqual(
        {'root_pipeline_key': [self.pipeline5_key.urlsafe().decode()]},
        task_list[0]['params'])

  def testTransitionRetryTaskParams(self):
    """Tests that transition_retry will enqueue retry tasks properly.

    Attempts multiple retries and verifies ETAs and task parameters.
    """
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        key=self.pipeline1_key,
        max_attempts=5,
        current_attempt=0,
        params_text=json.dumps(params),
        root_pipeline=self.pipeline5_key)
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)

    start_time = datetime.datetime.now()
    when_list = [
      start_time + datetime.timedelta(seconds=(30 * i))
      for i in range(5)
    ]
    closure_when_list = list(when_list)
    def fake_gettime():
      return closure_when_list.pop(0)
    self.context._gettime = fake_gettime

    for attempt, delay_seconds in enumerate([12, 18, 27, 40.5]):
      self.context.transition_retry(
          self.pipeline1_key, 'my message %d' % attempt)

      task_list = test_shared.get_tasks()
      test_shared.delete_tasks(task_list)
      self.assertEqual(1, len(task_list))
      task = task_list[0]

      self.assertEqual('/base-path/run', task['url'])
      self.assertEqual(
          {
              'pipeline_key': [self.pipeline1_key.urlsafe().decode()],
              'attempt': [str(attempt + 1)],
              'purpose': ['start']
          }, task['params'])

      next_eta = when_list[attempt] + datetime.timedelta(seconds=delay_seconds)
      self.assertEqual(next_eta, test_shared.utc_to_local(task['eta']))

      pipeline_record = self.pipeline1_key.get()
      self.assertEqual(attempt + 1, pipeline_record.current_attempt)
      self.assertEqual(next_eta, pipeline_record.next_retry_time)
      self.assertEqual('my message %d' % attempt,
                        pipeline_record.retry_message)

    # Simulate last attempt.
    self.context.transition_retry(self.pipeline1_key, 'my message 5')

    # A finalize task should be enqueued.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(1, len(task_list))

    self.assertEqual('/base-path/fanout_abort', task_list[0]['url'])
    self.assertEqual(
        {'root_pipeline_key': [self.pipeline5_key.urlsafe().decode()]},
        task_list[0]['params'])

  def testBeginAbortMissing(self):
    """Tests begin_abort when the pipeline is missing."""
    self.assertTrue(self.pipeline1_key.get() is None)
    self.assertFalse(
        self.context.begin_abort(self.pipeline1_key, 'error message'))

  def testBeginAbortAlreadyAborted(self):
    """Tests begin_abort when the pipeline was already aborted."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.ABORTED,
        abort_requested=False,
        key=self.pipeline1_key,
        params_text=json.dumps(params))
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)

    self.assertFalse(
        self.context.begin_abort(self.pipeline1_key, 'error message'))

  def testBeginAbortAlreadySignalled(self):
    """Tests begin_abort when the pipeline has already been signalled."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        abort_requested=True,
        key=self.pipeline1_key,
        params_text=json.dumps(params))
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)

    self.assertFalse(
        self.context.begin_abort(self.pipeline1_key, 'error message'))

  def testBeginAbortTaskEnqueued(self):
    """Tests that a successful begin_abort will enqueue an abort task."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.RUN,
        key=self.pipeline1_key,
        params_text=json.dumps(params))
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)

    self.assertTrue(
        self.context.begin_abort(self.pipeline1_key, 'error message'))

    # A finalize task should be enqueued.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(1, len(task_list))

    self.assertEqual('/base-path/fanout_abort', task_list[0]['url'])
    self.assertEqual(
        {'root_pipeline_key': [self.pipeline1_key.urlsafe().decode()]},
        task_list[0]['params'])

  def testContinueAbort(self):
    """Tests the whole life cycle of continue_abort."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record1 = _PipelineRecord(
        status=_PipelineRecord.RUN,
        key=self.pipeline1_key,
        params_text=json.dumps(params),
        root_pipeline=self.pipeline1_key)
    pipeline_record2 = _PipelineRecord(
        status=_PipelineRecord.RUN,
        key=self.pipeline2_key,
        params_text=json.dumps(params),
        root_pipeline=self.pipeline1_key)
    pipeline_record3 = _PipelineRecord(
        status=_PipelineRecord.RUN,
        key=self.pipeline3_key,
        params_text=json.dumps(params),
        root_pipeline=self.pipeline1_key)
    pipeline_record4 = _PipelineRecord(
        status=_PipelineRecord.ABORTED,
        key=self.pipeline4_key,
        params_text=json.dumps(params),
        root_pipeline=self.pipeline1_key)
    pipeline_record5 = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline5_key,
        params_text=json.dumps(params),
        root_pipeline=self.pipeline1_key)

    ndb.put_multi([pipeline_record1, pipeline_record2, pipeline_record3,
            pipeline_record4, pipeline_record5])

    self.context.continue_abort(self.pipeline1_key, max_to_notify=2)

    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(3, len(task_list))
    # For deterministic tests.
    task_list.sort(key=lambda x: x['params'].get('pipeline_key', []))
    continuation_task, first_task, second_task = task_list

    # Abort for the first pipeline
    self.assertEqual('/base-path/abort', first_task['url'])
    self.assertEqual(
        {'pipeline_key': [self.pipeline1_key.urlsafe().decode()],
         'purpose': ['abort']},
        first_task['params'])

    # Abort for the second pipeline
    self.assertEqual('/base-path/abort', second_task['url'])
    self.assertEqual(
        {'pipeline_key': [self.pipeline2_key.urlsafe().decode()],
         'purpose': ['abort']},
        second_task['params'])

    # Continuation
    self.assertEqual('/base-path/fanout_abort', continuation_task['url'])
    self.assertEqual(set(['cursor', 'root_pipeline_key']),
                      set(continuation_task['params'].keys()))
    self.assertEqual(self.pipeline1_key.urlsafe().decode(),
                      continuation_task['params']['root_pipeline_key'][0])
    self.assertTrue(continuation_task['name'].endswith('-0'))
    cursor = continuation_task['params']['cursor'][0]

    # Now run the continuation task
    self.context.task_name = continuation_task['name']
    self.context.continue_abort(
        self.pipeline1_key, cursor=cursor, max_to_notify=1)

    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(2, len(task_list))
    # For deterministic tests.
    task_list.sort(key=lambda x: x['params'].get('pipeline_key', []))
    second_continuation_task, fifth_task = task_list

    # Abort for the third pipeline
    self.assertEqual('/base-path/abort', fifth_task['url'])
    self.assertEqual(
        {'pipeline_key': [self.pipeline3_key.urlsafe().decode()],
         'purpose': ['abort']},
        fifth_task['params'])

    # Another continuation
    self.assertEqual('/base-path/fanout_abort',
                      second_continuation_task['url'])
    self.assertEqual(set(['cursor', 'root_pipeline_key']),
                      set(second_continuation_task['params'].keys()))
    self.assertEqual(
        self.pipeline1_key.urlsafe().decode(),
        second_continuation_task['params']['root_pipeline_key'][0])
    self.assertTrue(second_continuation_task['name'].endswith('-1'))
    cursor2 = second_continuation_task['params']['cursor'][0]

    # Now run another continuation task.
    self.context.task_name = second_continuation_task['name']
    self.context.continue_abort(
        self.pipeline1_key, cursor=cursor2, max_to_notify=2)

    # This task will find two pipelines that are already in terminal states,
    # and skip then.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(1, len(task_list))
    third_continuation_task = task_list[0]

    self.assertEqual('/base-path/fanout_abort',
                      third_continuation_task['url'])
    self.assertEqual(set(['cursor', 'root_pipeline_key']),
                      set(third_continuation_task['params'].keys()))
    self.assertEqual(
        self.pipeline1_key.urlsafe().decode(),
        third_continuation_task['params']['root_pipeline_key'][0])
    self.assertTrue(third_continuation_task['name'].endswith('-2'))
    cursor3 = third_continuation_task['params']['cursor'][0]

    # Run the third continuation task, which will do nothing.
    self.context.task_name = second_continuation_task['name']
    self.context.continue_abort(
        self.pipeline1_key, cursor=cursor3, max_to_notify=2)

    # Nothing left to do.
    task_list = test_shared.get_tasks()
    test_shared.delete_tasks(task_list)
    self.assertEqual(0, len(task_list))

  def testTransitionAbortedMissing(self):
    """Tests transition_aborted when the pipeline is missing."""
    self.assertTrue(self.pipeline1_key.get() is None)
    self.context.transition_aborted(self.pipeline1_key)
    # That's it. No exception raised.

  def testTransitionAbortedBadStatus(self):
    """Tests transition_aborted when the pipeline is in a bad state."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    finalized_time = datetime.datetime.now()
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.ABORTED,
        key=self.pipeline1_key,
        params_text=json.dumps(params),
        finalized_time=finalized_time)
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)

    self.context.transition_aborted(self.pipeline1_key)

    # Finalized time will stay the same.
    after_record = self.pipeline1_key.get()
    self.assertEqual(pipeline_record.finalized_time,
                      after_record.finalized_time)

  def testTransitionAbortedSuccess(self):
    """Tests when transition_aborted is successful."""
    params = {
        'backoff_seconds': 12,
        'backoff_factor': 1.5,
        'max_attempts': 5,
        'task_retry': False,
    }
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.WAITING,
        key=self.pipeline1_key,
        params_text=json.dumps(params))
    pipeline_record.put()
    self.assertTrue(self.pipeline1_key.get() is not None)

    self.context.transition_aborted(self.pipeline1_key)

    after_record = self.pipeline1_key.get()
    self.assertEqual(_PipelineRecord.ABORTED, after_record.status)
    self.assertTrue(pipeline_record.finalized_time is None)
    self.assertTrue(isinstance(after_record.finalized_time, datetime.datetime))


class EvaluateErrorTest(test_shared.TaskRunningMixin, TestBase):
  """Task execution tests for error situations."""

  def setUp(self):
    """Sets up the test harness."""
    super(EvaluateErrorTest, self).setUp()
    self.pipeline_key = ndb.Key(_PipelineRecord, '1')
    self.slot_key = ndb.Key(_SlotRecord, 'red')
    self.context = pipeline._PipelineContext(
        'my-task1', 'default', '/base-path')
    
  def testPipelineMissing(self):
    """Tests running a pipeline key that's disappeared."""
    self.assertTrue(self.pipeline_key.get() is None)
    self.context.evaluate(self.pipeline_key)
    # That's it. No exception raised.

  def testPipelineBadStatus(self):
    """Tests running a pipeline that has an invalid status."""
    pipeline_record = _PipelineRecord(
        status=_PipelineRecord.DONE,
        key=self.pipeline_key)
    pipeline_record.put()
    self.assertTrue(self.pipeline_key.get() is not None)
    self.context.evaluate(self.pipeline_key)

  def testDefaultSlotMissing(self):
    """Tests when the default slot is missing."""
    pipeline_record = _PipelineRecord(
        root_pipeline=self.pipeline_key,
        status=_PipelineRecord.WAITING,
        params_text=json.dumps({
            'output_slots': {'default': self.slot_key.urlsafe().decode()}}),
        key=self.pipeline_key)
    pipeline_record.put()
    self.assertTrue(self.slot_key.get() is None)
    self.assertTrue(self.pipeline_key.get() is not None)
    self.context.evaluate(self.pipeline_key)
    # That's it. No exception raised.

  def testRootPipelineMissing(self):
    """Tests when the root pipeline record is missing."""
    missing_key = ndb.Key(_PipelineRecord, 'unknown')
    slot_record = _SlotRecord(key=self.slot_key)
    slot_record.put()
    pipeline_record = _PipelineRecord(
        root_pipeline=missing_key,
        status=_PipelineRecord.WAITING,
        params_text=json.dumps({
            'output_slots': {'default': self.slot_key.urlsafe().decode()}}),
        key=self.pipeline_key)
    pipeline_record.put()
    self.assertTrue(missing_key.get() is None)
    self.assertTrue(self.slot_key.get() is not None)
    self.assertTrue(self.pipeline_key.get() is not None)
    self.context.evaluate(self.pipeline_key)
    # That's it. No exception raised.

  def testResolutionError(self):
    """Tests when the pipeline class couldn't be found."""
    slot_record = _SlotRecord(key=self.slot_key)
    slot_record.put()
    pipeline_record = _PipelineRecord(
        root_pipeline=self.pipeline_key,
        status=_PipelineRecord.WAITING,
        class_path='does.not.exist',
        params_text=json.dumps({
            'output_slots': {'default': self.slot_key.urlsafe().decode()}}),
        key=self.pipeline_key)
    pipeline_record.put()
    self.assertTrue(self.slot_key.get() is not None)
    self.assertTrue(self.pipeline_key.get() is not None)
    self.assertRaises(ImportError, self.context.evaluate, self.pipeline_key)


class DumbSync(pipeline.Pipeline):
  """A dumb pipeline that's synchronous."""

  def run(self, *args):
    pass


class DumbAsync(pipeline.Pipeline):
  """A dumb pipeline that's asynchronous."""

  async_ = True

  def run(self):
    self.complete()


class DumbGenerator(pipeline.Pipeline):
  """A dumb pipeline that's a generator that yeilds nothing."""

  def run(self):
    if False:
      yield 1


class DumbGeneratorYields(pipeline.Pipeline):
  """A dumb pipeline that's a generator that yields something."""

  def run(self, block=False):
    yield DumbSync(1)
    result = yield DumbSync(2)
    if block:
      yield DumbSync(3, result)


class DiesOnCreation(pipeline.Pipeline):
  """A pipeline that raises an exception on insantiation."""

  def __init__(self, *args, **kwargs):
    raise Exception('This will not work!')


class DiesOnRun(pipeline.Pipeline):
  """A pipeline that raises an exception when it's executed."""

  def run(self):
    raise Exception('Cannot run this one!')


class RetryAfterYield(pipeline.Pipeline):
  """A generator pipeline that raises a Retry exception after yielding once."""

  def run(self):
    yield DumbSync()
    raise pipeline.Retry('I want to retry now!')


class DiesAfterYield(pipeline.Pipeline):
  """A generator pipeline that dies after yielding once."""

  def run(self):
    yield DumbSync()
    raise Exception('Whoops I will die now!')


class RetriesOnRun(pipeline.Pipeline):
  """A pipeline that raises a Retry exception on run."""

  def run(self):
    raise pipeline.Retry('Gotta go and retry now!')


class AbortsOnRun(pipeline.Pipeline):
  """A pipeline that raises an Abort exception on run."""

  def run(self):
    raise pipeline.Abort('Gotta go and abort now!')


class AsyncCannotAbort(pipeline.Pipeline):
  """An async pipeline that cannot be aborted once active."""

  async_ = True

  def run(self):
    pass


class AbortAfterYield(pipeline.Pipeline):
  """A generator pipeline that raises an Abort exception after yielding once."""

  def run(self):
    yield DumbSync()
    raise pipeline.Abort('I want to abort now!')


class AsyncCanAbort(pipeline.Pipeline):
  """An async pipeline that cannot be aborted once active."""

  async_ = True

  def run(self):
    pass

  def try_cancel(self):
    return True


class SyncMissedOutput(pipeline.Pipeline):
  """A sync pipeline that forgets to fill in a named output slot."""

  output_names = ['another']

  def run(self):
    return 5


class GeneratorMissedOutput(pipeline.Pipeline):
  """A generator pipeline that forgets to fill in a named output slot."""

  output_names = ['another']

  def run(self):
    if False:
      yield 1


class TaskRunningTest(test_shared.TaskRunningMixin, TestBase):
  """End-to-end tests for task-running and race-condition situations.

  Many of these are cases where an executor task runs for a second time when
  it shouldn't have or some kind of transient error occurred.
  """

  def setUp(self):
    """Sets up the test harness."""
    super(TaskRunningTest, self).setUp()
    self.pipeline_key = ndb.Key(_PipelineRecord, 'one')
    self.pipeline2_key = ndb.Key(_PipelineRecord, 'two')
    self.slot_key = ndb.Key(_SlotRecord, 'red')

    self.slot_record = _SlotRecord(key=self.slot_key)
    self.pipeline_record = _PipelineRecord(
        root_pipeline=self.pipeline_key,
        status=_PipelineRecord.WAITING,
        class_path='does.not.exist',
        params_text=json.dumps({
                 'output_slots': {'default': self.slot_key.urlsafe().decode()},
                 'args': [],
                 'kwargs': {},
                 'task_retry': False,
                 'backoff_seconds': 1,
                 'backoff_factor': 2,
                 'max_attempts': 4,
                 'queue_name': 'default',
                 'base_path': '',
               }),
        key=self.pipeline_key,
        max_attempts=4)
    self.barrier_record = _BarrierRecord(
            parent=self.pipeline_key,
            id=_BarrierRecord.FINALIZE,
            target=self.pipeline_key,
            root_pipeline=self.pipeline_key,
            blocking_slots=[self.slot_key])

    self.context = pipeline._PipelineContext(
        'my-task1', 'default', '/base-path')

  def testSubstagesRunImmediately(self):
    """Tests that sub-stages with no blocking slots are run immediately."""
    self.pipeline_record.class_path = '{}.DumbGeneratorYields'.format(__name__)
    ndb.put_multi([self.pipeline_record, self.slot_record, self.barrier_record])

    before_record = self.pipeline_key.get()
    self.assertEqual([], before_record.fanned_out)

    self.context.evaluate(self.pipeline_key)

    after_record = self.pipeline_key.get()
    self.assertEqual(2, len(after_record.fanned_out))
    child1_key, child2_key = after_record.fanned_out

    task_list = test_shared.get_tasks()
    self.assertEqual(1, len(task_list))
    fanout_task = task_list[0]

    # Verify that the start time is set for non-blocked child pipelines.
    child_record_list = ndb.get_multi(after_record.fanned_out)
    for child_record in child_record_list:
      self.assertTrue(child_record.start_time is not None)

    # One fan-out task with both children.
    self.assertEqual(
        [self.pipeline_key.urlsafe().decode()],
        fanout_task['params']['parent_key'])
    self.assertEqual(
        ['0', '1'],
        fanout_task['params']['child_indexes'])
    self.assertEqual('/base-path/fanout', fanout_task['url'])

    # Only finalization barriers present.
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.START, parent=child1_key).get() is None)
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.START, parent=child2_key).get() is None)
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.FINALIZE, parent=child1_key).get() is not None)
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.FINALIZE, parent=child2_key).get() is not None)

  def testSubstagesBlock(self):
    """Tests that sub-stages with pending inputs will have a barrier added."""
    self.pipeline_record.class_path = '{}.DumbGeneratorYields'.format(__name__)
    params = self.pipeline_record.params.copy()
    params.update({
        'output_slots': {'default': self.slot_key.urlsafe().decode()},
        'args': [{'type': 'value', 'value': True}],
        'kwargs': {},
    })
    self.pipeline_record.params_text = json.dumps(params)
    ndb.put_multi([self.pipeline_record, self.slot_record, self.barrier_record])

    before_record = self.pipeline_key.get()
    self.assertEqual([], before_record.fanned_out)

    self.context.evaluate(self.pipeline_key)

    after_record = self.pipeline_key.get()
    self.assertEqual(3, len(after_record.fanned_out))

    task_list = test_shared.get_tasks()
    self.assertEqual(1, len(task_list))
    fanout_task = task_list[0]

    # Only two children should start.
    self.assertEqual('/base-path/fanout', fanout_task['url'])
    self.assertEqual(
        [self.pipeline_key.urlsafe().decode()],
        fanout_task['params']['parent_key'])
    self.assertEqual(
        ['0', '1'],
        fanout_task['params']['child_indexes'])

    run_children = set(after_record.fanned_out[int(i)]
                       for i in fanout_task['params']['child_indexes'])
    self.assertEqual(2, len(run_children))
    child1_key, child2_key = run_children
    other_child_key = list(set(after_record.fanned_out) - run_children)[0]

    # Only a start barrier inserted for the one pending child.
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.START, parent=child1_key).get() is None)
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.START, parent=child2_key).get() is None)
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.START, parent=other_child_key).get() is not None)
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.FINALIZE, parent=child1_key).get() is not None)
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.FINALIZE, parent=child2_key).get() is not None)
    self.assertTrue(ndb.Key(_BarrierRecord, _BarrierRecord.FINALIZE, parent=other_child_key).get() is not None)

  def testFannedOutOrdering(self):
    """Tests that the fanned_out property lists children in code order."""
    self.pipeline_record.class_path = '{}.DumbGeneratorYields'.format(__name__)
    params = self.pipeline_record.params.copy()
    params.update({
        'output_slots': {'default': self.slot_key.urlsafe().decode()},
        'args': [{'type': 'value', 'value': True}],
        'kwargs': {},
    })
    self.pipeline_record.params_text = json.dumps(params)
    ndb.put_multi([self.pipeline_record, self.slot_record, self.barrier_record])

    before_record = self.pipeline_key.get()
    self.assertEqual([], before_record.fanned_out)

    self.context.evaluate(self.pipeline_key)

    after_record = self.pipeline_key.get()
    self.assertEqual(3, len(after_record.fanned_out))

    children = ndb.get_multi(after_record.fanned_out)
    self.assertEqual(1, children[0].params['args'][0]['value'])
    self.assertEqual(2, children[1].params['args'][0]['value'])
    self.assertEqual(3, children[2].params['args'][0]['value'])

  def testSyncWaitingStartRerun(self):
    """Tests a waiting, sync pipeline being re-run after it already output."""
    self.pipeline_record.class_path = '{}.DumbSync'.format(__name__)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    before_record = self.slot_key.get()
    self.assertEqual(_SlotRecord.WAITING, before_record.status)
    self.assertTrue(before_record.fill_time is None)
    self.context.evaluate(self.pipeline_key)

    after_record = self.slot_key.get()
    self.assertEqual(_SlotRecord.FILLED, after_record.status)
    self.assertTrue(after_record.fill_time is not None)

    after_pipeline = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_pipeline.status)

    self.context.evaluate(self.pipeline_key)
    second_after_record = self.slot_key.get()
    self.assertEqual(_SlotRecord.FILLED, second_after_record.status)
    self.assertTrue(second_after_record.fill_time is not None)

    # The output slot fill times are different, which means the pipeline re-ran.
    self.assertNotEqual(second_after_record.fill_time, after_record.fill_time)

  def testSyncFinalizingRerun(self):
    """Tests a finalizing, sync pipeline task being re-run."""
    self.pipeline_record.class_path = '{}.DumbSync'.format(__name__)
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = json.dumps(None)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_record.status)

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    second_after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_record.status)

    # Finalized time will stay the same.
    self.assertEqual(after_record.finalized_time,
                      second_after_record.finalized_time)

  def testSyncDoneFinalizeRerun(self):
    """Tests a done, sync pipeline task being re-refinalized."""
    now = datetime.datetime.utcnow()
    self.pipeline_record.class_path = '{}.DumbSync'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.DONE
    self.pipeline_record.finalized_time = now
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = json.dumps(None)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_record.status)

    # Finalize time stays the same.
    self.assertEqual(now, after_record.finalized_time)

  def testAsyncWaitingRerun(self):
    """Tests a waiting, async pipeline task being re-run."""
    self.pipeline_record.class_path = '{}.DumbAsync'.format(__name__)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    before_record = self.slot_key.get()
    self.assertEqual(_SlotRecord.WAITING, before_record.status)
    self.assertTrue(before_record.fill_time is None)
    self.context.evaluate(self.pipeline_key)

    after_record = self.slot_key.get()
    self.assertEqual(_SlotRecord.FILLED, after_record.status)
    self.assertTrue(after_record.fill_time is not None)

    after_pipeline = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.RUN, after_pipeline.status)

    self.context.evaluate(self.pipeline_key)
    second_after_record = self.slot_key.get()
    self.assertEqual(_SlotRecord.FILLED, second_after_record.status)
    self.assertTrue(second_after_record.fill_time is not None)

    # The output slot fill times are different, which means the pipeline re-ran.
    self.assertNotEqual(second_after_record.fill_time, after_record.fill_time)

  def testAsyncRunRerun(self):
    """Tests a run, async pipeline task being re-run."""
    self.pipeline_record.class_path = '{}.DumbAsync'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.RUN
    ndb.put_multi([self.pipeline_record, self.slot_record])

    before_record = self.slot_key.get()
    self.assertEqual(_SlotRecord.WAITING, before_record.status)
    self.assertTrue(before_record.fill_time is None)
    self.context.evaluate(self.pipeline_key)

    after_record = self.slot_key.get()
    self.assertEqual(_SlotRecord.FILLED, after_record.status)
    self.assertTrue(after_record.fill_time is not None)

    after_pipeline = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.RUN, after_pipeline.status)

    self.context.evaluate(self.pipeline_key)
    second_after_record = self.slot_key.get()
    self.assertEqual(_SlotRecord.FILLED, second_after_record.status)
    self.assertTrue(second_after_record.fill_time is not None)

    # The output slot fill times are different, which means the pipeline re-ran.
    self.assertNotEqual(second_after_record.fill_time, after_record.fill_time)

  def testAsyncFinalizingRerun(self):
    """Tests a finalizing, async pipeline task being re-run."""
    self.pipeline_record.class_path = '{}.DumbAsync'.format(__name__)
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = json.dumps(None)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_record.status)

    after_pipeline = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_pipeline.status)

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    second_after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_record.status)

    # Finalized time will stay the same.
    self.assertEqual(after_record.finalized_time,
                      second_after_record.finalized_time)

  def testAsyncDoneFinalizeRerun(self):
    """Tests a done, async pipeline task being re-finalized."""
    now = datetime.datetime.utcnow()
    self.pipeline_record.class_path = '{}.DumbAsync'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.DONE
    self.pipeline_record.finalized_time = now
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = json.dumps(None)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_record.status)

    # Finalize time stays the same.
    self.assertEqual(now, after_record.finalized_time)

  def testNonYieldingGeneratorWaitingFilled(self):
    """Tests a waiting, non-yielding generator will fill its output slot."""
    self.pipeline_record.class_path = '{}.DumbGenerator'.format(__name__)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.assertEqual(_SlotRecord.WAITING, self.slot_key.get().status)
    self.context.evaluate(self.pipeline_key)

    # Output slot is filled.
    after_slot = self.slot_key.get()
    self.assertEqual(_SlotRecord.FILLED, after_slot.status)

    # Pipeline is now in the run state.
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.RUN, after_record.status)

  def testNonYieldingGeneratorRunNotFilledRerun(self):
    """Tests a run, non-yielding generator with a not filled output slot.

    This happens when the generator yields no children and is moved to the
    RUN state but then fails before it could output to the default slot.
    """
    self.pipeline_record.class_path = '{}.DumbGenerator'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.RUN
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.assertEqual(_SlotRecord.WAITING, self.slot_key.get().status)
    self.context.evaluate(self.pipeline_key)

    # Output slot is filled.
    after_slot = self.slot_key.get()
    self.assertEqual(_SlotRecord.FILLED, after_slot.status)

  def testGeneratorRunReRun(self):
    """Tests a run, yielding generator that is re-run."""
    self.pipeline_record.class_path = '{}.DumbGeneratorYields'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.RUN
    self.pipeline_record.fanned_out = [self.pipeline2_key]
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key)
    # Output slot wasn't filled.
    after_slot = self.slot_key.get()
    self.assertEqual(_SlotRecord.WAITING, after_slot.status)

    # Status hasn't changed.
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.RUN, after_record.status)

  def testGeneratorFinalizingRerun(self):
    """Tests a finalizing, generator pipeline task being re-run."""
    self.pipeline_record.class_path = '{}.DumbGeneratorYields'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.RUN
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = json.dumps(None)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_record.status)

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    second_after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_record.status)

    # Finalized time will stay the same.
    self.assertEqual(after_record.finalized_time,
                      second_after_record.finalized_time)

  def testGeneratorDoneFinalizeRerun(self):
    """Tests a done, generator pipeline task being re-run."""
    now = datetime.datetime.utcnow()
    self.pipeline_record.class_path = '{}.DumbGeneratorYields'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.DONE
    self.pipeline_record.finalized_time = now
    self.slot_record.status = _SlotRecord.FILLED
    self.slot_record.value_text = json.dumps(None)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.FINALIZE)
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.DONE, after_record.status)

    # Finalize time stays the same.
    self.assertEqual(now, after_record.finalized_time)

  def testFromIdFails(self):
    """Tests when evaluate's call to from_id fails a retry attempt is made."""
    self.pipeline_record.class_path = '{}.DiesOnCreation'.format(__name__)
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.assertEqual(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertEqual('Exception: This will not work!',
                      after_record.retry_message)

  def testMismatchedAttempt(self):
    """Tests when the task's current attempt does not match the datastore."""
    self.pipeline_record.class_path = '{}.DiesOnRun'.format(__name__)
    self.pipeline_record.current_attempt = 3
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key,
                          purpose=_BarrierRecord.START,
                          attempt=1)

    # Didn't run because no state change occurred, retry count is the same.
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(3, after_record.current_attempt)
    self.assertEqual(None, after_record.retry_message)

  def testPastMaxAttempts(self):
    """Tests when the current attempt number is beyond the max attempts.

    This could happen if the user edits 'max_attempts' during execution.
    """
    self.pipeline_record.class_path = '{}.DiesOnRun'.format(__name__)
    self.pipeline_record.current_attempt = 5
    self.pipeline_record.max_attempts = 3
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key,
                          purpose=_BarrierRecord.START,
                          attempt=5)

    # Didn't run because no state change occurred, retry count is the same.
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(5, after_record.current_attempt)
    self.assertEqual(None, after_record.retry_message)

  def testPrematureRetry(self):
    """Tests when the current retry request came prematurely."""
    now = datetime.datetime.utcnow()
    self.pipeline_record.class_path = '{}.DiesOnRun'.format(__name__)
    self.pipeline_record.current_attempt = 1
    self.pipeline_record.max_attempts = 3
    self.pipeline_record.next_retry_time = now + datetime.timedelta(seconds=30)
    ndb.put_multi([self.pipeline_record, self.slot_record])

    self.assertRaises(
        pipeline.UnexpectedPipelineError,
        self.context.evaluate,
        self.pipeline_key,
        purpose=_BarrierRecord.START,
        attempt=1)

    # Didn't run because no state change occurred, retry count is the same.
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertEqual(None, after_record.retry_message)

  def testRunExceptionRetry(self):
    """Tests that exceptions in Sync/Async pipelines cause a retry."""
    self.pipeline_record.class_path = '{}.DiesOnRun'.format(__name__)
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.assertEqual(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertEqual('Exception: Cannot run this one!',
                      after_record.retry_message)

  def testRunForceRetry(self):
    """Tests that explicit Retry on a synchronous pipeline."""
    self.pipeline_record.class_path = '{}.RetriesOnRun'.format(__name__)
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.assertEqual(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertEqual('Gotta go and retry now!',
                      after_record.retry_message)

  def testGeneratorExceptionRetry(self):
    """Tests that exceptions in a generator pipeline cause a retry."""
    self.pipeline_record.class_path = '{}.DiesAfterYield'.format(__name__)
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.assertEqual(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertEqual('Exception: Whoops I will die now!',
                      after_record.retry_message)

  def testGeneratorForceRetry(self):
    """Tests when a generator raises a user-initiated retry exception."""
    self.pipeline_record.class_path = '{}.RetryAfterYield'.format(__name__)
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.assertEqual(0, self.pipeline_record.current_attempt)
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertEqual('I want to retry now!', after_record.retry_message)

  def testNonAsyncAbortSignal(self):
    """Tests when a non-async pipeline receives the abort signal."""
    self.pipeline_record.class_path = '{}.DumbSync'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.WAITING
    self.assertTrue(self.pipeline_record.finalized_time is None)
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.ABORTED, after_record.status)
    self.assertEqual(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testAbortRootPipelineFastPath(self):
    """Tests root pipeline status also functions as the abort signal."""
    root_pipeline = _PipelineRecord(
        root_pipeline=self.pipeline2_key,
        status=_PipelineRecord.RUN,
        class_path='does.not.exist',
        params_text=json.dumps({
                 'output_slots': {'default': self.slot_key.urlsafe().decode()},
                 'args': [],
                 'kwargs': {},
                 'task_retry': False,
                 'backoff_seconds': 1,
                 'backoff_factor': 2,
                 'max_attempts': 4,
                 'queue_name': 'default',
                 'base_path': '',
               }),
        key=self.pipeline2_key,
        is_root_pipeline=True,
        max_attempts=4,
        abort_requested=True)

    # Use DiesOnRun to ensure that we don't actually run the pipeline.
    self.pipeline_record.class_path = '{}.DiesOnRun'.format(__name__)
    self.pipeline_record.root_pipeline = self.pipeline2_key

    ndb.put_multi([self.pipeline_record, self.slot_record, root_pipeline])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.ABORTED, after_record.status)
    self.assertEqual(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testNonAsyncAbortSignalRepeated(self):
    """Tests when a non-async pipeline has the abort request repeated.

    Tests the case of getting the abort signal is successful, and that the
    pipeline will finalize before being aborted.
    """
    self.pipeline_record.class_path = '{}.DumbSync'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.WAITING
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.ABORTED, after_record.status)
    self.assertEqual(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

    # Run a second time-- this should be ignored.
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)
    after_record2 = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.ABORTED, after_record.status)
    self.assertEqual(0, after_record2.current_attempt)
    self.assertTrue(after_record2.retry_message is None)
    self.assertTrue(after_record2.abort_message is None)
    self.assertEqual(after_record.finalized_time, after_record2.finalized_time)

  def testAsyncAbortSignalBeforeStart(self):
    """Tests when an async pipeline has an abort request and has not run yet.

    Verifies that the pipeline will be finalized and transitioned to ABORTED.
    """
    self.pipeline_record.class_path = '{}.DumbAsync'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.WAITING
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.ABORTED, after_record.status)
    self.assertEqual(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testAsyncAbortSignalDisallowed(self):
    """Tests when an async pipeline receives abort but try_cancel is False."""
    self.pipeline_record.class_path = '{}.AsyncCannotAbort'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.RUN
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.RUN, after_record.status)
    self.assertEqual(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is None)

  def testAsyncAbortSignalAllowed(self):
    """Tests when an async pipeline receives abort but try_cancel is True."""
    self.pipeline_record.class_path = '{}.AsyncCanAbort'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.RUN
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.ABORTED, after_record.status)
    self.assertEqual(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testGeneratorAbortException(self):
    """Tests when a generator raises an abort after it's begun yielding."""
    self.pipeline_record.class_path = '{}.AbortAfterYield'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.RUN
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.ABORTED, after_record.status)
    self.assertEqual(0, after_record.current_attempt)
    self.assertTrue(after_record.retry_message is None)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is not None)

  def testRetryWhenSyncDoesNotFillSlot(self):
    """Tests when a sync pipeline does not fill a slot that it will retry."""
    self.pipeline_record.class_path = '{}.SyncMissedOutput'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.WAITING
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertEqual(
        'SlotNotFilledError: Outputs {{\'another\'}} for pipeline ID "one" '
        'were never filled by "{}.SyncMissedOutput".'.format(__name__),
        after_record.retry_message)

  def testNonYieldingGeneratorDoesNotFillSlot(self):
    """Tests non-yielding pipelines that do not fill a slot will retry."""
    self.pipeline_record.class_path = '{}.GeneratorMissedOutput'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.WAITING
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertEqual(
        'SlotNotFilledError: Outputs {{\'another\'}} for pipeline ID "one" '
        'were never filled by "{}.GeneratorMissedOutput".'.format(__name__),
        after_record.retry_message)

  def testAbortWithBadInputs(self):
    """Tests aborting a pipeline with unresolvable input slots."""
    self.pipeline_record.class_path = '{}.DumbSync'.format(__name__)
    self.pipeline_record.params['args'] = [
        {'type': 'slot',
         'slot_key': 'aglteS1hcHAtaWRyGQsSEF9BRV9DYXNjYWRlX1Nsb3QiA3JlZAw'}
    ]
    self.pipeline_record.status = _PipelineRecord.WAITING
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.ABORT)

    # Forced into the abort state.
    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.ABORTED, after_record.status)

  def testPassBadValue(self):
    """Tests when a pipeline passes a non-serializable value to a child."""
    self.pipeline_record.class_path = '{}.PassBadValue'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.WAITING
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertIn('Bad child arguments. TypeError', after_record.retry_message)
    self.assertIn('is not JSON serializable', after_record.retry_message)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is None)

  def testReturnBadValue(self):
    """Tests when a pipeline returns a non-serializable value."""
    self.pipeline_record.class_path = '{}.ReturnBadValue'.format(__name__)
    self.pipeline_record.status = _PipelineRecord.WAITING
    ndb.put_multi([self.pipeline_record, self.slot_record])
    self.context.evaluate(self.pipeline_key, purpose=_BarrierRecord.START)

    after_record = self.pipeline_key.get()
    self.assertEqual(_PipelineRecord.WAITING, after_record.status)
    self.assertEqual(1, after_record.current_attempt)
    self.assertIn('Bad return value. TypeError', after_record.retry_message)
    self.assertIn('is not JSON serializable', after_record.retry_message)
    self.assertTrue(after_record.abort_message is None)
    self.assertTrue(after_record.finalized_time is None)


class HandlersPrivateTest(TestBase):
  """Tests that the pipeline request handlers are all private."""

  def testBarrierHandler(self):
    """Tests the _BarrierHandler."""
    app = Flask(__name__)
    app.add_url_rule('/', view_func=pipeline._BarrierHandler.as_view('barrier'))
    client = app.test_client()
    response = client.post('/')
    self.assertEqual(403, response.status_code)

  def testPipelineHandler(self):
    """Tests the _PipelineHandler."""
    app = Flask(__name__)
    app.add_url_rule('/', view_func=pipeline._PipelineHandler.as_view('pipeline'))
    client = app.test_client()
    response = client.post('/')
    self.assertEqual(403, response.status_code)

  def testFanoutAbortHandler(self):
    """Tests the _FanoutAbortHandler."""
    app = Flask(__name__)
    app.add_url_rule('/', view_func=pipeline._FanoutAbortHandler.as_view('fanout_abort'))
    client = app.test_client()
    response = client.post('/')
    self.assertEqual(403, response.status_code)

  def testFanoutHandler(self):
    """Tests the _FanoutHandler."""
    app = Flask(__name__)
    app.add_url_rule('/', view_func=pipeline._FanoutHandler.as_view('fanout'))
    client = app.test_client()
    response = client.post('/')
    self.assertEqual(403, response.status_code)

  def testCleanupHandler(self):
    """Tests the _CleanupHandler."""
    app = Flask(__name__)
    app.add_url_rule('/', view_func=pipeline._CleanupHandler.as_view('cleanup'))
    client = app.test_client()
    response = client.post('/')
    self.assertEqual(403, response.status_code)


class InternalOnlyPipeline(pipeline.Pipeline):
  """Pipeline with internal-only callbacks."""

  async_ = True

  def run(self):
    pass


class AdminOnlyPipeline(pipeline.Pipeline):
  """Pipeline with internal-only callbacks."""

  async_ = True
  admin_callbacks = True

  def run(self):
    pass

  def callback(self, **kwargs):
    pass


class PublicPipeline(pipeline.Pipeline):
  """Pipeline with public callbacks."""

  async_ = True
  public_callbacks = True

  def run(self):
    pass

  def callback(self, **kwargs):
    return (200, 'text/plain', repr(kwargs))


class DummyKind(ndb.Expando):
  pass


class NoTransactionPipeline(PublicPipeline):
  """Pipeline that verifies the callback is executed outside a transaction."""

  def callback(self, **kwargs):
    if ndb.in_transaction():
      try:
        # If we are in non xg-transaction, we should be unable to write to 24
        # new entity groups (1 is used to read pipeline state).
        # Assumes the entity group limit is 25 (was previously 5).
        for _ in range(24):
          DummyKind().put()
        try:
          # Verify something is not wrong in the testbed and/or limits changed
          DummyKind().put()
          return (500, 'text/plain', 'More than 5 entity groups used.')
        except BadRequestError:
          return (203, 'text/plain', 'In a XG transaction')
      except BadRequestError:
        return (202, 'text/plain', 'In a non-XG transaction')
    else:
      return (201, 'text/plain', 'Outside a transaction.')


class NoXgTransactionPipeline(NoTransactionPipeline):
  """Pipeline that verifies the callback is in non-XG transaction."""
  _callback_xg_transaction = False


class XgTransactionPipeline(NoTransactionPipeline):
  """Pipeline that verifies the callback is in a XG transaction."""
  _callback_xg_transaction = True


class CallbackHandlerTest(TestBase):
  """Tests for the _CallbackHandler class."""

  def setUp(self):
    super().setUp()
    app = Flask(__name__)
    app.add_url_rule('/', view_func=pipeline._CallbackHandler.as_view('callback'))
    self.client = app.test_client()

  def testErrors(self):
    """Tests for error conditions."""
    response = self.client.get('/', query_string={'red': 'one', 'blue': 'two'})
    self.assertEqual(400, response.status_code)

    # Non-existent pipeline.
    response = self.client.get('/?pipeline_id=blah&red=one&blue=two')
    self.assertEqual(400, response.status_code)

    # Pipeline exists but class path is bogus.
    stage = InternalOnlyPipeline()
    stage.start()

    pipeline_record = pipeline.models._PipelineRecord.get_by_id(
        stage.pipeline_id)
    params = pipeline_record.params
    params['class_path'] = 'does.not.exist'
    pipeline_record.params_text = json.dumps(params)
    pipeline_record.put()

    response = self.client.get('/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    self.assertEqual(400, response.status_code)

    # Internal-only callbacks.
    stage = InternalOnlyPipeline()
    stage.start()
    response = self.client.get('/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    self.assertEqual(400, response.status_code)

    # Admin-only callbacks but not admin.
    stage = AdminOnlyPipeline()
    stage.start()
    response = self.client.get('/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    self.assertEqual(400, response.status_code)

  def testAdminOnly(self):
    """Tests accessing a callback that is admin-only."""
    stage = AdminOnlyPipeline()
    stage.start()

    os.environ['USER_IS_ADMIN'] = '1'
    try:
      response = self.client.get('/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    finally:
      del os.environ['USER_IS_ADMIN']

    self.assertEqual(200, response.status_code)

  def testPublic(self):
    """Tests accessing a callback that is public."""
    stage = PublicPipeline()
    stage.start()
    response = self.client.get('/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    self.assertEqual(200, response.status_code)

  def testReturnValue(self):
    """Tests when the callback has a return value to render as output."""
    stage = PublicPipeline()
    stage.start()
    response = self.client.get('/?pipeline_id=%s&red=one&blue=two' % stage.pipeline_id)
    self.assertEqual(200, response.status_code)
    self.assertEqual(
      b"{'red': 'one', 'blue': 'two'}",
      response.data)
    self.assertEqual('text/plain', response.headers['Content-Type'])

  def RunTransactionTest(self, stage, expected_code):
    stage.start()
    response = self.client.get('/?pipeline_id=%s' % stage.pipeline_id)
    self.assertEqual(expected_code, response.status_code)

  def testNoTransaction(self):
    """Tests that the callback is not called from within a trans. by default."""
    self.RunTransactionTest(NoTransactionPipeline(), 201)

  def testNonXgTransaction(self):
    """Tests that the callback is called within a single EG transaction."""
    self.RunTransactionTest(NoXgTransactionPipeline(), 202)

  def testXgTransaction(self):
    """Tests that the callback is called within a cross EG transaction."""
    self.RunTransactionTest(XgTransactionPipeline(), 203)

  def testGiveUpOnTask(self):
    """Tests that after N retries the task is abandoned."""
    response = self.client.get('/?pipeline_id=does_not_exist')
    self.assertEqual(400, response.status_code)

    response = self.client.get('/?pipeline_id=does_not_exist', headers={
      'X_APPENGINE_TASKRETRYCOUNT': '10'
    })
    self.assertEqual(200, response.status_code)


class CleanupHandlerTest(test_shared.TaskRunningMixin, TestBase):
  """Tests for the _CleanupHandler class."""

  def testSuccess(self):
    """Tests successfully deleting all child pipeline elements."""
    self.assertEqual(0, len(_PipelineRecord.query().fetch()))
    self.assertEqual(0, len(_SlotRecord.query().fetch()))
    self.assertEqual(0, len(_BarrierRecord.query().fetch()))
    self.assertEqual(0, len(_StatusRecord.query().fetch()))

    stage = OutputlessPipeline()
    stage.start(idempotence_key='banana')
    stage.set_status('My status here!')
    self.assertEqual(1, len(_PipelineRecord.query().fetch()))
    self.assertEqual(1, len(_SlotRecord.query().fetch()))
    self.assertEqual(1, len(_BarrierRecord.query().fetch()))
    self.assertEqual(1, len(_StatusRecord.query().fetch()))
    self.assertEqual(1, len(_BarrierIndex.query().fetch()))

    stage.cleanup()
    task_list = self.get_tasks()
    self.assertEqual(2, len(task_list))

    # The order of the tasks (start or cleanup) is unclear, so
    # fish out the one that's the cleanup task and run it directly.
    for task in task_list:
      if task['url'] == '/_ah/pipeline/cleanup':
        self.run_task(task)

    self.assertEqual(0, len(_PipelineRecord.query().fetch()))
    self.assertEqual(0, len(_SlotRecord.query().fetch()))
    self.assertEqual(0, len(_BarrierRecord.query().fetch()))
    self.assertEqual(0, len(_StatusRecord.query().fetch()))
    self.assertEqual(0, len(_BarrierIndex.query().fetch()))


class FanoutHandlerTest(test_shared.TaskRunningMixin, TestBase):
  """Tests for the _FanoutHandler class."""

  def testOldStyle(self):
    """Tests the old fanout parameter style for backwards compatibility."""
    stage = DumbGeneratorYields()
    stage.start(idempotence_key='banana')
    task_list = self.get_tasks()
    test_shared.delete_tasks(task_list)
    self.run_task(task_list[0])

    task_list = self.get_tasks()
    self.assertEqual(1, len(task_list))
    fanout_task = task_list[0]
    self.assertEqual('/_ah/pipeline/fanout', fanout_task['url'])

    after_record = stage._pipeline_key.get()

    fanout_task['body'] = base64.b64encode(urllib.parse.urlencode(
      [('pipeline_key', after_record.fanned_out[0].urlsafe().decode()),
       ('pipeline_key', after_record.fanned_out[1].urlsafe().decode())]).encode('utf-8'))
    test_shared.delete_tasks(task_list)
    self.run_task(fanout_task)

    task_list = self.get_tasks()
    test_shared.delete_tasks(task_list)

    self.assertEqual(2, len(task_list))
    for task in task_list:
      self.assertEqual('/_ah/pipeline/run', task['url'])
    children_keys = [
        ndb.Key(urlsafe=t['params']['pipeline_key'][0]) for t in task_list]

    self.assertEqual(set(children_keys), set(after_record.fanned_out))


################################################################################
# Begin functional test section!

class RunOrder(ndb.Model):
  """Saves the order of method calls."""

  order = ndb.TextProperty(repeated=True)

  @classmethod
  def add(cls, message):
    def txn():
      runorder = RunOrder.get_by_id('singleton')
      if runorder is None:
        runorder = RunOrder(id='singleton')
      runorder.order.append(message)
      runorder.put()
    ndb.transaction(txn)

  @classmethod
  def get(cls):
    runorder = RunOrder.get_by_id('singleton')
    if runorder is None:
      return []
    else:
      return [str(s) for s in runorder.order]


class SaveRunOrder(pipeline.Pipeline):
  """Pipeline that saves the run order message supplied."""

  def run(self, message):
    RunOrder.add(message)


class EchoSync(pipeline.Pipeline):
  """Pipeline that echos input."""

  def run(self, *args):
    if not args:
      return None
    if len(args) == 1:
      return args[0]
    return args


class EchoAsync(pipeline.Pipeline):
  """Asynchronous pipeline that echos input."""

  async_ = True

  def run(self, *args):
    encoded_args = base64.b64encode(pickle.dumps(args)).decode('utf-8')
    self.get_callback_task(
        params=dict(return_value=encoded_args)).add()

  def callback(self, return_value):
    args = pickle.loads(base64.b64decode(return_value.encode('utf-8')))
    if not args:
      self.complete(None)
    elif len(args) == 1:
      self.complete(args[0])
    else:
      self.complete(args)

  def run_test(self, *args):
    encoded_args = base64.b64encode(pickle.dumps(args)).decode('utf-8')
    self.callback(encoded_args)


class EchoNamedSync(pipeline.Pipeline):
  """Pipeline that echos named inputs to named outputs."""

  def run(self, **kwargs):
    prefix = kwargs.get('prefix', '')
    if prefix:
      del kwargs['prefix']
    for name, value in list(kwargs.items()):
      self.fill(name, prefix + value)


class EchoParticularNamedSync(EchoNamedSync):
  """Has preexisting output names so it can be used as a root pipeline."""

  output_names = ['one', 'two', 'three', 'four']


class EchoNamedAsync(pipeline.Pipeline):
  """Asynchronous pipeline that echos named inputs to named outputs."""

  async_ = True

  def run(self, **kwargs):
    self.get_callback_task(params=kwargs).add()

  def callback(self, **kwargs):
    prefix = kwargs.get('prefix', '')
    if prefix:
      del kwargs['prefix']
    for name, value in list(kwargs.items()):
      self.fill(name, prefix + value)
    self.complete()

  def run_test(self, **kwargs):
    self.callback(**kwargs)


class EchoNamedHalfAsync(pipeline.Pipeline):
  """Pipeline that echos to named outputs and completes async.

  This is different than the other EchoNamedAsync because it fills all the
  slots except the default slot immediately, and then uses a callback to
  finally complete.
  """

  async_ = True
  output_names = ['one', 'two', 'three', 'four']

  def run(self, **kwargs):
    prefix = kwargs.get('prefix', '')
    if prefix:
      del kwargs['prefix']
    for name, value in list(kwargs.items()):
      self.fill(name, prefix + value)
    self.get_callback_task(params=kwargs).add()

  def callback(self, **kwargs):
    self.complete()

  def run_test(self, **kwargs):
    prefix = kwargs.get('prefix', '')
    if prefix:
      del kwargs['prefix']
    for name, value in list(kwargs.items()):
      self.fill(name, prefix + value)
    self.callback(**kwargs)


class EchoParticularNamedAsync(EchoNamedAsync):
  """Has preexisting output names so it can be used as a root pipeline."""

  output_names = ['one', 'two', 'three', 'four']


class FillAndPass(pipeline.Pipeline):
  """Test pipeline that fills some outputs and passes the rest to a child."""

  def run(self, to_fill, **kwargs):
    for name in to_fill:
      self.fill(name, kwargs.pop(name))
    adjusted_kwargs = {}
    for name, value in list(kwargs.items()):
      adjusted_kwargs[name] = value
    if adjusted_kwargs:
      yield EchoNamedSync(**adjusted_kwargs)


class FillAndPassParticular(FillAndPass):
  """Has preexisting output names so it can be used as a root pipeline."""

  output_names = ['one', 'two', 'three', 'four']


class StrictChildInheritsAll(pipeline.Pipeline):
  """Test pipeline whose strict child inherits all outputs."""

  output_names = ['one', 'two', 'three', 'four']

  def run(self, **kwargs):
    yield EchoParticularNamedSync(**kwargs)


class StrictChildGeneratorInheritsAll(pipeline.Pipeline):
  """Test pipeline whose strict child generator inherits all outputs."""

  output_names = ['one', 'two', 'three', 'four']

  def run(self, **kwargs):
    yield FillAndPassParticular(list(kwargs.keys()), **kwargs)


class ConsumePartialChildrenStrict(pipeline.Pipeline):
  """Test pipeline that consumes a subset of a strict child's outputs."""

  def run(self, **kwargs):
    result = yield EchoParticularNamedSync(**kwargs)
    yield EchoSync(result.one, result.two)


class ConsumePartialChildren(pipeline.Pipeline):
  """Test pipeline that consumes a subset of a dynamic child's outputs."""

  def run(self, **kwargs):
    result = yield EchoNamedSync(**kwargs)
    yield EchoSync(result.one, result.two)


class DoNotConsumeDefault(pipeline.Pipeline):
  """Test pipeline that does not consume a child's default output."""

  def run(self, value):
    yield EchoSync('not used')
    yield EchoSync(value)


class TwoLevelFillAndPass(pipeline.Pipeline):
  """Two-level deep version of fill and pass."""

  output_names = ['one', 'two', 'three', 'four']

  def run(self, **kwargs):
    # This stage will prefix any keyword args with 'first-'.
    stage = yield FillAndPass(
        [],
        prefix='first-',
        one=kwargs.get('one'),
        two=kwargs.get('two'))
    adjusted_kwargs = kwargs.copy()
    adjusted_kwargs['one'] = stage.one
    adjusted_kwargs['two'] = stage.two
    adjusted_kwargs['prefix'] = 'second-'
    # This stage will prefix any keyword args with 'second-'. That means
    # any args that were passed in from the output of the first stage will
    # be prefixed twice: 'second-first-<kwarg>'.
    yield FillAndPass([], **adjusted_kwargs)


class DivideWithRemainder(pipeline.Pipeline):
  """Divides a number, returning the divisor and the quotient."""

  output_names = ['remainder']

  def run(self, dividend, divisor):
    self.fill(self.outputs.remainder, dividend % divisor)
    return dividend // divisor


class EuclidGCD(pipeline.Pipeline):
  """Does the Euclidean Greatest Common Factor recursive algorithm."""

  output_names = ['gcd']

  def run(self, a, b):
    a, b = max(a, b), min(a, b)
    if b == 0:
      self.fill(self.outputs.gcd, a)
      return
    result = yield DivideWithRemainder(a, b)
    recurse = yield EuclidGCD(b, result.remainder)


class UnusedOutputReference(pipeline.Pipeline):
  """Test pipeline that touches a child output but doesn't consume it."""

  def run(self):
    result = yield EchoParticularNamedSync(
        one='red', two='blue', three='green', four='yellow')
    print(result.one)
    print(result.two)
    print(result.three)
    yield EchoSync(result.four)


class AccessUndeclaredDefaultOnly(pipeline.Pipeline):
  """Test pipeline accesses undeclared output of a default-only pipeline."""

  def run(self):
    result = yield EchoSync('hi')
    yield EchoSync(result.does_not_exist)


class RunMethod(pipeline.Pipeline):
  """Test pipeline that outputs what method was used for running it."""

  def run(self):
    return 'run()'

  def run_test(self):
    return 'run_test()'


class DoAfter(pipeline.Pipeline):
  """Test the After clause."""

  def run(self):
    first = yield SaveRunOrder('first')
    second = yield SaveRunOrder('first')

    with pipeline.After(first, second):
      third = yield SaveRunOrder('third')
      fourth = yield SaveRunOrder('third')


class DoAfterNested(pipeline.Pipeline):
  """Test the After clause in multiple nestings."""

  def run(self):
    first = yield SaveRunOrder('first')
    second = yield SaveRunOrder('first')

    with pipeline.After(first, second):
      third = yield SaveRunOrder('third')
      fourth = yield SaveRunOrder('third')

      with pipeline.After(third, fourth):
        with pipeline.After(third):
          yield SaveRunOrder('fifth')

        with pipeline.After(fourth):
          yield SaveRunOrder('fifth')


class DoAfterList(pipeline.Pipeline):
  """Test the After clause with a list of jobs."""

  def run(self):
    job_list = []
    for i in range(10):
      job = yield EchoNamedHalfAsync(
          one='red', two='blue', three='green', four='yellow')
      job_list.append(job)

    with pipeline.After(*job_list):
      combined = yield common.Concat(*[job.one for job in job_list])
      result = yield SaveRunOrder(combined)
      with pipeline.After(result):
        yield SaveRunOrder('twelfth')


class DoInOrder(pipeline.Pipeline):
  """Test the InOrder clause."""

  def run(self):
    with pipeline.InOrder():
      yield SaveRunOrder('first')
      yield SaveRunOrder('second')
      yield SaveRunOrder('third')
      yield SaveRunOrder('fourth')


class DoInOrderNested(pipeline.Pipeline):
  """Test the InOrder clause when nested."""

  def run(self):
    with pipeline.InOrder():
      yield SaveRunOrder('first')
      yield SaveRunOrder('second')

      with pipeline.InOrder():
        # Should break.
        yield SaveRunOrder('third')
        yield SaveRunOrder('fourth')


class MixAfterInOrder(pipeline.Pipeline):
  """Test mixing After and InOrder clauses."""

  def run(self):
    first = yield SaveRunOrder('first')
    with pipeline.After(first):
      with pipeline.InOrder():
        yield SaveRunOrder('second')
        yield SaveRunOrder('third')
        fourth = yield SaveRunOrder('fourth')
    with pipeline.InOrder():
      with pipeline.After(fourth):
        yield SaveRunOrder('fifth')
        yield SaveRunOrder('sixth')


class RecordFinalized(pipeline.Pipeline):
  """Records calls to finalized."""

  def run(self, depth):
    yield SaveRunOrder('run#%d' % depth)

  def finalized(self):
    RunOrder.add('finalized#%d' % self.args[0])

  def finalized_test(self):
    RunOrder.add('finalized_test#%d' % self.args[0])


class NestedFinalize(pipeline.Pipeline):
  """Test nested pipelines are finalized in a reasonable order."""

  def run(self, depth):
    if depth == 0:
      return
    yield RecordFinalized(depth)
    yield NestedFinalize(depth - 1)


class YieldBadValue(pipeline.Pipeline):
  """Test pipeline that yields something that's not a pipeline."""

  def run(self):
    yield 5


class YieldChildTwice(pipeline.Pipeline):
  """Test pipeline that yields the same child pipeline twice."""

  def run(self):
    child = EchoSync('bad')
    yield child
    yield child


class FinalizeFailure(pipeline.Pipeline):
  """Test when finalized raises an error."""

  def run(self):
    pass

  def finalized(self):
    raise Exception('Doh something broke!')


class SyncForcesRetry(pipeline.Pipeline):
  """Test when a synchronous pipeline raises the Retry exception."""

  def run(self):
    raise pipeline.Retry('We need to try this again')


class AsyncForcesRetry(pipeline.Pipeline):
  """Test when a synchronous pipeline raises the Retry exception."""

  async_ = True

  def run(self):
    raise pipeline.Retry('We need to try this again')

  def run_test(self):
    raise pipeline.Retry('We need to try this again')


class GeneratorForcesRetry(pipeline.Pipeline):
  """Test when a generator pipeline raises the Retry exception."""

  def run(self):
    if False:
      yield 1
    raise pipeline.Retry('We need to try this again')


class SyncRaiseAbort(pipeline.Pipeline):
  """Raises an abort signal."""

  def run(self):
    RunOrder.add('run SyncRaiseAbort')
    raise pipeline.Abort('Gotta bail!')

  def finalized(self):
    RunOrder.add('finalized SyncRaiseAbort: %s' % self.was_aborted)


class AsyncRaiseAbort(pipeline.Pipeline):
  """Raises an abort signal in an asynchronous pipeline."""

  async_ = True

  def run(self):
    raise pipeline.Abort('Gotta bail!')

  def run_test(self):
    raise pipeline.Abort('Gotta bail!')


class GeneratorRaiseAbort(pipeline.Pipeline):
  """Raises an abort signal in a generator pipeline."""

  def run(self):
    if False:
      yield 1
    raise pipeline.Abort('Gotta bail!')


class AbortAndRecordFinalized(pipeline.Pipeline):
  """Records calls to finalized."""

  def run(self):
    RunOrder.add('run AbortAndRecordFinalized')
    yield SyncRaiseAbort()

  def finalized(self):
    RunOrder.add('finalized AbortAndRecordFinalized: %s' %
                 self.was_aborted)


class SetStatusPipeline(pipeline.Pipeline):
  """Simple pipeline that just sets its status a few times."""

  def run(self):
    self.set_status(message='My message')
    self.set_status(console_url='/path/to/my/console')
    self.set_status(status_links=dict(one='/red', two='/blue'))
    self.set_status(message='My message',
                    console_url='/path/to/my/console',
                    status_links=dict(one='/red', two='/blue'))


class PassBadValue(pipeline.Pipeline):
  """Simple pipeline that passes along a non-JSON serializable value."""

  def run(self):
    yield EchoSync(object())


class ReturnBadValue(pipeline.Pipeline):
  """Simple pipeline that returns a non-JSON serializable value."""

  def run(self):
    return object()


class EchoParams(pipeline.Pipeline):
  """Echos the parameters this pipeline has."""

  def run(self):
    ALLOWED = ('backoff_seconds', 'backoff_factor', 'max_attempts', 'target')
    return dict((key, getattr(self, key)) for key in ALLOWED)


class WithParams(pipeline.Pipeline):
  """Simple pipeline that uses the with_params helper method."""

  def run(self):
    foo = yield EchoParams().with_params(
        max_attempts=8,
        backoff_seconds=99,
        target='other-backend')
    yield EchoSync(foo, 'stuff')


class FunctionalTest(test_shared.TaskRunningMixin, TestBase):
  """End-to-end tests for various Pipeline constructs."""

  def setUp(self):
    """Sets up the test harness."""
    super(FunctionalTest, self).setUp()

  def testStartSync(self):
    """Tests starting and executing just a synchronous pipeline."""
    stage = EchoSync(1, 2, 3)
    self.assertFalse(stage.async_)
    self.assertEqual((1, 2, 3), EchoSync(1, 2, 3).run(1, 2, 3))
    outputs = self.run_pipeline(stage)
    self.assertEqual([1, 2, 3], outputs.default.value)

  def testStartAsync(self):
    """Tests starting and executing an asynchronous pipeline."""
    stage = EchoAsync(1, 2, 3)
    self.assertTrue(stage.async_)
    outputs = self.run_pipeline(stage)
    self.assertEqual([1, 2, 3], outputs.default.value)

  def testSyncNamedOutputs(self):
    """Tests a synchronous pipeline with named outputs."""
    stage = EchoParticularNamedSync(
        one='red', two='blue', three='green', four='yellow')
    self.assertFalse(stage.async_)
    outputs = self.run_pipeline(stage)
    self.assertEqual(None, outputs.default.value)
    self.assertEqual('red', outputs.one.value)
    self.assertEqual('blue', outputs.two.value)
    self.assertEqual('green', outputs.three.value)
    self.assertEqual('yellow', outputs.four.value)

  def testAsyncNamedOutputs(self):
    """Tests an asynchronous pipeline with named outputs."""
    stage = EchoParticularNamedAsync(
        one='red', two='blue', three='green', four='yellow')
    self.assertTrue(stage.async_)
    outputs = self.run_pipeline(stage)
    self.assertEqual(None, outputs.default.value)
    self.assertEqual('red', outputs.one.value)
    self.assertEqual('blue', outputs.two.value)
    self.assertEqual('green', outputs.three.value)
    self.assertEqual('yellow', outputs.four.value)

  def testInheirtOutputs(self):
    """Tests when a pipeline generator child inherits all parent outputs."""
    stage = FillAndPassParticular(
        [],
        one='red', two='blue', three='green', four='yellow',
        prefix='passed-')
    outputs = self.run_pipeline(stage)
    self.assertEqual(None, outputs.default.value)
    self.assertEqual('passed-red', outputs.one.value)
    self.assertEqual('passed-blue', outputs.two.value)
    self.assertEqual('passed-green', outputs.three.value)
    self.assertEqual('passed-yellow', outputs.four.value)

  def testInheritOutputsPartial(self):
    """Tests when a pipeline generator child inherits some parent outputs."""
    stage = FillAndPassParticular(
        ['one', 'three'],
        one='red', two='blue', three='green', four='yellow',
        prefix='passed-')
    outputs = self.run_pipeline(stage)
    self.assertEqual(None, outputs.default.value)
    self.assertEqual('red', outputs.one.value)
    self.assertEqual('passed-blue', outputs.two.value)
    self.assertEqual('green', outputs.three.value)
    self.assertEqual('passed-yellow', outputs.four.value)

  def testInheritOutputsStrict(self):
    """Tests strict child of a pipeline generator inherits all outputs."""
    stage = StrictChildInheritsAll(
        one='red', two='blue', three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEqual(None, outputs.default.value)
    self.assertEqual('red', outputs.one.value)
    self.assertEqual('blue', outputs.two.value)
    self.assertEqual('green', outputs.three.value)
    self.assertEqual('yellow', outputs.four.value)

  def testInheritChildSyncStrictMissing(self):
    """Tests when a strict child pipeline does not output to a required slot."""
    stage = StrictChildInheritsAll(
        one='red', two='blue', three='green')
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testInheritChildSyncStrictNotDeclared(self):
    """Tests when a strict child pipeline outputs to an undeclared name."""
    stage = StrictChildInheritsAll(
        one='red', two='blue', three='green', four='yellow', five='undeclared')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testInheritGeneratorStrict(self):
    """Tests when a strict child pipeline inherits all outputs."""
    stage = StrictChildGeneratorInheritsAll(
        one='red', two='blue', three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEqual(None, outputs.default.value)
    self.assertEqual('red', outputs.one.value)
    self.assertEqual('blue', outputs.two.value)
    self.assertEqual('green', outputs.three.value)
    self.assertEqual('yellow', outputs.four.value)

  def testInheritGeneratorStrictMissing(self):
    """Tests when a strict child generator does not output to a slot."""
    stage = StrictChildGeneratorInheritsAll(
        one='red', two='blue', three='green')
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testInheritGeneratorStrictNotDeclared(self):
    """Tests when a strict child generator outputs to an undeclared name."""
    stage = StrictChildGeneratorInheritsAll(
        one='red', two='blue', three='green', four='yellow', five='undeclared')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testPartialConsumptionStrict(self):
    """Tests when a parent pipeline consumes a subset of strict child outputs.

    When the child is strict, then partial consumption is fine since all
    outputs must be declared ahead of time.
    """
    stage = ConsumePartialChildrenStrict(
        one='red', two='blue', three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEqual(['red', 'blue'], outputs.default.value)

  def testPartialConsumptionDynamic(self):
    """Tests when a parent pipeline consumes a subset of dynamic child outputs.

    When the child is dynamic, then all outputs must be consumed by the caller.
    """
    stage = ConsumePartialChildren(
        one='red', two='blue', three='green', four='yellow')
    with self.assertRaises(pipeline.SlotNotDeclaredError):
        self.run_pipeline(stage)

  def testNoDefaultConsumption(self):
    """Tests when a parent pipeline does not consume default output."""
    stage = DoNotConsumeDefault('hi there')
    outputs = self.run_pipeline(stage)
    self.assertEqual('hi there', outputs.default.value)

  def testGeneratorNoChildren(self):
    """Tests when a generator pipeline yields no children."""
    self.assertRaises(StopIteration, FillAndPass([]).run([]).__next__)
    stage = FillAndPass([])
    outputs = self.run_pipeline(stage)
    self.assertTrue(outputs.default.value is None)

  def testSyncMissingNamedOutput(self):
    """Tests when a sync pipeline does not output to a named output."""
    stage = EchoParticularNamedSync(one='red', two='blue', three='green')
    self.assertFalse(stage.async_)
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testGeneratorNoChildrenMissingNamedOutput(self):
    """Tests a missing output from a generator with no child pipelines."""
    stage = FillAndPassParticular(
        ['one', 'two', 'three'],
        one='red', two='blue', three='green')
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testSyncUndeclaredOutput(self):
    """Tests when a strict sync pipeline outputs to an undeclared output."""
    stage = EchoParticularNamedSync(
        one='red', two='blue', three='green', four='yellow', other='stuff')
    self.assertFalse(stage.async_)
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testGeneratorChildlessUndeclaredOutput(self):
    """Tests when a childless generator outputs to an undeclared output."""
    stage = FillAndPassParticular(
        ['one', 'two', 'three', 'four', 'other'],
        one='red', two='blue', three='green', four='yellow', other='stuff')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testRootGeneratorChildInheritOutputUndeclared(self):
    """Tests when root's child inherits all and outputs to a bad name."""
    stage = FillAndPassParticular(
        ['one', 'two'],
        one='red', two='blue', three='green', four='yellow', other='stuff')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testDeepGeneratorChildInheritOutputUndeclared(self):
    """Tests when a pipeline that is not the root outputs to a bad name."""
    stage = TwoLevelFillAndPass(
        one='red', two='blue', three='green', four='yellow', other='stuff')
    self.assertRaises(pipeline.SlotNotDeclaredError, self.run_pipeline, stage)

  def testDeepGenerator(self):
    """Tests a multi-level generator."""
    stage = TwoLevelFillAndPass(
        one='red', two='blue', three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEqual(None, outputs.default.value)
    self.assertEqual('second-first-red', outputs.one.value)
    self.assertEqual('second-first-blue', outputs.two.value)
    self.assertEqual('second-green', outputs.three.value)
    self.assertEqual('second-yellow', outputs.four.value)

  def testDeepGenerator_Huge(self):
    """Tests a multi-level generator with huge inputs and outputs."""
    big_data = 'blue' * 1000000
    stage = TwoLevelFillAndPass(
        one='red', two=big_data, three='green', four='yellow')
    outputs = self.run_pipeline(stage)
    self.assertEqual(None, outputs.default.value)
    self.assertEqual('second-first-red', outputs.one.value)
    self.assertEqual('second-first-' + big_data, outputs.two.value)
    self.assertEqual('second-green', outputs.three.value)
    self.assertEqual('second-yellow', outputs.four.value)

  def testOnlyConsumePassedOnOutputs(self):
    """Tests that just accessing a Slot on a PipelineFuture won't consume it."""
    stage = UnusedOutputReference()
    outputs = self.run_pipeline(stage)
    self.assertEqual('yellow', outputs.default.value)

  def testAccessUndeclaredOutputsBreaks(self):
    """Tests errors accessing undeclared outputs on a default-only pipeline."""
    stage = AccessUndeclaredDefaultOnly()
    self.assertRaises(pipeline.SlotNotFilledError, self.run_pipeline, stage)

  def testGeneratorRecursive(self):
    """Tests a recursive nesting of generators."""
    stage = EuclidGCD(1071, 462)
    outputs = self.run_pipeline(stage)
    self.assertEqual(21, outputs.gcd.value)

    stage = EuclidGCD(1071, 463)
    outputs = self.run_pipeline(stage)
    self.assertEqual(1, outputs.gcd.value)

  def testAfter(self):
    """Tests the After() class."""
    stage = DoAfter()
    self.run_pipeline(stage)
    self.assertEqual(['first', 'first', 'third', 'third'],
                      RunOrder.get())

  def testAfterWithNesting(self):
    """Tests that After() nesting of the same dependencies doesn't break."""
    stage = DoAfterNested()
    self.run_pipeline(stage)
    self.assertEqual(['first', 'first', 'third', 'third', 'fifth', 'fifth'],
                      RunOrder.get())

  def testAfterWithList(self):
    """Tests that After() with a list of dependencies works."""
    stage = DoAfterList()
    self.run_pipeline(stage)
    self.assertEqual( ['redredredredredredredredredred', 'twelfth'],
                      RunOrder.get())

  def testInOrder(self):
    """Tests the InOrder() class."""
    stage = DoInOrder()
    self.run_pipeline(stage)
    self.assertEqual(['first', 'second', 'third', 'fourth'],
                      RunOrder.get())

  def testInOrderNesting(self):
    """Tests that InOrder nesting is not allowed."""
    stage = DoInOrderNested()
    with self.assertRaises(pipeline.UnexpectedPipelineError):
      self.run_pipeline(stage)

  def testMixAfterInOrder(self):
    """Tests nesting Afters in InOrder blocks and vice versa."""
    stage = MixAfterInOrder()
    self.run_pipeline(stage)
    self.assertEqual(['first', 'second', 'third', 'fourth', 'fifth', 'sixth'],
                      RunOrder.get())

  def testFinalized(self):
    """Tests the order of finalization."""
    stage = NestedFinalize(5)
    self.run_pipeline(stage)
    run_order = RunOrder.get()

    # Ensure each entry is unique.
    self.assertEqual(10, len(set(run_order)))

    # That there are 5 run entries that are in reasonable order.
    run_entries = [
        int(r[len('run#'):]) for r in run_order
        if r.startswith('run#')]
    self.assertEqual(5, len(run_entries))
    self.assertEqual([5, 4, 3, 2, 1], run_entries)

    # That there are 5 finalized entries that are in reasonable order.
    if self.test_mode:
      finalized_name = 'finalized_test#'
    else:
      finalized_name = 'finalized#'

    finalized_entries = [
        int(r[len(finalized_name):]) for r in run_order
        if r.startswith(finalized_name)]
    self.assertEqual(5, len(finalized_entries))
    self.assertEqual([5, 4, 3, 2, 1], finalized_entries)

  def testRunTest(self):
    """Tests that run_test is preferred over run for test mode."""
    stage = RunMethod()
    outputs = self.run_pipeline(stage)
    if self.test_mode:
      self.assertEqual('run_test()', outputs.default.value)
    else:
      self.assertEqual('run()', outputs.default.value)

  def testYieldBadValue(self):
    """Tests yielding something that is invalid."""
    stage = YieldBadValue()
    self.assertRaises(
        pipeline.UnexpectedPipelineError, self.run_pipeline, stage)

  def testYieldPipelineInstanceTwice(self):
    """Tests when a Pipeline instance is yielded multiple times."""
    stage = YieldChildTwice()
    self.assertRaises(
        pipeline.UnexpectedPipelineError, self.run_pipeline, stage)

  def testFinalizeException(self):
    """Tests that finalized exceptions just raise up without being caught."""
    stage = FinalizeFailure()
    try:
      self.run_pipeline(stage)
      self.fail('Should have raised')
    except Exception as e:
      self.assertEqual('Doh something broke!', str(e))

  def testSyncRetryException(self):
    """Tests when a sync generator raises a Retry exception."""
    stage = SyncForcesRetry()
    self.assertRaises(pipeline.Retry, self.run_pipeline, stage)

  def testAsyncRetryException(self):
    """Tests when an async generator raises a Retry exception."""
    stage = AsyncForcesRetry()
    self.assertRaises(pipeline.Retry, self.run_pipeline, stage)

  def testGeneratorRetryException(self):
    """Tests when a generator raises a Retry exception."""
    stage = GeneratorForcesRetry()
    self.assertRaises(pipeline.Retry, self.run_pipeline, stage)

  def testSyncAbortException(self):
    """Tests when a sync pipeline raises an abort exception."""
    stage = SyncRaiseAbort()
    self.assertRaises(pipeline.Abort, self.run_pipeline, stage)

  def testAsyncAbortException(self):
    """Tests when an async pipeline raises an abort exception."""
    stage = AsyncRaiseAbort()
    self.assertRaises(pipeline.Abort, self.run_pipeline, stage)

  def testGeneratorAbortException(self):
    """Tests when a generator pipeline raises an abort exception."""
    stage = GeneratorRaiseAbort()
    self.assertRaises(pipeline.Abort, self.run_pipeline, stage)

  def testAbortThenFinalize(self):
    """Tests that pipelines are finalized after abort is raised.

    This test requires special handling for different modes to confirm that
    finalization happens after abort in production mode.
    """
    stage = AbortAndRecordFinalized()
    if self.test_mode:
      # Finalize after abort doesn't happen in test mode.
      try:
        self.run_pipeline(stage)
        self.fail('Should have raised')
      except Exception as e:
        self.assertEqual('Gotta bail!', str(e))

      run_order = RunOrder.get()
      self.assertEqual(['run AbortAndRecordFinalized', 'run SyncRaiseAbort'],
                        run_order)
    else:
      self.run_pipeline(stage, _task_retry=False, _require_slots_filled=False)
      # Non-deterministic results for finalize. Must equal one of these two.
      expected_order1 = [
          'run AbortAndRecordFinalized',
          'run SyncRaiseAbort',
          'finalized SyncRaiseAbort: True',
          'finalized AbortAndRecordFinalized: True',
      ]
      expected_order2 = [
          'run AbortAndRecordFinalized',
          'run SyncRaiseAbort',
          'finalized AbortAndRecordFinalized: True',
          'finalized SyncRaiseAbort: True',
      ]
      run_order = RunOrder.get()
      self.assertTrue(run_order == expected_order1 or
                      run_order == expected_order2,
                      'Found order: %r' % run_order)

  def testSetStatus_Working(self):
    """Tests that setting the status does not raise errors."""
    stage = SetStatusPipeline()
    self.run_pipeline(stage)
    # That's it. No exceptions raised.

  def testPassBadValue(self):
    """Tests when a pipeline passes a non-serializable value to a child."""
    stage = PassBadValue()
    self.assertRaises(TypeError, self.run_pipeline, stage)

  def testReturnBadValue(self):
    """Tests when a pipeline returns a non-serializable value."""
    stage = ReturnBadValue()
    self.assertRaises(TypeError, self.run_pipeline, stage)

  def testWithParams(self):
    """Tests when a pipeline uses the with_params helper."""
    stage = WithParams()
    outputs = self.run_pipeline(stage)
    if self.test_mode:
      # In test mode you cannot modify the runtime parameters.
      self.assertEqual(
          [
            {
              'backoff_seconds': 15,
              'backoff_factor': 2,
              'target': None,
              'max_attempts': 3
            },
            'stuff'
          ],
          outputs.default.value)
    else:
      self.assertEqual(
          [
            {
              'backoff_seconds': 99,
              'backoff_factor': 2,
              'target': 'other-backend',
              'max_attempts': 8
            },
            'stuff',
          ],
          outputs.default.value)


class FunctionalTestModeTest(test_shared.TestModeMixin, FunctionalTest):
  """Runs all functional tests in test mode."""

  DO_NOT_DELETE = 'Seriously... We only need the class declaration.'

  def testInOrderNesting(self):
    """Tests that InOrder nesting is not allowed."""
    # This test is not valid in test mode (see InOrder.__enter__)
    pass

  def testPartialConsumptionDynamic(self):
    """Tests when a parent pipeline consumes a subset of dynamic child outputs."""
    # This test is not valid in test mode (does not raise, raises in regular mode)
    pass

class StatusTest(TestBase):
  """Tests for the status handlers."""

  def setUp(self):
    """Sets up the test harness."""
    TestBase.setUp(self)

    self.fill_time = datetime.datetime(2010, 12, 10, 13, 55, 16, 416567)

    self.pipeline1_key = ndb.Key(_PipelineRecord, 'one')
    self.pipeline2_key = ndb.Key(_PipelineRecord, 'two')
    self.pipeline3_key = ndb.Key(_PipelineRecord, 'three')
    self.slot1_key = ndb.Key(_SlotRecord, 'red')
    self.slot2_key = ndb.Key(_SlotRecord, 'blue')
    self.slot3_key = ndb.Key(_SlotRecord, 'green')

    self.slot1_record = _SlotRecord(
        key=self.slot1_key,
        root_pipeline=self.pipeline1_key)
    self.slot2_record = _SlotRecord(
        key=self.slot2_key,
        root_pipeline=self.pipeline1_key)
    self.slot3_record = _SlotRecord(
        key=self.slot3_key,
        root_pipeline=self.pipeline1_key)

    self.base_params = {
       'args': [],
       'kwargs': {},
       'task_retry': False,
       'backoff_seconds': 1,
       'backoff_factor': 2,
       'max_attempts': 4,
       'queue_name': 'default',
       'base_path': '',
       'after_all': [],
    }
    self.params1 = self.base_params.copy()
    self.params1.update({
       'output_slots': {'default': self.slot1_key.urlsafe().decode()},
    })
    self.params2 = self.base_params.copy()
    self.params2.update({
       'output_slots': {'default': self.slot2_key.urlsafe().decode()},
    })
    self.params3 = self.base_params.copy()
    self.params3.update({
       'output_slots': {'default': self.slot3_key.urlsafe().decode()},
    })

    self.pipeline1_record = _PipelineRecord(
        root_pipeline=self.pipeline1_key,
        status=_PipelineRecord.RUN,
        class_path='does.not.exist1',
        params_text=json.dumps(self.params1),
        key=self.pipeline1_key,
        max_attempts=4)
    self.pipeline2_record = _PipelineRecord(
        root_pipeline=self.pipeline1_key,
        status=_PipelineRecord.WAITING,
        class_path='does.not.exist2',
        params_text=json.dumps(self.params2),
        key=self.pipeline2_key,
        max_attempts=3)
    self.pipeline3_record = _PipelineRecord(
        root_pipeline=self.pipeline1_key,
        status=_PipelineRecord.DONE,
        class_path='does.not.exist3',
        params_text=json.dumps(self.params3),
        key=self.pipeline3_key,
        max_attempts=2)

    self.barrier1_record = _BarrierRecord(
        parent=self.pipeline1_key,
        id=_BarrierRecord.FINALIZE,
        target=self.pipeline1_key,
        root_pipeline=self.pipeline1_key,
        blocking_slots=[self.slot1_key])
    self.barrier2_record = _BarrierRecord(
        parent=self.pipeline2_key,
        id=_BarrierRecord.FINALIZE,
        target=self.pipeline2_key,
        root_pipeline=self.pipeline1_key,
        blocking_slots=[self.slot2_key])
    self.barrier2_record_start = _BarrierRecord(
        parent=self.pipeline2_key,
        id=_BarrierRecord.START,
        target=self.pipeline2_key,
        root_pipeline=self.pipeline1_key,
        blocking_slots=[])
    self.barrier3_record = _BarrierRecord(
        parent=self.pipeline3_key,
        id=_BarrierRecord.FINALIZE,
        target=self.pipeline3_key,
        root_pipeline=self.pipeline1_key,
        blocking_slots=[self.slot3_key])

  def testGetTimestampMs(self):
    """Tests for the _get_timestamp_ms function."""
    when = datetime.datetime(2010, 12, 10, 13, 55, 16, 416567)
    self.assertEqual(1291989316416, pipeline._get_timestamp_ms(when))

  def testGetInternalStatus_Missing(self):
    """Tests for _get_internal_status when the pipeline is missing."""
    try:
      pipeline._get_internal_status(pipeline_key=self.pipeline1_key)
      self.fail('Did not raise')
    except pipeline.PipelineStatusError as e:
      self.assertEqual('Could not find pipeline ID "one"', str(e))

  def testGetInternalStatus_OutputSlotMissing(self):
    """Tests for _get_internal_status when the output slot is missing."""
    try:
      pipeline._get_internal_status(
          pipeline_key=self.pipeline1_key,
          pipeline_dict={self.pipeline1_key: self.pipeline1_record},
          barrier_dict={self.barrier1_record.key: self.barrier1_record})
      self.fail('Did not raise')
    except pipeline.PipelineStatusError as e:
      self.assertEqual(
          'Default output slot with '
          'key=aglteS1hcHAtaWRyGgsSEV9BRV9QaXBlbGluZV9TbG90IgNyZWQM '
          'missing for pipeline ID "one"', str(e))

  def testGetInternalStatus_FinalizeBarrierMissing(self):
    """Tests for _get_internal_status when the finalize barrier is missing."""
    try:
      pipeline._get_internal_status(
          pipeline_key=self.pipeline1_key,
          pipeline_dict={self.pipeline1_key: self.pipeline1_record},
          slot_dict={self.slot1_key: self.slot1_record})
      self.fail('Did not raise')
    except pipeline.PipelineStatusError as e:
      self.assertEqual(
          'Finalization barrier missing for pipeline ID "one"', str(e))

  def testGetInternalStatus_Finalizing(self):
    """Tests for _get_internal_status when the status is finalizing."""
    self.slot1_record.status = _SlotRecord.FILLED
    self.slot1_record.fill_time = self.fill_time

    expected = {
      'status': 'finalizing',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'outputs': {
        'default': self.slot1_key.urlsafe().decode(),
      },
      'args': [],
      'classPath': 'does.not.exist1',
      'children': [],
      'endTimeMs': 1291989316416,
      'maxAttempts': 4,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEqual(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key: self.barrier1_record}))

  def testGetInternalStatus_Retry(self):
    """Tests for _get_internal_status when the status is retry."""
    self.pipeline2_record.next_retry_time = self.fill_time
    self.pipeline2_record.retry_message = 'My retry message'

    expected = {
      'status': 'retry',
      'lastRetryMessage': 'My retry message',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'startTimeMs': 1291989316416,
      'outputs': {
        'default': self.slot2_key.urlsafe().decode(),
      },
      'args': [],
      'classPath': 'does.not.exist2',
      'children': [],
      'maxAttempts': 3,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEqual(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline2_key,
        pipeline_dict={self.pipeline2_key: self.pipeline2_record},
        slot_dict={self.slot2_key: self.slot1_record},
        barrier_dict={self.barrier2_record.key: self.barrier2_record}))

  def testGetInternalStatus_Waiting(self):
    """Tests for _get_internal_status when the status is waiting."""
    expected = {
      'status': 'waiting',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'outputs': {
        'default': self.slot2_key.urlsafe().decode(),
      },
      'args': [],
      'classPath': 'does.not.exist2',
      'children': [],
      'maxAttempts': 3,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEqual(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline2_key,
        pipeline_dict={self.pipeline2_key: self.pipeline2_record},
        slot_dict={self.slot2_key: self.slot1_record},
        barrier_dict={
            self.barrier2_record.key: self.barrier2_record,
            self.barrier2_record_start.key: self.barrier2_record_start}))

  def testGetInternalStatus_Run(self):
    """Tests for _get_internal_status when the status is run."""
    self.pipeline1_record.start_time = self.fill_time

    expected = {
      'status': 'run',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'startTimeMs': 1291989316416,
      'outputs': {
        'default': self.slot1_key.urlsafe().decode(),
      },
      'args': [],
      'classPath': 'does.not.exist1',
      'children': [],
      'maxAttempts': 4,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEqual(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key: self.barrier1_record}))

  def testGetInternalStatus_RunAfterRetry(self):
    """Tests _get_internal_status when a stage is re-run on retrying."""
    self.pipeline1_record.start_time = self.fill_time
    self.pipeline1_record.next_retry_time = self.fill_time
    self.pipeline1_record.retry_message = 'My retry message'
    self.pipeline1_record.current_attempt = 1

    expected = {
      'status': 'run',
      'currentAttempt': 2,
      'lastRetryMessage': 'My retry message',
      'afterSlotKeys': [],
      'startTimeMs': 1291989316416,
      'outputs': {
        'default': self.slot1_key.urlsafe().decode(),
      },
      'args': [],
      'classPath': 'does.not.exist1',
      'children': [],
      'maxAttempts': 4,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEqual(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key: self.barrier1_record}))

  def testGetInternalStatus_Aborted(self):
    """Tests for _get_internal_status when the status is aborted."""
    self.pipeline1_record.status = _PipelineRecord.ABORTED
    self.pipeline1_record.abort_message = 'I had to bail'

    expected = {
      'status': 'aborted',
      'currentAttempt': 1,
      'afterSlotKeys': [],
      'abortMessage': 'I had to bail',
      'outputs': {
        'default': self.slot1_key.urlsafe().decode(),
      },
      'args': [],
      'classPath': 'does.not.exist1',
      'children': [],
      'maxAttempts': 4,
      'kwargs': {},
      'backoffFactor': 2,
      'backoffSeconds': 1,
      'queueName': 'default'
    }

    self.assertEqual(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key: self.barrier1_record}))

  def testGetInternalStatus_MoreParams(self):
    """Tests for _get_internal_status with children, slots, and outputs."""
    self.pipeline1_record.start_time = self.fill_time
    self.pipeline1_record.fanned_out = [
        self.pipeline2_key, self.pipeline3_key]
    self.pipeline1_record.params['args'] = [
        {'type': 'slot', 'slot_key': 'foobar'},
        {'type': 'slot', 'slot_key': 'meepa'},
    ]
    self.pipeline1_record.params['kwargs'] = {
        'my_arg': {'type': 'slot', 'slot_key': 'other'},
        'second_arg': {'type': 'value', 'value': 1234},
    }
    self.pipeline1_record.params['output_slots'] = {
      'default': self.slot1_key.urlsafe().decode(),
      'another_one': self.slot2_key.urlsafe().decode(),
    }
    self.pipeline1_record.params['after_all'] = [
      self.slot2_key.urlsafe().decode(),
    ]

    expected = {
        'status': 'run',
        'currentAttempt': 1,
        'afterSlotKeys': [
          'aglteS1hcHAtaWRyGwsSEV9BRV9QaXBlbGluZV9TbG90IgRibHVlDA'
        ],
        'startTimeMs': 1291989316416,
        'outputs': {
          'default': 'aglteS1hcHAtaWRyGgsSEV9BRV9QaXBlbGluZV9TbG90IgNyZWQM',
          'another_one':
              'aglteS1hcHAtaWRyGwsSEV9BRV9QaXBlbGluZV9TbG90IgRibHVlDA',
        },
        'args': [
          {'type': 'slot', 'slotKey': 'foobar'},
          {'type': 'slot', 'slotKey': 'meepa'}
        ],
        'classPath': 'does.not.exist1',
        'children': ['two', 'three'],
        'maxAttempts': 4,
        'kwargs': {
          'my_arg': {'type': 'slot', 'slotKey': 'other'},
          'second_arg': {'type': 'value', 'value': 1234},
        },
        'backoffFactor': 2,
        'backoffSeconds': 1,
        'queueName': 'default'
    }

    self.assertEqual(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key: self.barrier1_record}))

  def testGetInternalStatus_StatusRecord(self):
    """Tests for _get_internal_status when the status record is present."""
    status_record = _StatusRecord(
        key=ndb.Key(_StatusRecord, self.pipeline1_key.string_id()),
        message='My status message',
        status_time=self.fill_time,
        console_url='/path/to/console',
        link_names=['one', 'two', 'three'],
        link_urls=['/first', '/second', '/third'],
        root_pipeline=self.pipeline1_key)

    expected = {
        'status': 'run',
        'currentAttempt': 1,
        'afterSlotKeys': [],
        'statusTimeMs': 1291989316416,
        'outputs': {
          'default': self.slot1_key.urlsafe().decode(),
        },
        'args': [],
        'classPath': 'does.not.exist1',
        'children': [],
        'maxAttempts': 4,
        'kwargs': {},
        'backoffFactor': 2,
        'backoffSeconds': 1,
        'queueName': 'default',
        'statusLinks': {
          'three': '/third',
          'two': '/second',
          'one': '/first'
        },
        'statusConsoleUrl': '/path/to/console',
        'statusMessage': 'My status message',
    }

    self.assertEqual(expected, pipeline._get_internal_status(
        pipeline_key=self.pipeline1_key,
        pipeline_dict={self.pipeline1_key: self.pipeline1_record},
        slot_dict={self.slot1_key: self.slot1_record},
        barrier_dict={self.barrier1_record.key: self.barrier1_record},
        status_dict={status_record.key: status_record}))

  def testGetInternalSlot_Missing(self):
    """Tests _get_internal_slot when the slot is missing."""
    try:
      pipeline._get_internal_slot(slot_key=self.slot1_key)
      self.fail('Did not raise')
    except pipeline.PipelineStatusError as e:
      self.assertEqual(
          'Could not find data for output slot key '
          '"aglteS1hcHAtaWRyGgsSEV9BRV9QaXBlbGluZV9TbG90IgNyZWQM".',
          str(e))

  def testGetInternalSlot_Filled(self):
    """Tests _get_internal_slot when the slot is filled."""
    self.slot1_record.status = _SlotRecord.FILLED
    self.slot1_record.filler = self.pipeline2_key
    self.slot1_record.fill_time = self.fill_time
    self.slot1_record.root_pipeline = self.pipeline1_key
    self.slot1_record.value_text = json.dumps({
        'one': 1234, 'two': 'hello'})
    expected = {
        'status': 'filled',
        'fillerPipelineId': 'two',
        'value': {'two': 'hello', 'one': 1234},
        'fillTimeMs': 1291989316416
    }
    self.assertEqual(
        expected,
        pipeline._get_internal_slot(
            slot_key=self.slot1_key,
            slot_dict={self.slot1_key: self.slot1_record}))

  def testGetInternalSlot_Waiting(self):
    """Tests _get_internal_slot when the slot is waiting."""
    self.slot1_record.status = _SlotRecord.WAITING
    self.slot1_record.root_pipeline = self.pipeline1_key
    expected = {
        'status': 'waiting',
        'fillerPipelineId': 'two',
    }
    self.assertEqual(
        expected,
        pipeline._get_internal_slot(
            slot_key=self.slot1_key,
            slot_dict={self.slot1_key: self.slot1_record},
            filler_pipeline_key=self.pipeline2_key))

  def testGetStatusTree_RootMissing(self):
    """Tests get_status_tree when the root pipeline is missing."""
    try:
      pipeline.get_status_tree(self.pipeline1_key.string_id())
      self.fail('Did not raise')
    except pipeline.PipelineStatusError as e:
      self.assertEqual('Could not find pipeline ID "one"', str(e))

  def testGetStatusTree_NotRoot(self):
    """Tests get_status_tree when the pipeline query is not the root."""
    found1_root = self.pipeline1_record.root_pipeline
    found2_root = self.pipeline2_record.root_pipeline

    self.assertEqual(found1_root, self.pipeline1_key)
    self.assertEqual(found2_root, self.pipeline1_key)

    ndb.put_multi([self.pipeline1_record, self.pipeline2_record,
            self.slot1_record, self.slot2_record,
            self.barrier1_record, self.barrier2_record])

    pipeline.get_status_tree(self.pipeline2_key.string_id())

    expected = {
        'pipelines': {
            'one': {
                'afterSlotKeys': [],
                'args': [],
                'backoffFactor': 2,
                'backoffSeconds': 1,
                'children': [],
                'classPath': 'does.not.exist1',
                'currentAttempt': 1,
                'kwargs': {},
                'maxAttempts': 4,
                'outputs': {
                    'default': self.slot1_key.urlsafe().decode()
                },
                'queueName': 'default',
                'status': 'run',
            },
        },
        'rootPipelineId': 'one',
        'slots': {},
    }

    self.assertEqual(
        expected,
        pipeline.get_status_tree(self.pipeline2_key.string_id()))

  def testGetStatusTree_NotRoot_MissingParent(self):
    """Tests get_status_tree with a non-root pipeline and missing parent."""
    found1_root = self.pipeline1_record.root_pipeline
    found2_root = self.pipeline2_record.root_pipeline

    self.assertEqual(found1_root, self.pipeline1_key)
    self.assertEqual(found2_root, self.pipeline1_key)

    # Don't put pipeline1_record
    ndb.put_multi([self.pipeline2_record, self.slot1_record, self.slot2_record,
            self.barrier1_record, self.barrier2_record])

    try:
      pipeline.get_status_tree(self.pipeline1_key.string_id())
      self.fail('Did not raise')
    except pipeline.PipelineStatusError as e:
      self.assertEqual('Could not find pipeline ID "one"', str(e))

  def testGetStatusTree_ChildMissing(self):
    """Tests get_status_tree when a fanned out child pipeline is missing."""
    self.pipeline1_record.fanned_out = [self.pipeline2_key]
    ndb.put_multi([self.pipeline1_record, self.barrier1_record, self.slot1_record])

    try:
      pipeline.get_status_tree(self.pipeline1_key.string_id())
      self.fail('Did not raise')
    except pipeline.PipelineStatusError as e:
      self.assertEqual(
          'Pipeline ID "one" points to child ID "two" which does not exist.',
          str(e))

  def testGetStatusTree_Example(self):
    """Tests a full example of a good get_status_tree response."""
    self.pipeline1_record.fanned_out = [self.pipeline2_key, self.pipeline3_key]
    self.slot1_record.root_pipeline = self.pipeline1_key
    self.pipeline3_record.finalized_time = self.fill_time

    # This one looks like a child, but it will be ignored since it is not
    # reachable from the root via the fanned_out property.
    bad_pipeline_key = ndb.Key(_PipelineRecord, 'ignored')
    bad_pipeline_record = _PipelineRecord(
        root_pipeline=self.pipeline1_key,
        status=_PipelineRecord.RUN,
        class_path='does.not.exist4',
        params_text=json.dumps(self.params1),
        key=bad_pipeline_key,
        max_attempts=4)

    ndb.put_multi([
        self.pipeline1_record, self.pipeline2_record, self.pipeline3_record,
        self.barrier1_record, self.barrier2_record, self.barrier3_record,
        self.slot1_record, self.slot2_record, self.slot3_record,
        bad_pipeline_record])

    expected = {
        'rootPipelineId': 'one',
        'pipelines': {
            'three': {
              'status': 'done',
              'currentAttempt': 1,
              'afterSlotKeys': [],
              'outputs': {
                  'default': self.slot3_key.urlsafe().decode()
              },
              'args': [],
              'classPath': 'does.not.exist3',
              'children': [],
              'endTimeMs': 1291989316416,
              'maxAttempts': 2,
              'kwargs': {},
              'backoffFactor': 2,
              'backoffSeconds': 1,
              'queueName': 'default'
            },
            'two': {
                'status': 'run',
                'currentAttempt': 1,
                'afterSlotKeys': [],
                'outputs': {
                    'default': self.slot2_key.urlsafe().decode()
                },
                'args': [],
                'classPath': 'does.not.exist2',
                'children': [],
                'maxAttempts': 3,
                'kwargs': {},
                'backoffFactor': 2,
                'backoffSeconds': 1,
                'queueName': 'default'
            },
            'one': {
                'status': 'run',
                'currentAttempt': 1,
                'afterSlotKeys': [],
                'outputs': {
                    'default': self.slot1_key.urlsafe().decode()
                },
                'args': [],
                'classPath': 'does.not.exist1',
                'children': ['two', 'three'],
                'maxAttempts': 4,
                'kwargs': {},
                'backoffFactor': 2,
                'backoffSeconds': 1,
                'queueName': 'default'
            }
        },
        'slots': {
            self.slot2_key.urlsafe().decode(): {
                'status': 'waiting',
                'fillerPipelineId': 'two'
            },
            self.slot3_key.urlsafe().decode(): {
                'status': 'waiting',
                'fillerPipelineId': 'three'
            }
        }
    }

    self.assertEqual(
        expected,
        pipeline.get_status_tree(self.pipeline1_key.string_id()))

  def testGetPipelineNames(self):
    """Tests the get_pipeline_names function."""
    names = pipeline.get_pipeline_names()
    self.assertTrue(None not in names)  # No base-class Pipeline
    self.assertIn(EchoAsync._class_path, names)

    found = False
    for name in names:
      # Name may be relative to another module, like 'foo.pipeline.common...'
      found = 'pipeline.common.All' in name
      if found:
        break
    self.assertTrue(found)

  def testGetRootList(self):
    """Tests the get_root_list function."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(idempotence_key='banana')
    stage.set_status('This one has a message')

    stage2 = EchoSync('one')
    stage2.start(idempotence_key='lemon')

    found = pipeline.get_root_list()
    self.assertFalse('cursor' in found)  # No next page available

    found_names = [
        (p['pipelineId'], p['classPath']) for p in found['pipelines']]
    expected = [
        ('lemon', EchoSync._class_path),
        ('banana', NothingPipeline._class_path)
    ]
    self.assertEqual(expected, found_names)

    self.assertEqual('This one has a message',
                      found['pipelines'][1]['statusMessage'])

  def testGetRootList_FinalizeBarrierMissing(self):
    """Tests get_status_tree when a finalization barrier is missing."""
    stage = NothingPipeline('one', 'two', three='red', four=1234)
    stage.start(idempotence_key='banana')
    stage.set_status('This one has a message')

    stage_key = ndb.Key(_PipelineRecord, stage.pipeline_id)
    finalization_key = ndb.Key(_BarrierRecord, _BarrierRecord.FINALIZE,
        parent=stage_key)
    finalization_key.delete()

    found = pipeline.get_root_list()
    self.assertFalse('cursor' in found)  # No next page available

    found_names = [
        (p['pipelineId'], p['classPath']) for p in found['pipelines']]
    expected = [
        ('banana', '')
    ]
    self.assertEqual(expected, found_names)

    self.assertEqual(
        'Finalization barrier missing for pipeline ID "%s"' % stage.pipeline_id,
        found['pipelines'][0]['status'])

  def testGetRootListCursor(self):
    """Tests the count and cursor behavior of get_root_list."""
    NothingPipeline().start(idempotence_key='banana')
    NothingPipeline().start(idempotence_key='lemon')

    # Find newest
    found = pipeline.get_root_list(count=1)
    self.assertIn('cursor', found)
    self.assertEqual(1, len(found['pipelines']))
    self.assertEqual('lemon', found['pipelines'][0]['pipelineId'])

    # Find next newest, and no cursor should be returned.
    found = pipeline.get_root_list(count=1, cursor=found['cursor'])
    self.assertFalse('cursor' in found)
    self.assertEqual(1, len(found['pipelines']))
    self.assertEqual('banana', found['pipelines'][0]['pipelineId'])

  def testGetRootListClassPath(self):
    """Tests filtering a root list to a single class_path."""
    NothingPipeline().start(idempotence_key='banana')
    NothingPipeline().start(idempotence_key='lemon')
    EchoSync('one').start(idempotence_key='tomato')

    found = pipeline.get_root_list(class_path=NothingPipeline.class_path)
    self.assertEqual([NothingPipeline._class_path, NothingPipeline._class_path],
                      [p['classPath'] for p in found['pipelines']])

    found = pipeline.get_root_list(class_path=EchoSync.class_path)
    self.assertEqual([EchoSync._class_path],
                      [p['classPath'] for p in found['pipelines']])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
