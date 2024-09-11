"""Shared code used by Pipeline API tests."""

import base64
import calendar
import datetime
import logging
import random
import re
import urllib
import urllib.parse

from flask import Flask
from google.appengine.api import apiproxy_stub_map

import pipeline

# For convenience.
_PipelineRecord = pipeline.models._PipelineRecord
_SlotRecord = pipeline.models._SlotRecord
_BarrierRecord = pipeline.models._BarrierRecord

def get_tasks(queue_name='default'):
  """Gets pending tasks from a queue, adding a 'params' dictionary to them.

  Code originally from:
    http://code.google.com/p/pubsubhubbub/source/browse/trunk/hub/testutil.py
  """
  taskqueue_stub = apiproxy_stub_map.apiproxy.GetStub('taskqueue')

  stub_globals = taskqueue_stub.GetTasks.__globals__
  old_format = stub_globals['_FormatEta']
  # Yes-- this is a vicious hack to have the task queue stub return the
  # ETA of tasks as datetime instances instead of text strings.
  stub_globals['_FormatEta'] = \
      lambda x: datetime.datetime.utcfromtimestamp(x / 1000000.0)
  try:
    task_list = taskqueue_stub.GetTasks(queue_name)
  finally:
    stub_globals['_FormatEta'] = old_format

  adjusted_task_list = []
  for task in task_list:
    for header, value in task['headers']:
      if (header == 'content-type' and
          value == 'application/x-www-form-urlencoded'):
        task['params'] = urllib.parse.parse_qs(base64.b64decode(task['body']).decode('latin1'))
        break
    adjusted_task_list.append(task)
  return adjusted_task_list


def delete_tasks(task_list, queue_name='default'):
  """Deletes a set of tasks from a queue."""
  taskqueue_stub = apiproxy_stub_map.apiproxy.GetStub('taskqueue')
  for task in task_list:
    # NOTE: Use Delete here instad of DeleteTask because DeleteTask will
    # remove the task's name from the list of tombstones, which will cause
    # some tasks to run multiple times in tests if barriers fire twice.
    taskqueue_stub._GetGroup().GetQueue(queue_name).Delete(task['name'])

def utc_to_local(utc_datetime):
    timestamp = calendar.timegm(utc_datetime.timetuple())
    local_datetime = datetime.datetime.fromtimestamp(timestamp)
    return local_datetime.replace(microsecond=utc_datetime.microsecond)

class TaskRunningMixin:
  """A mix-in that runs a Pipeline using tasks."""

  def setUp(self):
    """Sets up the test harness."""
    super().setUp()
    self.taskqueue_stub = apiproxy_stub_map.apiproxy.GetStub('taskqueue')
    self.queue_name = 'default'
    self.base_path = '/_ah/pipeline'
    self.test_mode = False
    self.app = Flask(__name__)

  def tearDown(self):
    """Make sure all tasks are deleted."""
    delete_tasks(self.get_tasks())

  def get_tasks(self):
    """Gets pending tasks, adding a 'params' dictionary to them."""
    task_list = get_tasks(self.queue_name)
    # Shuffle the task list to actually test out-of-order execution.
    random.shuffle(task_list)
    return task_list

  def run_task(self, task):
      """Runs the given task against the pipeline handlers."""
      name = task['name']
      method = task['method']
      url = task['url']
      headers = dict(task['headers'])
      data = base64.b64decode(task['body'])

      headers.update({
        'X-AppEngine-TaskName': name,
        'X-AppEngine-QueueName': self.queue_name,
      })
      
      match_url = url
      for pattern, handler_class in pipeline.create_handlers_map(self.base_path):
        the_match = re.match('^%s$' % pattern, match_url)
        if the_match:
          break
      else:
        logging.warning('No matching handler for "%s %s"' % (method, url))
        return

      handler = handler_class()
      
      logging.debug('Executing "%s %s" name="%s"', method, url, name)
      with self.app.test_request_context(url, method=method, data=data, headers=headers):
        getattr(handler, method.lower())(*the_match.groups())

  def run_pipeline(self, pipeline, *args, **kwargs):
    """Runs the pipeline and returns outputs."""
    require_slots_filled = kwargs.pop('_require_slots_filled', True)
    task_retry = kwargs.pop('_task_retry', True)

    pipeline.task_retry = task_retry
    pipeline.start(*args, **kwargs)
    while True:
      task_list = self.get_tasks()
      if not task_list:
        break

      for task in task_list:
        self.run_task(task)
        delete_tasks([task])

    if require_slots_filled:
      for slot_record in _SlotRecord.all():
        self.assertEqual(_SlotRecord.FILLED, slot_record.status,
                          '_SlotRecord = %r' % slot_record.key())
      for barrier_record in _BarrierRecord.all():
        self.assertEqual(_BarrierRecord.FIRED, barrier_record.status,
                          '_BarrierRecord = %r' % barrier_record.key())
      for pipeline_record in _PipelineRecord.all():
        self.assertEqual(_PipelineRecord.DONE, pipeline_record.status,
                          '_PipelineRecord = %r' % pipeline_record.key())

    return pipeline.__class__.from_id(pipeline.pipeline_id).outputs


class TestModeMixin:
  """A mix-in that runs a pipeline using the test mode."""

  def setUp(self):
    super().setUp()
    self.test_mode = True

  def run_pipeline(self, pipeline, *args, **kwargs):
    """Runs the pipeline."""
    kwargs.pop('_require_slots_filled', True)  # Unused
    kwargs.pop('_task_retry', True)  # Unused
    pipeline.start_test(*args, **kwargs)
    return pipeline.outputs
