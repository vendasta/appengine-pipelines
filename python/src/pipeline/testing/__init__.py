"""Shared code used by Pipeline API tests."""

import base64
import calendar
import datetime
import logging
import os
import random
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
        task['params'] = urllib.parse.parse_qs(base64.b64decode(task['body']).decode())
        break
    adjusted_task_list.append(task)
  return adjusted_task_list


def delete_tasks(task_list, queue_name='default'):
  """Deletes a set of tasks from a queue."""
  taskqueue_stub = apiproxy_stub_map.apiproxy.GetStub('taskqueue')
  for task in task_list:
    # NOTE: Use Delete here instead of DeleteTask because DeleteTask will remove the task's name from the list of
    # tombstones, which will cause some tasks to run multiple times in tests if barriers fire twice.
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
    self.app.testing = True
    for route, handler in pipeline.create_handlers_map():
      self.app.add_url_rule(route, view_func=handler.as_view(route.lstrip("/")))
    try:
      import mapreduce
      for route, handler in mapreduce.create_handlers_map():
        self.app.add_url_rule(route, view_func=handler.as_view(route.lstrip("/")))
    except ImportError:
      pass

  def tearDown(self):
    """Make sure all tasks are deleted."""
    if self.taskqueue_stub._queues.get(self.queue_name):
      delete_tasks(self.get_tasks(), queue_name=self.queue_name)

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

      os.environ['HTTP_X_APPENGINE_TASKNAME'] = name
      os.environ['HTTP_X_APPENGINE_QUEUENAME'] = self.queue_name

      with self.app.test_client() as c:
          response = c.open(url, method=method, data=data, headers=headers)
          if response.status_code != 200:
              logging.error('Task failed: %s %s %s %s %s %s',
                            name, method, url, headers, data, response.data)

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
        delete_tasks([task], queue_name=self.queue_name)

    if require_slots_filled:
      for slot_record in _SlotRecord.query():
        self.assertEqual(_SlotRecord.FILLED, slot_record.status,
                          '_SlotRecord = %r' % slot_record.key)
      for barrier_record in _BarrierRecord.query():
        self.assertEqual(_BarrierRecord.FIRED, barrier_record.status,
                          '_BarrierRecord = %r' % barrier_record.key)
      for pipeline_record in _PipelineRecord.query():
        self.assertEqual(_PipelineRecord.DONE, pipeline_record.status,
                          '_PipelineRecord = %r' % pipeline_record.key)

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
