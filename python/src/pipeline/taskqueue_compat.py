"""Compatibility layer for migrating from App Engine Task Queue to Cloud Tasks.

This module provides a drop-in replacement for google.appengine.api.taskqueue
using the Cloud Tasks API (google.cloud.tasks_v2).

Uses AppEngineHttpRequest to dispatch tasks, which preserves X-AppEngine-*
headers (TaskName, QueueName, TaskRetryCount) and uses App Engine's internal
routing, maintaining full backward compatibility with existing handler auth.
"""

import base64
import datetime
import logging
import os
import urllib.parse
from typing import Dict, Any, List

try:
    from google.cloud import tasks_v2
    from google.protobuf import timestamp_pb2
except ImportError:
    tasks_v2 = None
    timestamp_pb2 = None


_TEST_MODE = False


def set_test_mode(enabled: bool):
    global _TEST_MODE
    _TEST_MODE = enabled


class Error(Exception):
    """Base taskqueue error type."""


class TombstonedTaskError(Error):
    """Task name has been tombstoned (task was recently deleted/completed)."""


class TaskAlreadyExistsError(Error):
    """Task with the same name already exists."""


class InvalidTaskError(Error):
    """Task parameters are invalid."""


class Task:
    """Compatibility wrapper for Task Queue Task using Cloud Tasks."""

    def __init__(self, url=None, params=None, name=None, method='POST',
                 headers=None, countdown=None, eta=None, target=None, **kwargs):
        self.url = url
        self.params = params or {}
        self.name = name
        self.method = method.upper()
        self.headers = headers or {}
        self.countdown = countdown
        self.target = target
        self.kwargs = kwargs

        if eta and countdown:
            raise InvalidTaskError('Cannot specify both countdown and eta')
        if eta:
            self.eta = eta
        elif countdown:
            self.eta = datetime.datetime.utcnow() + datetime.timedelta(seconds=countdown)
        else:
            self.eta = None

        if target:
            app_id = os.environ.get('GOOGLE_CLOUD_PROJECT', os.environ.get('GAE_APPLICATION', ''))
            self.headers['Host'] = f'{target}.{app_id}.appspot.com'

    @property
    def payload(self):
        if self.params:
            return urllib.parse.urlencode(self.params, doseq=True)
        return ''

    def add(self, queue_name='default', transactional=False):
        if transactional:
            logging.warning(
                "Cloud Tasks does not support transactional task enqueueing. "
                "Task will be added non-transactionally. Ensure handlers are idempotent."
            )

        queue = Queue(queue_name)
        queue.add(self)


class Queue:
    """Compatibility wrapper for Task Queue Queue using Cloud Tasks."""

    def __init__(self, name='default'):
        self.name = name
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = tasks_v2.CloudTasksClient()
        return self._client

    def _get_queue_path(self):
        project = os.environ.get('GOOGLE_CLOUD_PROJECT')
        location = os.environ.get('CLOUD_TASKS_LOCATION', 'us-central1')

        if not project:
            raise ValueError(
                "GOOGLE_CLOUD_PROJECT environment variable must be set"
            )

        return self.client.queue_path(project, location, self.name)

    def add(self, task_or_tasks):
        tasks_list = task_or_tasks if isinstance(task_or_tasks, list) else [task_or_tasks]

        if _TEST_MODE:
            from . import taskqueue_test_stub
            stub = taskqueue_test_stub.get_test_stub()
            first_error = None
            for task_obj in tasks_list:
                task_dict = self._task_to_dict(task_obj)
                try:
                    stub.add_task(self.name, task_dict)
                except Exception as e:
                    error_msg = str(e).lower()
                    if 'already exists' in error_msg:
                        if first_error is None:
                            first_error = TaskAlreadyExistsError(str(e))
                    elif 'tombstoned' in error_msg:
                        if first_error is None:
                            first_error = TombstonedTaskError(str(e))
                    else:
                        raise
            if first_error is not None:
                raise first_error
            return

        queue_path = self._get_queue_path()

        for task_obj in tasks_list:
            try:
                cloud_task = self._convert_task(task_obj)
                self.client.create_task(request={
                    "parent": queue_path,
                    "task": cloud_task
                })
            except Exception as e:
                error_msg = str(e).lower()
                if 'already exists' in error_msg or 'task_already_exists' in error_msg:
                    raise TaskAlreadyExistsError(f"Task already exists: {e}")
                elif 'tombstoned' in error_msg or 'recently deleted' in error_msg:
                    raise TombstonedTaskError(f"Task was tombstoned: {e}")
                else:
                    raise

    def _task_to_dict(self, task_obj: Task) -> Dict[str, Any]:
        body = b''
        headers = list(task_obj.headers.items()) if task_obj.headers else []

        if task_obj.params:
            body = urllib.parse.urlencode(task_obj.params, doseq=True).encode('utf-8')
            headers.append(('content-type', 'application/x-www-form-urlencoded'))

        return {
            'name': task_obj.name or '',
            'url': task_obj.url,
            'method': task_obj.method,
            'headers': headers,
            'body': base64.b64encode(body),
            'eta': task_obj.eta,
        }

    def _convert_task(self, task_obj: Task) -> tasks_v2.Task:
        http_method = self._get_http_method(task_obj.method)

        headers = dict(task_obj.headers) if task_obj.headers else {}

        body = b''
        if task_obj.params:
            body = urllib.parse.urlencode(task_obj.params, doseq=True).encode('utf-8')
            headers['Content-Type'] = 'application/x-www-form-urlencoded'

        app_engine_request = tasks_v2.AppEngineHttpRequest(
            http_method=http_method,
            relative_uri=task_obj.url,
            headers=headers,
            body=body,
        )

        cloud_task = tasks_v2.Task(app_engine_http_request=app_engine_request)

        if task_obj.name:
            queue_path = self._get_queue_path()
            cloud_task.name = f"{queue_path}/tasks/{task_obj.name}"

        if task_obj.eta:
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(task_obj.eta)
            cloud_task.schedule_time = timestamp
        elif task_obj.countdown:
            schedule_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=task_obj.countdown)
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(schedule_time)
            cloud_task.schedule_time = timestamp

        return cloud_task

    def _get_http_method(self, method: str) -> tasks_v2.HttpMethod:
        method_map = {
            'POST': tasks_v2.HttpMethod.POST,
            'GET': tasks_v2.HttpMethod.GET,
            'HEAD': tasks_v2.HttpMethod.HEAD,
            'PUT': tasks_v2.HttpMethod.PUT,
            'DELETE': tasks_v2.HttpMethod.DELETE,
            'PATCH': tasks_v2.HttpMethod.PATCH,
        }
        return method_map.get(method, tasks_v2.HttpMethod.POST)
