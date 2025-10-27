"""Compatibility layer for migrating from App Engine Task Queue to Cloud Tasks.

This module provides a drop-in replacement for google.appengine.api.taskqueue
using the Cloud Tasks API (google.cloud.tasks_v2).
"""

import base64
import os
import urllib.parse
from typing import Optional, List, Dict, Any

try:
    from google.cloud import tasks_v2
    from google.protobuf import duration_pb2, timestamp_pb2
except ImportError:
    # For testing environments where Cloud Tasks client may not be available
    tasks_v2 = None
    duration_pb2 = None
    timestamp_pb2 = None

import datetime


# Test mode flag
_TEST_MODE = False


def set_test_mode(enabled: bool):
    """Enable or disable test mode.

    In test mode, tasks are stored in memory instead of being sent to Cloud Tasks.

    Args:
        enabled: True to enable test mode, False to disable
    """
    global _TEST_MODE
    _TEST_MODE = enabled


# Exception classes for backward compatibility
class Error(Exception):
    """Base taskqueue error type."""


class TombstonedTaskError(Error):
    """Task name has been tombstoned (task was recently deleted/completed)."""


class TaskAlreadyExistsError(Error):
    """Task with the same name already exists."""


class Task:
    """Compatibility wrapper for Task Queue Task using Cloud Tasks."""

    def __init__(self, url=None, params=None, name=None, method='POST',
                 headers=None, countdown=None, eta=None, **kwargs):
        """Initialize a Task.

        Args:
            url: The URL path for the task handler
            params: Dictionary of parameters to encode as form data
            name: Optional task name (for deduplication)
            method: HTTP method (default: POST)
            headers: Dictionary of HTTP headers
            countdown: Time in seconds from now when task should execute
            eta: datetime when task should execute
            **kwargs: Additional arguments (for compatibility)
        """
        self.url = url
        self.params = params or {}
        self.name = name
        self.method = method.upper()
        self.headers = headers or {}
        self.countdown = countdown
        self.eta = eta
        self.kwargs = kwargs

    def add(self, queue_name='default', transactional=False):
        """Add this task to a queue.

        Args:
            queue_name: Name of the queue
            transactional: If True, task is added transactionally (NOTE: Cloud Tasks
                          doesn't support transactional tasks - this is ignored with a warning)
        """
        if transactional:
            # Cloud Tasks doesn't support transactional tasks
            # We log a warning but proceed - in practice, Pipeline API can tolerate
            # some duplicate task executions due to its idempotency design
            import logging
            logging.warning(
                "Cloud Tasks does not support transactional task enqueueing. "
                "Task will be added non-transactionally. Ensure handlers are idempotent."
            )

        queue = Queue(queue_name)
        queue.add(self)


class Queue:
    """Compatibility wrapper for Task Queue Queue using Cloud Tasks."""

    def __init__(self, name='default'):
        """Initialize a Queue.

        Args:
            name: Queue name
        """
        self.name = name
        self._client = None

    @property
    def client(self):
        """Lazy-load Cloud Tasks client."""
        if self._client is None:
            self._client = tasks_v2.CloudTasksClient()
        return self._client

    def _get_queue_path(self):
        """Get the full queue path for Cloud Tasks.

        Returns:
            Queue path in format: projects/PROJECT/locations/LOCATION/queues/QUEUE
        """
        project = os.environ.get('GOOGLE_CLOUD_PROJECT')
        location = os.environ.get('CLOUD_TASKS_LOCATION', 'us-central1')

        if not project:
            raise ValueError(
                "GOOGLE_CLOUD_PROJECT environment variable must be set"
            )

        return self.client.queue_path(project, location, self.name)

    def _build_task_url(self, relative_url: str) -> str:
        """Build full task URL.

        Args:
            relative_url: Relative URL path (e.g., '/_ah/pipeline/run')

        Returns:
            Full URL for the task
        """
        # For App Engine, we can construct the URL from environment variables
        service = os.environ.get('GAE_SERVICE', 'default')
        version = os.environ.get('GAE_VERSION', '1')
        project = os.environ.get('GOOGLE_CLOUD_PROJECT')

        # For App Engine Standard, use the appspot.com URL
        # Format: https://VERSION-dot-SERVICE-dot-PROJECT.REGION_ID.r.appspot.com/PATH
        if version and service and project:
            if service == 'default':
                host = f"https://{project}.appspot.com"
            else:
                host = f"https://{service}-dot-{project}.appspot.com"

            return f"{host}{relative_url}"

        # Fallback for local testing
        return f"http://localhost:8080{relative_url}"

    def add(self, task_or_tasks):
        """Add task(s) to the queue.

        Args:
            task_or_tasks: A Task instance or list of Task instances

        Raises:
            TaskAlreadyExistsError: If a task with the same name already exists
            TombstonedTaskError: If task name was recently used
        """
        tasks_list = task_or_tasks if isinstance(task_or_tasks, list) else [task_or_tasks]

        # Use test stub in test mode
        if _TEST_MODE:
            from . import taskqueue_test_stub
            stub = taskqueue_test_stub.get_test_stub()
            for task_obj in tasks_list:
                task_dict = self._task_to_dict(task_obj)
                try:
                    stub.add_task(self.name, task_dict)
                except Exception as e:
                    error_msg = str(e).lower()
                    if 'already exists' in error_msg:
                        raise TaskAlreadyExistsError(str(e))
                    elif 'tombstoned' in error_msg:
                        raise TombstonedTaskError(str(e))
                    else:
                        raise
            return

        # Production mode: use Cloud Tasks
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
                # Map Cloud Tasks errors to Task Queue errors for compatibility
                if 'already exists' in error_msg or 'task_already_exists' in error_msg:
                    raise TaskAlreadyExistsError(f"Task already exists: {e}")
                elif 'tombstoned' in error_msg or 'recently deleted' in error_msg:
                    raise TombstonedTaskError(f"Task was tombstoned: {e}")
                else:
                    raise

    def _task_to_dict(self, task_obj: Task) -> Dict[str, Any]:
        """Convert a Task to a dictionary for testing.

        Args:
            task_obj: Task instance

        Returns:
            Dictionary representation of the task
        """
        # Encode parameters as form data
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
        """Convert a compatibility Task to a Cloud Tasks Task.

        Args:
            task_obj: Task instance

        Returns:
            Cloud Tasks Task message
        """
        # Build the HTTP request
        url = self._build_task_url(task_obj.url)

        http_request = tasks_v2.HttpRequest(
            url=url,
            http_method=self._get_http_method(task_obj.method)
        )

        # Encode parameters as form data if present
        if task_obj.params:
            # Use doseq=True to properly encode list values as multiple parameters
            # e.g., {'child_indexes': [0, 2]} becomes 'child_indexes=0&child_indexes=2'
            body = urllib.parse.urlencode(task_obj.params, doseq=True).encode('utf-8')
            http_request.body = body
            http_request.headers['Content-Type'] = 'application/x-www-form-urlencoded'

        # Add custom headers
        if task_obj.headers:
            http_request.headers.update(task_obj.headers)

        # Build the task
        cloud_task = tasks_v2.Task(http_request=http_request)

        # Set task name if provided
        if task_obj.name:
            queue_path = self._get_queue_path()
            cloud_task.name = f"{queue_path}/tasks/{task_obj.name}"

        # Set schedule time (eta or countdown)
        if task_obj.eta:
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(task_obj.eta)
            cloud_task.schedule_time = timestamp
        elif task_obj.countdown:
            schedule_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=task_obj.countdown)
            timestamp = timestamp_pb2.Timestamp()
            timestamp.FromDatetime(schedule_time)
            cloud_task.schedule_time = timestamp

        return cloud_task

    def _get_http_method(self, method: str) -> tasks_v2.HttpMethod:
        """Convert HTTP method string to Cloud Tasks enum.

        Args:
            method: HTTP method string (e.g., 'POST', 'GET')

        Returns:
            Cloud Tasks HttpMethod enum value
        """
        method_map = {
            'POST': tasks_v2.HttpMethod.POST,
            'GET': tasks_v2.HttpMethod.GET,
            'HEAD': tasks_v2.HttpMethod.HEAD,
            'PUT': tasks_v2.HttpMethod.PUT,
            'DELETE': tasks_v2.HttpMethod.DELETE,
            'PATCH': tasks_v2.HttpMethod.PATCH,
        }
        return method_map.get(method, tasks_v2.HttpMethod.POST)
