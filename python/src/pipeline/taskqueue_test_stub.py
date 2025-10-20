"""Test stub for taskqueue compatibility layer.

This provides an in-memory task queue for testing purposes, replacing
the App Engine taskqueue stub.
"""

import base64
import datetime
import threading
import urllib.parse
from typing import Dict, List, Any


class TaskQueueTestStub:
    """In-memory task queue for testing."""

    def __init__(self):
        """Initialize the test stub."""
        self._lock = threading.Lock()
        self._queues: Dict[str, List[Dict[str, Any]]] = {}
        self._tombstones: Dict[str, set] = {}  # Track recently deleted task names

    def get_tasks(self, queue_name='default') -> List[Dict[str, Any]]:
        """Get all tasks from a queue.

        Args:
            queue_name: Name of the queue

        Returns:
            List of task dictionaries
        """
        with self._lock:
            return list(self._queues.get(queue_name, []))

    def add_task(self, queue_name: str, task_dict: Dict[str, Any]):
        """Add a task to a queue.

        Args:
            queue_name: Name of the queue
            task_dict: Task dictionary

        Raises:
            Exception: If task name is tombstoned or already exists
        """
        with self._lock:
            # Initialize queue if needed
            if queue_name not in self._queues:
                self._queues[queue_name] = []
                self._tombstones[queue_name] = set()

            # Check for tombstoned tasks
            task_name = task_dict.get('name')
            if task_name and task_name in self._tombstones[queue_name]:
                raise Exception(f"Task name tombstoned: {task_name}")

            # Check for duplicate task names
            existing_names = {t.get('name') for t in self._queues[queue_name]}
            if task_name and task_name in existing_names:
                raise Exception(f"Task already exists: {task_name}")

            self._queues[queue_name].append(task_dict)

    def delete_task(self, queue_name: str, task_name: str):
        """Delete a task from a queue.

        Args:
            queue_name: Name of the queue
            task_name: Name of the task to delete
        """
        with self._lock:
            if queue_name in self._queues:
                self._queues[queue_name] = [
                    t for t in self._queues[queue_name]
                    if t.get('name') != task_name
                ]
                # Add to tombstones
                if queue_name not in self._tombstones:
                    self._tombstones[queue_name] = set()
                self._tombstones[queue_name].add(task_name)

    def clear_queue(self, queue_name: str):
        """Clear all tasks from a queue.

        Args:
            queue_name: Name of the queue
        """
        with self._lock:
            if queue_name in self._queues:
                # Tombstone all tasks
                for task in self._queues[queue_name]:
                    task_name = task.get('name')
                    if task_name:
                        if queue_name not in self._tombstones:
                            self._tombstones[queue_name] = set()
                        self._tombstones[queue_name].add(task_name)
                self._queues[queue_name] = []

    def clear_all(self):
        """Clear all queues."""
        with self._lock:
            self._queues = {}
            self._tombstones = {}


# Global test stub instance
_test_stub = None


def get_test_stub() -> TaskQueueTestStub:
    """Get or create the global test stub instance."""
    global _test_stub
    if _test_stub is None:
        _test_stub = TaskQueueTestStub()
    return _test_stub


def reset_test_stub():
    """Reset the global test stub."""
    global _test_stub
    _test_stub = TaskQueueTestStub()
