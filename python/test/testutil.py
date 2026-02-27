"""Test utilities for the Google App Engine Pipeline API."""

import os

from pipeline import taskqueue_compat, taskqueue_test_stub


class TestSetupMixin:

    TEST_APP_ID = 'my-app-id'
    TEST_VERSION_ID = 'my-version.1234'

    def setUp(self):
        super().setUp()

        os.environ['GOOGLE_CLOUD_PROJECT'] = self.TEST_APP_ID
        os.environ['GAE_APPLICATION'] = self.TEST_APP_ID
        os.environ['GAE_VERSION'] = self.TEST_VERSION_ID
        os.environ['HTTP_HOST'] = '%s.appspot.com' % self.TEST_APP_ID
        os.environ['DEFAULT_VERSION_HOSTNAME'] = os.environ['HTTP_HOST']
        os.environ['GAE_SERVICE'] = 'foo-module'

        taskqueue_compat.set_test_mode(True)
        taskqueue_test_stub.reset_test_stub()

    def tearDown(self):
        super().tearDown()
        for key in ('GOOGLE_CLOUD_PROJECT', 'GAE_APPLICATION', 'GAE_VERSION',
                     'HTTP_HOST', 'DEFAULT_VERSION_HOSTNAME', 'GAE_SERVICE'):
            os.environ.pop(key, None)
