# Migration Guide: App Engine Standard Libraries to Google Cloud SDKs

This guide documents the migration from App Engine bundled services (`appengine-python-standard`) to standalone Google Cloud SDKs.

## Overview

The migration removes dependency on `appengine-python-standard` and migrates to:
- **google-cloud-ndb** (for Datastore/NDB)
- **google-cloud-tasks** (for Task Queue)
- **google-cloud-storage** (for GCS)
- Custom compatibility layers for Users API and Task Queue

## Changes Made

### 1. Dependencies

**Removed:**
- `appengine-python-standard==1.1.10`

**Added:**
- `google-cloud-ndb>=2.3.0`
- `google-cloud-tasks>=2.16.0`
- `google-cloud-storage>=2.0.0` (already present)

### 2. NDB Migration

**Old:**
```python
from google.appengine.ext import ndb
```

**New:**
```python
from google.cloud import ndb
```

**Changes:**
- All NDB model definitions remain the same (backward compatible)
- Transaction options now use `ndb.TransactionOptions` instead of importing from `datastore_rpc`
- NDB requires a context for each request (handled automatically in Flask middleware)

**Files Updated:**
- `src/pipeline/models.py`
- `src/pipeline/util.py`
- `src/pipeline/pipeline.py`
- `demo/main.py`

### 3. Task Queue → Cloud Tasks

**Implementation:**
Created a compatibility layer (`taskqueue_compat.py`) that provides the same API as `google.appengine.api.taskqueue` but uses Cloud Tasks underneath.

**Old:**
```python
from google.appengine.api import taskqueue
task = taskqueue.Task(url='/path', params={'key': 'value'})
task.add(queue_name='default')
```

**New (same API, different backend):**
```python
from pipeline import taskqueue_compat as taskqueue  # Compatibility layer
task = taskqueue.Task(url='/path', params={'key': 'value'})
task.add(queue_name='default')  # Now uses Cloud Tasks
```

**Important Notes:**
- Transactional tasks are not supported by Cloud Tasks. Tasks are added non-transactionally with a warning.
- The Pipeline API is designed to be idempotent, so this limitation is acceptable.
- Task names are used for deduplication.

**Files Updated:**
- `src/pipeline/pipeline.py` (import changed)
- `src/pipeline/taskqueue_compat.py` (new compatibility layer)
- `src/pipeline/taskqueue_test_stub.py` (test infrastructure)

### 4. Users API

**Implementation:**
Created a compatibility layer (`users_compat.py`) that parses App Engine authentication headers directly.

**Old:**
```python
from google.appengine.api import users
user = users.get_current_user()
is_admin = users.is_current_user_admin()
```

**New:**
```python
from pipeline import users_compat as users
user = users.get_current_user()  # Parses X-Appengine-User-* headers
is_admin = users.is_current_user_admin()
```

**Files Updated:**
- `src/pipeline/pipeline.py`
- `src/pipeline/status_ui.py`
- `src/pipeline/users_compat.py` (new compatibility layer)

### 5. App Identity API

**Old:**
```python
from google.appengine.api import app_identity
bucket = app_identity.get_default_gcs_bucket_name()
```

**New:**
```python
import os
bucket = os.environ.get('GCS_BUCKET') or f"{os.environ['GAE_APPLICATION'].split('~')[-1]}.appspot.com"
```

**Files Updated:**
- `src/pipeline/storage.py`

### 6. Testing Infrastructure

**Changes:**
- Replaced `apiproxy_stub_map` with custom test stubs
- Created `taskqueue_test_stub.py` for in-memory task queue testing
- Updated `testing/__init__.py` to use new stubs

## Deployment Steps

### Prerequisites

1. **Create Cloud Tasks Queues:**

   Before deploying, create Cloud Tasks queues to replace App Engine Task Queues:

   ```bash
   # Create default queue
   gcloud tasks queues create default \
     --location=us-central1

   # If you have custom queues, create them too
   # gcloud tasks queues create my-queue --location=us-central1
   ```

2. **Grant Cloud Tasks permissions:**

   ```bash
   # Get your App Engine service account
   PROJECT_ID=$(gcloud config get-value project)
   SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

   # Grant Cloud Tasks permissions
   gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member="serviceAccount:${SERVICE_ACCOUNT}" \
     --role="roles/cloudtasks.enqueuer"
   ```

### Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Update environment variables in `app.yaml`:**
   ```yaml
   env_variables:
     # Optional: Override default GCS bucket
     # GCS_BUCKET: "my-custom-bucket"

     # Cloud Tasks configuration (required)
     CLOUD_TASKS_LOCATION: "us-central1"  # Must match queue location
   ```

3. **Update runtime in `app.yaml`:**
   ```yaml
   runtime: python313
   entrypoint: gunicorn -b :$PORT main:app
   ```

### Testing

1. **Run tests locally:**
   ```bash
   # Install test dependencies
   pip install pytest

   # Run tests
   pytest test/
   ```

2. **Local development:**
   For local development, you'll need to set up:
   - Datastore emulator: `gcloud beta emulators datastore start`
   - Cloud Tasks emulator or use test mode (automatic in tests)

### Deployment

1. **Deploy to App Engine:**
   ```bash
   gcloud app deploy app.yaml
   ```

2. **Verify deployment:**
   - Check that pipelines can be created
   - Verify tasks are being executed
   - Monitor Cloud Tasks queue in GCP Console

## Data Compatibility

✅ **No data migration required!**

- NDB entities remain 100% compatible
- Existing pipeline records can be read and processed
- Entity kinds remain the same (`_AE_Pipeline_Record`, etc.)

## Backward Compatibility

### What's Compatible:
- All NDB model definitions
- Entity data in Datastore
- Pipeline API surface (start, run, yield, etc.)
- Task Queue API (through compatibility layer)

### What's Changed:
- Import paths for NDB, Task Queue, and Users API
- Task Queue backend (now uses Cloud Tasks)
- Transactional tasks not supported (logged warning)
- App Identity replaced with environment variables

## Troubleshooting

### Common Issues

**1. "No default cloud storage bucket" error:**
- Set `GCS_BUCKET` environment variable in `app.yaml`
- Or ensure `GAE_APPLICATION` is set (automatic on App Engine)

**2. "GOOGLE_CLOUD_PROJECT environment variable must be set" error:**
- Ensure you're running on App Engine or set the variable manually
- For local dev: `export GOOGLE_CLOUD_PROJECT=your-project-id`

**3. Tasks not being executed:**
- Verify Cloud Tasks queues exist: `gcloud tasks queues list --location=us-central1`
- Check IAM permissions for the App Engine service account
- Verify `CLOUD_TASKS_LOCATION` matches queue location

**4. "Context is not set" NDB errors:**
- Ensure Flask middleware is properly configured (see `demo/main.py`)
- NDB context must be created for each request

**5. Authentication not working:**
- Verify `login: required` is set in `app.yaml` for protected routes
- Check that App Engine authentication headers are present
- For local dev, you may need to mock the headers

### Debug Mode

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Rollback Plan

If you need to rollback:

1. **Keep the old version deployed:**
   ```bash
   gcloud app versions list
   # Traffic split to old version if needed
   gcloud app services set-traffic default --splits=OLD_VERSION=1
   ```

2. **Revert dependencies:**
   - Add back `appengine-python-standard==1.1.10`
   - Remove Google Cloud SDK dependencies
   - Revert import changes

## Performance Considerations

- **Cloud Tasks** has slightly higher latency than App Engine Task Queue (~100-200ms)
- **NDB** performance is similar with google-cloud-ndb
- Consider enabling NDB caching if needed: `_use_cache = True` in models

## Support and Resources

- [google-cloud-ndb documentation](https://googleapis.dev/python/python-ndb/latest/)
- [Cloud Tasks documentation](https://cloud.google.com/tasks/docs)
- [App Engine Python 3 migration guide](https://cloud.google.com/appengine/docs/standard/python3/migrating-to-python3)

## Summary

This migration successfully removes all dependencies on App Engine bundled services while maintaining:
- ✅ Full backward compatibility with existing pipeline data
- ✅ Same Pipeline API surface
- ✅ Existing tests continue to work
- ✅ Minimal code changes through compatibility layers

The main trade-off is that transactional task enqueueing is no longer supported, but the Pipeline API's idempotency design makes this acceptable.
