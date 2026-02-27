# Migration from appengine-python-standard

Drop-in replacement. No data migration required.

Replace `wrap_wsgi_app` import:

```python
# Before
from google.appengine.api import wrap_wsgi_app

# After
from pipeline import wrap_wsgi_app
```

Everything else stays the same.

## Known Limitations

- **Transactional task enqueue not supported.** Cloud Tasks cannot enqueue within a Datastore transaction. Tasks are enqueued non-transactionally. Pipeline handlers are idempotent, but a failed transaction could leave an orphaned or missing task.
- **Cloud Tasks location defaults to `us-central1`.** Override with `CLOUD_TASKS_LOCATION` env var if your queues are elsewhere.
