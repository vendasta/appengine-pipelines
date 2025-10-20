import logging
import os
import posixpath
import time
import uuid

from google.api_core.exceptions import TooManyRequests
from google.cloud import storage

client = storage.Client()

def _get_default_bucket():
  """Get the default GCS bucket for the application.

  Returns the bucket specified in the GCS_BUCKET environment variable,
  or falls back to the default App Engine bucket based on GAE_APPLICATION.
  """
  default_bucket = os.environ.get('GCS_BUCKET')

  if default_bucket is None:
    # Fall back to App Engine default bucket naming convention
    # Format: <project-id>.appspot.com
    app_id = os.environ.get('GAE_APPLICATION', os.environ.get('GOOGLE_CLOUD_PROJECT'))
    if app_id:
      # Remove the region prefix if present (e.g., "s~my-app" -> "my-app")
      app_id = app_id.split('~')[-1]
      default_bucket = f"{app_id}.appspot.com"

  if default_bucket is None:
    raise Exception(
        "No default cloud storage bucket has been set. "
        "Please set the GCS_BUCKET environment variable or ensure "
        "GAE_APPLICATION or GOOGLE_CLOUD_PROJECT is set.")

  return client.get_bucket(default_bucket)


def write_json_gcs(encoded_value, pipeline_id=None):
  """Writes a JSON encoded value to a Cloud Storage File.

  This function will store the blob in a GCS file in the default bucket under
  the appengine_pipeline directory. Optionally using another directory level
  specified by pipeline_id
  Args:
    encoded_value: The encoded JSON string.
    pipeline_id: A pipeline id to segment files in Cloud Storage, if none,
      the file will be created under appengine_pipeline

  Returns:
    The gcs blob name for the file that was created.
  """
  path_components = ["appengine_pipeline"]
  if pipeline_id:
    path_components.append(pipeline_id)
  path_components.append(uuid.uuid4().hex)
  # Use posixpath to get a / even if we're running on windows somehow
  file_name = posixpath.join(*path_components)

  blob = _get_default_bucket().blob(file_name)
  _MAX_RETRIES = 10
  for attempt in range(_MAX_RETRIES):
      try:
          blob.upload_from_string(encoded_value, content_type='application/json')
          break  # If the upload was successful, break the retry loop
      except TooManyRequests:
          if attempt < _MAX_RETRIES - 1:  # If this isn't the last attempt
              sleep_time = (2 ** attempt)  # Exponential backoff
              time.sleep(sleep_time)
          else:  # If this is the last attempt, re-raise the exception
              raise

  logging.debug("Created blob for filename = %s gs_key = %s", file_name, blob.self_link)
  return blob.name


def read_blob_gcs(blob_name):
  """Reads a blob as bytes from a Cloud Storage File.

  Args:
    blob_name: The name of the blob to read from.

  Returns:
    The bytes of the blob.
  """
  blob = _get_default_bucket().blob(blob_name)
  return blob.download_as_bytes()
