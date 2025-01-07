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

"""Demo Pipeline API application."""



import logging
import os
import sys
import time

from flask import Flask, render_template, request, redirect
from flask.views import MethodView
from google.appengine.api import mail, users
from google.appengine.ext import db
from google.appengine.api import wrap_wsgi_app
from google.appengine.api import full_app_id

sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

import pipeline
from pipeline import common

# Let anyone hit Pipeline handlers!
pipeline.set_enforce_auth(False)

################################################################################
# An example pipeline for generating reports.

class LongCount(pipeline.Pipeline):

  def run(self, entity_kind, property_name, value):
    cursor = None
    count = 0
    while True:
      query = db.GqlQuery(
          'SELECT * FROM {} WHERE {} = :1'.format(entity_kind, property_name),
          value.lower(), key_only=True, cursor=cursor)
      result = query.fetch(1000)
      count += len(result)
      if len(result) < 1000:
        return (entity_kind, property_name, value, count)
      else:
        cursor = query.cursor()


class SplitCount(pipeline.Pipeline):

  def run(self, entity_kind, property_name, *value_list):
    all_counts = []
    for value in value_list:
      stage = yield LongCount(entity_kind, property_name, value)
      all_counts.append(stage)

    yield common.Append(*all_counts)


class UselessPipeline(pipeline.Pipeline):
  """This pipeline is totally useless. It just demostrates that it will run
  in parallel, yield a named output, and properly be ignored by the system.
  """

  def run(self):
    if not self.test_mode and self.current_attempt == 1:
      self.set_status(message='Pretending to fail, will retry shortly.',
                      console_url='/static/console.html',
                      status_links={'Home': '/'})
      raise pipeline.Retry('Whoops, I need to retry')

    # Looks like a generator, but it's not.
    if False:
      yield common.Log.info('Okay!')

    self.fill('coolness', 1234)


class EmailCountReport(pipeline.Pipeline):

  def run(self, receiver_email_address, kind_count_list):
    body = [
        'At %s the counts are:' % time.ctime()
    ]
    result_sum = 0
    for (entity_kind, property_name, value, count) in kind_count_list:
      body.append('%s.%s = "%s" -> %d' % (
                  entity_kind, property_name, value, count))
      result_sum += count

    rendered = '\n'.join(body)
    logging.info('Email body is:\n%s', rendered)

    # Works in production, I swear!
    app_id = full_app_id.get()
    sender = '{}@{}.appspotmail.com'.format(app_id, app_id)
    try:
      mail.send_mail(
          sender=sender,
          to=receiver_email_address,
          subject='Entity count report',
          body=rendered)
    except (mail.InvalidSenderError, mail.InvalidEmailError):
      logging.exception('This should work in production.')

    return result_sum


class CountReport(pipeline.Pipeline):

  def run(self, email_address, entity_kind, property_name, *value_list):
    yield common.Log.info('UselessPipeline.coolness = %s',
                          (yield UselessPipeline()).coolness)

    split_counts = yield SplitCount(entity_kind, property_name, *value_list)
    yield common.Log.info('SplitCount result = %s', split_counts)

    with pipeline.After(split_counts):
      with pipeline.InOrder():
        yield common.Delay(seconds=1)
        yield common.Log.info('Done waiting')
        yield EmailCountReport(email_address, split_counts)

  def finalized(self):
    if not self.was_aborted:
      logging.info('All done! Found %s results', self.outputs.default.value)

################################################################################
# Silly guestbook application to run the pipelines on.

class GuestbookPost(db.Model):
  color = db.StringProperty()
  write_time = db.DateTimeProperty(auto_now_add=True)


class StartPipelineHandler(MethodView):
  def get(self):
    return render_template('start.html')

  def post(self):
    colors = [color for color in request.form.getlist('color') if color]
    job = CountReport(
        users.get_current_user().email(),
        GuestbookPost.kind(),
        'color',
        *colors)
    job.start()
    return redirect('/_ah/pipeline/status?root=%s' % job.pipeline_id)


class MainHandler(MethodView):
  def get(self):
    posts = GuestbookPost.all().order('-write_time').fetch(100)
    return render_template('guestbook.html', posts=posts)

  def post(self):
    color = request.form.get('color')
    if color:
      GuestbookPost(color=color.lower()).put()
    return redirect('/')


app = Flask(__name__)

app.wsgi_app = wrap_wsgi_app(app.wsgi_app, use_legacy_context_mode=True)

app.add_url_rule('/', view_func=MainHandler.as_view('main'))
app.add_url_rule('/pipeline', view_func=StartPipelineHandler.as_view('pipeline'))

routes = pipeline.create_handlers_map()
for route, handler in routes:
  app.add_url_rule(route, view_func=handler.as_view(route.lstrip('/')))
