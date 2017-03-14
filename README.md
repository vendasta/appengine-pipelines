# Google App Engine Pipeline API - Vendasta fork

This is a forked version of the [Google AppEngine Pipelines API](https://github.com/GoogleCloudPlatform/appengine-pipelines).

## Description

The Google App Engine Pipeline API connects together complex, workflows (including human tasks). The goals are flexibility, workflow reuse, and testability.

A primary use-case of the API is connecting together various App Engine MapReduces into a computational pipeline.

## Updating our fork from upstream

To pull in changes from the [upstream repo](https://github.com/GoogleCloudPlatform/appengine-pipelines):

### Clone this repo and `cd` into it

```
$ git clone https://github.com/vendasta/appengine-pipelines
$ cd appengine-pipelines
```

### Add a remote for Google's upstream repo

```
$ git remote add upstream https://github.com/GoogleCloudPlatform/appengine-pipelines
```

### Merge upstream, resolve conflicts if there are any

```
$ git fetch upstream
$ git merge upstream/master
```

### Update version in setup.py

To avoid confusion about this forked version vs. the upstream version.

