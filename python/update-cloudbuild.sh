#!/bin/bash

REPO_NAME="appengine-pipelines"
TRIGGER_NAME="appengine-pipelines-master"
TRIGGER_DESCRIPTION="Appengine Pipelines - Master"

gcloud builds triggers delete $TRIGGER_NAME --project=repcore-prod

gcloud builds triggers create github \
    --repo-owner="vendasta" \
    --repo-name=$REPO_NAME \
    --branch-pattern=^master$ \
    --included-files="python/**" \
    --build-config="python/cloudbuild-master.yaml" \
    --project=repcore-prod \
    --name=$TRIGGER_NAME \
    --description="$TRIGGER_DESCRIPTION" \


TRIGGER_NAME="appengine-pipelines-branches"
TRIGGER_DESCRIPTION="Appengine Pipelines - Branches"

gcloud builds triggers delete $TRIGGER_NAME --project=repcore-prod

gcloud builds triggers create github \
    --repo-owner="vendasta" \
    --repo-name=$REPO_NAME \
    --branch-pattern=^master$ \
    --included-files="python/**" \
    --build-config="python/cloudbuild-branches.yaml" \
    --project=repcore-prod \
    --name=$TRIGGER_NAME \
    --description="$TRIGGER_DESCRIPTION" \
