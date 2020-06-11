#!/bin/bash

set -e

cat app.yaml | envsubst > app.deploy.yaml

gcloud app deploy app.deploy.yaml --promote --quiet