#!/bin/bash

set -e

pipenv run pip freeze > requirements.txt

cat app.yaml | envsubst '$GH_TOKEN' > app.deploy.yaml

gcloud app deploy app.deploy.yaml --promote --quiet --project nephele-project