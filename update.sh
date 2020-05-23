#!/bin/bash

set -e

# pull latest data
rm -rf inf-covid19-data
git clone https://github.com/inf-covid19/data.git inf-covid19-data

# perform update
pipenv run python similarity.py

# commit files
git add --all
git commit -m "Scheduled updates ($(date +'%F %T %Z'))"

# push files
# git push
