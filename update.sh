#!/bin/bash

set -e

# pull latest data
wget -O data.zip https://github.com/inf-covid19/data/archive/master.zip
7z data.zip

# perform update
pipenv run python similarity.py

# commit files
git add --all
git commit -m "Scheduled updates ($(date +'%F %T %Z'))"

# push files
# git push
