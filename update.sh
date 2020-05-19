#!/bin/bash

# pull latest data
git -C ./data pull

# perform update
pipenv run python similarity.py

# commit files
git add --all
git commit -m "Scheduled updates ($(date +'%F %T %Z'))"

# push files
# git push
