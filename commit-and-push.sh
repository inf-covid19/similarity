#!/bin/bash

set -e

cd ./inf-covid19-similarity-data

git config --local user.email "bot@ufrgs.dev"
git config --local user.name "inf-bot"

git add --all
git diff-index HEAD --quiet || git commit -m "Updates from application ($(date +'%F %T %Z'))"

remote_repo="https://inf-bot:${GITHUB_TOKEN}@github.com/inf-covid19/similarity-data.git"
git push "${remote_repo}" HEAD:master