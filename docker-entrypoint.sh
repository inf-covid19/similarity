#!/bin/bash

set -e

# clone latest inf-covid19/data
git -C /opt/services/percy/inf-covid19-data pull origin master

# clone latest inf-covid19/similarity
git -C /opt/services/percy/inf-covid19-similarity-data pull origin master

exec "$@"