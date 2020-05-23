#!/bin/bash

# clone latest inf-covid19/data
git -C inf-covid19-data pull

# clone latest inf-covid19/similarity
git -C inf-covid19-similarity pull

# run application
python -m gunicorn -t 300 --bind 0.0.0.0:$PORT server:app