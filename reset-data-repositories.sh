#!/usr/bin/env bash

DATA=inf-covid19-data
SIMILARITY_DATA=inf-covid19-similarity-data

rm -rf $DATA || true
rm -rf $SIMILARITY_DATA || true

mkdir $DATA && git -C $DATA init && git -C $DATA remote add origin https://github.com/inf-covid19/data.git
mkdir $SIMILARITY_DATA && git -C $SIMILARITY_DATA init && git -C $SIMILARITY_DATA remote add origin https://github.com/inf-covid19/similarity-data.git