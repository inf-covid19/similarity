from flask import Flask, make_response
from os import path, stat
import json
import pandas as pd
import numpy as np
import multiprocessing as mp
from flask_cors import CORS
import threading as th
import time

from clusters import process, per_similarity, per_single_timeline
from common import metadata_changed

app = Flask(__name__)
CORS(app)


def region_worker(metadata, df, region):
    df = per_single_timeline(metadata, region, df)
    df.to_csv(path.join('output', 'per_region', f'{region}.csv'), index=False)


class Manager(object):
    def __init__(self):
        self.metadata = {}
        self.region_process = {}
        self.df_attributes = None

    def get_regions(self):
        return self.df_attributes

    def get_region(self, region):
        try:
            region_file = path.join('output', 'per_region', f'{region}.csv')
            if time.time() - stat(region_file).st_ctime < 60 * 60 * 48:
                return pd.read_csv(region_file)
        except Exception as ex:
            print('---something broke---', region, ex)

        if region not in self.region_process:
            self.region_process[region] = mp.Process(
                target=region_worker,
                args=(self.metadata, self.df_attributes, region),
                daemon=True
            )
            self.region_process[region].start()
        return None


manager = Manager()


@app.before_first_request
def startup():
    print('Loading metadata...')
    with open(path.join('inf-covid19-data', 'data', 'metadata.json')) as f:
        manager.metadata = json.load(f)

    if metadata_changed():
        # process atributes
        print('Processing attributes...')
        df_attributes = process(manager.metadata)

        # clustering by attributes
        print('Clustering by attributes...')
        clusters = per_similarity(df_attributes)
        df_attributes['cluster'] = clusters.labels_

        manager.df_attributes = df_attributes

        df_attributes = df_attributes.sort_values(by=['cluster', 'key'])
        df_attributes.to_csv(
            path.join('output', 'region_clusters.csv'), index=False)
    else:
        print('Loading attributes...')
        df_attributes = pd.read_csv(path.join('output', 'region_clusters.csv'))
        manager.df_attributes = df_attributes

    len_clusters = len(manager.df_attributes['cluster'].unique())
    print(
        f'  Found {len(manager.df_attributes)} regions across {len_clusters} clusters.')


@app.route('/api/v1')
def health():
    return {'health': manager.df_attributes is not None}


@app.route('/api/v1/regions')
def index():
    df = manager.get_regions()
    headers = {
        'Content-Type': 'text/plain; charset=UTF-8',
        'Cache-Control': 'public, max-age=7200'
    }
    return df.to_csv(index=False), 200, headers


@app.route('/api/v1/regions/<string:region>')
def get(region):
    df = manager.get_region(region)

    if df is None:
        return '', 202, {'Cache-Control': 'no-store'}

    headers = {
        'Content-Type': 'text/plain; charset=UTF-8',
        'Cache-Control': 'public, max-age=86400'
    }
    return df.to_csv(index=False), 200, headers
