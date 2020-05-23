from flask import Flask, make_response
from os import path, stat, getcwd
import json
import pandas as pd
import numpy as np
import multiprocessing as mp
from flask_cors import CORS
import threading as th
import time
from sultan.api import Sultan
import signal

from clusters import process, per_similarity, per_single_timeline
from common import metadata_changed

app = Flask(__name__)
CORS(app)


# TODO(felipemfp):
# * validate per commit date instead of creation date
# * regenerate regions.csv every day (days changes)
# * regenerate region.csv every day, but return file if exists (so it's ready for tomorrow or next usage)

def update_data_repository():
    try:
        with Sultan.load() as s:
            s.bash(path.join(getcwd(), 'commit-and-push.sh')).run()
    except:
        pass


def regions_worker(metadata):
    regions_file = path.join('inf-covid19-similarity-data', 'regions.csv')

    df = None
    if metadata_changed() or not path.isfile(regions_file):
        print('[regions_worker] Processing attributes...')
        df = process(metadata)

        print('[regions_worker] Clustering by attributes...')
        clusters = per_similarity(df)
        df['cluster'] = clusters.labels_

        df = df.sort_values(by=['cluster', 'key'])
        df.to_csv(regions_file, index=False)

        update_data_repository()
    else:
        print('[regions_worker] Loading attributes...')
        df = pd.read_csv(regions_file)

    len_clusters = len(df['cluster'].unique())
    print(
        f'[regions_worker]   Found {len(df)} regions across {len_clusters} clusters.')

    return df


def region_worker(metadata, df, region):
    print(f"  [{region}] Starting worker...")
    region_file = path.join('inf-covid19-similarity-data', 'by_key', f'{region}.csv')
    df = per_single_timeline(metadata, region, df)
    df.to_csv(region_file, index=False)
    update_data_repository()
    print(f"  [{region}] done.")


class Manager(object):
    def __init__(self):
        self.metadata = {}
        self.df = None
        self.df_result = None
        self.region_results = {}
        self.pool = mp.Pool(processes=4)

    def get_regions(self):
        if self.df is not None:
            return self.df

        if self.df_result is None:
            self.df_result = self.pool.apply_async(
                regions_worker,
                args=(self.metadata,),
            )

        try:
            self.df = self.df_result.get(5)
            return self.df
        except:
            return None

    def get_region(self, region):
        try:
            region_file = path.join(
                'inf-covid19-similarity-data', 'by_key', f'{region}.csv')
            if time.time() - stat(region_file).st_ctime < 60 * 60 * 48:
                df = pd.read_csv(region_file)
                return df
        except:
            pass

        if self.df is not None and region not in self.region_results:
            self.region_results[region] = self.pool.apply_async(
                region_worker,
                args=(self.metadata, self.df, region),
            )
        return None


manager = Manager()


@app.before_first_request
def startup():
    print('Loading metadata...')
    with open(path.join('inf-covid19-data', 'data', 'metadata.json')) as f:
        manager.metadata = json.load(f)
    manager.get_regions()


@app.route('/api/v1')
def health():
    processes = dict()
    for region, result in manager.region_results.items():
        if result.ready():
            processes[region] = 'done' if result.successful() else 'failed'
        else:
            processes[region] = 'in_progress'

    return {
        'health': manager.get_regions() is not None,
        'processes': processes,
    }


@app.route('/api/v1/regions')
def index():
    df = manager.get_regions()
    if df is None:
        return '', 202, {'Cache-Control': 'no-store'}
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
