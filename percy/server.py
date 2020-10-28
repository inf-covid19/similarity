from flask import Flask, make_response
from os import path, stat, getcwd
import json
import pandas as pd
import numpy as np
import multiprocessing.dummy as mp
from flask_cors import CORS
import threading as th
import time
from sultan.api import Sultan
import signal
import traceback
import logging

from percy.clusters import process, process_with_days, per_similarity, per_single_timeline
from percy.common import metadata_changed

app = Flask(__name__)
CORS(app)

gunicorn_logger = logging.getLogger('gunicorn.error')
if gunicorn_logger is not None:
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

DATA = 'inf-covid19-data'
SIMILARITY_DATA = 'inf-covid19-similarity-data'


def update_data_repository():
    try:
        with Sultan.load() as s:
            s.bash(path.join(getcwd(), 'commit-and-push.sh')).run()
    except:
        app.logger.error(f'[update_data_repository] failed.')
        trace_info = traceback.format_exc().splitlines()
        app.logger.error(f'[update_data_repository]  ' + f'\n  '.join(trace_info))
        pass


def get_latest_commit_date(repo, filename):
    try:
        with Sultan.load(env={'PAGER': 'cat'}) as s:
            result = s.git(f'-C {repo} log -1 --format=%ct "{filename}"').run()
            return int(''.join(result.stdout).strip())
    except:
        app.logger.error(f'[get_latest_commit_date] failed.')
        trace_info = traceback.format_exc().splitlines()
        app.logger.error(f'[get_latest_commit_date]  ' + f'\n  '.join(trace_info))
        return 0


def bootstrap_worker(_metadata):
    app.logger.info(f"[bootstrap_worker] Starting worker...")
    try:
        with Sultan.load() as s:
            s.git(f'-C {DATA} pull --depth 1 origin master').run()
            s.git(f'-C {SIMILARITY_DATA} pull --depth 1 origin master').run()

        regions_file = path.join(SIMILARITY_DATA, 'regions.csv')
        metadata_file = path.join('data', 'metadata.json')

        df = None
        metadata = _metadata.copy()
        if metadata_changed() or not path.isfile(regions_file):
            app.logger.info('[bootstrap_worker] Loading metadata...')
            with open(path.join(DATA, metadata_file)) as f:
                metadata = json.load(f)

            app.logger.info('[bootstrap_worker] Processing attributes...')
            df = process(metadata)

            app.logger.info('[bootstrap_worker] Clustering by attributes...')
            clusters = per_similarity(df)
            df['cluster'] = clusters.labels_

            df = df.sort_values(by=['cluster', 'key'])

            app.logger.info('[bootstrap_worker] Saving regions.csv...')
            df.to_csv(regions_file, index=False)

            app.logger.info('[bootstrap_worker] Commit and push...')
            update_data_repository()
        else:
            app.logger.info('[bootstrap_worker] Loading attributes...')
            is_up_to_date = time.time() - get_latest_commit_date(SIMILARITY_DATA,
                                                                'regions.csv') < 60 * 60 * 24

            if len(metadata) == 0:
                app.logger.info('[bootstrap_worker] Loading metadata...')
                with open(path.join(DATA, metadata_file)) as f:
                    metadata = json.load(f)

            df = pd.read_csv(regions_file)
            if not is_up_to_date:
                app.logger.info('[bootstrap_worker] Updating attributes...')
                df = process_with_days(metadata, df)

        len_clusters = len(df['cluster'].unique())
        app.logger.info(f'[bootstrap_worker] Loaded {len(df)} regions across {len_clusters} clusters.')

        return df, metadata
    except:
        app.logger.error(f'[bootstrap_worker] failed.')
        trace_info = traceback.format_exc().splitlines()
        app.logger.error(
            f'[bootstrap_worker]  ' + f'\n  '.join(trace_info))


def region_worker(metadata, df, region):
    app.logger.info(f"[region_worker<{region}>] Starting worker...")
    try:
        region_file = path.join(SIMILARITY_DATA, 'by_key', f'{region}.csv')
        df = per_single_timeline(metadata, region, df)
        df.to_csv(region_file, index=False)
        update_data_repository()
        app.logger.info(f"[region_worker<{region}>] done.")
    except:
        app.logger.error(f'[region_worker<{region}>] failed.')
        trace_info = traceback.format_exc().splitlines()
        app.logger.error(
            f'[region_worker<{region}>]  ' + f'\n  '.join(trace_info))


class Manager(object):
    def __init__(self):
        self.pool = mp.Pool(processes=4)

        self.metadata = {}
        self.df = None

        self.region_results = {}

        self.bootstrap = None

        self.pull()

    def pull(self):
        self.pulled_at = time.time()
        self.bootstrap = self.pool.apply_async(
            bootstrap_worker,
            args=(self.metadata,),
        )

    def is_loaded(self):
        return len(self.metadata) > 0 and self.df is not None

    def is_ready(self):
        if self.bootstrap is not None:
            try:
                self.df, self.metadata = self.bootstrap.get(5)
            except:
                pass

        if time.time() - self.pulled_at > 60 * 60 * 2:
            self.pull()

        return self.is_loaded()

    def get_regions(self):
        if not self.is_ready():
            return None

        return self.df

    def load_region(self, region):
        if region not in self.region_results:
            self.region_results[region] = self.pool.apply_async(
                region_worker,
                args=(self.metadata, self.df, region),
            )

    def is_cacheable(self):
        return self.bootstrap is not None and self.bootstrap.ready()

    def get_region(self, region):
        if not self.is_ready():
            return None, False

        try:
            region_file = path.join('by_key', f'{region}.csv')
            is_up_to_date = time.time() - get_latest_commit_date(SIMILARITY_DATA,
                                                                 region_file) < 60 * 60 * 24
            if not is_up_to_date:
                self.load_region(region)
            df = pd.read_csv(path.join(SIMILARITY_DATA, region_file))
            return df, is_up_to_date
        except:
            pass

        self.load_region(region)
        return None, False


manager = Manager()


@app.before_first_request
def startup():
    manager.is_ready()


@app.route('/_ah/health')
def health():
    return {'status': 'healthy'}


@app.route('/api/v1')
def index():
    processes = dict()

    bootstrap = 'in_progress'
    if manager.bootstrap and manager.bootstrap.ready():
        bootstrap = 'done' if manager.bootstrap.successful() else 'failed'

    for region, result in manager.region_results.items():
        if result.ready():
            processes[region] = 'done' if result.successful() else 'failed'
        else:
            processes[region] = 'in_progress'

    return {
        'ready': manager.is_ready(),
        'processes': processes,
        'bootstrap': bootstrap,
    }


@app.route('/api/v1/regions')
def list_regions():
    df = manager.get_regions()
    if df is None:
        return '', 202, {'Cache-Control': 'no-store'}

    headers = {
        'Content-Type': 'text/plain; charset=UTF-8',
    }

    if manager.is_cacheable():
        headers['Cache-Control'] = 'public, max-age=7200'

    return df.to_csv(index=False), 200, headers


@app.route('/api/v1/regions/<string:region>')
def show_region(region):
    df, is_up_to_date = manager.get_region(region)

    if df is None:
        return '', 202, {'Cache-Control': 'no-store'}

    headers = {
        'Content-Type': 'text/plain; charset=UTF-8',
    }

    if is_up_to_date and manager.is_cacheable():
        headers['Cache-Control'] = 'public, max-age=86400'

    return df.to_csv(index=False), 200, headers
