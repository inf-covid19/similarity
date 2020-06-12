import json
from os import getcwd, path, getenv
import logging

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from scipy.cluster.hierarchy import dendrogram
from sklearn.datasets import load_iris
from sklearn.cluster import AgglomerativeClustering
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.manifold import SpectralEmbedding
from sklearn.metrics.pairwise import paired_distances
from sklearn import cluster, covariance, manifold

from fnc.mappings import merge, get

from percy.countries import process_country
from percy.brazil import process_brazil
from percy.sweden import process_sweden
from percy.united_states_of_america import process_united_states_of_america
from percy.common import normalize_timeline

COUNTRY_PROCESS_MAPPING = {
    'Brazil': process_brazil,
    'Sweden': process_sweden,
    'United_States_of_America': process_united_states_of_america,
}


def process(metadata):
    logger = logging.getLogger('percy.server')
    region_attributes = pd.DataFrame()


    logger.debug('[process] starting...')
    for country, data in metadata.items():
        logger.debug(f'[process] processing {country}...')
        attributes = process_country(country, data)
        if attributes is not None:
            logger.debug(f'[process] concatening {country}...')
            region_attributes = pd.concat([region_attributes, attributes], ignore_index=True)

        if country in COUNTRY_PROCESS_MAPPING:
            logger.debug(f'[process] processing regions from {country}...')
            process_fn = COUNTRY_PROCESS_MAPPING[country]
            subregion_attributes = process_fn(country, metadata[country])
            logger.debug(f'[process] concatening regions from {country}...')
            region_attributes = pd.concat([region_attributes, subregion_attributes], ignore_index=True)


    logger.debug(f'[process] adding population density information...')
    region_attributes['population_density'] = region_attributes['population']/region_attributes['area_km']

    return process_with_days(metadata, region_attributes)


def process_with_days(metadata, df):
    logger = logging.getLogger('percy.server')
    logger.debug(f'[process_with_days] adding days information...')
    df['days'] = df.apply(lambda r: len(get_timeline(metadata, r)), axis=1)
    return df


def per_similarity(region_attributes):
    logger = logging.getLogger('percy.server')

    logger.debug('[per_similarity] selecting features...')
    df = region_attributes[['population', 'area_km']]
    logger.debug('[per_similarity] converting to numpy...')
    X = df.to_numpy()
    logger.debug('[per_similarity] apply standard scaler...')
    X_std = StandardScaler().fit_transform(X)
    logger.debug('[per_similarity] instantiate agglomerative clustering...')
    model = AgglomerativeClustering(distance_threshold=0.1, n_clusters=None)
    logger.debug('[per_similarity] fit agglomerative clustering...')
    model = model.fit(X_std)
    return model


def should_get_distances(a_timeline, b_timeline):
    a_len, b_len = len(a_timeline), len(b_timeline)

    if min(a_len, b_len) == 0:
        return False

    if b_len < a_len - 30:
        return False

    return True


def normalize_timelines(A, B):
    TIMELINE_WINDOW = 0 - int(getenv('SIMILARITY_TIME_WINDOW', 10))
    length = min(len(A), len(B))
    return A[:length][TIMELINE_WINDOW:], B[:length][TIMELINE_WINDOW:]


def get_distances(a_timeline, b_timeline):
    A, B = normalize_timelines(a_timeline, b_timeline)

    cases_distance = get_distance(A, B, ['cases'])
    deaths_distance = get_distance(A, B, ['deaths'])
    cases_per_100k_distance = get_distance(A, B, ['cases_per_100k'])
    deaths_per_100k_distance = get_distance(A, B, ['deaths_per_100k'])

    return cases_distance, deaths_distance, cases_per_100k_distance, deaths_per_100k_distance


def per_single_timeline(metadata, a_key, df):
    data = []
    columns=['region', 'cases_distance', 'deaths_distance', 'cases_per_100k_distance', 'deaths_per_100k_distance', 'is_same_cluster']
    
    a_df = df[df['key'] == a_key]
    if len(a_df) == 0:
        return pd.DataFrame(data, columns=columns)

    a_row = a_df.iloc[0]
    a_cluster = a_row['cluster']
    a_timeline = get_timeline(metadata, a_row)
    
    for i, b_row in df.iterrows():
        b_key = b_row['key']

        if a_key == b_key:
            continue

        b_cluster = b_row['cluster']
        b_timeline = get_timeline(metadata, b_row)

        if not should_get_distances(a_timeline, b_timeline):
            continue

        cases_distance, deaths_distance, cases_per_100k_distance, deaths_per_100k_distance = get_distances(a_timeline, b_timeline)

        data.append([
            b_key,
            cases_distance,
            deaths_distance,
            cases_per_100k_distance,
            deaths_per_100k_distance,
            a_cluster == b_cluster,
        ])

    df = pd.DataFrame(data, columns=columns)
    return df


def per_timeline(metadata, df, offset=0):
    data = []

    idx = offset+1
    count = len(df)
    for _, a_row in df[offset:offset+500].iterrows():
        a_key = a_row['key']
        a_cluster = a_row['cluster']
        a_timeline = get_timeline(metadata, a_row)

        print('  ', str(idx).rjust(len(str(count)), ' '), '/', count)

        for _, b_row in df.iloc[idx:].iterrows():
            b_key = b_row['key']
            b_cluster = b_row['cluster']
            b_timeline = get_timeline(metadata, b_row)

            if not should_get_distances(a_timeline, b_timeline):
                continue

            cases_distance, deaths_distance, cases_per_100k_distance, deaths_per_100k_distance = get_distances(a_timeline, b_timeline)

            data.append([
                a_key,
                b_key,
                cases_distance,
                deaths_distance,
                cases_per_100k_distance,
                deaths_per_100k_distance,
                a_cluster == b_cluster,
            ])
        idx += 1

    df = pd.DataFrame(data, columns=['region_a', 'region_b', 'cases_distance', 'deaths_distance', 'cases_per_100k_distance', 'deaths_per_100k_distance', 'is_same_cluster'])
    return df


def get_distance(A, B, features, metric='manhattan'):
    length = len(A)
    distances = paired_distances(A[features], B[features], metric='manhattan')
    return np.average(distances, weights=[max(0.1, x/length) for x in range(1, length+1)])


timeline_cache = {}

def get_timeline(metadata, row):
    logger = logging.getLogger('percy.server')
    key = row['key']
    population = row['population']

    try:
        if key not in timeline_cache:
            key_path = key.split('.regions.')
            is_country = len(key_path) == 1

            if not is_country:
                country, region = key_path
                key_path = [country, 'regions', region]

            filename = get(key_path + ['file'], metadata)

            df = pd.read_csv(path.join('inf-covid19-data', filename), parse_dates=True)

            df = normalize_timeline(key, df, get(key_path, metadata))

            df['cases_per_100k'] = df['cases'] / population * 100000
            df['deaths_per_100k'] = df['deaths'] / population * 100000

            timeline_cache[key] = df

        return timeline_cache[key].copy()
    except Exception:
        logger.warn(f"[get_timeline] unable to get_timeline for {key}")
        return pd.DataFrame()
