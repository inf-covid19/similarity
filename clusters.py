import json
from os import getcwd, path

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

from countries import process_country
from brazil import process_brazil
from sweden import process_sweden
from common import normalize_timeline

COUNTRY_PROCESS_MAPPING = {
    'Brazil': process_brazil,
    'Sweden': process_sweden,
}


def process(metadata):
    region_attributes = pd.DataFrame()

    for country, data in metadata.items():
        attributes = process_country(country, data)
        if attributes is not None:
            region_attributes = pd.concat([region_attributes, attributes], ignore_index=True)

        if country in COUNTRY_PROCESS_MAPPING:
            process_fn = COUNTRY_PROCESS_MAPPING[country]
            subregion_attributes = process_fn(country, metadata[country])
            region_attributes = pd.concat([region_attributes, subregion_attributes], ignore_index=True)

    region_attributes['population_density'] = region_attributes['population']/region_attributes['area_km']
    region_attributes['days'] = region_attributes.apply(lambda r: len(get_timeline(metadata, r['key'])), axis=1)

    return region_attributes


def per_similarity(region_attributes):
    df = region_attributes[['population', 'area_km', 'population_density']]
    X = df.to_numpy()
    X_std = StandardScaler().fit_transform(X)
    model = AgglomerativeClustering(distance_threshold=0.1, n_clusters=None)
    model = model.fit(X_std)
    return model


def per_timeline(metadata, df):
    data = []
    features = ['cases', 'deaths', 'cases_daily', 'deaths_daily']

    idx = 1
    count = len(df)
    for _, a_row in df.iterrows():
        a_key = a_row['key']
        a_cluster = a_row['cluster']
        a_timeline = get_timeline(metadata, a_key)

        print('  ', str(idx).rjust(len(str(count)), ' '), '/', count)

        for _, b_row in df.iloc[idx:].iterrows():
            b_key = b_row['key']
            b_cluster = b_row['cluster']
            b_timeline = get_timeline(metadata, b_key)

            length = min(len(a_timeline), len(b_timeline))

            if length == 0:
                continue

            distance = paired_distances(
                a_timeline[features][:length],
                b_timeline[features][:length],
                metric='manhattan'
            )

            data.append([
                a_key,
                b_key,
                np.average(distance, weights=[min(0.1, x/length) for x in range(1, length+1)]),
                length,
                a_cluster == b_cluster,
            ])
        idx += 1

    df = pd.DataFrame(data, columns=['region_a', 'region_b', 'distance', 'days', 'is_same_cluster'])
    return df


timeline_cache = {}

def get_timeline(metadata, key):
    if key in timeline_cache:
        return timeline_cache[key]

    key_path = key.split('.regions.')
    is_country = len(key_path) == 1

    if not is_country:
        country, region = key_path
        key_path = [country, 'regions', region]

    filename = get(key_path + ['file'], metadata)

    df = pd.read_csv(path.join('data', filename), parse_dates=True)

    df = normalize_timeline(key, df, get(key_path, metadata))

    timeline_cache[key] = df

    return df