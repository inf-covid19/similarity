from os import path
import json
import pandas as pd
import numpy as np
import multiprocessing as mp

from sklearn import preprocessing
from clusters import process, per_similarity, per_timeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity

TOP_K = 100


def get_top_k_target(df, attr):
    return df[attr].sort_values(ascending=True).iloc[min(TOP_K, len(df))-1]


def similarity_worker(args):
    metadata, df, offset = args
    return per_timeline(metadata, df, offset)


if __name__ == "__main__":
    print('Loading metadata...')
    metadata = {}
    with open(path.join('inf-covid19-data', 'data', 'metadata.json')) as f:
        metadata = json.load(f)

    # process atributes
    print('Processing attributes...')
    df_attributes = process(metadata)
    print(f'  Found {len(df_attributes)} regions.')

    # clustering by attributes
    print('Clustering by attributes...')
    clusters = per_similarity(df_attributes)
    df_attributes['cluster'] = clusters.labels_

    df_attributes = df_attributes.sort_values(by=['cluster', 'key'])

    df_attributes.to_csv(path.join('output', 'region_clusters.csv'), index=False)

    clusters = df_attributes['cluster'].unique()
    print(f'  Found {len(clusters)} clusters.')

    # similarity by timeline
    print('Calculating similarity by timeline...')

    df_similarities = None
    with mp.Pool(processes=3) as pool:
        dfs = pool.map(similarity_worker, ((metadata, df_attributes, offset) for offset in range(0, len(df_attributes), 500)))
        df_similarities = pd.concat(dfs, ignore_index=True)

    # save each region
    print('Saving output file for each region...')
    for region in df_attributes['key']:
        is_a = df_similarities['region_a'] == region
        is_b = df_similarities['region_b'] == region
        region_df = df_similarities[is_a | is_b].copy()

        if len(region_df) == 0:
            continue

        region_df['region'] = region_df.apply(lambda r: r['region_b'] if r['region_a'] == region else r['region_a'], axis=1)

        region_df = region_df[['region', 'cases_distance', 'deaths_distance', 'cases_per_100k_distance', 'deaths_per_100k_distance', 'is_same_cluster']]

        within_cases_top_k = region_df['cases_distance'] <= get_top_k_target(region_df, 'cases_distance')
        within_deaths_top_k = region_df['deaths_distance'] <= get_top_k_target(region_df, 'deaths_distance')
        within_cases_per_100k_top_k = region_df['cases_per_100k_distance'] <= get_top_k_target(region_df, 'cases_per_100k_distance')
        within_deaths_per_100k_top_k = region_df['deaths_per_100k_distance'] <= get_top_k_target(region_df, 'deaths_per_100k_distance')

        within_top_k = within_cases_top_k | within_deaths_top_k | within_cases_per_100k_top_k | within_deaths_per_100k_top_k 
        within_same_cluster = region_df['is_same_cluster'] == True
        region_df[within_top_k | within_same_cluster].to_csv(path.join('output', 'per_region', f'{region}.csv'), index=False)

