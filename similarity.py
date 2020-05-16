from os import path
import json
import pandas as pd
import numpy as np

from sklearn import preprocessing
from clusters import process, per_similarity, per_timeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity

TOP_K = 100

if __name__ == "__main__":
    print('Loading metadata...')
    metadata = {}
    with open(path.join('data', 'data', 'metadata.json')) as f:
        metadata = json.load(f)

    # process atributes
    print('Processing attributes...')
    df_attributes = process(metadata)

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
    df_similarities = per_timeline(metadata, df_attributes)
    # df_similarities.to_csv(path.join('output', 'region_similarities.csv'), index=False)

    # save each region
    print('Saving output file for each region...')
    for region in df_attributes['key']:
        within_a = df_similarities['region_a'] == region
        within_b = df_similarities['region_b'] == region
        region_df = df_similarities[within_a | within_b].copy()

        if len(region_df) == 0:
            continue

        region_df['region'] = region_df.apply(lambda r: r['region_b'] if r['region_a'] == region else r['region_a'], axis=1)

        region_df['similarity'] = 1 - (region_df['distance'] / region_df['distance'].max())

        region_df = region_df[['region', 'similarity', 'distance', 'days', 'is_same_cluster']]

        days_factor = region_df['days'] / region_df['days'].max()
        cluster_factor = [1.25 if x else 1.0 for x in region_df['is_same_cluster']]

        region_df['score'] = region_df['similarity'] * days_factor * cluster_factor
        region_df = region_df.sort_values(by=['score'], ascending=[False])
        score_target = region_df['score'].iloc[min(TOP_K, len(region_df)) - 1]

        within_top_k = region_df['score'] >= score_target
        within_same_cluster = region_df['is_same_cluster'] == True
        region_df[within_top_k | within_same_cluster].to_csv(path.join('output', 'per_region', f'{region}.csv'), index=False)
