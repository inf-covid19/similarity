import json
from os import getcwd, path

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from scipy.cluster.hierarchy import dendrogram
from sklearn.datasets import load_iris
from sklearn.cluster import AgglomerativeClustering
from sklearn.preprocessing import StandardScaler
from sklearn.manifold import SpectralEmbedding

from fnc.mappings import merge

from countries import process_country
from brazil import process_brazil
from sweden import process_sweden

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

    return region_attributes



def per_similarity(region_attributes):
    df = region_attributes[['population', 'area_km', 'population_density']]
    X = df.to_numpy()
    X_std = StandardScaler().fit_transform(X)
    model = AgglomerativeClustering(distance_threshold=0.1, n_clusters=None)
    model = model.fit(X_std)
    return model


def per_timeline(regions):
    pass
