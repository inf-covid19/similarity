import pandas as pd
from os import path, getcwd
from fnc.sequences import groupby

from percy.common import make_attributes


def process_sweden(key, metadata):
    key_list = []
    population_list = []
    area_km_list = []

    for region_key, data in metadata['regions'].items():
        df = pd.read_csv(path.join('inf-covid19-data', data['file']), parse_dates=False)
        df = df[['estimated_population_2019', 'area_km2']]
        for _, row in df.iterrows():
            area_km = row['area_km2']
            population = row['estimated_population_2019']
            composed_key = f'{key}.regions.{region_key}'
            key_list.append(composed_key)
            population_list.append(population)
            area_km_list.append(area_km)
            break

    return make_attributes(key_list, population_list, area_km_list)
