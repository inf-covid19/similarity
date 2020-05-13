import pandas as pd
from os import path, getcwd
from fnc.sequences import groupby

from common import make_attributes


def process_brazil(key, metadata):
    key_list = []
    population_list = []
    area_km_list = []

    area_by_city_df = pd.read_csv(path.join('raw', 'brazil', 'area_by_city.csv'))[
        ['CD_GCMUN', 'AR_MUN_2018']].set_index('CD_GCMUN')
    area_by_state_df = pd.read_csv(path.join('raw', 'brazil', 'area_by_state.csv'))[
        ['CD_GCUF', 'AR_MUN_2018']].set_index('CD_GCUF')


    parent_regions = [x for x in metadata['regions'].items() if 'parent' not in x[1]]
    regions_by_parent = groupby([1, 'parent'], metadata['regions'].items())

    for region_key, data in parent_regions:
        df = pd.read_csv(path.join('data', data['file']), parse_dates=[0])
        df = df[['state', 'city', 'place_type', 'estimated_population_2019', 'city_ibge_code']].drop_duplicates()
        for _, row in df.iterrows():
            code = row['city_ibge_code']
            place_type = row['place_type']
            population = row['estimated_population_2019']

            area_df = area_by_state_df if place_type == 'state' else area_by_city_df
            if code not in area_df.index:
                continue

            area_km = float(area_df.loc[code]['AR_MUN_2018'].replace(',', '.'))

            composed_key = f'{key}.regions.{region_key}'
            if place_type == 'city':
                [subregion_key] = [x[0] for x in regions_by_parent[region_key] if x[1]['name'] == row['city']]
                composed_key = f'{key}.regions.{subregion_key}'

            key_list.append(composed_key)
            population_list.append(population)
            area_km_list.append(area_km)

    return make_attributes(key_list, population_list, area_km_list)
