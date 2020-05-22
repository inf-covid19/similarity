import pandas as pd
from os import path, getcwd
from fnc.sequences import groupby

from common import make_attributes


def get_fips(state, county=0):
    state_part = str(int(state)).rjust(2, '0')
    county_part = str(int(county)).rjust(3, '0')
    return int(f'{state_part}{county_part}')


def process_united_states_of_america(key, metadata):
    key_list = []
    population_list = []
    area_km_list = []

    area_by_fips = pd.read_csv(path.join('raw', 'united_states_of_america', 'DataSet.csv'))[['fips', 'LND110210']].set_index('fips')

    # convert to square km
    area_by_fips['LND110210'] *= 2.59

    pop_by_fips = pd.read_csv(path.join('raw', 'united_states_of_america', 'co-est2019-alldata.csv'), encoding='ISO-8859-1')[['STATE', 'COUNTY', 'POPESTIMATE2019']]
    pop_by_fips['FIPS'] = pop_by_fips.apply(lambda r: get_fips(r['STATE'], r['COUNTY']), axis=1)
    pop_by_fips = pop_by_fips[['FIPS', 'POPESTIMATE2019']].set_index("FIPS")

    parent_regions = [x for x in metadata['regions'].items() if 'parent' not in x[1]]
    regions_by_parent = groupby([1, 'parent'], metadata['regions'].items())

    for region_key, data in parent_regions:
        df = pd.read_csv(path.join('inf-covid19-data', data['file']), parse_dates=False)
        df = df[['state', 'county', 'place_type', 'fips']].drop_duplicates()
        for _, row in df.iterrows():
            fips = row['fips']
            place_type = row['place_type']

            if place_type == 'state':
                fips = get_fips(fips)

            if fips not in pop_by_fips.index or fips not in area_by_fips.index:
                continue

            area_km = area_by_fips.loc[fips]['LND110210']
            population = pop_by_fips.loc[fips]['POPESTIMATE2019']

            composed_key = f'{key}.regions.{region_key}'
            if place_type == 'county':
                [subregion_key] = [x[0] for x in regions_by_parent[region_key] if x[1]['name'] == row['county']]
                composed_key = f'{key}.regions.{subregion_key}'

            key_list.append(composed_key)
            population_list.append(population)
            area_km_list.append(area_km)

    return make_attributes(key_list, population_list, area_km_list)