import pandas as pd
from os import path

from fnc.mappings import merge, get
from datetime import timedelta

from utils import checksum


def make_attributes(key_list, population_list, area_km_list):
    return pd.DataFrame(data={
        'key': key_list,
        'population': population_list,
        'area_km': area_km_list,
    })


PLACE_TYPE_COLUMN_MAPPING = {
    'state': "state",
    'city': "city",
    'county': "county",
}

REGION_CUSTOM_CONFIG = {
    'Brazil': {
        'columns': {
            'cases': "confirmed",
        },
    },
}


def normalize_timeline(key, df, region_data):
    country, region, is_country = parse_key(key)

    date_column = 'dateRep' if is_country else get(
        [country, 'columns', 'date'], REGION_CUSTOM_CONFIG, default='date')
    cases_column = 'cases' if is_country else get(
        [country, 'columns', 'cases'], REGION_CUSTOM_CONFIG, default='cases')
    deaths_column = 'deaths' if is_country else get(
        [country, 'columns', 'deaths'], REGION_CUSTOM_CONFIG, default='deaths')

    if not is_country:
        name_column = get(region_data['place_type'],
                          PLACE_TYPE_COLUMN_MAPPING, default='region')
        df = df[df['place_type'] == region_data['place_type']]
        df = df[df[name_column] == region_data['name']]

    df[date_column] = pd.to_datetime(df[date_column], dayfirst=is_country)
    df = df.sort_values(by=[date_column], ascending=[True])

    if is_country:
        df[cases_column] = df[cases_column].cumsum()
        df[deaths_column] = df[deaths_column].cumsum()

    df = df[df[cases_column] > 0]

    prev_date = None
    prev_cases = 0
    prev_deaths = 0

    timeline = []
    for _, row in df.iterrows():
        date = row[date_column]

        if prev_date and (date - prev_date).days > 1:
            missing_date = prev_date + timedelta(days=1)
            while (date - missing_date).days >= 1:
                timeline.append([
                    missing_date,
                    prev_cases,
                    0,
                    prev_deaths,
                    0
                ])
                missing_date = missing_date + timedelta(days=1)

        cases = row[cases_column]
        cases_daily = max(0, cases - prev_cases)
        deaths = row[deaths_column]
        deaths_daily = max(0, deaths - prev_deaths)

        timeline.append([
            date,
            cases,
            cases_daily,
            deaths,
            deaths_daily
        ])

        prev_cases = cases
        prev_deaths = deaths
        prev_date = date

    return pd.DataFrame(timeline, columns=['date', 'cases', 'cases_daily', 'deaths', 'deaths_daily'])


def parse_key(key):
    key_parts = key.split(".regions.")
    is_country = len(key_parts) == 1
    country = key
    region = key
    if not is_country:
        country, region = key_parts
    return country, region, is_country


def metadata_changed():
    metadata_path = path.join('inf-covid19-data', 'data', 'metadata.json')
    checksum_path = path.join('inf-covid19-similarity-data', 'CHECKSUM')

    is_changed = True
    current_hash = checksum(metadata_path)

    try:
        with open(checksum_path, 'r') as checksum_file:
            previous_hash = checksum_file.read()
            is_changed = previous_hash != current_hash
    except:
        pass
    
    with open(checksum_path, 'w') as checksum_file:
        checksum_file.write(current_hash)
    
    return is_changed