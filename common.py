import pandas as pd


def make_attributes(key_list, population_list, area_km_list):
    return pd.DataFrame(data={
        'key': key_list,
        'population': population_list,
        'area_km': area_km_list,
    })


def normalize_timeline(df, date_column='date', cases_column='cases', deaths_column='deaths', daily=False):
    return df
