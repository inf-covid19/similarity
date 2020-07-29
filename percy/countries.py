import pandas as pd
import numpy as np
from os import path


from percy.common import normalize_timeline, make_attributes


def area_by_country():
    df = pd.read_csv(path.join('raw', 'area_by_country.csv'))
    area_map = {}
    for _, row in df.iterrows():
        year = 2019
        code = row["Country Code"]
        area = np.nan
        for year in range(2019, 1959, -1):
            year_area = row[str(year)]
            if np.isnan(year_area):
                continue
            area = year_area
            break
        if not np.isnan(area):
            area_map[code] = area
    return area_map


def process_country(key, metadata):
    area_map = area_by_country()
    code = metadata['countryTerritoryCode']

    timeline = []
    if code not in area_map:
        return None

    area_km = area_map[code]

    df = pd.read_csv(
        path.join('inf-covid19-data', metadata['file']),
        parse_dates=[0],
        dayfirst=True,
    )

    population = get_population(df)
    if np.isnan(population):
        return None

    return make_attributes([key], [population], [area_km])


def get_population(df):
    keys = ['popData2019', 'popData2018']
    for key in keys:
        try:
            return df[key].iloc[0]
        except:
            pass
    return np.nan
