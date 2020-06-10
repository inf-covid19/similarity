import pandas as pd
import urllib.parse
import urllib.request
from os import path
import time
from sultan.api import Sultan


with Sultan.load() as s:
    s.git('-C inf-covid19-similarity-data pull').run()

df = pd.read_csv('inf-covid19-similarity-data/regions.csv')

df = df.sort_values('population_density', ascending=False)

print("Requesting update...")
for key in df['key']:
    print(f"  {key}")
    region = urllib.parse.quote(key)
    if path.isfile(path.join("inf-covid19-similarity-data", 'by_key', f'{key}.csv')):
        print(f"  ... skipped.")
        continue
    urllib.request.urlretrieve(f'https://covid19-similarity.herokuapp.com/api/v1/regions/{region}', f'./tmp/{key}.csv')
    print(f"  ... done.")
    time.sleep(120)