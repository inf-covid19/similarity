import pandas as pd
import urllib.parse
import urllib.request
from os import path
import time
from sultan.api import Sultan


def get_latest_commit_date(region):
    try:
        with Sultan.load(env={'PAGER': 'cat'}) as s:
            result = s.git(f'-C inf-covid19-similarity-data log -1 --format=%ct "by_key/{region}.csv"').run()
            return int(''.join(result.stdout).strip())
    except:
        return 0


with Sultan.load() as s:
    s.git('-C inf-covid19-data pull origin master').run()
    s.git('-C inf-covid19-similarity-data pull origin master').run()

df = pd.read_csv('inf-covid19-similarity-data/regions.csv')

df = df.sort_values('population', ascending=False)

print("Requesting update...")
for key in df['key']:
    print(f"  {key}")

    is_up_to_date = time.time() - get_latest_commit_date(key) < 60 * 60 * 24
    if not is_up_to_date:
        region = urllib.parse.quote(key)
        urllib.request.urlretrieve(f'http://localhost:8000/api/v1/regions/{region}', f'./tmp/{key}.csv')
        print(f"  ... done.")
        time.sleep(120)
    else:
        print('  ... up to date.')

