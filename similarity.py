from os import path
import json

from clusters import process, per_similarity


if __name__ == "__main__":
    metadata = {}
    with open(path.join('data', 'data', 'metadata.json')) as f:
        metadata = json.load(f)

    region_attributes = process(metadata)

    clusters = per_similarity(region_attributes)
    # clusters = per_timeline(region_timeline)

    region_attributes['cluster'] = clusters.labels_

    clusters = region_attributes['cluster'].unique()

    for cluster in clusters:
        print(region_attributes[region_attributes['cluster'] == cluster])

    print(region_attributes.groupby('cluster').count()['key'].sort_values())

    print(len(region_attributes))
    print(len(clusters))

    region_attributes.to_csv('region_attributes.csv', index=False)


    # print(region_attributes)
    # print(clusters.labels_)
    # print(clusters.get_params())
