import sys
import json
import urllib

import requests
from tqdm import tqdm

from cdm_catcher import BASE_URL

CDM_URL = urllib.parse.urlparse(
    BASE_URL).netloc.split(':')[0].replace('server', 'cdm')

def list_collections():
    response = requests.get(
        f'https://{CDM_URL}/digital/bl/dmwebservices/index.php'
        f'?q=dmGetCollectionList/json')
    data = response.json()
    return {c['alias']: c['name'] for c in response.json()}

def collection_size(col_alias):
    url = (f'https://{CDM_URL}/digital/bl/dmwebservices/index.php?'
           'q=dmQuery/tb_ltrs_s/CISOSEARCHALL^0^all^/title'
           '/nosort/%s/%s/1/0/0/0/0/0/json')
    response = requests.get(url % (0, 0))
    data = response.json()
    return data['pager']['total']

def list_itemIds(col_alias):
    url = (f'https://{CDM_URL}/digital/bl/dmwebservices/index.php?'
           'q=dmQuery/tb_ltrs_s/CISOSEARCHALL^0^all^/title'
           '/nosort/%s/%s/1/0/0/0/0/0/json')
    batch_size = 100
    total = collection_size(col_alias)
    for batch in range(0, total, batch_size):
        response = requests.get(url % (batch_size, batch))
        data = response.json()
        for item in data['records']:
            yield item['pointer']

def get_record(col_alias, record_id):
    response = requests.get(
        f'https://{CDM_URL}/digital/bl/dmwebservices/index.php'
        f'?q=dmGetItemInfo{col_alias}/{record_id}/json')
    data = response.json()
    if data.get('message') == 'Requested item not found':
        return None
    return response.json()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('USAGE python cdm_export.py COL_ALIAS')
        sys.exit(1)
    col_alias = sys.argv[1]
    if not col_alias.startswith('/'):
        col_alias = f'/{col_alias}'
    collections = list_collections()
    if col_alias not in collections:
        for path, name in collections.items():
            print(f' {path:<20} {name}')
        print(
            f'Error: unknown collection "{col_alias}"'
            ', valid collections are listed above')
        sys.exit(1)

    with open('export.json', 'w') as out:
        for item_id in tqdm(
                list_itemIds(col_alias),
                total=collection_size(col_alias)):
            out.write(json.dumps(get_record(col_alias, item_id)) + '\n')
