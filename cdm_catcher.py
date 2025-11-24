import sys
import os
import csv
import logging.config
import urllib.parse

import requests
from lxml import etree
from zeep import Client
from zeep.loader import load_external
from tqdm import tqdm

# note that the CDM_BASE_URL can be found by visiting the following URL:
# https://<YOUR PREFIX>.contentdm.oclc.org/utils/diagnostics

for var_name in ['CDM_USER', 'CDM_PASS', 'CDM_LICENSE', 'CDM_BASE_URL']:
    missing = False
    if var_name not in os.environ:
        print(f'ERROR: Missing environment variable "{var_name}"')
        missing = True

if missing:
    print("""
On windows type: SET <var_name>=<value>
For each missing variable, before starting the script. """)
    sys.exit(1)

USER = os.environ['CDM_USER']
PASSWORD = os.environ['CDM_PASS']
LICENSE = os.environ['CDM_LICENSE']
BASE_URL = os.environ['CDM_BASE_URL']
WSDL_URL = ('https://worldcat.org/webservices/'
            'contentdm/catcher/6.0/CatcherService.wsdl')
BASE_DIR = os.path.dirname(__file__)
XSD_FILE = 'catcher.xsd'
CATCHER_NS = 'http://catcherws.cdm.oclc.org/v6.0.0/'

LOG_CONFIG = {
    'version': 1,
    'formatters': {
        'verbose': {
            'format': '%(name)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'zeep.transports': {
            'level': 'DEBUG',
            'propagate': True,
            'handlers': ['console'],
        },
    }
}


class ContentDMCatcherClient:
    """
    Client that connects to ContentDM Catcher WSDL/Soap API
    See: https://help.oclc.org/Metadata_Services/CONTENTdm/CONTENTdm_Catcher
    """
    def __init__(self, base_url, user, password, license, debug=False):
        self.base_url = base_url
        self.user = user
        self.password = password
        self.license = license
        self.client = Client(WSDL_URL)

        if debug:
            logging.config.dictConfig(LOG_CONFIG)

        # load local xsd because we can't get metadataList type
        # to work in online version
        self.client.wsdl.types.create_new_document(
            load_external(open(XSD_FILE, 'rb'), None),
            f'file://{BASE_DIR}').resolve()

    def get_record(self, col_alias, record_id):
        cdm_domain = urllib.parse.urlparse(
            self.base_url).netloc.split(':')[0].replace('server', 'cdm')
        response = requests.get(
            f'https://{cdm_domain}/digital/bl/dmwebservices/index.php'
            f'?q=dmGetItemInfo{col_alias}/{record_id}/json')
        data = response.json()
        if data.get('message') == 'Requested item not found':
            return None
        return response.json()

    def get_collections(self):
        xml = self.client.service.getCONTENTdmCatalog(
            self.base_url, self.user, self.password, self.license)
        doc = etree.fromstring(xml.encode('utf8'))
        collections = {}
        for col in doc.xpath('/collinfo/collection'):
            alias = col.xpath('string(collection_alias)')
            name = col.xpath('string(collection_name)')
            collections[alias] = name
        return collections

    def get_collection_fields(self, col_alias):
        xml = self.client.service.getCONTENTdmCollectionConfig(
            self.base_url, self.user, self.password, self.license, col_alias)
        doc = etree.fromstring(xml.encode('utf8'))
        fields = {}
        for field in doc.xpath('/fields/field'):
            nick = field.xpath('string(nickname)')
            name = field.xpath('string(name)')
            fields[nick] = name
        return fields

    def update_metadata(self,
                        col_alias,
                        record_id,
                        field_nick,
                        value,
                        disable_validation=True):
        types = self.client.type_factory(CATCHER_NS)
        metadata = types.metadataWrapper(
            types.metadataList([
                types.metadata(field='dmrecord', value=record_id),
                types.metadata(field=field_nick, value=value)]))

        txn_log = self.client.service.processCONTENTdm(
            'edit',
            self.base_url,
            self.user,
            self.password,
            self.license,
            col_alias,
            str(disable_validation).lower(),
            metadata)
        return txn_log


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('USAGE python cdm_catcher.py INPUT_CSV_FILE')
        sys.exit(1)

    input_csv_file = sys.argv[1]
    cols = ['col_alias', 'record_id', 'field', 'value']

    updates = []
    for row_num, row in enumerate(csv.reader(open(sys.argv[1]))):
        if row_num == 0:
            missing_columns = set(cols) - set(row)
            if missing_columns:
                print('Error: Missing columns in input csv: {missing_columns}')
                sys.exit(1)
            continue
        updates.append(dict(zip(cols, row)))

    client = ContentDMCatcherClient(BASE_URL, USER, PASSWORD, LICENSE)

    collections = client.get_collections()
    collection_fields = {}
    current_records = {}

    updated = 0
    skipped = 0

    for row_num, data in enumerate(tqdm(updates)):
        col_alias = data['col_alias']
        field = data['field']
        record_id = data['record_id']
        value = data['value']

        # test if collection exists
        if col_alias not in collections:
            for path, name in collections.items():
                print(f' {path:<20} {name}')
            print(
                f'Error: unknown collection "{col_alias}" on row {row_num + 2}'
                ', valid collections are listed above')
            sys.exit(1)

        # teat if field is valid
        if col_alias not in collection_fields:
            collection_fields[col_alias] = client.get_collection_fields(
                col_alias)
        if field not in collection_fields[col_alias]:
            for nick, name in collection_fields[col_alias].items():
                print(f' {nick:<20} {name}')
            print(
                f'Error: unknown field nick "{field}" on row {row_num + 2}'
                ', valid fields are listed above')
            sys.exit(1)

        # test if record exists
        current_record = current_records.get((col_alias, record_id))
        if current_record is None:
            current_record = client.get_record(col_alias, record_id)
            if current_record is None:
                print('Error: unknown record id "{}" on row {}'.format(
                    record_id, row_num + 2))
                sys.exit(1)
            current_records[(col_alias, record_id)] = current_record

        # test if record has similar value
        if current_record.get(field) == value:
            skipped += 1
            continue

        # update the value
        txn_log = client.update_metadata(col_alias, record_id, field, value)
        if txn_log.startswith('Error detail:'):
            msg = txn_log.splitlines()[0].split(':', 1)[1].strip()
            print(f'Error {msg} on row {row_num + 2}')
            sys.exit()
        updated += 1

    print(f'Updated {updated} records, skipped {skipped}.')
