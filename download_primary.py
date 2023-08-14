from textwrap import indent
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os
import sys
from tqdm import tqdm
import json

from settings import DATA_DIR


# Setup code for downloading the data
# A retry strategy is required to mitigate a temporary infrastructure 504 infrastructure issue.
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[requests.codes.gateway_timeout],
    allowed_methods=["HEAD", "GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
https = requests.Session()
https.mount("https://", adapter)

# Note that API versions have changed. Old dataset IDs in the APi response have the field 'dataset_id_version'.
CELLXGENE_PRODUCTION_ENDPOINT = 'https://api.cellxgene.cziscience.com'
COLLECTIONS = CELLXGENE_PRODUCTION_ENDPOINT + "/dp/v1/collections/"
DATASETS = CELLXGENE_PRODUCTION_ENDPOINT + "/dp/v1/datasets/"


# GET all the public collection UUIDs
r = https.get(COLLECTIONS)
r.raise_for_status()
collections = sorted(r.json()['collections'], key=lambda key: key['created_at'], reverse=True)

# Gather collection for each collection and its datasets
keep_collection = {'datasets', 'name'}
all_collections = []
for collection in collections:
    print('GET ' + COLLECTIONS + collection['id'])
    r1 = https.get(COLLECTIONS + collection['id'], timeout=5)
    r1.raise_for_status()
    collection_collection = r1.json()
    #remove = set(collection_collection) - keep_collection
    #for key in remove: del collection_collection[key]
    all_collections.append(collection_collection)

if not os.path.exists(DATA_DIR+"data"):
    os.makedirs(DATA_DIR+"data")


unique_dataset_links = {}

TOTAL_PRIMARY_CELL_COUNT    = 0
TOTAL_SECONDARY_CELL_COUNT  = 0

for collection in tqdm(all_collections, desc='Extracting links from the collection metadata'):
    print(f"Collection : {collection['name']}")
    primary_cell_count = 0
    secondary_cell_count = 0
    extra_cell_count = 0
    primary_dataset_links = {}
    secondary_dataset_links = {}

    for dataset in collection['datasets']:
        diseases = dataset['disease']
        for disease in diseases:
            if (str(disease['label']).lower() == 'normal' and str(dataset['organism'][0]['label']).lower() == 'homo sapiens'):
                for asset in dataset['dataset_assets']:
                    if asset['filetype'] == 'H5AD':
                        if dataset['is_primary_data'] == 'PRIMARY':
                            primary_cell_count += dataset['cell_count']
                            primary_dataset_links[dataset['id']] = {
                                'dataset_name'      : dataset['name'],
                                'is_primary_data'   : dataset['is_primary_data'],
                                'asset_dataset_id'  : asset['dataset_id'],
                                'asset_id'          : asset['id']
                            }

                        elif dataset['is_primary_data'] == 'SECONDARY':
                            secondary_cell_count += dataset['cell_count']
                            secondary_dataset_links[dataset['id']] = {
                                'dataset_name'      : dataset['name'],
                                'is_primary_data'   : dataset['is_primary_data'],
                                'asset_dataset_id'  : asset['dataset_id'],
                                'asset_id'          : asset['id']
                            }
                        else:
                            extra_cell_count += dataset['cell_count']

                print(f"\tDataset : {dataset['is_primary_data']} : {dataset['id']} : {dataset['name']} ")
    TOTAL_PRIMARY_CELL_COUNT    += primary_cell_count
    TOTAL_SECONDARY_CELL_COUNT  += secondary_cell_count
    print(f"\n\tPRIMARY CELL COUNT : {primary_cell_count} \t SECONDARY CELL COUNT : {secondary_cell_count} \t EXTRA CELL COUNT : {extra_cell_count}")
    

    # Condition 1 : CELL COUNTS : Primary > Secondary:
    if (primary_cell_count > secondary_cell_count) and (primary_cell_count > 0):
        unique_dataset_links = {**unique_dataset_links, **primary_dataset_links}
        print('\tAdded PRIMARY DATASET LINKS : ', json.dumps(primary_dataset_links, indent=4))

    # Condition 2 : CELL COUNTS : Primary < Secondary:
    elif (primary_cell_count < secondary_cell_count) and (secondary_cell_count > 0):
        unique_dataset_links = {**unique_dataset_links, **secondary_dataset_links}
        print('\tAdded SECONDARY DATASET LINKS : ',json.dumps(secondary_dataset_links, indent=4))

    # Condition 3 : CELL COUNTS : Both Zero (Non Human Collection. - Skip.)
    else:
        print('\tNO LINKS ADDED.')
    print("\n")
print(f"Total PRIMARY CELL COUNT : {TOTAL_PRIMARY_CELL_COUNT} \t TOTAL SECONDARY CELL COUNT : {TOTAL_SECONDARY_CELL_COUNT}")
print(f"\nAll Links : \n{json.dumps(unique_dataset_links, indent=4)}")


# START THE DOWNLOAD.
print("*"*200,"\n","*"*200)

print("\nStarting the download\n")

for dataset_id, dataset_meta in tqdm(unique_dataset_links.items(), desc=('CZI Dataset Download : ')):
    DATASET_REQUEST = DATASETS + dataset_meta['asset_dataset_id'] + "/asset/" + dataset_meta['asset_id']
    r2 = requests.post(DATASET_REQUEST)
    r2.raise_for_status()
    presigned_url = r2.json()['presigned_url']
    headers = {'range': 'bytes=0-0'}
    r3 = https.get(presigned_url, headers=headers)
    if r3.status_code == requests.codes.partial:
        print("\t** " + r3.headers['Content-Range'])
    download_name = DATA_DIR + dataset_id + '.h5ad'
    if os.path.isfile(download_name):
        print(f'\t** Download skipped. {download_name} already exists')
    else:
        print(f'\t** Downloading as {download_name}')
        with https.get(presigned_url, stream=True, timeout=10) as r3:
            r3.raise_for_status()
            with open(download_name, 'wb') as out:
                try:
                    for chunk in r3.iter_content(chunk_size=8192):
                        out.write(chunk)
                except:
                    print(f"ERROR: Deleting partially downloaded file {download_name}")
                    os.remove(download_name)
                    sys.exit(1)
      






