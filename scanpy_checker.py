import scanpy as sc
import numpy as np
import os
import pandas as pd
from pathlib import Path
from settings import DATA_DIR
from collections import defaultdict
import scanpy as sc
from anndata import AnnData


sc.settings.verbosity = 3   
FILENAME = 'status_log_2.csv'

def get_all_dataset():
    data_dir = Path(DATA_DIR)
    all_datasets = [ds for ds in data_dir.glob('*.h5ad')]
    return all_datasets

def create_save_file(all_datasets):
    status = 'unchecked' 
    df = pd.DataFrame(columns=['Dataset_ID', 'Path', 'Status'], index=None)
    for i, dataset in enumerate(all_datasets):
        df.loc[i,:] = [str(dataset).split('/')[-1].split('.')[0], dataset, status]
    df.to_csv(FILENAME, index=None)
    print('Created File.')

def load_csv(all_datasets, filename=FILENAME, ):
    if not os.path.exists(filename):
        create_save_file(all_datasets)
    df = pd.read_csv(filename)
    print(df)
    for index, row in df.iterrows():
        
        if row[2] == 'unchecked':
            try:
                sc_df = sc.read_h5ad(row['Path'])
                df.loc[index, 'Status'] = 'successfully_opened'
                print(f"Succesfully opened\t{row['Dataset_ID']}")
                print(f'Dataset : { df.iloc[index, 1] }')
                print(f'{sc_df}\n')
                df.to_csv(FILENAME)

            except Exception as e:
                print(f"Failed to open\t{row['Dataset_ID']}")
                df.loc[index, 'Status'] = 'failed_to_open'
                df.to_csv(FILENAME)
                print(e)

            print('-'*100,'\n')    
        

if __name__ == '__main__':
    all_datasets = get_all_dataset()
    load_csv(all_datasets)