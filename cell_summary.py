import os
import numpy as np
import pandas as pd
import h5py
from pathlib import Path

from settings import DATA_DIR

data_dir = Path(DATA_DIR)


def get_summary(dataset_name, all_dataset_df):
    dataset_id = str(dataset_name).split('/')[-1].split('.')[0] 
    f = h5py.File(dataset_name, "r")
    title = f['uns']['title'][()].decode('utf-8')
    print(f"Reading {dataset_name} ", title)

    tissue_dict     = pd.Series(f['obs']['__categories/tissue']).str.decode('utf-8')
    cell_dict       = pd.Series(f['obs']['__categories/cell_type']).str.decode('utf-8')
    disease_dict    = pd.Series(f['obs']['__categories/disease']).str.decode('utf-8')
    cell_id_dict    = pd.Series(f['obs']['__categories/cell_type_ontology_term_id']).str.decode('utf-8')
    sex_types       = pd.Series(f['obs']['__categories/sex']).str.decode('utf-8')[f['obs']['sex']] if ('sex' in pd.Series(f['obs']['__categories'])) else pd.Series(['unknown'] * len(f['obs']['cell_type']))
    stage_types     = pd.Series(f['obs']['__categories/development_stage']).str.decode('utf-8')[f['obs']['development_stage']] if ('development_stage' in pd.Series(f['obs']['__categories'])) else pd.Series(['unknown'] * len(f['obs']['cell_type']))
    ethnicity_types = pd.Series(f['obs']['__categories/ethnicity']).str.decode('utf-8')[f['obs']['ethnicity']] if ('ethnicity' in pd.Series(f['obs']['__categories'])) else pd.Series(['unknown'] * len(f['obs']['cell_type']))
    donors          = pd.Series(f['obs']['__categories/donor_uuid']).str.decode('utf-8')[f['obs']['donor_uuid']] if ('donor_uuid' in pd.Series(f['obs']['__categories'])) else pd.Series(['unknown'] * len(f['obs']['cell_type']))
    tissue_id_dict  = pd.Series(f['obs']['__categories/tissue_ontology_term_id']).str.decode('utf-8')

    tissues         = tissue_dict[f['obs']['tissue']]
    tissue_ids      = tissue_id_dict[f['obs']['tissue_ontology_term_id']]
    cell_types      = cell_dict[f['obs']['cell_type']]
    cell_type_ids   = cell_id_dict[f['obs']['cell_type_ontology_term_id']]
    diseases        = disease_dict[f['obs']['disease']]
    
    new_data = pd.DataFrame({
        'dataset_name'              : pd.Series([title]*len(tissues)),
        'dataset_id'                : pd.Series([dataset_id]*len(tissues)),
        'tissue'                    : tissues.values,
        'tissue_ontology_term_id'   : tissue_ids.values,
        'cell_type'                 : cell_types.values,
        'cell_type_ontology_id'     : cell_type_ids.values,
        'disease'                   : diseases.values,
        'sex'                       : sex_types.values,
        'donor_id'                  : donors.values,
        'ethnicity'                 : ethnicity_types.values,
        'development_stage'         : stage_types.values,
    })
    
    new_data = new_data.loc[new_data.disease == 'normal', :]

    group_by_cols = [
        'dataset_name',
        'dataset_id',
        'tissue', 
        'tissue_ontology_term_id',
        'sex',
        'donor_id',
        'development_stage',
        'ethnicity',
        'cell_type_ontology_id',
        'cell_type',
    ]

    df_summary = new_data.groupby(group_by_cols).agg(cell_count = ('disease', 'count'))
    print(df_summary)
    df_summary.to_csv('data/all_dataset_summaries/' + dataset_id + '_summary.csv',)
    df_summary = pd.read_csv('data/all_dataset_summaries/' + dataset_id + '_summary.csv')
    result_table = pd.concat([all_dataset_df, df_summary])
    result_table.to_csv('data/master_table.csv', index=None)


def get_all_dataset_table():
    if not os.path.exists('data'):
        os.mkdir('data')
        os.mkdir('data/all_dataset_summaries')
    if not os.path.exists('data/master_table.csv'):
        df = pd.DataFrame(
            columns = [ 
                'dataset_name',
                'dataset_id',
                'tissue', 
                'tissue_ontology_term_id',
                'sex',
                'donor_id',
                'development_stage',
                'ethnicity',
                'cell_type_ontology_id',
                'cell_type',
                'cell_count'
                ], 
            index=None)
        df.to_csv('data/master_table.csv', index=None)
        
    df = pd.read_csv('data/master_table.csv')
    return df


if __name__ == '__main__':
    for dataset_name in data_dir.glob('*.h5ad'):
    #for dataset_name in [str(data_dir)+'/c42c8ad3-9761-49e5-b9bf-ee8ebd50416f.h5ad', str(data_dir)+'/f75f2ff4-2884-4c2d-b375-70de37a34507.h5ad']:
        try:
            all_dataset_df = get_all_dataset_table()
            get_summary(str(dataset_name), all_dataset_df)
        except Exception as e:
            print(f"{dataset_name} => {str(e)}")