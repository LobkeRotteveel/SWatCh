""" Merged cleaned data

Script to merge cleaned site, sample, and method data.


Approach:
    * Import and merge data
    * Remove duplicates

Assumptions:

Notes:

Input:
    * cleaned_sites_[database name].csv
    * cleaned_samples_[database name].csv
    * cleaned_methods_[database name].csv

Output:
    * sites.csv
    * sites_duplicated.csv
    * samples.csv
    * methods.csv
"""

import os
import pandas as pd
from tqdm import tqdm
from swatch_utils import set_type


print('Importing and merging data...')

path = r'/mnt/boggart/Files/Documents/university/research/SWatCh/input datasets/cleaned datasets'
directory = os.listdir(path)

print('Sites')
df_sites = pd.DataFrame()
for file in tqdm(directory):
    if 'cleaned_sites' in file:
        temp_df = pd.read_csv(file)
        df_sites = df_sites.append(temp_df, sort=True, ignore_index=True)

# correct wrong coordinates
corrected_sites = pd.read_csv('sites_wrongcoordinates_checked.csv')
corrected_sites = set_type.sites(corrected_sites)
corrected_sites = corrected_sites.drop('OBJECTID', axis='columns')
df_sites = df_sites.append(corrected_sites, sort=True, ignore_index=True)


print('Samples')
df = pd.DataFrame()
for file in tqdm(directory):
    if 'cleaned_samples' in file:
        temp_df = pd.read_csv(file, low_memory=False)
        df = df.append(temp_df, sort=True, ignore_index=True)

print('Methods')
df_methods = pd.DataFrame()
for file in tqdm(directory):
    if 'cleaned_methods' in file:
        temp_df = pd.read_csv(file, low_memory=False)
        df_methods = df_methods.append(temp_df, sort=True, ignore_index=True)


print('Removing duplicated data...')

# identify duplicated sites
dupes = df_sites[df_sites.duplicated(subset=['site_id','country'],
    keep=False)]

# remove duplicated sites
df_sites = df_sites.drop_duplicates(subset=['site_id','country'],
    keep='last')
df_sites['temp_id'] = df_sites['site_id'] + df_sites['country']
keep_temp_ids = df_sites['temp_id'].unique()
unique_sites = df_sites['site_id'].unique()

merge_cols = df_sites[['site_id','country']]
for i in [df, df_methods]:
    i = pd.merge(i, merge_cols, on='site_id', how='inner')
    i['temp_id'] = i['site_id'] + i['country']
    i = i[i['temp_id'].isin(keep_temp_ids)]
    i = i.drop(['temp_id','country'], axis='columns')
    i = i[i['site_id'].isin(unique_sites)]

# remove duplicated samples
df = df.drop_duplicates(subset=[
    'site_id',
    'parameter_name',
    'parameter_fraction',
    'value',
    'date',
    ], keep='first')

# remove duplicated methods
df_methods = df_methods.drop_duplicates(subset=[
    'site_id',
    'parameter_name',
    'parameter_fraction',
    'method_id',
    ], keep='last')


# remove sites identified as incorrect in GIS
df_sites_final = pd.read_csv('sites_noGEMStat_final.csv', usecols=['site_id'])
df = df[df['site_id'].isin(df_sites_final['site_id'])]
df_methods = df_methods[df_methods['site_id'].isin(df_sites_final['site_id'])]


print('Exporting data...')

dupes.to_csv('sites_duplicated.csv', index=False, encoding='UTF-8')
df_sites.to_csv('sites.csv', index=False, encoding='UTF-8')
df.to_csv('samples.csv', index=False, encoding='UTF-8')
df_methods.to_csv('methods.csv', index=False, encoding='utf-8')

print('Done!')
