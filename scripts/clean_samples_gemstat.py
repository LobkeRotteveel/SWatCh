""" Clean sample data from GEMStat

Script to clean sample data obtained from the International Centre for Water
Resources and Global Change's Global Water Quality database and information
system (GEMStat).
Data source: https://gemstat.org/custom-data-request/

Approach:
    * Extract data
    * Standardize column names
    * Standardize naming conventions
    * Standardize dataframe structure
    * Set data types
    * Standardize units
    * Standardize float data
    * Remove impossible values
    * Remove duplicates (keeping first occurence)
    * Extract method data
    * Finalize site data

Assumptions:
    * Where value is below detection limit, the reported value is the
      detection limit.

Input:
    * raw_samples_gemstat_[parameter].csv
    * intermediate_sites_gemstat.csc

Output:
    * cleaned_sites_gemstat.csv
    * cleaned_samples_gemstat.csv
    * cleaned_methods_gemstat.csv
"""

import os
import pandas as pd
import numpy as np
from swatch_utils import set_type
from swatch_utils import convert
from swatch_utils import extract
from swatch_utils import check


print('Importing data...')

sites = pd.read_csv('intermediate_sites_gemstat.csv')
method_ref = pd.read_excel('raw_metadata_gemstat.xls',
    sheet_name='Methods_Metadata', usecols=[
    'Analysis Method Code',
    'Method Name',
    'Method Type',
    'Method Number',
    'Method Source',
    'Method Description',
    ])

path = r'/mnt/boggart/Files/Documents/university/research/SWatCh/input datasets/gemstat'
directory = os.listdir(path)

df = pd.DataFrame()

semi_list = [
    'raw_samples_gemstat_al.csv',
    'raw_samples_gemstat_ca.csv',
    'raw_samples_gemstat_f.csv',
    'raw_samples_gemstat_k.csv',
    'raw_samples_gemstat_mg.csv',
    'raw_samples_gemstat_na.csv',
    'raw_samples_gemstat_p.csv',
    'raw_samples_gemstat_ph.csv',
    'raw_samples_gemstat_so4.csv',
    ]
comma_list = [
    'raw_samples_gemstat_nox.csv',
    'raw_samples_gemstat_oc.csv',
    'raw_samples_gemstat_po4.csv',
    'raw_samples_gemstat_temperature.csv'
    ]

for file in directory:
    if file in semi_list:
        df_temp = pd.read_csv(file, sep=';', encoding='utf-8')
        df = df.append(df_temp, sort=True, ignore_index=True)
    if file in comma_list:
        df_temp = pd.read_csv(file, sep=',', encoding='utf-8')
        df = df.append(df_temp, sort=True, ignore_index=True)


print('Cleaning data...')

# extract data - selected sites
sites['site_id_gemstat'] = sites['site_id_gemstat'].str.lower()
sites_selected = sites[['site_id', 'site_id_gemstat']]
df = df.rename({'GEMS Station Number':'site_id_gemstat'}, axis='columns')
df['site_id_gemstat'] = df['site_id_gemstat'].str.lower()
df = df.merge(sites_selected, on='site_id_gemstat', how='right')
df = df.drop('site_id_gemstat', axis='columns')

# extract data - QAQC'ed data
df = df[~df['Data Quality'].isin(['Suspect','Poor'])]
df = df.drop('Data Quality', axis='columns')

# extract data - measured values within limit of quantification
df = df[~df['Value Flags'].isin(['>','~'])]
df['Value Flags'] = np.where(df['Value Flags'] == '---',
    np.NaN, df['Value Flags'])

# extract data - parameters
param_exclude = [
    'SAR',
    'NOxN',
    'TEMP-Air',
    'POC',
    ]
df = df[~df['Parameter Code'].isin(param_exclude)]

# extract data - measured values
df['Analysis Method Code'] = df['Analysis Method Code'].astype(str)
df = df[~df['Analysis Method Code'].str.contains('CALC')]

# standardize column names
df = df.rename({
    'Sample Date':'date',
    'Sample Time':'time',
    'Depth':'depth',
    'Parameter Code':'parameter_name',
    'Analysis Method Code':'method_id',
    'Value Flags':'bdl_flag',
    'Value':'value',
    'Unit':'unit',
    }, axis='columns')

# standardize naming conventions
param_map = {
    'Al-Dis':'Al',
    'Ca-Dis':'Ca',
    'K-Dis':'K',
    'Na-Dis':'Na',
    'NO2N':'NO2 as N',
    'SO4-Dis':'SO4',
    'TEMP':'temperature',
    'Al-Tot':'Al',
    'F-Dis':'F',
    'Mg-Dis':'Mg',
    'NO3N':'NO3 as N',
    'DOC':'OC',
    'DRP':'PO4',
    'Ca-Tot':'Ca',
    'K-Tot':'K',
    'Mg-Tot':'Mg',
    'Na-Tot':'Na',
    'TOC':'OC',
    'TRP':'PO4',
    'DIP':'PO4',
    }
frac_map = {
    'Al-Dis':'dissolved',
    'Ca-Dis':'dissolved',
    'K-Dis':'dissolved',
    'Na-Dis':'dissolved',
    'NO2N':'unspecified',
    'pH':'unspecified',
    'SO4-Dis':'dissolved',
    'TEMP':'unspecified',
    'Al-Tot':'total',
    'F-Dis':'dissolved',
    'Mg-Dis':'dissolved',
    'NO3N':'unspecified',
    'DOC':'dissolved',
    'DRP':'dissolved',
    'Ca-Tot':'total',
    'K-Tot':'total',
    'Mg-Tot':'total',
    'Na-Tot':'total',
    'TOC':'total',
    'TRP':'total',
    'DIP':'dissolved',
    }

df['parameter_fraction'] = df['parameter_name']
df['parameter_name'] = df['parameter_name'].replace(param_map)
df['parameter_fraction'] = df['parameter_fraction'].replace(frac_map)

unit_map = {
    '�g/g':'ug/g',
    '---':'unit',
    '�c':'deg_c',
    }
df['unit'] = df['unit'].str.lower()
df['unit'] = df['unit'].replace(unit_map)

# standardize ion measurement notation
df['value'] = np.where(df['parameter_name'] == 'NO2 as N',
    df['value'] * 3.2845, df['value']) # NO2 as N to NO2 ion
df['value'] = np.where(df['parameter_name'] == 'NO3 as N',
    df['value'] * 4.4268, df['value']) # NO3 as N to NO3 ion
df['value'] = np.where(df['parameter_name'] == 'NH4 as N',
    df['value'] * 1.2878, df['value']) # NH4 as N to NH4 ion
df['value'] = np.where(df['parameter_name'] == 'PO4 as P',
    df['value'] * 3.06, df['value']) # PO4 as P to PO4 ion
ion_map = {
    'NO2 as N':'NO2',
    'NO3 as N':'NO3',
    'NH4 as N':'NH4',
    'PO4 as P':'PO4',
}
df['parameter_name'] = df['parameter_name'].replace(ion_map)

# standardize dataframe structure
df['timezone'] = ''
df['database'] = 'GEMStat'
df['status'] = ''

# standardize data types
df['date'] = df['date'].astype(str) + ' ' + df['time'].astype(str)
df = set_type.samples(df)

# standardize units
df = df.groupby(['parameter_name','unit']).filter(check.units)
df = df.groupby(['parameter_name','unit']).apply(convert.chemistry,
    param_col='parameter_name',
    unit_col='unit',
    value_col='value')

# standardize float data
df['value'] = df['value'].round(decimals=3)
df['depth'] = df['depth'].round(decimals=3)

# remove impossible values
df = df[(df['value'] >= 0) | (df['parameter_name'] == 'temperature')]

# remove duplicates
df = df.drop_duplicates(subset=[
    'site_id',
    'date',
    'parameter_name',
    'parameter_fraction',
    'value',
    ], keep='first')

# extract methods
method_ref = method_ref.rename({
    'Analysis Method Code':'method_id',
    'Method Name':'method_name',
    'Method Type':'method_type',
    'Method Number':'method_agency_number',
    'Method Source':'method_agency',
    'Method Description':'method_description',
    }, axis='columns')
methods = extract.methods(df,
    site_col ='site_id',
    param_col = 'parameter_name',
    frac_col = 'parameter_fraction',
    date_col = 'date',
    method_col = 'method_id')

methods = methods.merge(method_ref, on='method_id', how='left')

methods['database'] = 'GEMStat'
methods['method_reference'] = ''

methods = set_type.methods(methods)

# finalize site data
sites_final = df['site_id'].unique()
sites = sites[sites['site_id'].isin(sites_final)]


print('Exporting data...')

sites.to_csv('cleaned_sites_gemstat.csv', index=False, encoding='utf-8')
methods.to_csv('cleaned_methods_gemstat.csv', index=False, encoding='utf-8')
df.to_csv('cleaned_samples_gemstat.csv', index=False, encoding='utf-8')

print('Done!')
