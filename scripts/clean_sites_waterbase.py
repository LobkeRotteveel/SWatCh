""" Clean site data from Waterbase

Script to clean site data obtained from the European Environment Agency
Waterbase.
Data source: https://www.eea.europa.eu/data-and-maps/data/waterbase-water-quality-2

Approach:
    * Extract data
    * Standardize column names
    * Standardize naming conventions
    * Standardize units
    * Standardize dataframe structure
    * Standardize data
    * Remove duplicates

Input:
    * raw_sites_waterbase.csv

Output:
    * intermediate_sites_waterbase.csv
"""

import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from swatch_utils import set_type


print('Importing data...')

# import data
chunked = pd.read_csv('raw_sites_waterbase.csv', chunksize=10000000)


print('Cleaning data...')

for i, df in tqdm(enumerate(chunked)):
    # extract data
    df = df[df['confidentialityStatus'] == 'F']
    df = df.drop('confidentialityStatus', axis='columns')

    df = df[(df['waterBodyIdentifierScheme'] != 'euGroundWaterBodyCode') & (df['waterBodyIdentifierScheme'] != 'eionetGroundWaterBodyCode')]
    df = df.drop('waterBodyIdentifierScheme', axis='columns')

    # standardize column names
    df = df.rename({
        'monitoringSiteIdentifier':'site_id',
        'monitoringSiteIdentifierScheme':'agency',
        'waterBodyIdentifier':'water_id',
        'lon':'longitude',
        'lat':'latitude',
        }, axis='columns')

    # standardize naming conventions
    agency_map = {
        'eionetMonitoringSiteCode':'European Environment Information Observation Network',
        'euMonitoringSiteCode':'European Union Water Framework Directive',
    }
    df['agency'] = df['agency'].replace(agency_map)

    # export sample data
    if i == 0:
        df.to_csv('intermediate_samples_waterbase_chemistry.csv',
            index=False, encoding='utf-8', mode='w', header=True)
    else:
        df.to_csv('intermediate_samples_waterbase_chemistry.csv',
            index=False, encoding='utf-8', mode='a', header=False)

df = pd.read_csv('intermediate_samples_waterbase_chemistry.csv')

# standardize dataframe structure
df['site_name'] = ''
df['site_type'] = ''
df['elevation'] = np.NaN
df['coordinate_system'] = ''
df['state_province'] = ''
df['country'] = ''
df['water_name'] = ''
df['water_mrt'] = np.NaN
df['water_volume'] = np.NaN
df['catchment_id'] = ''
df['catchment_name'] = ''
df['catchment_area'] = ''
df['database'] = 'Waterbase'

# standardize data
df = set_type.sites(df)

# remove duplicates
df = df.drop_duplicates(subset='site_id', keep='first')


print('Exporting data...')

df.to_csv('intermediate_sites_waterbase.csv', index=False, encoding='utf-8')

print('Done!')
