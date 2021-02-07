""" Clean site data from ECCC

Script to clean site data obtained from Environment and Climate Change Canada
National Long-term Water Quality Monitoring Database.
Data source: http://data.ec.gc.ca/data/substances/monitor/national-long-
             term-water-quality-monitoring-data/

Approach:
    * Standardize column names
    * Standardize naming conventions
    * Standardize dataframe structure
    * Standardize data
    * Remove duplicates (keeping first occurrence)

Assumptions:
    * Province listed as 'US' is located in the United States of America

Input:
    * raw_sites_eccc.csv

Output:
    * intermediate_sites_eccc.csv
"""

import pandas as pd
import numpy as np
from swatch_utils import set_type


print('Importing data...')

df = pd.read_csv('raw_sites_eccc.csv', sep=',', encoding='ISO-8859-1',
    usecols=[
    'SITE_NO',
    'SITE_NAME',
    'SITE_TYPE',
    'LATITUDE',
    'LONGITUDE',
    'DATUM',
    'PROV_TERR',
    'PEARSEDA',
    ], dtype=str)


print('Cleaning data...')

# standardize column names
df = df.rename({
    'SITE_NO':'site_id',
    'SITE_NAME':'site_name',
    'SITE_TYPE':'site_type',
    'DATUM':'coordinate_system',
    'PROV_TERR':'state_province',
    'PEARSEDA':'catchment_name',
    }, axis='columns')
df.columns = df.columns.str.lower()

# standardize naming conventions
water_map = {
    'RIVER/RIVIÈRE':'river',
    'LAKE/LAC':'lake',
    'POND/ÉTANG':'pond',
    }
df['site_type'] = df['site_type'].replace(water_map)

province_map = {
    'QC':'Quebec',
    'NB':'New Brunswick',
    'US':np.NaN,
    'AB':'Alberta',
    'BC':'British Columbia',
    'MB':'Manitoba',
    'NL':'Newfoundland and Labrador',
    'NS':'Nova Scotia',
    'NU':'Nunavut',
    'NT':'Northwest Territories',
    'ON':'Ontario',
    'PE':'Prince Edward Island',
    'SK':'Saskatchewan',
    'YT':'Yukon',
    }
df['state_province'] = df['state_province'].replace(province_map)

df['country'] = 'Canada'
df['country'] = np.where(df['state_province'].isnull(),
    'United States of America', df['country'])

# standardize dataframe structure
df['elevation'] = np.NaN
df['water_id'] = ''
df['water_name'] = ''
df['water_mrt'] = np.NaN
df['water_volume'] = 'np.NaN'
df['catchment_id'] = ''
df['catchment_area'] = np.NaN
df['agency'] = 'Environment and Climate Change Canada'
df['database'] = 'ECCC'

# standardize data
df = set_type.sites(df)

# drop duplicates
df = df.drop_duplicates(subset='site_id', keep='first')

print('Exporting data...')

df.to_csv('intermediate_sites_eccc.csv', index=False, encoding='UTF-8')

print('Done!')
