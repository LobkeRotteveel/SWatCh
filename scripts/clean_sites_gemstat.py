""" Clean site data from GEMStat

Script to clean site data obtained from the International Centre for Water
Resources and Global Change's Global Water Quality database and information
system (GEMStat).
Data source: https://gemstat.org/custom-data-request/

    * Extract data
    * Standardize column names
    * Standardize naming conventions
    * Standardize units
    * Standardize dataframe structure
    * Standardize data
    * Remove dupicates (keeping first occurence)

Assumptions:
    * Site elevation is in metres (m)
    * Catchment area is in square kilometres (km2)

Input:
    * raw_metadata_gemstat.csv

Output:
    * intermediate_sites_gemstat.csv
"""

import pandas as pd
import numpy as np
from swatch_utils import set_type


print('Importing data...')

df = pd.read_excel('raw_metadata_gemstat.xls', sheet_name='Station_Metadata',
    usecols=[
    'GEMS Station Number',
    'Local Station Number',
    'Country Name',
    'Water Type',
    'Station Identifier',
    'Water Body Name',
    'Main Basin',
    'Upstream Basin Area',
    'Elevation',
    'Responsible Collection Agency',
    'Latitude',
    'Longitude',
    'Lake Volume',
    'Average Retention',
    ])


print('Cleaning data...')

# extract data - site type
df = df[df['Water Type'] != 'Groundwater station']

# standardize column names
df = df.rename({
    'GEMS Station Number':'site_id_gemstat',
    'Local Station Number':'site_id',
    'Country Name':'country',
    'Water Type':'site_type',
    'Station Identifier':'site_name',
    'Water Body Name':'water_name',
    'Main Basin':'catchment_name',
    'Upstream Basin Area':'catchment_area',
    'Elevation':'elevation',
    'Responsible Collection Agency':'agency',
    'Latitude':'latitude',
    'Longitude':'longitude',
    'Lake Volume':'water_volume',
    'Average Retention':'water_mrt',
    }, axis='columns')

# standardize naming conventions
type_map = {
    'River station':'river',
    'Reservoir station':'reservoir',
    'Lake station':'lake',
    }
df['site_type'] = df['site_type'].replace(type_map)

df['site_id'] = np.where(df['site_id'].isnull(),
    df['site_id_gemstat'], df['site_id'])

# standardize units
df['water_volume'] = df['water_volume'] / 1000000000 # km3 to m3

# standardize dataframe structure
df['coordinate_system'] = 'wgs84'
df['state_province'] = ''
df['water_id'] = ''
df['catchment_id'] = ''
df['database'] = 'GEMStat'

# standardize data
df = set_type.sites(df)

# drop duplicates
df = df.drop_duplicates(subset='site_id', keep='first')


print('Exporting data...')

df.to_csv('intermediate_sites_gemstat.csv', index=False, encoding='utf-8')

print('Done!')
