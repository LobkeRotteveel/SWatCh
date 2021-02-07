""" Clean site data from GloRiCh

Script to clean site data obtained from the Global River Chemistry Database.
Data source: https://doi.pangaea.de/10.1594/PANGAEA.902360

Approach:
    * Combine data
    * Standardize column names
    * Standardize naming conventions
    * Standardize units
    * Standardize dataframe structure
    * Standardize data
    * Remove dupicates (keeping first occurence)

Assumptions:
    * State named "AK" in Canada is Alaska
    * State named "NW" in Canada is Northwest Territories
    * State named "BC" in Canada is British Columbia

Notes:
    * There is a lot of really great information in the GloRiCh catchment
      data file. These data are not applicable for the GloFAD project, but
      may be highly useful for other projects.

Input:
    * raw_sites_glorich.csv

Output:
    * intermediate_sites_glorich.csv
"""

import pandas as pd
import numpy as np
from swatch_utils import set_type


print('Importing data...')

df = pd.read_csv('raw_sites_glorich.csv', sep=',', encoding='ISO-8859-1',
    usecols=[
    'STAT_ID',
    'STATION_NAME',
    'STATION_ID_ORIG',
    'Country',
    'State',
    'Latitude',
    'Longitude',
    'CoordinateSystem',
    ])
df_catch = pd.read_csv('raw_catchments_glorich.csv', sep=',',
    encoding='ISO-8859-1', usecols=[
    'STAT_ID',
    'Catch_ID',
    'Shape_Area',
    ])


print('Cleaning data...')

# combine data
df = df.merge(df_catch, how='left', on='STAT_ID')

# standardize column names
df.columns = df.columns.str.lower()
df = df.rename({
    'stat_id':'site_id_glorich',
    'station_name':'site_name',
    'station_id_orig':'site_id',
    'state':'state_province',
    'coordinatesystem':'coordinate_system',
    'catch_id':'catchment_id',
    'shape_area':'catchment_area',
    }, axis='columns')

# standardize naming conventions
df['site_id'] = np.where(df['site_id'].isnull(),
    df['site_id_glorich'], df['site_id'])

state_map = {
    'AB':'Alberta',
    'AK':'Alaska',
    'BC':'British Columbia',
    'NW':'Northwest Territories',
    'YT':'Yukon',
    'AL':'Alaska',
    'AR':'Arkansas',
    'AZ':'Arizona',
    'CA':'California',
    'CO':'Colorado',
    'CT':'Connecticut',
    'FL':'Florida',
    'GA':'Georgia',
    'GU':'Guam',
    'HI':'Hawaii',
    'IA':'Iowa',
    'ID':'Idaho',
    'IL':'Illinois',
    'IN':'Indiana',
    'KS':'Kansas',
    'KY':'Kentucky',
    'LA':'Louisiana',
    'MA':'Massachusetts',
    'MD':'Maryland',
    'ME':'Maine',
    'MI':'Michigan',
    'MN':'Minnesota',
    'MO':'Missouri',
    'MS':'Mississippi',
    'MT':'Montana',
    'NC':'North Carolina',
    'ND':'North Dakota',
    'NE':'Nebraska',
    'NH':'New Hampshire',
    'NJ':'New Jersey',
    'NM':'New Mexico',
    'NV':'Nevada',
    'NY':'New York',
    'OH':'Ohio',
    'OK':'Oklahoma',
    'OR':'Oregon',
    'PA':'Pennsylvania',
    'PR':'Puerto Rico',
    'RI':'Rhode Island',
    'SC':'South Carolina',
    'SD':'South Dakota',
    'TN':'Tennessee',
    'TX':'Texas',
    'UT':'Utah',
    'VA':'Virginia',
    'VT':'Vermont',
    'WA':'Washington',
    'WI':'Wisconsin',
    'WV':'West Virginia',
    'WY':'Wyoming',
}
df['state_province'] = df['state_province'].replace(state_map)

coord_map = {
    'NA1983':'nad83',
    'WGS 1984':'wgs84',
    'WGS1984':'wgs84',
    'WGS84':'wgs84',
    'Gauss\x96Krüger, Zone 4, DHDN':'dhdn_gk_zone4',
    'ERTS_UTM, Zone 33N, 7 digits':'etrs89_zone33_7dig',
    'British National Grid':'bng',
    'NatGrid NZ':'nzng',
    'National Grid Australia':'ang_zone55',
    np.nan:'unknown',
}
df['coordinate_system'] = df['coordinate_system'].replace(coord_map)

# standardize units
df['catchment_area'] = df['catchment_area'] * 0.000001 # m2 to km2

# standardize dataframe structure
df['site_type'] = 'river'
df['elevation'] = np.NaN
df['water_id'] = ''
df['water_name'] = ''
df['water_mrt'] = np.NaN
df['water_volume'] = np.NaN
df['catchment_name'] = ''
df['agency'] = np.NaN
df['database'] = 'GloRiCh'

# standardize data
df = set_type.sites(df)

# drop duplicates
df = df.drop_duplicates(subset='site_id', keep='first')


print('Exporting data...')

df.to_csv('intermediate_sites_glorich.csv', index=False, encoding='utf-8')

print('Done!')
