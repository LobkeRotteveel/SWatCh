""" Clean site data from NWQMC

Script to clean site data obtained from the National Water Quality
Monitoring Council Water Quality Database.
Data source: https://www.waterqualitydata.us/portal/

Approach:
    * Extract data
    * Standardize column names
    * Standardize naming conventions
    * Standardize units
    * Standardize dataframe structure
    * Standardize data
    * Remove dupicates (keeping first occurence)

Assumptions:
    * Horizontal and vertical datums are the same
    * Country codes are in ISO Alpha 2 format

Input:
    * raw_sites_nwqmc.csv

Output:
    * intermediate_sites_nwqmc.csv
"""

import re
import pandas as pd
import numpy as np
from swatch_utils import set_type


print('Importing data...')

df = pd.read_csv('raw_sites_nwqmc.csv', usecols=[
    'MonitoringLocationDescriptionText',
    'OrganizationFormalName',
    'MonitoringLocationIdentifier',
    'MonitoringLocationName',
    'MonitoringLocationTypeName',
    'DrainageAreaMeasure/MeasureValue',
    'DrainageAreaMeasure/MeasureUnitCode',
    'LatitudeMeasure',
    'LongitudeMeasure',
    'HorizontalCoordinateReferenceSystemDatumName',
    'CountryCode',
    'StateCode',
    'ProviderName',
    ], dtype=str, encoding='utf-8', engine='python', error_bad_lines=False,)


print('Cleaning data...')

# extract data - good quality sites
notes_list = df['MonitoringLocationDescriptionText'].unique()
poor_pattern = r'error|guess'
poor_list = []

for note in notes_list:
    if re.findall(poor_pattern, str(note)):
       poor_list.append(note)

df = df[~df['MonitoringLocationDescriptionText'].isin(poor_list)]
df = df.drop('MonitoringLocationDescriptionText', axis='columns')

# extract data - monitoring location type
type_list = [
    'BEACH Program Site-Channelized stream',
    'BEACH Program Site-Great Lake',
    'BEACH Program Site-Lake',
    'BEACH Program Site-River/Stream',
    'Canal Drainage',
    'Canal Irrigation',
    'Canal Transport',
    'Channelized Stream',
    'Constructed Diversion Dam',
    'Constructed Wetland',
    'Great Lake',
    'Lake',
    'Reservoir',
    'River/Stream',
    'River/Stream Ephemeral',
    'River/Stream Intermittent',
    'River/Stream Perennial',
    'Riverine Impoundment',
    'Wetland Riverine-Emergent',
    'Wetland Undifferentiated',
    ]
df = df[df['MonitoringLocationTypeName'].isin(type_list)]

# standardize column names
df = df.rename({
    'OrganizationFormalName':'agency',
    'MonitoringLocationIdentifier':'site_id',
    'MonitoringLocationName':'site_name',
    'MonitoringLocationTypeName':'site_type',
    'DrainageAreaMeasure/MeasureValue':'catchment_area',
    'DrainageAreaMeasure/MeasureUnitCode':'catchment_unit',
    'LatitudeMeasure':'latitude',
    'LongitudeMeasure':'longitude',
    'HorizontalCoordinateReferenceSystemDatumName':'coordinate_system',
    'CountryCode':'country',
    'StateCode':'state_province',
    'ProviderName':'database',
    }, axis='columns')

# standardize naming conventions
type_map = {
    'beach program site-channelized stream':'canal',
    'beach program site-great lake':'lake',
    'beach program site-lake':'lake',
    'beach program site-river/stream':'river',
    'canal drainage':'canal',
    'canal irrigation':'canal',
    'canal transport':'canal',
    'channelized stream':'canal',
    'constructed diversion dam':'impoundment',
    'constructed wetland':'wetland',
    'great lake':'lake',
    'river/stream':'river',
    'river/stream ephemeral':'stream',
    'river/stream intermittent':'stream',
    'river/stream perennial':'river',
    'riverine impoundment':'impoundment',
    'wetland riverine-emergent':'wetland',
    'wetland undifferentiated':'wetland',
    }
df['site_type'] = df['site_type'].str.lower()
df['site_type'] = df['site_type'].replace(type_map)

country_map = {
    'US':'USA',
    'CA':'Canada',
    'MX':'Mexico',
    'CV':'Cape Verde',
    'CN':'China',
    'IN':'India',
    '0A':'unknown (listed as 0A)',
    np.nan:'unknown',
}
df['country'] = df['country'].replace(country_map)

state_map = {
    19:'Iowa',
    96:'Lake Erie',
    97:'Niagara River and Lake Ontario',
    2:'Alaska',
    1:'Alabama',
    12:'Florida',
    13:'Georgia',
    28:'Mississippi',
    29:'Missouri',
    5:'Arkansas',
    40:'Oklahoma',
    4:'Arizona',
    32:'Nevada',
    6:'California',
    35:'New Mexico',
    8:'Colorado',
    49:'Utah',
    9:'Connecticut',
    25:'Massachusetts',
    45:'South Carolina',
    47:'Tennessee',
    51:'Virginia',
    66:'Guam',
    60:'American Samoa',
    15:'Hawaii',
    69:'Northern Mariana Islands',
    31:'Nebraska',
    30:'Montana',
    16:'Idaho',
    56:'Wyoming',
    41:'Oregon',
    17:'Illinois',
    18:'Indiana',
    20:'Kansas',
    39:'Ohio',
    21:'Kentucky',
    22:'Louisiana',
    44:'Rhode Island',
    50:'Vermont',
    34:'New Jersey',
    10:'Delaware',
    24:'Maryland',
    54:'West Virginia',
    11:'District of Columbia',
    37:'North Carolina',
    23:'Maine',
    26:'Michigan',
    27:'Minnesota',
    92:'Lake Michigan',
    55:'Wisconsin',
    95:'Palmyra Atoll',
    94:'St. Clair River, Detroit River, and Lake St. Clair',
    38:'North Dakota',
    93:'Lake Huron',
    46:'South Dakota',
    33:'New Hampshire',
    36:'New York',
    42:'Pennsylvania',
    48:'Texas',
    72:'Puerto Rico',
    53:'Washington',
    81:'Baker Island',
    0:'unknown (listed as 0)',
    3:'American Samoa',
    7:'Canal Zone',
    78:'Virgin Islands',
}
df['state_province'] = pd.to_numeric(df['state_province'])
df['state_province'] = df['state_province'].replace(state_map)

coord_map = {
    'unkwn':'unknown',
}
df['coordinate_system'] = df['coordinate_system'].str.lower()
df['coordinate_system'] = df['coordinate_system'].replace(coord_map)

# standardize units
df['catchment_area'] = pd.to_numeric(df['catchment_area'], errors='coerce')
df['catchment_area'] = np.where(df['catchment_unit'] == 'ha',
    df['catchment_area'] * 0.01, df['catchment_area']) # ha to km2
df['catchment_area'] = np.where(df['catchment_unit'] == 'sq mi',
    df['catchment_area'] * 2.589988, df['catchment_area']) # ha to km2
df = df.drop('catchment_unit', axis='columns')

# standardize dataframe structure
df['elevation'] = np.NaN
df['water_id'] = ''
df['water_name'] = ''
df['water_mrt'] = np.NaN
df['water_volume'] = np.NaN
df['catchment_id'] = ''
df['catchment_name'] = ''

# standardize data
df = set_type.sites(df)

# drop duplicates
df = df.drop_duplicates(subset='site_id', keep='first')


print('Exporting data...')

df.to_csv('intermediate_sites_nwqmc.csv', index=False, encoding='utf-8')

print('Done!')
