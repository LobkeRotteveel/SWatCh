"""Clean samples data from McMurdo

Script to clean sample data obtained from the McMurdo Dry Valleys McMurdo Dry
Valleys Long Term Ecological Research Network.
Data source: http://mcm.lternet.edu/power-search/data-set

Approach:
    * Extract data
    * Combine data
    * Standardize column names
    * Standardize dataframe structure
    * Standardize naming conventions
    * Standardize data types
    * Standardize units
    * Standardize float data
    * Remove impossible values
    * Remove duplicates (keeping first occurence)
    * Extract method data
    * Extract site data

Assumptions:
    * Text in "Note" columns indicates some issue with data or sample
      collection; assume these data are invalid and omit.
    * Since all other data are dissolved fractions, assume that NH4, NO3, and
      NO2 are also reported as dissolved.

Notes:
    * No site data are available; manually obtain site data after running this
      script.

Input:
    * raw_samples_mcmurdo_[sample_type].csv

Output:
    * intermediate_sites_mcmurdo.csv
    * cleaned_samples_mcmurdo.csv
    * cleaned_methods_mcmurdo.csv
"""

import pandas as pd
import numpy as np
from swatch_utils import restructure
from swatch_utils import set_type
from swatch_utils import convert
from swatch_utils import extract
from swatch_utils import check

print('Importing data...')

lake_doc = pd.read_csv('raw_samples_mcmurdo_lake_doc.csv', dtype=str,
    usecols={
    'LIMNO_RUN',
    'LOCATION NAME',
    'LOCATION CODE',
    'DATE_TIME',
    'DEPTH (m)',
    'DOC (mg/L)',
    'DOC Comments',
    }, parse_dates=['DATE_TIME'])

# extract data
lake_doc = lake_doc[lake_doc['DOC Comments'].isnull()]
lake_doc = lake_doc.drop('DOC Comments', axis='columns')


lake_ion = pd.read_csv('raw_samples_mcmurdo_lake_ion.csv', dtype=str,
    skiprows=range(0,27), encoding='iso-8859-1', usecols={
    'LIMNO_RUN',
    'LOCATION NAME',
    'LOCATION CODE',
    'DATE_TIME',
    'Depth (m)',
    'Sample Comments',
    'Na (mg/L)',
    'Na COMMENTS',
    'K (mg/L)',
    'K COMMENTS',
    'Mg (mg/L)',
    'Mg COMMENTS',
    'Ca (mg/L)',
    'Ca COMMENTS',
    'F (mg/L)',
    'F COMMENTS',
    'Cl (mg/L)',
    'Cl COMMENTS',
    'SO4 (mg/L)',
    'SO4 COMMENTS',
    }, parse_dates=['DATE_TIME'])

# extract data
lake_ion = lake_ion[lake_ion['Sample Comments'].isnull()]
lake_ion = lake_ion.drop('Sample Comments', axis='columns')

lake_ion_comments = [
    'Na COMMENTS',
    'K COMMENTS',
    'Mg COMMENTS',
    'Ca COMMENTS',
    'F COMMENTS',
    'Cl COMMENTS',
    'SO4 COMMENTS',
    ]
lake_ion_cols = [
    'Na (mg/L)',
    'K (mg/L)',
    'Mg (mg/L)',
    'Ca (mg/L)',
    'F (mg/L)',
    'Cl (mg/L)',
    'SO4 (mg/L)',
    ]

for comment, col in zip(lake_ion_comments, lake_ion_cols):
    lake_ion[col] = np.where(lake_ion[comment].isnull(),
        lake_ion[col], np.NaN)
    lake_ion = lake_ion.drop(comment, axis='columns')


lake_nut = pd.read_csv('raw_samples_mcmurdo_lake_nutrient.csv', dtype=str,
    skiprows=range(0,26), encoding='iso-8859-1',
    usecols={
    'LIMNO_RUN',
    'LOCATION NAME',
    'LOCATION CODE',
    'DATE_TIME',
    'DEPTH (m)',
    'NH4 (uM)',
    'NH4 COMMENTS',
    'NO2 (uM)',
    'NO2 COMMENTS',
    'NO3 (uM)',
    'NO3 COMMENTS',
    'SRP (uM)',
    'SRP COMMENTS',
    }, parse_dates=['DATE_TIME'])

# extract data
lake_nut_comments = [
    'NH4 COMMENTS',
    'NO2 COMMENTS',
    'NO3 COMMENTS',
    'SRP COMMENTS',
    ]
lake_nut_cols = [
    'NH4 (uM)',
    'NO2 (uM)',
    'NO3 (uM)',
    'SRP (uM)',
    ]

for comment, col in zip(lake_nut_comments, lake_nut_cols):
    lake_nut[col] = np.where(lake_nut[comment].isnull(),
        lake_nut[col], np.NaN)
    lake_nut = lake_nut.drop(comment, axis='columns')


lake_ph = pd.read_csv('raw_samples_mcmurdo_lake_ph.csv', dtype=str,
    usecols=[
    'LIMNO_RUN',
    'LOCATION NAME',
    'LOCATION CODE',
    'DATE_TIME',
    'DEPTH (m)',
    'pH',
    'pH COMMENTS',
    ], parse_dates=['DATE_TIME'])

# extract data
lake_ph = lake_ph[lake_ph['pH COMMENTS'].isnull()]
lake_ph = lake_ph.drop('pH COMMENTS', axis='columns')


stream_doc = pd.read_csv('raw_samples_mcmurdo_stream_doc.csv', dtype=str,
    skiprows=range(0,26), usecols=[
    'STRMGAGEID',
    'LOCATION',
    'DATE_TIME',
    'DOC (mg/L)',
    'DOC Comments',
    ], parse_dates=['DATE_TIME'])

# extract data
stream_doc = stream_doc[stream_doc['DOC Comments'].isnull()]
stream_doc = stream_doc.drop('DOC Comments', axis='columns')


stream_ion = pd.read_csv('raw_samples_mcmurdo_stream_ion.csv', dtype=str,
    skiprows=range(0,26), usecols=[
    'STRMGAGEID',
    'LOCATION',
    'DATE_TIME',
    'Na (mg/L)',
    'Na COMMENTS',
    'K (mg/L)',
    'K COMMENTS',
    'Mg (mg/L)',
    'Mg COMMENTS',
    'Ca (mg/L)',
    'Ca COMMENTS',
    'F (mg/L)',
    'F COMMENTS',
    'Cl (mg/L)',
    'Cl COMMENTS',
    'SO4 (mg/L)',
    'SO4 COMMENTS',
    'Sample Comments',
    ], parse_dates=['DATE_TIME'])

# extract data
stream_ion = stream_ion[stream_ion['Sample Comments'].isnull()]
stream_ion = stream_ion.drop('Sample Comments', axis='columns')

stream_ion_comments = [
    'Na COMMENTS',
    'K COMMENTS',
    'Mg COMMENTS',
    'Ca COMMENTS',
    'F COMMENTS',
    'Cl COMMENTS',
    'SO4 COMMENTS',
    ]
stream_ion_cols = [
    'Na (mg/L)',
    'K (mg/L)',
    'Mg (mg/L)',
    'Ca (mg/L)',
    'F (mg/L)',
    'Cl (mg/L)',
    'SO4 (mg/L)',
    ]

for comment, col in zip(stream_ion_comments, stream_ion_cols):
    stream_ion[col] = np.where(stream_ion[comment].isnull(),
        stream_ion[col], np.NaN)
    stream_ion = stream_ion.drop(comment, axis='columns')


stream_nut = pd.read_csv('raw_samples_mcmurdo_stream_nutrient.csv',
    dtype=str, skiprows=range(0,27), encoding='iso-8859-1', usecols=[
    'STRMGAGEID',
    'LOCATION',
    'DATE_TIME',
    'N-NO3 (ug/L)',
    'N-NO3 COMMENTS',
    'N-NO2 (ug/L)',
    'N-NO2 COMMENTS',
    'N-NH4 (ug/L)',
    'N-NH4 COMMENTS',
    'SRP (ug/L)',
    'SRP COMMENTS',
    'SAMPLE_COMMENTS',
    'ANALYTICAL_METHOD',
    ], parse_dates=['DATE_TIME'])

# extract data
stream_nut = stream_nut[stream_nut['SAMPLE_COMMENTS'].isnull()]
stream_nut = stream_nut.drop('SAMPLE_COMMENTS', axis='columns')

stream_nut_comments = [
    'N-NO3 COMMENTS',
    'N-NO2 COMMENTS',
    'N-NH4 COMMENTS',
    'SRP COMMENTS',
    ]
stream_nut_cols = [
    'N-NO3 (ug/L)',
    'N-NO2 (ug/L)',
    'N-NH4 (ug/L)',
    'SRP (ug/L)',
    ]

for comment, col in zip(stream_nut_comments, stream_nut_cols):
    stream_nut[col] = np.where(stream_nut[comment].isnull(),
        stream_nut[col], np.NaN)
    stream_nut = stream_nut.drop(comment, axis='columns')


print('Cleaning data...')

# combine data
lake_dfs = [
    lake_doc,
    lake_ion,
    lake_nut,
    lake_ph,
    ]
stream_dfs = [
    stream_doc,
    stream_ion,
    stream_nut,
    ]
df = pd.DataFrame()

for frame in lake_dfs:
    frame['site_type'] = 'lake'
    df = df.append(frame, ignore_index=True)
for frame in stream_dfs:
    frame['site_type'] = 'stream'
    df = df.append(frame, ignore_index=True)
df = df.reset_index(drop=True)

# standardize column names
df['DEPTH (m)'] = np.where(df['DEPTH (m)'].isnull(),
    df['Depth (m)'], df['DEPTH (m)'])
df['LOCATION CODE'] = np.where(df['LOCATION CODE'].isnull(),
    df['STRMGAGEID'], df['LOCATION CODE'])
df['LOCATION NAME'] = np.where(df['LOCATION NAME'].isnull(),
    df['LOCATION'], df['LOCATION NAME'])
df = df.drop(['Depth (m)', 'STRMGAGEID', 'LOCATION'], axis='columns')

df = df.rename({
    'LIMNO_RUN':'water_id',
    'LOCATION NAME':'site_name',
    'LOCATION CODE':'site_id',
    'DATE_TIME':'date',
    'DEPTH (m)':'depth',
    'DOC (mg/L)':'OC',
    'Na (mg/L)':'Na',
    'K (mg/L)':'K',
    'Mg (mg/L)':'Mg',
    'Ca (mg/L)':'Ca',
    'F (mg/L)':'F',
    'Cl (mg/L)':'Cl',
    'SO4 (mg/L)':'SO4',
    'NH4 (uM)':'NH4',
    'NO2 (uM)':'NO2',
    'NO3 (uM)':'NO3',
    'SRP (uM)':'PO4 umol/l',
    'N-NO3 (ug/L)':'NO3 as N',
    'N-NO2 (ug/L)':'NO4 as N',
    'N-NH4 (ug/L)':'NH4 as N',
    'SRP (ug/L)':'PO4',
    'pH':'pH',
    'ANALYTICAL_METHOD':'method_id',
    }, axis='columns')

# standardize dataframe structure
df = restructure.df(df,
    ['water_id',
    'site_id',
    'site_name',
    'site_type',
    'date',
    'depth',
    'method_id'
    ])

df['unit'] = df['param']
df['parameter_fraction'] = 'dissolved'
df = df.rename({'param':'parameter_name'}, axis='columns')

df['timezone'] = ''
df['bdl_flag'] = ''
df['database'] = 'McMurdo'
df['status'] = ''

# standardize naming conventions
unit_map = {
    'OC':'mg/l',
    'Na':'mg/l',
    'K':'mg/l',
    'Mg':'mg/l',
    'Ca':'mg/l',
    'F':'mg/l',
    'Cl':'mg/l',
    'SO4':'mg/l',
    'NH4':'umol/l',
    'NO2':'umol/l',
    'NO3':'umol/l',
    'PO4 umol/l':'umol/l',
    'NH4 as N':'ug/l',
    'NO2 as N':'ug/l',
    'NO3 as N':'ug/l',
    'PO4':'ug/l',
    'pH':'unit',
    }
df['unit'] = df['unit'].replace(unit_map)

df['parameter_fraction'] = np.where(df['parameter_name'] == 'pH',
    'unspecified', df['parameter_fraction'])
df['parameter_name'] = df['parameter_name'].replace({'PO4 umol/l':'PO4'})

# standardize data types
df = set_type.samples(df)

# standardize units
df = df.groupby(['parameter_name','unit']).filter(check.units)
df.index = pd.RangeIndex(len(df.index))
df = df.groupby(['parameter_name','unit']).apply(convert.chemistry,
    param_col='parameter_name',
    unit_col='unit',
    value_col='value')

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

methods = extract.methods(df,
    site_col ='site_id',
    param_col = 'parameter_name',
    frac_col = 'parameter_fraction',
    date_col = 'date',
    method_col = 'method_id')

methods_temp = df[['site_id', 'site_type']]
methods = methods.merge(methods_temp, on='site_id', how='left')

methods['method_name'] = ''
methods['method_reference'] = ''

chrom_list = [
    'OC',
    'Na',
    'K',
    'Mg',
    'Ca',
    'F',
    'Cl',
    'SO4',
    ]
lachat_list = [
    'NH4',
    'NO2',
    'NO3',
    'PO4',
    ]

for param in chrom_list:
    methods['method_name'] = np.where(methods['parameter_name'] == param,
        'ion chromatography by DX-300 ion chromatographic system',
        methods['method_name'])
for param in lachat_list:
    methods['method_name'] = np.where(methods['parameter_name'] == param,
        'analyzed in Crary Lab in McMurdo using a Lachat Autoanalyzer',
        methods['method_name'])

for param in chrom_list:
    methods['method_reference'] = np.where((methods['parameter_name'] == param) & (methods['site_type'] == 'lake'),
        'http://mcm.lternet.edu/content/limnological-chemistry-ion-concentrations-and-silicon',
        methods['method_reference'])
for param in lachat_list:
    methods['method_reference'] = np.where((methods['parameter_name'] == param) & (methods['site_type'] == 'lake'),
        'http://mcm.lternet.edu/content/macronutrient-concentrations-nh4-no3-no2-po4-lakes',
        methods['method_reference'])

for param in chrom_list:
    methods['method_reference'] = np.where((methods['parameter_name'] == param) & (methods['site_type'] == 'stream'),
        'http://mcm.lternet.edu/content/stream-chemistry-and-ion-concentrations',
        methods['method_reference'])
for param in lachat_list:
    methods['method_reference'] = np.where((methods['parameter_name'] == param) & (methods['site_type'] == 'stream'),
        'http://mcm.lternet.edu/content/stream-nutrients-nitrate-nitrite-ammonium-reactive-phosphorus',
        methods['method_reference'])
methods['method_reference'] = np.where((methods['parameter_name'] == 'oc') & (methods['site_type'] == 'stream'),
    'http://mcm.lternet.edu/content/stream-chemistry-dissolved-organic-carbon',
    methods['method_reference'])

methods = methods.drop('site_type', axis='columns')

methods['database'] = 'McMurdo'
methods['method_type'] = ''
methods['method_agency'] = ''
methods['method_agency_number'] = ''
methods['method_description'] = ''

methods = set_type.methods(methods)

# extract site information
sites = df[['site_id','site_name','site_type','water_id']]
df = df.drop(['site_name','site_type','water_id'], axis='columns')
sites = sites.drop_duplicates(subset='site_id', keep='first')


print('Exporting data...')

df.to_csv('cleaned_samples_mcmurdo.csv',
    index=False, encoding='utf-8')
sites.to_csv('intermediate_sites_mcmurdo.csv',
    index=False, encoding='utf-8')
methods.to_csv('cleaned_methods_mcmurdo.csv',
    index=False, encoding='utf-8')

print('Done!')
