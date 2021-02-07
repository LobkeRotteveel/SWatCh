""" Clean site data from Waterbase

Script to clean site data obtained from the European Environment Agency
Waterbase.
Data source: https://www.eea.europa.eu/data-and-maps/data/waterbase-water-
             quality-2

Approach:
    * Extract data
    * Standardize column names
    * Standardize naming conventions
    * Standardize dataframe structure
    * Standardize data types
    * Standardize below detection limit notation
    * Standardize units
    * Standardize float data
    * Remove impossible values
    * Remove duplicates (keeping first occurence)
    * Extract method data
    * Finalize site data

Assumptions:
    * depth is measured in metres
    * detection limits are in the same units as measured values

Notes:
    * Method reference columns are omitted because there are hundreds of
      sources per one site-parameter combination for the same date range.

Input:
    * raw_samples_waterbase.csv
    * intermediate_sites_waterbase.csc

Output:
    * cleaned_sites_waterbase.csv
    * cleaned_samples_waterbase.csv
    * cleaned_methods_waterbase.csv
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
from swatch_utils import set_type
from swatch_utils import convert
from swatch_utils import extract
from swatch_utils import check


print('Importing data...')

# import data
chunked = pd.read_csv('raw_samples_waterbase.csv', dtype=str,
    usecols= [
    'monitoringSiteIdentifier',
    'parameterWaterBodyCategory',
    'observedPropertyDeterminandCode',
    'procedureAnalysedFraction',
    'procedureAnalysedMedia',
    'resultUom',
    'phenomenonTimeSamplingDate',
    'resultObservedValue',
    'resultQualityObservedValueBelowLOQ',
    'procedureLOQValue',
    'procedureAnalyticalMethod',
    'parameterSampleDepth',
    'resultObservationStatus',
    'Remarks',
    'metadata_observationStatus',
    'metadata_statements',
    ], parse_dates=['phenomenonTimeSamplingDate'], chunksize=10000000)

sites = pd.read_csv('intermediate_sites_waterbase.csv', dtype=str)


print('Cleaning data...')

for i, df in tqdm(enumerate(chunked)):

    # extract data - selected sites
    selected_sites = sites['site_id'].unique()
    df['monitoringSiteIdentifier'] = df['monitoringSiteIdentifier'].str.lower()
    df = df[df['monitoringSiteIdentifier'].isin(selected_sites)]

    # extract data - site type
    df = df[df['parameterWaterBodyCategory'].isin(['RW','LW'])]

    # extract data - fractions
    df = df[df['procedureAnalysedFraction'] != 'SPM']

    # extract data - parameters
    params_det = [
    'CAS_7429-90-5', # Al
    'CAS_7439-89-6,' # Fe
    'CAS_7440-70-2', # Ca
    'CAS_7439-95-4', # Mg
    'CAS_7440-09-7', # K
    'CAS_7440-23-5', # Na
    'CAS_16887-00-6', # Cl
    'CAS_18785-72-3', # SO4
    'CAS_14797-65-0', # NO2
    'CAS_14797-55-8', # NO3
    'CAS_14265-44-2', # PO4
    'CAS_7723-14-0', # P (total)
    'CAS_16984-48-8', # F
    'CAS_14798-03-9', # NH4
    'EEA_3133-05-9', # DOC
    'EEA_3133-06-0', # TOC
    'EEA_3152-01-0', # pH
    'EEA_3121-01-5', # temperature
    ]
    df = df[df['observedPropertyDeterminandCode'].isin(params_det)]

    # extract data - media
    df = df[df['procedureAnalysedMedia'] == 'water']
    df = df.drop('procedureAnalysedMedia', axis='columns')

    # extract data - QAQC'ed data
    df = df[df['resultObservationStatus'].isin(['A',np.NaN])]
    df = df.drop('resultObservationStatus', axis='columns')

    df = df[df['Remarks'].isnull()]
    df = df[df['metadata_statements'].isnull()]
    df = df.drop(['Remarks','metadata_statements'], axis='columns')

    df = df[df['metadata_observationStatus'] != 'U']

    # standardize column names
    df = df.rename({
        'monitoringSiteIdentifier':'site_id',
        'parameterWaterBodyCategory':'site_type',
        'observedPropertyDeterminandCode':'parameter_name',
        'procedureAnalysedFraction':'parameter_fraction',
        'parameterSampleDepth':'depth',
        'procedureAnalyticalMethod':'method_id',
        'resultUom':'unit',
        'phenomenonTimeSamplingDate':'date',
        'resultObservedValue':'value',
        'resultQualityObservedValueBelowLOQ':'bdl_flag',
        'procedureLOQValue':'dl',
        'metadata_observationStatus':'status',
            }, axis='columns')

    # standardize naming conventions
    df['status'] = df['status'].replace({'V':'P','A':'V'})

    df['site_type'] = df['site_type'].replace({'RW':'river','LW':'lake'})

    param_map = {
        'CAS_7429-90-5':'Al',
        'CAS_7439-89-6,':'Fe',
        'CAS_7440-70-2':'Ca',
        'CAS_7439-95-4':'Mg',
        'CAS_7440-09-7':'K',
        'CAS_7440-23-5':'Na',
        'CAS_16887-00-6':'Cl',
        'CAS_18785-72-3':'SO4',
        'CAS_14797-65-0':'NO2',
        'CAS_14797-55-8':'NO3',
        'CAS_14265-44-2':'PO4',
        'CAS_7723-14-0':'P',
        'CAS_16984-48-8':'F',
        'CAS_14798-03-9':'NH4',
        'EEA_3133-05-9':'OC',
        'EEA_3133-06-0':'OC',
        'EEA_3152-01-0':'pH',
        'EEA_3121-01-5':'temperature',
        }
    df['parameter_name'] = df['parameter_name'].replace(param_map)

    df['parameter_fraction'] = np.where(df['parameter_name'].isin(['temperature','pH']),
        'unspecified',df['parameter_fraction'])
        # falsely recorded as disoslved or total

    unit_map = {
    'mg{no3}/l':'mg/l',
    'mg{nh4}/l':'mg/l',
    'mg{c}/l':'mg/l',
    '[ph]':'unit',
    'cel':'deg_c',
    'mg{no2}/l':'mg/l',
    }
    df['unit'] = df['unit'].str.lower()
    df['unit'] = df['unit'].replace(unit_map)


    # standardize dataframe structure
    df['database'] = 'Waterbase'
    df['timezone'] = ''

    # set data types
    df = set_type.samples(df)

    # standardize detection limit notation
    df['bdl_flag'] = df['bdl_flag'].replace({'0':np.NaN, '1':'<'})

    df['dl'] = pd.to_numeric(df['dl'])
    df['bdl_flag'] = np.where(df['value'] < df['dl'],
        '<', df['bdl_flag'])
    df['value'] = np.where(df['value'] < df['dl'],
        df['dl'], df['value'])
    df = df.drop('dl', axis='columns')


    # standardize units
    df = df.groupby(['parameter_name','unit']).filter(check.units)
    df = df.groupby(['unit','parameter_name']).apply(convert.chemistry,
        param_col='parameter_name',
        unit_col='unit',
        value_col='value')

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

    # export sample data
    if i == 0:
        df.to_csv('intermediate_samples_waterbase.csv',
            index=False, encoding='utf-8', mode='w', header=True)
    else:
        df.to_csv('intermediate_samples_waterbase.csv',
            index=False, encoding='utf-8', mode='a', header=False)


print('Finalizing data...')

# append chemistry and discharge data
df_final = pd.read_csv('intermediate_samples_waterbase.csv', low_memory=False,
    parse_dates=['date'])
# finalize selected sites
site_types = df_final[['site_id','site_type']]
site_types = site_types.drop_duplicates(subset=['site_id','site_type'])
sites_selected = site_types['site_id'].astype(str).unique()
sites['site_id'] = sites['site_id'].astype(str)
sites = sites[sites['site_id'].isin(sites_selected)]
sites = sites.merge(site_types, on='site_id', how='inner')
df_final = df_final.drop('site_type', axis='columns')
sites.to_csv('cleaned_sites_waterbase.csv', index=False, encoding='utf-8')

# extract methods
methods = extract.methods(df_final,
    site_col ='site_id',
    param_col = 'parameter_name',
    frac_col = 'parameter_fraction',
    date_col = 'date',
    method_col = 'method_id')

methods['database'] = 'Waterbase'
methods['method_name'] = ''
methods['method_type'] = ''
methods['method_agency'] = ''
methods['method_agency_number'] = ''
methods['method_description'] = ''
methods['method_reference'] = ''

methods = set_type.methods(methods)

methods.to_csv('cleaned_methods_waterbase.csv', index=False, encoding='utf-8')
df_final.to_csv('cleaned_samples_waterbase.csv', index=False, encoding='utf-8')

print('Done!')
