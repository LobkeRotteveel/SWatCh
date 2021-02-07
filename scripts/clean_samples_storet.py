""" Clean sample data from NWQMC

Script to clean sample data obtained from the National Water Quality
Monitoring Council Water Quality Database.
Data source: https://www.waterqualitydata.us/portal/

Approach:
    * Extract data
    * Standardize column names
    * Standardize naming conventions
    * Standardize dataframe structure
    * Standardize data types
    * Standardize below detection limit notation
    * Standardize units
    * Standrdize float data
    * Remove impossible values
    * Remove duplicates (keeping first occurrence)
    * Extract method data
    * Finalize site data

Assumptions:
    * Average values are removed; therefore, start and end dates are the
      same and end dates/times are not required.
    * The ResultDepthHeightMeasure/MeasureValue column contains the depth at
      which samples were collected.

Notes:
    * Water in mixing zones is not included; this is and area where water
      from an effluent point source, and pollutant levels are permitted to be
      higher than in the rest of the water body.

Input:
    * raw_samples_nwqmc.csv
    * intermediate_sites_nwqmc.csc

Output:
    * cleaned_sites_nwqmc.csv
    * cleaned_samples_nwqmc.csv
    * cleaned_methods_nwqmc.csv
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
from swatch_utils import set_type
from swatch_utils import convert
from swatch_utils import extract
from swatch_utils import check


print('Importing data...')

chunked = pd.read_csv('raw_samples_nwqmc.csv', usecols=[
    'ActivityTypeCode',
    'ActivityMediaName',
    'ActivityMediaSubdivisionName',
    'ActivityStartDate',
    'ActivityStartTime/Time',
    'ActivityStartTime/TimeZoneCode',
    'MonitoringLocationIdentifier',
    'ActivityCommentText',
    'ResultDetectionConditionText',
    'CharacteristicName',
    'ResultSampleFractionText',
    'ResultMeasureValue',
    'ResultMeasure/MeasureUnitCode',
    'MeasureQualifierCode',
    'ResultStatusIdentifier',
    'ResultValueTypeName',
    'ResultCommentText',
    'ResultDepthHeightMeasure/MeasureValue',
    'ResultDepthHeightMeasure/MeasureUnitCode',
    'ResultAnalyticalMethod/MethodIdentifierContext',
    'ResultAnalyticalMethod/MethodIdentifier',
    'ResultAnalyticalMethod/MethodName',
    'MethodDescriptionText',
    'ResultLaboratoryCommentText',
    'DetectionQuantitationLimitMeasure/MeasureValue',
    'DetectionQuantitationLimitMeasure/MeasureUnitCode',
    'ProviderName',
    ], dtype=str,
    parse_dates=['ActivityStartDate'], encoding='utf-8', engine='python',
    error_bad_lines=False, chunksize=1000000)

sites = pd.read_csv('intermediate_sites_nwqmc.csv', dtype=str)


print('Cleaning data...')

for i, df in tqdm(enumerate(chunked)):

    # extract data - sites
    sites['site_id'] = sites['site_id'].str.lower()
    sites_list = sites['site_id'].unique()
    df['MonitoringLocationIdentifier'] = df['MonitoringLocationIdentifier'].str.lower()
    df = df[df['MonitoringLocationIdentifier'].isin(sites_list)]

    # extract data - media
    df = df[df['ActivityMediaName'] == 'Water']
    df = df.drop('ActivityMediaName', axis=('columns'))

    df = df[df['ActivityMediaSubdivisionName'] == 'Surface Water']
    df = df.drop('ActivityMediaSubdivisionName', axis='columns')

    df = df[df['ResultSampleFractionText'] != 'Bed Sediment']

    # extract data - samples
    activity_list = [
        'Field Msr/Obs',
        'Field Msr/Obs-Habitat Assessment',
        'Field Msr/Obs-Incidental',
        'Field Msr/Obs-Portable Data Logger',
        'Quality Control Field Replicate Habitat Assessment',
        'Quality Control Field Replicate Msr/Obs',
        'Quality Control Field Replicate Portable Data Logger',
        'Quality Control Sample-Field Replicate',
        'Sample-Routine',
        'Sample-Routine Resample',
        np.nan
        ]
    df = df[df['ActivityTypeCode'].isin(activity_list)]
    df = df.drop('ActivityTypeCode', axis='columns')

    df = df[df['ResultValueTypeName'].isin(['Actual', np.nan])]
    df = df.drop('ResultValueTypeName', axis='columns')

    qualifier_list = [
        '*', # sample was warm
        'BMDL', # below method detection limit
        'BQL', # below quantitation limit
        'BRL', # below reporting limit
        'CON', # confirmed value
        'DL', # below method detection limit
        'DU', # below quantitation limit
        'GG', # between detection and quantitation limits
        'IDL', # below instrument detection limit
        'K', # below detection limit
        'KRMDL', # below method detection limit
        'KRPQL', # below reporting limit
        'L', # lowest reporting limit used
        'NA', # not applicable
        'OA3', # outlier, will be removed later
        'OS3', # outlier, will be removed later
        'OUT', # outlier, will be removed later
        'RNAF', # result not affected by QAQC issue
        'RSM', # sample verified by re-run
        'U', # below detection limit
        'UH', # below detection limit
        'Z', # value verified by re-run
        np.nan,
        ]
    df = df[df['MeasureQualifierCode'].isin(qualifier_list)]
    df = df.drop('MeasureQualifierCode', axis='columns')

    # extract data - QAQC'ed data
    result_list = [
        'Accepted',
        'Final',
        'Validated',
        'Preliminary',
        'Provisional',
        'Raw',
        'Unreviewed',
        ]
    df = df[df['ResultStatusIdentifier'].isin(result_list)]

    # extract data - reliable data
    df = df[df['ActivityCommentText'].isnull()]
    df = df[df['ResultCommentText'].isnull()]
    df = df[df['ResultLaboratoryCommentText'].isnull()]
    df = df.drop([
        'ActivityCommentText',
        'ResultCommentText',
        'ResultLaboratoryCommentText'],
        axis='columns')

    condition_list = [
        'Above Operating Range', # above instrument opperating range
        'Detected Not Quantified', # measured value not reported
        'High Moisture', # fermetation allowed to occur
        'Not Present', # not detected and no detection limit reported
        'Not Reported', # no value recorded
        'Present Above Quantification Limit', # value too high to quanify
        'Reported in Raw Data (attached)', # attached time series data
        'Value Decensored', # recommended to remove value
        ]
    df = df[~df['ResultDetectionConditionText'].isin(condition_list)]

    # standardize column names - create bdl_flag column
    df['ResultDetectionConditionText'] = np.where(df['ResultDetectionConditionText'].isnull(),
        '', '<')

    # parameters
    df = df[~df['CharacteristicName'].isin([
        'Turbidity',
        'Turbidity Field',
        'Aluminum, Organic Monomeric (reactive aluminum)',
        'Flow',
        'Discharge, River/Stream',
        ])]

    # standardize column names
    df = df.rename({
        'ResultAnalyticalMethod/MethodIdentifierContext':'method_agency',
        'ResultAnalyticalMethod/MethodIdentifier':'method_id',
        'ResultAnalyticalMethod/MethodName':'method_name',
        'ActivityStartDate':'date',
        'ActivityStartTime/Time':'time',
        'ActivityStartTime/TimeZoneCode':'timezone',
        'MonitoringLocationIdentifier':'site_id',
        'ResultDetectionConditionText':'bdl_flag',
        'CharacteristicName':'parameter_name',
        'ResultSampleFractionText':'parameter_fraction',
        'ResultMeasureValue':'value',
        'ResultMeasure/MeasureUnitCode':'unit',
        'ResultDepthHeightMeasure/MeasureValue':'depth',
        'ResultDepthHeightMeasure/MeasureUnitCode':'depth_unit',
        'MethodDescriptionText':'method_reference',
        'DetectionQuantitationLimitMeasure/MeasureValue':'dl',
        'DetectionQuantitationLimitMeasure/MeasureUnitCode':'dl_unit',
        'ProviderName':'database',
        'ResultStatusIdentifier':'status',
        }, axis='columns')

    # standardize naming conventions
    status_map = {
    'Accepted':'V',
    'Final':'V',
    'Validated':'V',
    'Preliminary':'P',
    'Provisional':'P',
    'Raw':'P',
    'Unreviewed':'P',
    }
    df['status'] = df['status'].replace(status_map)


    df = df[~df['parameter_fraction'].isin([
        'Suspended',
        'Inorganic',
        'Non-Filterable (Particle)',
        'Organic',
        ])]

    param_frac_map = {
        'Total Recoverable':'recoverable',
        'None':'unspecified',
        'Filtered, lab':'dissolved',
        'Filtered, field':'total',
        'Filterable':'dissolved',
        'Unfiltered, field':'total',
        'Unfiltered':'total',
        'Dissolved':'dissolved',
        'Extractable':'extractable',
        'Field':'field',
        'Total':'total',
        np.nan:'unspecified',
        }
    df['parameter_fraction'] = df['parameter_fraction'].replace(param_frac_map)

    parameter_name_map = {
        'Nitrite':'NO2',
        'Magnesium':'Mg',
        'Temperature, water':'temperature',
        'Nitrate':'NO3',
        'Aluminum':'Al',
        'Iron':'Fe',
        'Calcium':'Ca',
        'Chloride':'Cl',
        'Sulfate':'SO4',
        'Phosphorus':'P',
        'Sodium':'Na',
        'Potassium':'K',
        'Fluoride':'F',
        }
    df['parameter_name'] = df['parameter_name'].replace(parameter_name_map)

    df['parameter_name'] = np.where(df['unit'] == 'mg/l as P',
        df['parameter_name'] + ' as P', df['parameter_name'])
    df['parameter_name'] = np.where(df['unit'] == 'mg/l as N',
        df['parameter_name'] + ' as N', df['parameter_name'])

    df['parameter_fraction'] = np.where((df['parameter_name'] == 'temperature') & (df['parameter_fraction'] == 'total'),
        'unspecified', df['parameter_fraction']) # temperature is falsely recorded as "total"

    df = df[df['unit'] != 'mg/l CaCO3']
    unit_map = {
        'deg c':'deg_c',
        'deg f':'deg_f',
        'std units':'unit',
        'mg/l as p':'mg/l',
        'mg/l as n':'mg/l',
        'mole/l':'mol/l',
        'mg/l po4':'mg/l',
        }
    unit_cols = [
        'unit',
        'depth_unit',
        'dl_unit',
        ]
    for col in unit_cols:
        df[col] = df[col].str.lower()
        df[col] = df[col].replace(unit_map)

    # standardize data types
    df['date'] = df['date'].astype(str) + ' ' + df['time'].astype(str)
    df = set_type.samples(df)
    df['dl'] = pd.to_numeric(df['dl'], errors='coerce')
    df['depth'] = pd.to_numeric(df['depth'], errors='coerce')

    # standardize units
    df = df.groupby(['parameter_name','unit']).filter(check.units)
    df.index = pd.RangeIndex(len(df.index))
    df = df.groupby(['parameter_name','unit']).apply(convert.chemistry,
        param_col='parameter_name',
        unit_col='unit',
        value_col='value')
    df = df.groupby(['parameter_name','dl_unit']).apply(convert.chemistry,
        param_col='parameter_name',
        unit_col='dl_unit',
        value_col='dl')
    df = df.drop('dl_unit', axis='columns')

    df['depth'] = np.where(df['depth_unit'] == 'ft',
        df['depth'] / 3.28084, df['depth'])
    df = df.drop('depth_unit', axis='columns')

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
    df.index = pd.RangeIndex(len(df.index))
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

    # finalize below detection limit notation
    df['bdl_flag'] = np.where(df['value'] < df['dl'],
        '<', df['bdl_flag'])
    df['value'] = np.where(df['value'] < df['dl'],
        df['dl'], df['value'])
    df = df.drop('dl', axis='columns')

    # export sample data
    if i == 0:
        df.to_csv('intermediate_samples_nwqmc.csv', index=False,
            encoding='utf-8', mode='w', header=True)
    else:
        df.to_csv('intermediate_samples_nwqmc.csv', index=False,
            encoding='utf-8', mode='a', header=False)


# finalize selected sites
df_final = pd.read_csv('intermediate_samples_nwqmc.csv')

sites_selected = df_final['site_id'].astype(str).unique()
sites['site_id'] = sites['site_id'].astype(str)
sites = sites[sites['site_id'].isin(sites_selected)]
sites.to_csv('cleaned_sites_nwqmc.csv', index=False, encoding='utf-8')

# extract methods
method_ref = df_final[['method_id','method_agency','method_name','method_reference']]
method_ref = method_ref.drop_duplicates(subset=[
    'method_id',
    'method_agency',
    'method_name',
    'method_reference',
    ])

methods = extract.methods(df_final,
    site_col ='site_id',
    param_col = 'parameter_name',
    frac_col = 'parameter_fraction',
    date_col = 'date',
    method_col = 'method_id')

methods = methods.merge(method_ref, on='method_id', how='left')

methods['database'] = 'NWQMC'
methods['method_type'] = ''
methods['method_agency_number'] = methods['method_id']
methods['method_description'] = ''

methods = set_type.methods(methods)

df_final = df_final.drop([
   'method_agency',
   'method_name',
   'method_reference',
    ], axis='columns')

methods.to_csv('cleaned_methods_nwqmc.csv', index=False, encoding='utf-8')
df_final.to_csv('cleaned_samples_nwqmc.csv', index=False, encoding='utf-8')

print('Done!')
