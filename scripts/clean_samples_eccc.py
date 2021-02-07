""" Clean sample data from ECCC

Script to clean sample data obtained from Environment and Climate Change
Canada National Long-term Water Quality Monitoring Database.
Data source: http://data.ec.gc.ca/data/substances/monitor/national-long-
             term-water-quality-monitoring-data/

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
    * Remove duplicates
    * Extract method data
    * Finalize site data

Assumptions:
    * Units listed as "ďż˝G/L" and "ÂľG/L" are ug/L, based on plausible
      values.

Notes:
    * Both preliminary and validated data are retained to maintain maximum
      sample size. QAQC on preliminary data is recommended before use.

Input:
    * raw_samples_eccc_[waterbody].csv
    * intermediate_sites_eccc.csv
    * raw_methods_eccc.csv

Output:
    * cleaned_sites_eccc.csv
    * cleaned_samples_eccc.csv
    * cleaned_methods_eccc.csv
"""

import os
import re
import pandas as pd
import numpy as np
from swatch_utils import set_type
from swatch_utils import convert
from swatch_utils import extract
from swatch_utils import check


print('Importing data...')

path = r'/mnt/boggart/Files/Documents/university/research/SWatCh/input datasets/eccc'
directory = os.listdir(path)

df = pd.DataFrame()

for file in directory:
    if 'raw_samples' in file:
        df_temp = pd.read_csv(file, sep=',', encoding='ISO-8859-2')
        df = df.append(df_temp, sort=True, ignore_index=True)

sites = pd.read_csv('intermediate_sites_eccc.csv', dtype=str)

method_ref = pd.read_csv('raw_methods_eccc.csv', dtype=str, usecols=[
    'VMV_Code',
    'National_Method_Code',
    'Method_Title',
    ], encoding='ISO-8859-2')

print('Cleaning data...')

# extract data - sites
sites_list = sites['site_id'].astype(str).unique()
df['SITE_NO'] = df['SITE_NO'].str.lower()
df = df[df['SITE_NO'].isin(sites_list)]

# extract data - columns
df = df.drop([
    'VARIABLE_FR'
    ], axis='columns')

# extract data - parameters
params_all = df['VARIABLE'].unique()
params_extract = r'ALUMINUM|IRON|CALCIUM|MAGNESIUM|POTASSIUM|SODIUM|^CHLORIDE|FLUORIDE|PHOSPHATE|^PH|CARBON|SULPHATE|NITRATE|NITRITE|AMMONIUM|TEMP'
params_select = []
# unwanted parameters which meet above string match
params_drop = [
'PHENANTHRENE',
'CARBON DIOXIDE FREE (CALC)',
'DISSOLVED NITRITE/NITRATE',
'NITRITE & NITRATE UNFILTERED',
'ENDOSULFAN SULPHATE TOTAL',
'PHORATE',
'PHOSMET TOTAL',
'CARBONACEOUS OXYGEN DEMAND BOD5',
'CARBON DISSOLVED INORGANIC',
'CARBON TOTAL INORGANIC',
'NITRITE AND NITRATE (AS N)',
'PH(RAIN)',
'CARBON DISSOLVED TOTAL',
'TOTAL PETROLEUM HYDROCARBONS (TPH)',
'PHENOLS TOTAL',
'HYDROCARBONS, C10-C16, F2',
'HYDROCARBONS, C16-C34, F3',
'HYDROCARBONS, C34-C50, F4',
'HYDROCARBONS, C6-C10, F1-BTEX',
'NITRITE AND NITRATE UNFILTERED',
'NITRATE AND NITRITE (AS N)',
'PHOSALONE',
'PHOSMET',
'PHOSALONE TOTAL',
'TEMPERATURE AIR',
'TEMPERATURE (AIR)',
'PHORATE-D10',
'CARBON TOTAL ORGANIC (CALCD.)',
'PHOSPHORUS PARTICULATE (CALCD.)',
'BICARBONATE (CALCD.)',
'CARBONATE (CALCD.)',
'PH THEORETICAL (CALCD.)',
'SODIUM PERCENTAGE (CALCD.)',
'SODIUM ADSORPTION RATIO (CALCD.)',
'CARBON PARTICULATE ORGANIC',
'ALUMINUM SUSPENDED',
'IRON SUSPENDED',
'MAGNESIUM PARTICULATE',
'COMPLEXED ALUMINUM',
'COMPLEXED IRON',
]

for param in params_all:
    if re.findall(params_extract, param):
        params_select.append(param)
df = df.groupby('VARIABLE').filter(lambda x: x.name in params_select)
df = df[~df['VARIABLE'].isin(params_drop)]

# extract data - units
unit_drop = [
    '%',
    'MG/G',
    'ÂľG/G',
    'NG',
    ]
df = df[~df['UNIT_UNITE'].isin(unit_drop)]

# extract data - quantified values
df = df[df['FLAG_MARQUEUR'] != '>']

# standardize column names
df = df.rename(columns={
    'SITE_NO':'site_id',
    'DATE_TIME_HEURE':'date',
    'FLAG_MARQUEUR':'bdl_flag',
    'VALUE_VALEUR':'value',
    'SDL_LDE':'dl_sample',
    'MDL_LDM':'dl_method',
    'VMV_CODE':'method_id',
    'UNIT_UNITE':'unit',
    'VARIABLE':'parameter_name',
    'STATUS_STATUT':'status',
    })

# standardize naming conventions
param_map = {
    'CALCIUM DISSOLVED':'Ca',
    'CARBON DISSOLVED ORGANIC':'OC',
    'CHLORIDE DISSOLVED':'Cl',
    'FLUORIDE DISSOLVED':'F',
    'MAGNESIUM DISSOLVED':'Mg',
    'PH':'pH',
    'PHOSPHORUS TOTAL':'P',
    'PHOSPHORUS TOTAL DISSOLVED':'P',
    'POTASSIUM, FILTERED':'K',
    'SODIUM, FILTERED':'Na',
    'SULPHATE DISSOLVED':'SO4',
    'TEMPERATURE WATER':'temperature',
    'ALUMINUM TOTAL':'Al',
    'IRON TOTAL':'Fe',
    'ALUMINUM DISSOLVED':'Al',
    'IRON DISSOLVED':'Fe',
    'POTASSIUM DISSOLVED/FILTERED':'K',
    'SODIUM DISSOLVED/FILTERED':'Na',
    'ALUMINUM EXTRACTABLE':'Al',
    'CALCIUM EXTRACTABLE/UNFILTERED':'Ca',
    'CALCIUM TOTAL':'Ca',
    'IRON EXTRACTABLE':'Fe',
    'MAGNESIUM EXTRACTABLE/UNFILTERED':'Mg',
    'MAGNESIUM TOTAL':'Mg',
    'POTASSIUM EXTRACTABLE/UNFILTERED':'K',
    'POTASSIUM TOTAL':'K',
    'SODIUM EXTRACTABLE/UNFILTERED':'Na',
    'SODIUM TOTAL':'Na',
    'DISSOLVED NITROGEN NITRATE':'NO3 as N',
    'NITROGEN DISSOLVED NITRITE':'NO2 as N',
    'CALCIUM EXTRACTABLE':'Ca',
    'MAGNESIUM EXTRACTABLE':'Mg',
    'NITROGEN DISSOLVED NITRATE':'NO3 as N',
    'NITROGEN NITRITE':'NO2 as N',
    'POTASSIUM EXTRACTABLE':'K',
    'SODIUM EXTRACTABLE':'Na',
    'POTASSIUM DISSOLVED':'K',
    'SODIUM DISSOLVED':'Na',
    'CARBON TOTAL ORGANIC':'OC',
    'FLUORIDE':'F',
    'CHLORIDE':'Cl',
    'NITROGEN TOTAL NITRATE':'NO3',
    'SULPHATE':'SO4',
    'TEMPERATURE WATER (FIELD)':'temperature',
    'NITRATE (AS N)':'NO3 as N',
    'CALCIUM  TOTAL':'Ca',
    'POTASSIUM UNFILTERED':'K',
    'SODIUM UNFILTERED':'Na',
    'CARBON, TOTAL ORGANIC (NON PURGEABLE)':'OC',
    'CHLORIDE TOTAL':'Cl',
    'SULPHATE TOTAL':'SO4',
    'TOTAL NITRATE':'NO3',
    'TEMPERATURE (WATER)':'temperature',
    'PH (FIELD)':'pH',
    'NITRATE TOTAL':'NO3',
    'CALCIUM  EXTRACTABLE/UNFILTERED':'Ca',
    'ALUMINUM TOTAL RECOVERABLE':'Al',
    'CALCIUM TOTAL RECOVERABLE':'Ca',
    'IRON TOTAL RECOVERABLE':'Fe',
    'MAGNESIUM TOTAL RECOVERABLE':'Mg',
    'POTASSIUM TOTAL RECOVERABLE':'K',
    'SODIUM TOTAL RECOVERABLE':'Na',
    'ORTHOPHOSPHATE DISSOLVED/UNFILTERED':'PO4',
    'ORTHOPHOSPHATE DISSOLVED':'PO4',
    'NITROGEN, NITRATE':'NO3',
    'PHOSPHATE (AS P)':'PO4 as P',
}

frac_map = {
    'CALCIUM DISSOLVED':'dissolved',
    'CARBON DISSOLVED ORGANIC':'dissolved',
    'CHLORIDE DISSOLVED':'dissolved',
    'FLUORIDE DISSOLVED':'dissolved',
    'MAGNESIUM DISSOLVED':'dissolved',
    'PH':'unspecified',
    'PHOSPHORUS TOTAL':'total',
    'PHOSPHORUS TOTAL DISSOLVED':'dissolved',
    'POTASSIUM, FILTERED':'dissolved',
    'SODIUM, FILTERED':'dissolved',
    'SULPHATE DISSOLVED':'dissolved',
    'TEMPERATURE WATER':'unspecified',
    'ALUMINUM TOTAL':'total',
    'IRON TOTAL':'total',
    'ALUMINUM DISSOLVED':'dissolved',
    'IRON DISSOLVED':'dissolved',
    'POTASSIUM DISSOLVED/FILTERED':'dissolved',
    'SODIUM DISSOLVED/FILTERED':'dissolved',
    'ALUMINUM EXTRACTABLE':'extractable',
    'CALCIUM EXTRACTABLE/UNFILTERED':'extractable',
    'CALCIUM TOTAL':'total',
    'IRON EXTRACTABLE':'extractable',
    'MAGNESIUM EXTRACTABLE/UNFILTERED':'extractable',
    'MAGNESIUM TOTAL':'total',
    'POTASSIUM EXTRACTABLE/UNFILTERED':'extractable',
    'POTASSIUM TOTAL':'total',
    'SODIUM EXTRACTABLE/UNFILTERED':'extractable',
    'SODIUM TOTAL':'total',
    'DISSOLVED NITROGEN NITRATE':'dissolved',
    'NITROGEN DISSOLVED NITRITE':'dissolved',
    'CALCIUM EXTRACTABLE':'extractable',
    'MAGNESIUM EXTRACTABLE':'extractable',
    'NITROGEN DISSOLVED NITRATE':'dissolved',
    'NITROGEN NITRITE':'unspecified',
    'POTASSIUM EXTRACTABLE':'extractable',
    'SODIUM EXTRACTABLE':'extractable',
    'POTASSIUM DISSOLVED':'dissolved',
    'SODIUM DISSOLVED':'dissolved',
    'CARBON TOTAL ORGANIC':'total',
    'FLUORIDE':'unspecified',
    'CHLORIDE':'unspecified',
    'NITROGEN TOTAL NITRATE':'total',
    'SULPHATE':'unspecified',
    'TEMPERATURE WATER (FIELD)':'field',
    'NITRATE (AS N)':'unspecified',
    'CALCIUM  TOTAL':'total',
    'POTASSIUM UNFILTERED':'total',
    'SODIUM UNFILTERED':'total',
    'CARBON, TOTAL ORGANIC (NON PURGEABLE)':'total',
    'CHLORIDE TOTAL':'total',
    'SULPHATE TOTAL':'total',
    'TOTAL NITRATE':'total',
    'TEMPERATURE (WATER)':'unspecified',
    'PH (FIELD)':'field',
    'NITRATE TOTAL':'total',
    'CALCIUM  EXTRACTABLE/UNFILTERED':'extractable',
    'ALUMINUM TOTAL RECOVERABLE':'recoverable',
    'CALCIUM TOTAL RECOVERABLE':'recoverable',
    'IRON TOTAL RECOVERABLE':'recoverable',
    'MAGNESIUM TOTAL RECOVERABLE':'recoverable',
    'POTASSIUM TOTAL RECOVERABLE':'recoverable',
    'SODIUM TOTAL RECOVERABLE':'recoverable',
    'ORTHOPHOSPHATE DISSOLVED/UNFILTERED':'unspecified',
    'ORTHOPHOSPHATE DISSOLVED':'dissolved',
    'NITROGEN, NITRATE':'unspecified',
    'PHOSPHATE (AS P)':'unspecified',
}
df['parameter_fraction'] = df['parameter_name']
df['parameter_name'] = df['parameter_name'].replace(param_map)
df['parameter_fraction'] = df['parameter_fraction'].replace(frac_map)

unit_map = {
'ďż˝G/L':'ug/l',
'ÂľG/L':'ug/l',
'UG/L':'ug/l',
'MG/L':'mg/l',
'MG/L N':'mg/l',
'MG/L P':'mg/l',
'PH UNITS':'unit',
'PH':'unit',
'DEG C':'deg_c'
}
df = df.replace({'unit':unit_map})

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
df['database'] = 'ECCC'
df['timezone'] = ''
df['depth'] = np.NaN

# standardize data types
df = set_type.samples(df)
df['dl_sample'] = pd.to_numeric(df['dl_sample'], errors='coerce')
df['dl_method'] = pd.to_numeric(df['dl_method'], errors='coerce')

# standardize below detection limit notation
# use <= to account for bdl notation in database
bdl_map = {
    'T':'<', # trace value below detection limit
    ' ':np.NaN,
    'nan':np.NaN,
    }
df['bdl_flag'] = df['bdl_flag'].replace(bdl_map)

df['bdl_flag'] = np.where(df['value'] <= df['dl_sample'],
    '<', df['bdl_flag'])
df['value'] = np.where(df['value'] <= df['dl_sample'],
    df['dl_sample'], df['value'])

df['bdl_flag'] = np.where(df['value'] <= df['dl_method'],
    '<', df['bdl_flag'])
df['value'] = np.where(df['value'] <= df['dl_method'],
    df['dl_method'], df['value'])

df = df.drop(['dl_sample','dl_method'], axis='columns')

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
methods = extract.methods(df,
    site_col = 'site_id',
    param_col ='parameter_name',
    frac_col = 'parameter_fraction',
    date_col = 'date',
    method_col = 'method_id')

method_ref = method_ref.rename({
    'VMV_Code':'method_id',
    'National_Method_Code':'method_agency_number',
    'Method_Title':'method_description',
    }, axis='columns')

methods = methods.merge(method_ref, on='method_id', how='left')

methods['database'] = 'ECCC'
methods['method_name'] = ''
methods['method_type'] = ''
methods['method_agency'] = 'Environment and Climate Change Canada'
methods['method_reference'] = 'Environment and Climate Change Canada National Valid Method Variables, contact: EC.MSQEINFORMATION-WQMSINFORMATION.EC@CANADA.CA'

methods = set_type.methods(methods)

# finalize site data
sites_final = df['site_id'].unique()
sites = sites[sites['site_id'].isin(sites_final)]


print('Exporting data...')

df.to_csv('cleaned_samples_eccc.csv', index=False, encoding='utf-8')
sites.to_csv('cleaned_sites_eccc.csv', index=False, encoding='utf-8')
methods.to_csv('cleaned_methods_eccc.csv', index=False, encoding='utf-8')

print('Done!')
