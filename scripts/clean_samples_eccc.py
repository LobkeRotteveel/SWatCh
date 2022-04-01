""" Clean sample data from ECCC

Script to clean dataset obtained from Environment and Climate Change
Canada National Long-Term Water Quality Monitoring Data.
Data source: http://data.ec.gc.ca/data/substances/monitor/national-long-
             term-water-quality-monitoring-data/

Approach:
    * Merge datasets
    * Extract desired data
    * Format dataset structure
    * Format naming conventions
        * Parameter names
        * Parameter fractions
        * Unit names
        * Data types

Notes:
    * Both preliminary and validated data are retained to maintain maximum
      sample size. QAQC on preliminary data is recommended before use.
    * Encoding is not consistent across input datasets. Units decoded as
      "ďż˝G/L" and "ÂľG/L" are ug/L.
    * If a parameter is specified as "extractable/unfiltered", it is
      classified as "extractable".
    * If a parameter is not specified as "calculated", it is classified as
      "actual".
    * If a parameter is not specified as "field", it is classified as
      "Sample-Routine".

Input:
    * raw_eccc_samples_[waterbody].csv
    * raw_eccc_sites.csv
    * raw_eccc_methods.csv

Output:
    * cleaned_eccc.csv
"""


import os
import re
import pandas as pd
import numpy as np
import datetime as dt
from collections import namedtuple
from swatch_utils import set_type
from swatch_utils import convert
from swatch_utils import extract
from swatch_utils import check


parameters = namedtuple("parameters", "name fraction speciation type activity")
parameters = {
    "CALCIUM DISSOLVED": parameters(
        "Calcium",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CARBON DISSOLVED ORGANIC": parameters(
        "Organic carbon",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CHLORIDE DISSOLVED": parameters(
        "Chloride",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "FLUORIDE DISSOLVED": parameters(
        "Fluoride",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "MAGNESIUM DISSOLVED": parameters(
        "Magnesium",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "PH": parameters(
        "pH",
        np.nan,
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "PHOSPHORUS TOTAL": parameters(
        "Total Phosphorus, mixed forms",
        "Unfiltered",
        "as P",
        "Actual",
        "Sample-Routine",
    ),
    "PHOSPHORUS TOTAL DISSOLVED": parameters(
        "Total Phosphorus, mixed forms",
        "Filtered",
        "as P",
        "Actual",
        "Sample-Routine",
    ),
    "POTASSIUM, FILTERED": parameters(
        "Potassium",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SODIUM, FILTERED": parameters(
        "Sodium",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SULPHATE DISSOLVED": parameters(
        "Sulfate",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "TEMPERATURE WATER": parameters(
        "Temperature, water",
        np.nan,
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "ALUMINUM TOTAL": parameters(
        "Aluminum",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "IRON TOTAL": parameters(
        "Iron",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "ALUMINUM DISSOLVED": parameters(
        "Aluminum",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "IRON DISSOLVED": parameters(
        "Iron",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "POTASSIUM DISSOLVED/FILTERED": parameters(
        "Potassium",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SODIUM DISSOLVED/FILTERED": parameters(
        "Sodium",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "ALUMINUM EXTRACTABLE": parameters(
        "Aluminum",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CALCIUM EXTRACTABLE/UNFILTERED": parameters(
        "Calcium",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CALCIUM TOTAL": parameters(
        "Calcium",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "IRON EXTRACTABLE": parameters(
        "Iron",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "MAGNESIUM EXTRACTABLE/UNFILTERED": parameters(
        "Magnesium",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "MAGNESIUM TOTAL": parameters(
        "Magnesium",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "POTASSIUM EXTRACTABLE/UNFILTERED": parameters(
        "Potassium",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "POTASSIUM TOTAL": parameters(
        "Potassium",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SODIUM EXTRACTABLE/UNFILTERED": parameters(
        "Sodium",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SODIUM TOTAL": parameters(
        "Sodium",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "DISSOLVED NITROGEN NITRATE": parameters(
        "Nitrate",
        "Filtered",
        "as N",
        "Actual",
        "Sample-Routine",
    ),
    "NITROGEN DISSOLVED NITRITE": parameters(
        "Nitrite",
        "Filtered",
        "as NO2",
        "Actual",
        "Sample-Routine",
    ),
    "CALCIUM EXTRACTABLE": parameters(
        "Calcium",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "MAGNESIUM EXTRACTABLE": parameters(
        "Magnesium",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "NITROGEN DISSOLVED NITRATE": parameters(
        "Nitrate",
        "Filtered",
        "as NO3",
        "Actual",
        "Sample-Routine",
    ),
    "NITROGEN NITRITE": parameters(
        "Nitrite",
        "Unspecified",
        "as NO2",
        "Actual",
        "Sample-Routine",
    ),
    "POTASSIUM EXTRACTABLE": parameters(
        "Potassium",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SODIUM EXTRACTABLE": parameters(
        "Sodium",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "POTASSIUM DISSOLVED": parameters(
        "Potassium",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SODIUM DISSOLVED": parameters(
        "Sodium",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CARBON TOTAL ORGANIC": parameters(
        "Organic carbon",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "FLUORIDE": parameters(
        "Fluoride",
        "Unspecified",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CHLORIDE": parameters(
        "Chloride",
        "Unspecified",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "NITROGEN TOTAL NITRATE": parameters(
        "Nitrate",
        "Unfiltered",
        "as NO3",
        "Actual",
        "Sample-Routine",
    ),
    "SULPHATE": parameters(
        "Sulfate",
        "Unspecified",
        "as SO4",
        "Actual",
        "Sample-Routine",
    ),
    "TEMPERATURE WATER (FIELD)": parameters(
        "Temperature, water",
        np.nan,
        np.nan,
        "Actual",
        "Field Msr/Obs",
    ),
    "NITRATE (AS N)": parameters(
        "Nitrate",
        "Unspecified",
        "as N",
        "Actual",
        "Sample-Routine",
    ),
    "CALCIUM  TOTAL": parameters(
        "Calcium",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "POTASSIUM UNFILTERED": parameters(
        "Potassium",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SODIUM UNFILTERED": parameters(
        "Sodium",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CARBON, TOTAL ORGANIC (NON PURGEABLE)": parameters(
        "Organic carbon",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CHLORIDE TOTAL": parameters(
        "Chloride",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SULPHATE TOTAL": parameters(
        "Sulfate",
        "Unfiltered",
        "as SO4",
        "Actual",
        "Sample-Routine",
    ),
    "TOTAL NITRATE": parameters(
        "Nitrate",
        "Unfiltered",
        "as NO3",
        "Actual",
        "Sample-Routine",
    ),
    "TEMPERATURE (WATER)": parameters(
        "Temperature, water",
        np.nan,
        np.nan,
        "Actual",
        "Field Msr/Obs",
    ),  # assumed field measurement
    "PH (FIELD)": parameters(
        "pH",
        np.nan,
        np.nan,
        "Actual",
        "Field Msr/Obs",
    ),
    "PH THEORETICAL (CALCD.)": parameters(
        "pH",
        np.nan,
        np.nan,
        "Calculated",
        "Sample-Routine",
    ),
    "NITRATE TOTAL": parameters(
        "Nitrate",
        "Unfiltered",
        "as NO3",
        "Actual",
        "Sample-Routine",
    ),
    "CALCIUM  EXTRACTABLE/UNFILTERED": parameters(
        "Calcium",
        "Extractable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "ALUMINUM TOTAL RECOVERABLE": parameters(
        "Aluminum",
        "Total Recoverable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CALCIUM TOTAL RECOVERABLE": parameters(
        "Calcium",
        "Total Recoverable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "IRON TOTAL RECOVERABLE": parameters(
        "Iron",
        "Total Recoverable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "MAGNESIUM TOTAL RECOVERABLE": parameters(
        "Magnesium",
        "Total Recoverable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "POTASSIUM TOTAL RECOVERABLE": parameters(
        "Potassium",
        "Total Recoverable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "SODIUM TOTAL RECOVERABLE": parameters(
        "Sodium",
        "Total Recoverable",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "ORTHOPHOSPHATE DISSOLVED/UNFILTERED": parameters(
        "Orthophosphate",
        "Unfiltered",
        "as PO4",
        "Actual",
        "Sample-Routine",
    ),
    "ORTHOPHOSPHATE DISSOLVED": parameters(
        "Orthophosphate",
        "Unfiltered",
        "as PO4",
        "Actual",
        "Sample-Routine",
    ),
    "NITROGEN, NITRATE": parameters(
        "Nitrate",
        "Unspecified",
        "as NO3",
        "Actual",
        "Sample-Routine",
    ),
    "PHOSPHATE (AS P)": parameters(
        "Total Phosphorus, mixed forms",
        "Unspecified",
        "as P",
        "Actual",
        "Sample-Routine",
    ),
    "ALKALINITY TOTAL": parameters(
        "Alkalinity, total",
        "Unspecified",
        "AS Unspecified",
        "Actual",
        "Sample-Routine",
    ),
    "ALKALINITY TOTAL CACO3": parameters(
        "Alkalinity, carbonate",
        "Unspecified",
        "as CaCO3",
        "Actual",
        "Sample-Routine",
    ),
    "ALKALINITY GRAN CACO3": parameters(
        "Alkalinity, carbonate",
        "Unspecified",
        "as CaCO3",
        "Actual",
        "Sample-Routine",
    ),
    "ALKALINITY PHENOLPHTHALEIN CACO3": parameters(
        "Alkalinity, Phenolphthalein (total hydroxide+1/2 carbonate)",
        "Unspecified",
        "as CaCO3",
        "Actual",
        "Sample-Routine",
    ),
    "HARDNESS  TOTAL LABORATORY (CALCD.) CACO3": parameters(
        "Hardness, carbonate",
        "Unfiltered",
        "as CaCO3",
        "Calculated",
        "Sample-Routine",
    ),
    "HARDNESS TOTAL (CALCD.) CACO3": parameters(
        "Hardness, carbonate",
        "Unfiltered",
        "as CaCO3",
        "Calculated",
        "Sample-Routine",
    ),
    "HARDNESS NON-CARB. (CALCD.)": parameters(
        "Hardness, non-carbonate",
        "Unspecified",
        "as Unspecified",
        "Calculated",
        "Sample-Routine",
    ),
    "CARBON DIOXIDE FREE (CALC)": parameters(
        "Carbon Dioxide, free CO2",
        np.nan,
        np.nan,
        "Calculated",
        "Sample-Routine",
    ),
    "CARBON TOTAL INORGANIC": parameters(
        "Inorganic carbon",
        "Unfiltered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CARBON DISSOLVED INORGANIC": parameters(
        "Inorganic carbon",
        "Filtered",
        np.nan,
        "Actual",
        "Sample-Routine",
    ),
    "CARBON TOTAL ORGANIC (CALCD.)": parameters(
        "Organic carbon",
        "Unfiltered",
        np.nan,
        "Calculated",
        "Sample-Routine",
    ),
    "BICARBONATE (CALCD.)": parameters(
        "Bicarbonate",
        "Unspecified",
        np.nan,
        "Calculated",
        "Sample-Routine",
    ),
    "CARBONATE (CALCD.)": parameters(
        "Carbonate",
        "Unspecified",
        np.nan,
        "Calculated",
        "Sample-Routine",
    ),
}
parameters_name = dict()
for i in parameters:
    parameters_name[i] = parameters[i].name

parameters_fractions = dict()
for i in parameters:
    parameters_fractions[i] = parameters[i].fraction

parameters_speciation = dict()
for i in parameters:
    parameters_speciation[i] = parameters[i].speciation

parameters_type = dict()
for i in parameters:
    parameters_type[i] = parameters[i].type

parameters_activity = dict()
for i in parameters:
    parameters_activity[i] = parameters[i].activity

print("Importing data...")

path = r"/home/lobke/Boggart/Files/Manuscripts/SWatCh/for publication/data and scripts/scripts"
directory = os.listdir(path)

df = pd.DataFrame()

for file in directory:
    if "raw_eccc_samples" in file:
        df_temp = pd.read_csv(
            file,
            usecols=[
                "SITE_NO",
                "DATE_TIME_HEURE",
                "FLAG_MARQUEUR",
                "VALUE_VALEUR",
                "SDL_LDE",
                "MDL_LDM",
                "VMV_CODE",
                "UNIT_UNITE",
                "VARIABLE",
                "STATUS_STATUT",
            ],
            dtype={
                "SITE_NO": str,
                "DATE_TIME_HEURE": str,
                "FLAG_MARQUEUR": str,
                "VALUE_VALEUR": float,
                "SDL_LDE": float,
                "MDL_LDM": float,
                "VMV_CODE": str,
                "UNIT_UNITE": str,
                "VARIABLE": str,
                "STATUS_STATUT": str,
            },
            parse_dates=["DATE_TIME_HEURE"],
            encoding="ISO-8859-2",
        )
        df = pd.concat([df, df_temp], sort=True, ignore_index=True)

df_sites = pd.read_csv(
    "raw_eccc_sites.csv",
    usecols=["SITE_NO", "SITE_NAME", "SITE_TYPE", "LATITUDE", "LONGITUDE", "DATUM"],
    dtype=str,
    encoding="ISO-8859-15",
)
df = pd.merge(df, df_sites, on="SITE_NO", how="inner")

df_methods = pd.read_csv(
    "raw_eccc_methods.csv",
    usecols=[
        "VMV_CODE",
        "Method_Title",
    ],
    dtype=str,
    encoding="ISO-8859-2",
)
df = pd.merge(df, df_methods, on="VMV_CODE", how="left")


print("Extracting data...")

# use print-out to identify desired parameters and site types
print("Available Parameters:\n", file=open("raw_eccc_parameters_and_sites.txt", "a"))
print(df["VARIABLE"].unique(), file=open("raw_eccc_parameters_and_sites.txt", "a"))
print("\nAvailable Stite Types:", file=open("raw_eccc_parameters_and_sites.txt", "a"))
print(df["SITE_TYPE"].unique(), file=open("raw_eccc_parameters_and_sites.txt", "a"))

# extract data - parameters
df = df[df["VARIABLE"].isin(parameters.keys())]

# extract data - units
df = df[~df["UNIT_UNITE"].isin(["%", "ÂľG/G"])]  # remove illogical units


print("Formatting data...")

# format - column names
df = df.rename(
    columns={
        "SITE_NO": "MonitoringLocationID",
        "SITE_NAME": "MonitoringLocationName",
        "SITE_TYPE": "MonitoringLocationType",
        "LATITUDE": "MonitoringLocationLatitude",
        "LONGITUDE": "MonitoringLocationLongitude",
        "DATUM": "MonitoringLocationHorizontalCoordinateReferenceSystem",
        "DATE_TIME_HEURE": "ActivityStartDate",
        "VARIABLE": "CharacteristicName",
        "VALUE_VALEUR": "ResultValue",
        "UNIT_UNITE": "ResultUnit",
        "STATUS_STATUT": "ResultStatusID",
        "FLAG_MARQUEUR": "ResultDetectionCondition",
        "SDL_LDE": "ResultDetectionQuantitationLimitMeasure",
        "VMV_CODE": "ResultAnalyticalMethodID",
        "Method_Title": "ResultAnalyticalMethodName",
    }
)

# format - missing required columns
df["DatasetName"] = "ECCC National Long-Term Water Quality Monitoring Data"
df["ResultAnalyticalMethodContext"] = "VMV"
df["LaboratoryName"] = "Unspecified"
df["ActivityMediaName"] = "Surface Water"

# format - dates and times
df["ActivityStartTime"] = df["ActivityStartDate"].dt.time
df["ActivityStartDate"] = df["ActivityStartDate"].dt.date

# format - parameter naming
df["ResultSampleFraction"] = df["CharacteristicName"]
df["ResultSampleFraction"] = df["ResultSampleFraction"].replace(parameters_fractions)

df["MethodSpeciation"] = df["CharacteristicName"]
df["MethodSpeciation"] = df["MethodSpeciation"].replace(parameters_speciation)

df["ResultValueType"] = df["CharacteristicName"]
df["ResultValueType"] = df["ResultValueType"].replace(parameters_type)

df["ActivityType"] = df["CharacteristicName"]
df["ActivityType"] = df["ActivityType"].replace(parameters_activity)

df["CharacteristicName"] = df["CharacteristicName"].replace(parameters_name)

# format - site type
site_map = {
    "RIVER/RIVIÈRE": "Lake/Pond",
    "LAKE/LAC": "Lake/Pond",
    "POND/ÉTANG": "River/Stream",
}
df["MonitoringLocationType"] = df["MonitoringLocationType"].replace(site_map)

# format - status
status_map = {
    "V": "Validated",
    "P": "Preliminary",
}
df["ResultStatusID"] = df["ResultStatusID"].replace(status_map)

# format - units
unit_map = {
    "ďż˝G/L": "ug/l",
    "ÂľG/L": "ug/l",
    "UG/L": "ug/l",
    "MG/L": "mg/l",
    "MG/L N": "mg/l",
    "MG/L P": "mg/l",
    "PH UNITS": "None",
    "PH": "None",
    "DEG C": "deg C",
}
df["ResultUnit"] = df["ResultUnit"].replace(unit_map)
df["ResultDetectionQuantitationLimitUnit"] = df["ResultUnit"]


# format - below detection limit notation
df["ResultDetectionQuantitationLimitType"] = np.nan
bdl_map = {
    ">": "Above Detection/Quantification Limit",
    "T": "Below Detection/Quantification Limit",  # trace value below detection limit
    "<": "Below Detection/Quantification Limit",
    " ": np.nan,
    "nan": np.nan,
}
df["ResultDetectionCondition"] = df["ResultDetectionCondition"].replace(bdl_map)

df["ResultDetectionQuantitationLimitMeasure"] = np.where(
    df["ResultValue"] < df["ResultDetectionQuantitationLimitMeasure"],
    df["ResultValue"],
    df["ResultDetectionQuantitationLimitMeasure"],
)

# use <= to account for bdl notation in dataset
df["ResultDetectionCondition"] = np.where(
    df["ResultValue"] < df["ResultDetectionQuantitationLimitMeasure"],
    "Below Detection/Quantification Limit",
    df["ResultDetectionCondition"],
)

df["ResultDetectionQuantitationLimitType"] = np.where(
    ~df["ResultDetectionCondition"].isnull(),
    "Sample Detection Limit",
    df["ResultDetectionQuantitationLimitType"],
)

df["ResultDetectionCondition"] = np.where(
    df["ResultValue"] < df["MDL_LDM"],
    "Below Detection/Quantification Limit",
    df["ResultDetectionCondition"],
)

df["ResultDetectionQuantitationLimitType"] = np.where(
    df["ResultValue"] < df["MDL_LDM"],
    "Method Detection Level",
    df["ResultDetectionQuantitationLimitType"],
)

df["ResultDetectionQuantitationLimitMeasure"] = np.where(
    df["ResultValue"] < df["MDL_LDM"],
    df["MDL_LDM"],
    df["ResultDetectionQuantitationLimitMeasure"],
)

df = df.drop("MDL_LDM", axis="columns")


# delete records without detection limit or reported value information
df = df.dropna(
    axis="index",
    how="all",
    subset=["ResultDetectionQuantitationLimitMeasure", "ResultValue"],
)


df["ResultValue"] = np.where(
    ~df["ResultDetectionCondition"].isnull(),
    np.nan,
    df["ResultValue"],
)

df["ResultUnit"] = np.where(
    ~df["ResultDetectionCondition"].isnull(),
    np.nan,
    df["ResultUnit"],
)

df["ResultDetectionQuantitationLimitMeasure"] = np.where(
    df["ResultDetectionCondition"].isnull(),
    np.nan,
    df["ResultDetectionQuantitationLimitMeasure"],
)
df["ResultDetectionQuantitationLimitUnit"] = np.where(
    df["ResultDetectionCondition"].isnull(),
    np.nan,
    df["ResultDetectionQuantitationLimitUnit"],
)
df["ResultDetectionQuantitationLimitType"] = np.where(
    df["ResultDetectionCondition"].isnull(),
    np.nan,
    df["ResultDetectionQuantitationLimitType"],
)


print("Exporting data...")

df.to_csv("cleaned_eccc.csv", index=False, encoding="utf-8")


print("Done!")
