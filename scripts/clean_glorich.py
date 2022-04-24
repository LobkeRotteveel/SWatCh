""" Clean sample data from GloRiCh

Script to clean sample data obtained from the Global River Chemistry Database.
Data source: https://doi.pangaea.de/10.1594/PANGAEA.902360

Approach:
    * Merge datasets
    * Extract desired data
    * Format dataset structure and contents

Notes:
    * Both preliminary and validated data are retained to maintain maximum
      sample size. QAQC on preliminary data is recommended before use.

Input:
    * raw_glorich_samples.csv
    * raw_glorich_referrences.csv
    * raw_glorich_sites.csc

Output:
    * cleaned_glorich.csv
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
from pyproj import Transformer
from collections import namedtuple
from swatch_utils import restructure


""" Transform coordinates into WGS 1984

Input: pandas dataframe grouped by coordinate reference system

"""
coordinate_espg_map = {
    "NA1983": "epsg:4269",  # North American Datum 1983
    "Gauss\x96Krüger, Zone 4, DHDN": "epsg:31468",  # Deutsches Hauptdreiecksnetz Gauss-Kruger, Zone 4
    "ERTS_UTM, Zone 33N, 7 digits": "epsg:3045",  # European Terrestrial Reference System 1989 - Zone 33N, seven digits
    # assume WGS 1984 for "unknown"
}


def convert_coordinates(group):
    if group.name in coordinate_espg_map.keys():
        transformer = Transformer.from_crs(
            coordinate_espg_map[group.name],
            "epsg:4326",
            always_xy=True,
        )
        (
            group["MonitoringLocationLongitude"],
            group["MonitoringLocationLatitude"],
        ) = transformer.transform(
            group["MonitoringLocationLongitude"],
            group["MonitoringLocationLatitude"],
        )
    else:
        pass
    return group


# prepare dictionaries for re-naming parameters
parameters = namedtuple("parameters", "name fraction speciation type unit")
parameters = {
    "Alkalinity": parameters(
        "Alkalinity, total",  # assumed to be total alkalinity
        "Unspecified",
        "as Unspecified",
        "Actual",
        "ueq/L",
    ),
    "Ca": parameters(
        "Calcium",
        "Filtered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "Cl": parameters(
        "Chloride",
        "Filtered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "CO3": parameters(
        "Carbonate",
        "Unspecified",
        "as CO3",
        "Actual",
        "umol/L",
    ),
    "DIP": parameters(
        "Orthophosphate",  # inorganic phosphorus
        "Filtered",
        "as PO4",
        "Actual",
        "umol/L",
    ),
    "DNH4": parameters(
        "Ammonium",
        "Filtered",
        "as NH4",
        "Actual",
        "umol/L",
    ),
    "DOC": parameters(
        "Organic carbon",
        "Filtered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "DIC": parameters(
        "Inorganic carbon",
        "Filtered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "DP": parameters(
        "Total Phosphorus, mixed forms",
        "Filtered",
        "as P",
        "Actual",
        "umol/L",
    ),
    "F": parameters(
        "Fluoride",
        "Filtered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "HCO3": parameters(
        "Bicarbonate",
        "Unspecified",
        "as HCO3",
        "Actual",
        "umol/L",
    ),
    "K": parameters(
        "Potassium",
        "Filtered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "Mg": parameters(
        "Magnesium",
        "Filtered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "Na": parameters(
        "Sodium",
        "Filtered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "NO2": parameters(
        "Nitrite",
        "Filtered",
        "as NO2",
        "Actual",
        "umol/L",
    ),
    "NO3": parameters(
        "Nitrate",
        "Filtered",
        "as NO3",
        "Actual",
        "umol/L",
    ),
    "Temp_water": parameters(
        "Temperature, water",
        np.nan,
        np.nan,
        "Actual",
        "deg C",
    ),
    "TIP": parameters(
        "Orthophosphate",  # inorganic phosphorus
        "Unfiltered",
        "as PO4",
        "Actual",
        "umol/L",
    ),
    "TNH4": parameters(
        "Ammonium",
        "Unfiltered",
        "as NH4",
        "Actual",
        "umol/L",
    ),
    "TOC": parameters(
        "Organic carbon",
        "Unfiltered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "TIC": parameters(
        "Inorganic carbon",
        "Unfiltered",
        np.nan,
        "Actual",
        "umol/L",
    ),
    "TP": parameters(
        "Total Phosphorus, mixed forms",
        "Unfiltered",
        "as P",
        "Actual",
        "umol/L",
    ),
    "pH": parameters(
        "pH",
        np.nan,
        np.nan,
        "Actual",
        "None",
    ),
    "SO4": parameters(
        "Sulfate",
        "Filtered",
        "as SO4",
        "Actual",
        "umol/L",
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

parameters_unit = dict()
for i in parameters:
    parameters_unit[i] = parameters[i].unit


print("Importing data...")

df = pd.read_csv(
    "raw_glorich_samples.csv",
    usecols=[
        "STAT_ID",
        "RESULT_DATETIME",
        "SAMPLE_TIME_DESC",
        "SAMPLING_MODE",
        "Ref",
        "Temp_water_vrc",
        "pH_vrc",
        "Ca_vrc",
        "Mg_vrc",
        "K_vrc",
        "Na_vrc",
        "Cl_vrc",
        "SO4_vrc",
        "F_vrc",
        "TOC_vrc",
        "DOC_vrc",
        "NO3_vrc",
        "NO2_vrc",
        "TNH4_vrc",
        "DNH4_vrc",
        "TP_vrc",
        "DP_vrc",
        "TIP_vrc",
        "DIP_vrc",
        "Alkalinity_vrc",
        "HCO3_vrc",
        "CO3_vrc",
        "TIC_vrc",
        "DIC_vrc",
        "Temp_water",
        "pH",
        "Ca",
        "Mg",
        "K",
        "Na",
        "Cl",
        "SO4",
        "F",
        "TOC",
        "DOC",
        "NO3",
        "NO2",
        "TNH4",
        "DNH4",
        "TP",
        "DP",
        "TIP",
        "DIP",
        "Alkalinity",
        "HCO3",
        "CO3",
        "TIC",
        "DIC",
    ],
    dtype=str,  # imported as strings due to mixed data types
    parse_dates=["RESULT_DATETIME"],
)

df_sites = pd.read_csv(
    "raw_glorich_sites.csv",
    usecols=[
        "STAT_ID",
        "STATION_NAME",
        "STATION_ID_ORIG",
        "Latitude",
        "Longitude",
        "CoordinateSystem",
    ],
    dtype=str,
    encoding="ISO-8859-14",
)

df_references = pd.read_csv("raw_glorich_references.csv", dtype=str)
df_references = df_references.rename(
    columns={
        "Reference abbreviation in GLORICH": "Ref",
    }
)


print("Cleaning data...")

# extract data - sample type
df = df[df["SAMPLING_MODE"] == "single"]
df = df.drop("SAMPLING_MODE", axis="columns")


# extract data - remove data with unknown date information
df = df[df["SAMPLE_TIME_DESC"].isnull()]
df = df.drop("SAMPLE_TIME_DESC", axis="columns")


# combine comment columns so dataframe can be restructured
samples_list = [
    "Temp_water",
    "pH",
    "Ca",
    "Mg",
    "K",
    "Na",
    "Cl",
    "SO4",
    "F",
    "TOC",
    "DOC",
    "NO3",
    "NO2",
    "TNH4",
    "DNH4",
    "TP",
    "DP",
    "TIP",
    "DIP",
    "Alkalinity",
    "HCO3",
    "CO3",
    "TIC",
    "DIC",
]

comments_list = [
    "Temp_water_vrc",
    "pH_vrc",
    "Ca_vrc",
    "Mg_vrc",
    "K_vrc",
    "Na_vrc",
    "Cl_vrc",
    "SO4_vrc",
    "F_vrc",
    "TOC_vrc",
    "DOC_vrc",
    "NO3_vrc",
    "NO2_vrc",
    "TNH4_vrc",
    "DNH4_vrc",
    "TP_vrc",
    "DP_vrc",
    "TIP_vrc",
    "DIP_vrc",
    "Alkalinity_vrc",
    "HCO3_vrc",
    "CO3_vrc",
    "TIC_vrc",
    "DIC_vrc",
]


for comment, col in zip(comments_list, samples_list):
    # there are whitespaces in comments columns - remove them
    df[col] = df[col].astype(str) + df[comment].str.strip()
    df = df.drop(comment, axis="columns")


# format - dataframe structure
df = restructure.df(df, ["STAT_ID", "RESULT_DATETIME", "Ref"])

# due to the original dataset structure, there are many empty rows
df = df.dropna(
    axis="index",
    how="any",
    subset=["ResultValue"],
)

# format - combine dataframes
df = pd.merge(df, df_sites, on="STAT_ID", how="inner")
df = pd.merge(df, df_references, on="Ref", how="inner")
df = df.drop("Ref", axis="columns")


# format - column names and contents
df = df.rename(
    columns={
        "RESULT_DATETIME": "ActivityStartDate",
        "STAT_ID": "MonitoringLocationID_GloRiCh",
        "STATION_NAME": "MonitoringLocationName",
        "STATION_ID_ORIG": "MonitoringLocationID",
        "Latitude": "MonitoringLocationLatitude",
        "Longitude": "MonitoringLocationLongitude",
        "CoordinateSystem": "MonitoringLocationHorizontalCoordinateReferenceSystem",
        "Short description of reference or source of the data": "DatasetName",
    }
)


# format - fill blank site name columns
df["MonitoringLocationName"] = np.where(
    df["MonitoringLocationName"].isnull(),
    "Unspecified",
    df["MonitoringLocationName"],
)


# format - dates and times
df["ActivityStartTime"] = df["ActivityStartDate"].dt.time
df["ActivityStartDate"] = df["ActivityStartDate"].dt.date


# format - dataset name
df["DatasetName"] = "GloRiCh; source: " + df["DatasetName"].astype(str)


# format - station ID; use original ID where available
df["MonitoringLocationID"] = np.where(
    df["MonitoringLocationID"].isnull(),
    df["MonitoringLocationID_GloRiCh"],
    df["MonitoringLocationID"],
)
df = df.drop("MonitoringLocationID_GloRiCh", axis="columns")


""" format - coordinate reference system
Several coordinate systems in GloRich are not defined within
the WQX schema, so they are converted here (as opposed to in the
later formatting script).
"""
df = df.groupby(["MonitoringLocationHorizontalCoordinateReferenceSystem"]).apply(
    convert_coordinates
)
df["MonitoringLocationHorizontalCoordinateReferenceSystem"] = "WGS84"


# format - add missing required columns
df["ActivityMediaName"] = "Surface Water"
df["LaboratoryName"] = "Unspecified"
df["MonitoringLocationType"] = "River/Stream"
df["ResultComment"] = ""
# all values are measured in the lab, other than pH and temperature
df["ActivityType"] = np.where(
    df["CharacteristicName"].isin(
        [
            "pH",
            "Temp_water",
        ]
    ),
    "Field Msr/Obs",
    "Sample-Routine",
)
df["ResultAnalyticalMethodContext"] = np.nan
df["ResultAnalyticalMethodID"] = np.nan
df["ResultAnalyticalMethodName"] = "Undefined"
df["ResultStatusID"] = "Accepted"  # status not defined, but accepted in published paper


# format - parameter naming
df["ResultSampleFraction"] = df["CharacteristicName"]
df["ResultSampleFraction"] = df["ResultSampleFraction"].replace(parameters_fractions)

df["MethodSpeciation"] = df["CharacteristicName"]
df["MethodSpeciation"] = df["MethodSpeciation"].replace(parameters_speciation)

df["ResultValueType"] = df["CharacteristicName"]
df["ResultValueType"] = df["ResultValueType"].replace(parameters_type)

df["ResultUnit"] = df["CharacteristicName"]
df["ResultUnit"] = df["ResultUnit"].replace(parameters_unit)

df["CharacteristicName"] = df["CharacteristicName"].replace(parameters_name)


# format - add information from comments to other columns
df["ActivityType"] = np.where(
    df["ResultValue"].str.contains("LAB"),
    "Sample-Routine",
    df["ActivityType"],
)  # if measured in the field, this value is "Field Msr/Obs"
df["ResultValue"] = df["ResultValue"].str.replace("LAB", "")

df["ResultValueType"] = np.where(
    df["ResultValue"].str.contains("C"),
    "Calculated",
    df["ResultValueType"],
)
df["ResultValue"] = df["ResultValue"].str.replace("C", "")

df["ResultSampleFraction"] = np.where(
    df["ResultValue"].str.contains("U"),
    "Unfiltered",
    df["ResultSampleFraction"],
)
df["ResultValue"] = df["ResultValue"].str.replace("U", "")
df["ResultValue"] = df["ResultValue"].str.replace("C U", "")

# some values are averaged over a sampling period - remove
df = df[~df["ResultValue"].str.contains("avg")]
df["ResultValue"] = df["ResultValue"].str.replace("avg", "")

# format - detection limit information
df["ResultDetectionCondition"] = np.where(
    df["ResultValue"].str.contains("<"),
    "Below Detection/Quantification Limit",
    "",
)
df["ResultValue"] = df["ResultValue"].str.replace("<", "")

df["ResultStatusID"] = np.where(
    df["ResultValue"].str.contains(">"),
    "Rejected",  # value thought to be higher than reported
    df["ResultStatusID"],
)
df["ResultValue"] = df["ResultValue"].str.replace(">", "")


# specify data types after separating text and strings
df["ResultValue"] = pd.to_numeric(df["ResultValue"], errors="coerce")
df = df[~df["ResultValue"].isnull()]


# format - below detection limit notation
df["ResultDetectionQuantitationLimitMeasure"] = np.where(
    df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
    df["ResultValue"]
    * 2,  # values below detection limit were reported as half detection limit
    np.nan,
)

df["ResultValue"] = np.where(
    df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
    np.nan,
    df["ResultValue"],
)
df["ResultDetectionQuantitationLimitType"] = np.where(
    df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
    "Reporting Limit",
    "",
)
df["ResultDetectionQuantitationLimitUnit"] = np.where(
    df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
    df["ResultUnit"],
    "",
)
df["ResultUnit"] = np.where(
    df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
    "",
    df["ResultUnit"],
)


print("Exporting data...")

df.to_csv("cleaned_glorich.csv", index=False, encoding="utf-8")

print("Done!")
