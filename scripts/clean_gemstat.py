""" Clean sample data from GEMStat

Script to clean sample data obtained from the International Centre for Water
Resources and Global Change's Global Water Quality database and information
system (GEMStat).
Data source: https://gemstat.org/

Approach:
    * Merge datasets
    * Extract desired data
    * Format dataset structure and contents

Assumptions:
    * Where value is below detection limit, the reported value is the
      detection limit.
    * If a parameter is not specified as "calculated", it is classified as
      "actual".
    * If a parameter is not specified as "field", it is classified as
      "Sample-Routine".

Input:
    * raw_gemstat_samples_[site type]_[parameter].csv
    * raw_gemstat_metadata_[site type].csc

Output:
    * cleaned_gemstat.csv
"""

import os
import pandas as pd
import numpy as np
from collections import namedtuple


# prepare dictionaries for re-naming parameters
parameters = namedtuple("parameters", "name fraction speciation")
parameters = {
    "Na-Dis": parameters(
        "Sodium",
        "Filtered",
        np.nan,
    ),
    "Al-Dis": parameters(
        "Aluminum",
        "Filtered",
        np.nan,
    ),
    "Ca-Dis": parameters(
        "Calcium",
        "Filtered",
        np.nan,
    ),
    "K-Dis": parameters(
        "Potassium",
        "Filtered",
        np.nan,
    ),
    "Mg-Dis": parameters(
        "Magnesium",
        "Filtered",
        np.nan,
    ),
    "NO2N": parameters(
        "Nitrite",
        "Unfiltered",  # assumed
        "as N",
    ),
    "pH": parameters(
        "pH",
        np.nan,
        np.nan,
    ),
    "Alk-Tot": parameters(
        "Alkalinity, total",
        "Unfiltered",  # assumed
        "as Unspecified",
    ),
    "H-T": parameters(
        "Total hardness",
        "Unfiltered",  # assumed
        "as CaCO3",
    ),
    "SO4-Dis": parameters(
        "Sulfate",
        "Filtered",
        "as SO4",
    ),
    "TEMP": parameters(
        "Temperature, water",
        np.nan,
        np.nan,
    ),
    "Al-Tot": parameters(
        "Aluminum",
        "Unfiltered",
        np.nan,
    ),
    "F-Dis": parameters(
        "Fluoride",
        "Filtered",
        np.nan,
    ),
    "NO3N": parameters(
        "Nitrate",
        "Unfiltered",  # assumed
        "as N",
    ),
    "DRP": parameters(
        "Orthophosphate",
        "Filtered",
        "as P",
    ),
    "DOC": parameters(
        "Organic carbon",
        "Filtered",
        np.nan,
    ),
    "Na-Tot": parameters(
        "Sodium",
        "Unfiltered",
        np.nan,
    ),
    "Ca-Tot": parameters(
        "Calcium",
        "Unfiltered",
        np.nan,
    ),
    "K-Tot": parameters(
        "Potassium",
        "Unfiltered",
        np.nan,
    ),
    "Mg-Tot": parameters(
        "Magnesium",
        "Unfiltered",
        np.nan,
    ),
    "TRP": parameters(
        "Orthophosphate",
        "Unfiltered",
        "as P",
    ),
    "Alk-Phen": parameters(
        "Alkalinity, Phenolphthalein (total hydroxide+1/2 carbonate)",
        "Unfiltered",  # assumed
        "as CaCO3",
    ),
    "DIC": parameters(
        "Inorganic carbon",
        "Filtered",
        np.nan,
    ),
    "TIC": parameters(
        "Inorganic carbon",
        "Unfiltered",
        np.nan,
    ),
    "TOC": parameters(
        "Organic carbon",
        "Unfiltered",
        np.nan,
    ),
    "CO2": parameters(
        "Carbon Dioxide, free CO2",
        np.nan,
        np.nan,
    ),
    "DIP": parameters(
        "Total Phosphorus, mixed forms",
        "Filtered",
        "as P",
    ),
    "HCO3": parameters(
        "Bicarbonate",
        "Unfiltered",  # assumed
        np.nan,
    ),
    "H-Ca": parameters(
        "Hardness, Calcium",
        "Unfiltered",  # assumed
        "as CaCO3",
    ),
    "H-NC": parameters(
        "Hardness, non-carbonate",
        "Unfiltered",  # assumed
        "as CaCO3",
    ),
    "CO3": parameters("Carbonate", "Unfiltered", np.nan),  # assumed
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


print("Importing data...")

path = r"/home/lobke/Boggart/Files/Manuscripts/SWatCh/Data and Scripts/scripts"
directory = os.listdir(path)

df_sites = pd.DataFrame()
df_methods = pd.DataFrame()
df = pd.DataFrame()

# files are provided as comma or semicolon delimited
semi_list = [
    "raw_gemstat_samples_al.csv",
    "raw_gemstat_samples_ca.csv",
    "raw_gemstat_samples_f.csv",
    "raw_gemstat_samples_k.csv",
    "raw_gemstat_samples_mg.csv",
    "raw_gemstat_samples_na.csv",
    "raw_gemstat_samples_p.csv",
    "raw_gemstat_samples_ph.csv",
    "raw_gemstat_samples_so4.csv",
    "raw_gemstat_samples_lakes_Alkalinity.csv",
    "raw_gemstat_samples_lakes_Carbon.csv",
    "raw_gemstat_samples_lakes_Carbonate.csv",
    "raw_gemstat_samples_lakes_Dissolved_Gas.csv",
    "raw_gemstat_samples_lakes_Hardness.csv",
    "raw_gemstat_samples_rivers_Alkalinity.csv",
    "raw_gemstat_samples_rivers_Carbon.csv",
    "raw_gemstat_samples_rivers_Carbonate.csv",
    "raw_gemstat_samples_rivers_Dissolved_Gas.csv",
    "raw_gemstat_samples_rivers_Hardness.csv",
]
comma_list = [
    "raw_gemstat_samples_nox.csv",
    "raw_gemstat_samples_oc.csv",
    "raw_gemstat_samples_po4.csv",
    "raw_gemstat_samples_temperature.csv",
]

for file in directory:
    if file in semi_list:
        df_temp = pd.read_csv(
            file,
            dtype={
                "GEMS Station Number": str,
                "Sample Date": str,
                "Sample Time": str,
                "Depth": float,
                "Parameter Code": str,
                "Analysis Method Code": str,
                "Value Flags": str,
                "Value": float,
                "Unit": str,
                "Data Quality": str,
                "Remark": str,
                "Integrated Value": str,
            },
            sep=";",
            encoding="utf-8",
            parse_dates=["Sample Date"],
        )
        df = pd.concat([df, df_temp], sort=True, ignore_index=True)

    if file in comma_list:
        df_temp = pd.read_csv(
            file,
            dtype={
                "GEMS Station Number": str,
                "Sample Date": str,
                "Sample Time": str,
                "Depth": float,
                "Parameter Code": str,
                "Analysis Method Code": str,
                "Value Flags": str,
                "Value": float,
                "Unit": str,
                "Data Quality": str,
                "Remark": str,
                "Integrated Value": str,
            },
            sep=",",
            encoding="utf-8",
            parse_dates=["Sample Date"],
        )
        df = pd.concat([df, df_temp], sort=True, ignore_index=True)


for file in directory:
    if "raw_gemstat_metadata" in file:
        df_temp = pd.read_excel(
            file,
            sheet_name="Station_Metadata",
            usecols=[
                "GEMS Station Number",
                "Local Station Number",
                "Water Type",
                "Station Identifier",
                "Station Narrative",
                "Responsible Collection Agency",
                "Latitude",
                "Longitude",
            ],
            dtype={
                "GEMS Station Number": str,
                "Local Station Number": str,
                "Water Type": str,
                "Station Identifier": str,
                "Station Narrative": str,
                "Responsible Collection Agency": str,
                "Latitude": float,
                "Longitude": float,
            },
            engine="xlrd",
        )
        df_sites = pd.concat([df_sites, df_temp], sort=True, ignore_index=True)


# there are duplicated sites in the metadata files - remove
df_sites = df_sites.drop_duplicates(
    subset=["GEMS Station Number"],
    keep="first",
)
df = pd.merge(df, df_sites, on="GEMS Station Number", how="inner")

for file in directory:
    if "raw_gemstat_metadata" in file:
        df_temp = pd.read_excel(
            file,
            sheet_name="Methods_Metadata",
            usecols=[
                "Analysis Method Code",
                "Method Name",
                "Method Type",
                "Method Number",
                "Method Source",
                "Method Description",
            ],
            dtype=str,
        )
        df_methods = pd.concat([df_methods, df_temp], sort=True, ignore_index=True)


# there are duplicated methods in the metadata files - remove
df_methods = df_methods.drop_duplicates(
    subset=["Analysis Method Code"],
    keep="first",
)
df = pd.merge(df, df_methods, on="Analysis Method Code", how="left")


print("Extracting data...")

# extract data - sites
df = df[df["Water Type"] != "Groundwater station"]

# extract data - parameters
param_exclude = [
    "SAR",
    "NOxN",
    "TEMP-Air",
    "POC",
    "H-Mg",  # not defined in WXQ
]
df = df[~df["Parameter Code"].isin(param_exclude)]


# extract data - sample type
df = df[~df["Integrated Value"].isin(["Time Integration", "Vertical Integration"])]
df = df.drop("Integrated Value", axis="columns")
df = df[df["Unit"] != "%"]
df = df[df["Value Flags"] != "~"]  # estimated value


print("Formatting data...")

# format - combine columns where required
df["Method Number"] = np.where(
    df["Method Number"].isnull(),
    df["Analysis Method Code"],
    df["Method Number"],
)
df["ResultValueType"] = np.where(
    df["Analysis Method Code"].str.contains("CALC"),
    "Calculated",
    "Actual",
)
df = df.drop("Analysis Method Code", axis="columns")

df["Local Station Number"] = np.where(
    df["Local Station Number"].isnull(),
    df["GEMS Station Number"],
    df["Local Station Number"],
)
df = df.drop("GEMS Station Number", axis="columns")

df["Station Identifier"] = np.where(
    df["Station Identifier"].isnull(),
    df["Station Narrative"],  # description of site
    df["Station Identifier"],
)
df = df.drop("Station Narrative", axis="columns")

df["Method Name"] = np.where(
    df["Method Name"].isnull(),
    df["Method Description"],  # description of method
    df["Method Name"],
)
df["Method Name"] = np.where(
    df["Method Name"].isnull(),
    df["Method Type"],  # type of method
    df["Method Name"],
)
df = df.drop(["Method Description", "Method Type"], axis="columns")


# format - column names
df = df.rename(
    {
        "Data Quality": "ResultStatusID",
        "Depth": "ActivityDepthHeightMeasure",
        "Parameter Code": "CharacteristicName",
        "Remark": "ResultComment",
        "Sample Date": "ActivityStartDate",
        "Sample Time": "ActivityStartTime",
        "Unit": "ResultUnit",
        "Value": "ResultValue",
        "Value Flags": "ResultDetectionCondition",
        "Latitude": "MonitoringLocationLatitude",
        "Local Station Number": "MonitoringLocationID",
        "Longitude": "MonitoringLocationLongitude",
        "Responsible Collection Agency": "DatasetName",
        "Station Identifier": "MonitoringLocationName",
        "Water Type": "MonitoringLocationType",
        "Method Name": "ResultAnalyticalMethodName",
        "Method Number": "ResultAnalyticalMethodID",
        "Method Source": "ResultAnalyticalMethodContext",
    },
    axis="columns",
)

# format - add missing required columns
df["ActivityDepthHeightUnit"] = "m"
df["ActivityMediaName"] = "Surface Water"
df["LaboratoryName"] = "Unspecified"
df["MonitoringLocationHorizontalCoordinateReferenceSystem"] = "WGS84"


# format - activity type
df["ActivityType"] = np.where(
    df["CharacteristicName"].isin(["pH", "Temperature, water"]),
    "Field Msr/Obs",  # assumed
    "Sample-Routine",
)


# format - status
status_map = {
    "Fair": "Accepted",
    np.nan: "Accepted",  # assumed
    "Suspect": "Rejected",  # outside of technical limits for parameter
    "Good": "Accepted",
}
df["ResultStatusID"] = df["ResultStatusID"].replace(status_map)


# format - units
unit_map = {
    # "mg/l" already in correct units
    "---": "None",  # for pH
    "me/L": "meq/L",
    "�C": "deg C",
    "mmol/l": "umol/L",
}
df["ResultUnit"] = df["ResultUnit"].replace(unit_map)


# format - dataset name
df["DatasetName"] = "GEMStat; source: " + df["DatasetName"].astype(str)


# format - station type
type_map = {
    "River station": "River/Stream",
    "Reservoir station": "Reservoir",
    "Lake station": "Lake/Pond",
}
df["MonitoringLocationType"] = df["MonitoringLocationType"].replace(type_map)


# format - data source
source_map = {
    "APHA 2012": "APHA",
    "ASTM 1971": "ASTM",
    "Environment Canada 1974": "ENV/CANADA",
    "US-EPA 1974": "USEPA",
    "APHA 1998": "APHA",
    "JSA 1998": "OTHER",
    "ISO 1998": "ISO",
    "APHA 1975": "APHA",
    "ISO 1996": "ISO",
    "PALINTEST 2013": "PALINTEST LTD",
    "Environment Canada 2007": "ENV/CANADA",
    "Environment Canada 1995": "ENV/CANADA",
    "VMV Method": "VMV",
    "Merck 2001": "OTHER",
    "MERCK 2014": "OTHER",
    "Merck 2013": "OTHER",
    np.nan: "OTHER",
    "JWWA 2001": "OTHER",
    "Environment Canada 1988": "ENV/CANADA",
    "APHA 1976": "APHA",
    "Gravelet-Blondin et al, 1980": "OTHER",
    "ISO 1995": "ISO",
    "APHA 2017": "APHA",
    "APHA 1967": "APHA",
    "ISO 1999": "ISO",
}
df["ResultAnalyticalMethodContext"] = df["ResultAnalyticalMethodContext"].replace(
    source_map
)


# format - parameter names
df["ResultSampleFraction"] = df["CharacteristicName"]
df["ResultSampleFraction"] = df["ResultSampleFraction"].replace(parameters_fractions)

df["MethodSpeciation"] = df["CharacteristicName"]
df["MethodSpeciation"] = df["MethodSpeciation"].replace(parameters_speciation)

df["CharacteristicName"] = df["CharacteristicName"].replace(parameters_name)


# format - detection conditions
flag_map = {
    "---": np.nan,
    "<": "Below Detection/Quantification Limit",
}
df["ResultDetectionCondition"] = df["ResultDetectionCondition"].replace(flag_map)

# create detection limit column
df["ResultDetectionQuantitationLimitMeasure"] = np.where(
    ~df["ResultDetectionCondition"].isnull(),
    df["ResultValue"],
    np.nan,
)
# create detection limit unit column
df["ResultDetectionQuantitationLimitUnit"] = np.where(
    ~df["ResultDetectionCondition"].isnull(),
    df["ResultUnit"],
    np.nan,
)
# create detection limit type column
df["ResultDetectionQuantitationLimitType"] = np.where(
    ~df["ResultDetectionCondition"].isnull(),
    "Reporting Limit",  # type of detection limit not specified
    "",
)
# remove result info when values are below detection limit
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

# delete records without detection limit or reported value information
df = df.dropna(
    axis="index",
    how="all",
    subset=["ResultDetectionQuantitationLimitMeasure", "ResultValue"],
)


print("Exporting data...")

df.to_csv("cleaned_gemstat.csv", index=False, encoding="utf-8")

print("Done!")
