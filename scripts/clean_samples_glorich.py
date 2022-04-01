""" Clean sample data from GloRiCh

Script to clean sample data obtained from the Global River Chemistry Database.
Data source: https://doi.pangaea.de/10.1594/PANGAEA.902360

Approach:
    * Extract data
    * Standardize column names
    * Standardize dataframe structure
    * Standardize below detection limit notation
    * Standardize naming conventions
    * Standardize data types
    * Standardize units
    * Standardize float data
    * Remove impossible values
    * Remove duplicates
    * Extract method data
    * Finalize site data

Notes:
    * All columns are imported as strings due to mixed data types.
    * Values below detection limit were reported as half detetection limit,
      replace this with detection limit itself
    * Method column contains data source information (in which methods can
      be found)

Input:
    * raw_samples_glorich.csv
    * raw_referrences_glorich.csv
    * intermediate_sites_glorich.csc

Output:
    * cleaned_sites_glorich.csv
    * cleaned_samples_glorich.csv
    * cleaned_methods_glorich.csv
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
from swatch_utils import restructure
from swatch_utils import set_type
from swatch_utils import convert
from swatch_utils import extract
from swatch_utils import check


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
    dtype=str,
    parse_dates=["RESULT_DATETIME"],
    nrows=10000
    # chunksize=1000000,
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

print(df)

print("Cleaning data...")
# for i, df in tqdm(enumerate(chunked)):
#     # extract data - selected sites
# df = pd.merge(df, df_sites, on="STAT_ID", how="inner")


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
    # df[col] = np.where(df[comment].isin(exclude_list), np.NaN, df[col])
    # df[comment] = np.where(df[comment].isin(exclude_list), "", df[comment])
    # df[comment] = np.where(df[comment].isnull(), "", df[comment])
    df[col] = df[col].astype(str) + df[comment]
    df = df.drop(comment, axis="columns")

print(df.columns)

#     # standardize column names
#     df = df.rename(
#         {
#             "RESULT_DATETIME": "date",
#             "Ref": "method_id",
#         },
#         axis="columns",
#     )

# standardize dataframe structure
df = restructure.df(df, ["STAT_ID", "RESULT_DATETIME", "Ref"])

print(df)
#     df = df.rename({"param": "parameter_name"}, axis="columns")

# comment options are: <, >, U, C, C U, LAB, nan, avg (u is unfiltered)

#     df["timezone"] = ""
#     df["parameter_fraction"] = "dissolved"
#     df["unit"] = "umol/l"
#     df["bdl_flag"] = ""
#     df["depth"] = np.NaN
#     df["database"] = "GloRiCh"
#     df["status"] = ""

#     # standardize below detection limit notation
#     df["bdl_flag"] = np.where(df["value"].str.contains("<"), "<", "")
#     df["value"] = np.where(df["bdl_flag"] == "<", df["value"] * 2, df["value"])

#     # standardize naming conventions
#     df["parameter_fraction"] = np.where(
#         df["value"].str.contains("U"), "total", df["parameter_fraction"]
#     )  # U indicated unfiltered
#     df["value"] = df["value"].str.replace("<", "")
#     df["value"] = df["value"].str.replace("U", "")
#     df["value"] = df["value"].str.replace(" U", "")

#     df["unit"] = np.where(df["parameter_name"] == "pH", "unit", df["unit"])
#     df["unit"] = np.where(df["parameter_name"] == "Temp_water", "deg_c", df["unit"])

#     df["parameter_fraction"] = np.where(
#         df["parameter_name"] == "pH", "unspecified", df["parameter_fraction"]
#     )
#     df["parameter_fraction"] = np.where(
#         df["parameter_name"] == "Temp_water", "unspecified", df["parameter_fraction"]
#     )
#     df["parameter_fraction"] = np.where(
#         df["parameter_name"] == "TIP", "total", df["parameter_fraction"]
#     )
#     df["parameter_fraction"] = np.where(
#         df["parameter_name"] == "TNH4", "total", df["parameter_fraction"]
#     )
#     df["parameter_fraction"] = np.where(
#         df["parameter_name"] == "TP", "total", df["parameter_fraction"]
#     )
#     df["parameter_fraction"] = np.where(
#         df["parameter_name"] == "TOC", "total", df["parameter_fraction"]
#     )

#     param_map = {
#         "DIP": "PO4",
#         "DNH4": "NH4",
#         "DOC": "OC",
#         "DP": "P",
#         "TIP": "PO4",
#         "TNH4": "NH4",
#         "TOC": "OC",
#         "TP": "P",
#         "Temp_water": "temperature",
#     }
#     df["parameter_name"] = df["parameter_name"].replace(param_map)

#     # standardize data types
#     df = set_type.samples(df)

#     # standardize units
#     df = df.groupby(["parameter_name", "unit"]).filter(check.units)
#     df.index = pd.RangeIndex(len(df.index))
#     df = df.groupby(["parameter_name", "unit"]).apply(
#         convert.chemistry,
#         param_col="parameter_name",
#         unit_col="unit",
#         value_col="value",
#     )

#     # standardize float data
#     df["value"] = df["value"].round(decimals=3)
#     df["depth"] = df["depth"].round(decimals=3)

#     # remove impossible values
#     df = df[(df["value"] >= 0) | (df["parameter_name"] == "temperature")]

#     # remove duplicates
#     df = df.drop_duplicates(
#         subset=[
#             "site_id",
#             "date",
#             "parameter_name",
#             "parameter_fraction",
#             "value",
#         ],
#         keep="first",
#     )

#     # export sample data
#     if i == 0:
#         df.to_csv(
#             "intermediate_samples_glorich.csv",
#             index=False,
#             encoding="utf-8",
#             mode="w",
#             header=True,
#         )
#     else:
#         df.to_csv(
#             "intermediate_samples_glorich.csv",
#             index=False,
#             encoding="utf-8",
#             mode="a",
#             header=False,
#         )

# # finalize selected sites
# df = pd.read_csv("intermediate_samples_glorich.csv")

# sites_selected = df["site_id"].astype(str).unique()
# sites["site_id"] = sites["site_id"].astype(str)
# sites = sites[sites["site_id"].isin(sites_selected)]
# sites.to_csv("cleaned_sites_glorich.csv", index=False, encoding="utf-8")

# # extract methods
# df_methods = df_methods.rename(
#     {
#         "Reference abbreviation in GLORICH": "method_id",
#         "Short description of reference or source of the data": "df_methodserence",
#     },
#     axis="columns",
# )

# methods = extract.methods(
#     df,
#     site_col="site_id",
#     param_col="parameter_name",
#     frac_col="parameter_fraction",
#     date_col="date",
#     method_col="method_id",
# )

# methods = methods.merge(df_methods, on="method_id", how="left")

# methods["database"] = "GloRiCh"
# methods["method_name"] = ""
# methods["method_type"] = ""
# methods["method_agency"] = ""
# methods["method_agency_number"] = ""
# methods["method_description"] = ""

# methods = set_type.methods(methods)

# # finalize site data
# sites_final = df["site_id"].unique()
# sites = sites[sites["site_id"].isin(sites_final)]
# sites["agency"] = "Listed in Methods Dataframe"


# print("Exporting data...")

# df.to_csv("cleaned_samples_glorich.csv", index=False, encoding="utf-8")
# sites.to_csv("cleaned_sites_glorich.csv", index=False, encoding="utf-8")
# methods.to_csv("cleaned_methods_glorich.csv", index=False, encoding="utf-8")

# print("Done!")
