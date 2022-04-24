"""Clean samples data from McMurdo Dry Valleys LTER

Script to clean sample data obtained from the McMurdo Dry Valleys McMurdo Dry
Valleys Long Term Ecological Research Network.
Data source: http://mcm.lternet.edu/power-search/data-set

Approach:
    * Merge datasets
    * Extract desired data
    * Format dataset structure and contents

Notes:
    * Result status was set to "Preliminary" because the staus is reported
      within the comments. It is not feasible to review each comment to determine
      what the status is.
Input:
    * raw_mcmurdo_samples_[file name].csv
    * raw_mcmurdo_sites_[file name].csv

Output:
    * cleaned_mcmurdo.csv
"""

import os
import pandas as pd
import numpy as np
from collections import namedtuple
from swatch_utils import restructure


parameters = namedtuple("parameters", "name fraction speciation unit")
parameters = {
    "DIC (mg C/L)": parameters(
        "Inorganic carbon",
        "Filtered",
        np.nan,
        "mg/l",
    ),
    "pH": parameters(
        "pH",
        np.nan,
        np.nan,
        "None",
    ),
    "ca_mgl": parameters(
        "Calcium",
        "Filtered",
        np.nan,
        "mg/l",
    ),
    "cl_mgl": parameters(
        "Chloride",
        "Filtered",
        np.nan,
        "mg/l",
    ),
    "f_mgl": parameters(
        "Fluoride",
        "Filtered",
        np.nan,
        "mg/l",
    ),
    "k_mgl": parameters(
        "Potassium",
        "Filtered",
        np.nan,
        "mg/l",
    ),
    "mg_mgl": parameters(
        "Magnesium",
        "Filtered",
        np.nan,
        "mg/l",
    ),
    "na_mgl": parameters(
        "Sodium",
        "Filtered",
        np.nan,
        "mg/l",
    ),
    "so4_mgl": parameters(
        "Sulfate",
        "Filtered",
        np.nan,
        "mg/l",
    ),
    "doc_mgl": parameters(
        "Organic carbon",
        "Filtered",
        np.nan,
        "mg/l",
    ),
    "nh4_um": parameters(
        "Ammonium",
        "Filtered",
        "as Unspecified",
        "mmol/L",
    ),
    "no2_um": parameters(
        "Nitrite",
        "Filtered",
        "as Unspecified",
        "mmol/L",
    ),
    "no3_um": parameters(
        "Nitrate",
        "Filtered",
        "as Unspecified",
        "mmol/L",
    ),
    "alkalinity_meql": parameters(
        "Alkalinity, total",  # assumed to be total alkalinity
        "Unspecified",
        "as Unspecified",
        "ueq/L",
    ),
    "n_nh4_ugl": parameters(
        "Ammonium",
        "Filtered",
        "as Unspecified",
        "ug/l",
    ),
    "n_no2_ugl": parameters(
        "Nitrite",
        "Filtered",
        "as Unspecified",
        "ug/l",
    ),
    "n_no3_ugl": parameters(
        "Nitrate",
        "Filtered",
        "as Unspecified",
        "ug/l",
    ),
    "DIC (mg C/l)": parameters(
        "Inorganic carbon",
        "Filtered",
        np.nan,
        "mg/l",
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

parameters_unit = dict()
for i in parameters:
    parameters_unit[i] = parameters[i].unit


print("Importing data...")


path = r"/home/lobke/Boggart/Files/Manuscripts/SWatCh/Data and Scripts/scripts"
directory = os.listdir(path)

df_sites = pd.DataFrame()
df_samples = pd.DataFrame()


""" Create sites dataframe
Column headers are not standardized; thus, they are renamed to allow
datasets to be merged together.
"""
for file in directory:
    if file == "raw_mcmurdo_sites_lakedsc.csv":
        import_map = {
            "Lake": str,
            "lake code": str,
            "Latitude": float,
            "Longitude": float,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
        )
        rename_map = {
            "Lake": "MonitoringLocationName",
            "lake code": "MonitoringLocationID",
            "Latitude": "MonitoringLocationLatitude",
            "Longitude": "MonitoringLocationLongitude",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")
        df_temp["MonitoringLocationType"] = "Lake/Pond"
        df_sites = pd.concat([df_sites, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_sites_mcmlter-lake-limno-runs-20220321.csv":
        import_map = {
            "location_name": str,
            "location_code": str,
            "lat": float,
            "lon": float,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
        )
        rename_map = {
            "location_name": "MonitoringLocationName",
            "location_code": "MonitoringLocationID",
            "lat": "MonitoringLocationLatitude",
            "lon": "MonitoringLocationLongitude",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")
        df_temp["MonitoringLocationType"] = "Lake/Pond"
        df_sites = pd.concat([df_sites, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_sites_mcmlter-strm-locations-20220201.csv":
        import_map = {
            "location": str,
            "strmgageid": str,
            "latitude": float,
            "longitude": float,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
        )
        rename_map = {
            "location": "MonitoringLocationName",
            "strmgageid": "MonitoringLocationID",
            "latitude": "MonitoringLocationLatitude",
            "longitude": "MonitoringLocationLongitude",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")
        df_temp["MonitoringLocationType"] = "River/Stream"
        df_sites = pd.concat([df_sites, df_temp], sort=True, ignore_index=True)

# remove duplicates
df_sites = df_sites.drop_duplicates(subset=["MonitoringLocationID"], keep="first")


""" Create samples dataframe
Some aspects of the data are formatted (i.e., harmonized) to allow merging.
"""
for file in directory:
    if file == "raw_mcmurdo_samples_LIMNO_DIC_1.csv":
        import_map = {
            "LOCATION CODE": str,
            "DATE_TIME": str,
            "DEPTH (m)": float,
            "DIC (mg C/L)": float,
            "DIC COMMENTS": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["DATE_TIME"],
        )
        rename_map = {
            "LOCATION CODE": "MonitoringLocationID",
            "DATE_TIME": "ActivityStartDate",
            "DEPTH (m)": "ActivityDepthHeightMeasure",
            "DIC (mg C/L)": "ResultValue",
            "DIC COMMENTS": "ResultComment",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")
        df_temp["ActivityDepthHeightUnit"] = "m"
        df_temp["CharacteristicName"] = "DIC (mg C/L)"
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Priscu, J. 2018. Dissolved inorganic carbon (DIC) concentrations in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1993-2017, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/e68682ea6614259b4f091be206a773b8. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/dissolved-inorganic-carbon-dic-concentrations-discrete-water-column-samples-collected-lakes"

        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_LIMNO_PH_2.csv":
        import_map = {
            "LOCATION CODE": str,
            "DATE_TIME": str,
            "DEPTH (m)": float,
            "pH": float,
            "pH COMMENTS": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["DATE_TIME"],
        )
        rename_map = {
            "LOCATION CODE": "MonitoringLocationID",
            "DATE_TIME": "ActivityStartDate",
            "DEPTH (m)": "ActivityDepthHeightMeasure",
            "pH": "ResultValue",
            "pH COMMENTS": "ResultComment",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")
        df_temp["ActivityDepthHeightUnit"] = "m"
        df_temp["CharacteristicName"] = "pH"
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Priscu, J. 2019. Hydrogen ion concentrations (pH) in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1993-2018, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/a0c17e313c63f6b5e5e5e071e5ba6b4a. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/hydrogen-ion-concentrations-ph-discrete-water-column-samples-collected-lakes-mcmurdo-dry"

        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_mcmlter-lake-chemistry-20220207.csv":
        import_map = {
            "location_code": str,
            "date_time": str,
            "depth_m": float,
            "sample_comments": str,
            "na_mgl": float,
            "na_comments": str,
            "k_mgl": float,
            "k_comments": str,
            "mg_mgl": float,
            "mg_comments": str,
            "ca_mgl": float,
            "ca_comments": str,
            "f_mgl": float,
            "f_comments": str,
            "cl_mgl": float,
            "cl_comments": str,
            "so4_mgl": float,
            "so4_comments": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["date_time"],
        )

        # combine comment columns so dataframe can be restructured
        comments_list = list(filter(lambda x: "comments" in x, import_map.keys()))
        comments_list.remove("sample_comments")
        values_list = list(filter(lambda x: "mgl" in x, import_map.keys()))

        for comment, col in zip(comments_list, values_list):
            df_temp[col] = (
                df_temp[col].astype(str)
                + "---"
                + df_temp[comment].astype(str)
                + " "
                + df_temp["sample_comments"].astype(str)
            )

        df_temp = df_temp.drop(comments_list, axis="columns")
        df_temp = df_temp.drop("sample_comments", axis="columns")

        # format - dataframe structure
        df_temp = restructure.df(
            df_temp,
            [
                "location_code",
                "date_time",
                "depth_m",
            ],
        )
        df_temp = df_temp[~df_temp["ResultValue"].isnull()]

        rename_map = {
            "location_code": "MonitoringLocationID",
            "date_time": "ActivityStartDate",
            "depth_m": "ActivityDepthHeightMeasure",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")

        df_temp["ActivityDepthHeightUnit"] = "m"
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Priscu, J., Welch, K.A., Lyons, W. 2022. Ion concentrations in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1991-2019, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/31f7354d1a05679eb3ce7c384c6e2b22. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/ion-concentrations-discrete-water-column-samples-collected-lakes-mcmurdo-dry-valleys"

        # extract comments from values
        df_temp["ResultComment"] = df_temp["ResultValue"].str.split("---").str[1]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.replace(" nan", "")
        df_temp["ResultComment"] = df_temp["ResultComment"].str.replace("nan ", "")
        df_temp["ResultValue"] = df_temp["ResultValue"].str.split("---").str[0]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.strip()
        df_temp = df_temp[~df_temp["ResultValue"].str.contains("nan")]

        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_mcmlter-lake-doc-20220321.csv":
        import_map = {
            "location_code": str,
            "date_time": str,
            "depth_m": float,
            "doc_mgl": float,
            "doc_comments": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["date_time"],
        )

        rename_map = {
            "location_code": "MonitoringLocationID",
            "date_time": "ActivityStartDate",
            "depth_m": "ActivityDepthHeightMeasure",
            "doc_mgl": "ResultValue",
            "doc_comments": "ResultComment",
        }
        df_temp["ActivityDepthHeightUnit"] = "m"
        df_temp["CharacteristicName"] = "doc_mgl"
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Priscu, J. 2022. Dissolved organic carbon (DOC) concentrations in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1993-2022, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/a5d82d5d2167679c8ecff0d8ad06c0ee. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/dissolved-organic-carbon-concentrations-lakes"

        df_temp = df_temp.rename(rename_map, axis="columns")
        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_mcmlter-lake-nutrients-20220317.csv":
        import_map = {
            "location_code": str,
            "date_time": str,
            "depth_m": float,
            "sample_comments": str,
            "nh4_um": float,
            "nh4_comments": str,
            "no2_um": float,
            "no2_comments": str,
            "no3_um": float,
            "no3_comments": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["date_time"],
        )

        # combine comment columns so dataframe can be restructured
        comments_list = list(filter(lambda x: "comments" in x, import_map.keys()))
        comments_list.remove("sample_comments")
        values_list = list(filter(lambda x: "um" in x, import_map.keys()))

        for comment, col in zip(comments_list, values_list):
            df_temp[col] = (
                df_temp[col].astype(str)
                + "---"
                + df_temp[comment].astype(str)
                + " "
                + df_temp["sample_comments"].astype(str)
            )

        df_temp = df_temp.drop(comments_list, axis="columns")
        df_temp = df_temp.drop("sample_comments", axis="columns")

        # format - dataframe structure
        df_temp = restructure.df(
            df_temp,
            [
                "location_code",
                "date_time",
                "depth_m",
            ],
        )
        df_temp = df_temp[~df_temp["ResultValue"].isnull()]

        rename_map = {
            "location_code": "MonitoringLocationID",
            "date_time": "ActivityStartDate",
            "depth_m": "ActivityDepthHeightMeasure",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")

        df_temp["ActivityDepthHeightUnit"] = "m"
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Priscu, J. 2022. Nitrogen and phosphorus concentrations in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1993-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/5cba7e25aa687c1e989c72c3ee0a0f69. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/macronutrient-concentrations-nh4-no3-no2-po4-lakes"

        # extract comments from values
        df_temp["ResultComment"] = df_temp["ResultValue"].str.split("---").str[1]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.replace(" nan", "")
        df_temp["ResultComment"] = df_temp["ResultComment"].str.replace("nan ", "")
        df_temp["ResultValue"] = df_temp["ResultValue"].str.split("---").str[0]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.strip()
        df_temp = df_temp[~df_temp["ResultValue"].str.contains("nan")]

        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_mcmlter-strm-doc-20220201.csv":
        import_map = {
            "strmgageid": str,
            "date_time": str,
            "doc_mgl": float,
            "doc_comments": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["date_time"],
        )

        rename_map = {
            "strmgageid": "MonitoringLocationID",
            "date_time": "ActivityStartDate",
            "doc_mgl": "ResultValue",
            "doc_comments": "ResultComment",
        }
        df_temp["CharacteristicName"] = "doc_mgl"
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Gooseff, M.N., Lyons, W. 2022. Dissolved organic carbon (DOC) concentrations in glacial meltwater streams, McMurdo Dry Valleys, Antarctica (1990-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/878eccb6e5c8e492f933381b8c257d79. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/dissolved-organic-carbon-doc-concentrations-glacial-meltwater-streams-mcmurdo-dry-valleys"

        df_temp = df_temp.rename(rename_map, axis="columns")
        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_mcmlter-strm-ions-20220201.csv":
        import_map = {
            "strmgageid": str,
            "date_time": str,
            "sample_comments": str,
            "na_mgl": float,
            "na_comments": str,
            "k_mgl": float,
            "k_comments": str,
            "mg_mgl": float,
            "mg_comments": str,
            "ca_mgl": float,
            "ca_comments": str,
            "f_mgl": float,
            "f_comments": str,
            "cl_mgl": float,
            "cl_comments": str,
            "so4_mgl": float,
            "so4_comments": str,
            "alkalinity_meql": float,
            "alkalinity_comments": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["date_time"],
        )

        # combine comment columns so dataframe can be restructured
        comments_list = list(filter(lambda x: "comments" in x, import_map.keys()))
        comments_list.remove("sample_comments")
        values_list = list(filter(lambda x: "mgl" in x, import_map.keys()))
        values_list.append("alkalinity_meql")

        for comment, col in zip(comments_list, values_list):
            df_temp[col] = (
                df_temp[col].astype(str)
                + "---"
                + df_temp[comment].astype(str)
                + " "
                + df_temp["sample_comments"].astype(str)
            )

        df_temp = df_temp.drop(comments_list, axis="columns")
        df_temp = df_temp.drop("sample_comments", axis="columns")

        # format - dataframe structure
        df_temp = restructure.df(
            df_temp,
            [
                "strmgageid",
                "date_time",
            ],
        )
        df_temp = df_temp[~df_temp["ResultValue"].isnull()]

        rename_map = {
            "strmgageid": "MonitoringLocationID",
            "date_time": "ActivityStartDate",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")

        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Gooseff, M.N., Lyons, W. 2022. Ion concentrations in glacial meltwater streams, McMurdo Dry Valleys, Antarctica (1993-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/275ee580f3c93f077dd7ddcce1f2ecdd. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/ion-concentrations-glacial-meltwater-streams-mcmurdo-dry-valleys-antarctica-1993-2020"

        # extract comments from values
        df_temp["ResultComment"] = df_temp["ResultValue"].str.split("---").str[1]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.replace(" nan", "")
        df_temp["ResultComment"] = df_temp["ResultComment"].str.replace("nan ", "")
        df_temp["ResultValue"] = df_temp["ResultValue"].str.split("---").str[0]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.strip()
        df_temp = df_temp[~df_temp["ResultValue"].str.contains("nan")]

        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_mcmlter-strm-nutrients-20220201.csv":
        import_map = {
            "strmgageid": str,
            "date_time": str,
            "analytical_method": str,
            "sample_comments": str,
            "n_no3_ugl": float,
            "n_no3_comments": str,
            "n_no2_ugl": float,
            "n_no2_comments": str,
            "n_nh4_ugl": float,
            "n_nh4_comments": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["date_time"],
        )

        # combine comment columns so dataframe can be restructured
        comments_list = list(filter(lambda x: "comments" in x, import_map.keys()))
        comments_list.remove("sample_comments")
        values_list = list(filter(lambda x: "ugl" in x, import_map.keys()))

        for comment, col in zip(comments_list, values_list):
            df_temp[col] = (
                df_temp[col].astype(str)
                + "---"
                + df_temp[comment].astype(str)
                + " "
                + df_temp["sample_comments"].astype(str)
            )

        df_temp = df_temp.drop(comments_list, axis="columns")
        df_temp = df_temp.drop("sample_comments", axis="columns")

        # format - dataframe structure
        df_temp = restructure.df(
            df_temp,
            [
                "strmgageid",
                "date_time",
                "analytical_method",
            ],
        )
        df_temp = df_temp[~df_temp["ResultValue"].isnull()]

        rename_map = {
            "strmgageid": "MonitoringLocationID",
            "date_time": "ActivityStartDate",
            "analytical_method": "ResultAnalyticalMethodName",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Gooseff, M.N., Lyons, W. 2022. Nitrogen and phosphorus concentrations in glacial meltwater streams, McMurdo Dry Valleys, Antarctica (1993-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/f6131f5ef67901bc98027e9df55ec364. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/nitrogen-and-phosphorus-concentrations-glacial-meltwater-streams-mcmurdo-dry-valleys"

        # extract comments from values
        df_temp["ResultComment"] = df_temp["ResultValue"].str.split("---").str[1]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.replace(" nan", "")
        df_temp["ResultComment"] = df_temp["ResultComment"].str.replace("nan ", "")
        df_temp["ResultValue"] = df_temp["ResultValue"].str.split("---").str[0]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.strip()
        df_temp = df_temp[~df_temp["ResultValue"].str.contains("nan")]

        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_relcchem.csv":
        import_map = {
            "Location Code": str,
            "Date": str,
            "Comments": str,
            "DOC (mg/L)": float,
            "Na (mg/L)": float,
            "K (mg/L)": float,
            "Mg (mg/L)": float,
            "Ca (mg/L)": float,
            "F (mg/L)": float,
            "Cl (mg/L)": float,
            "SO4 (mg/L)": float,
            "meq alk": float,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["Date"],
        )

        # format - dataframe structure
        df_temp = restructure.df(
            df_temp,
            [
                "Location Code",
                "Date",
                "Comments",
            ],
        )
        df_temp = df_temp[~df_temp["ResultValue"].isnull()]

        rename_map = {
            "Location Code": "MonitoringLocationID",
            "Date": "ActivityStartDate",
            "Comments": "ResultComment",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Lyons, W., Mcknight, D.M. 2015. Stream Chemistry for Reactivated Channel. Environmental Data Initiative. DOI: 10.6073/pasta/ed143e49e82d0aaa1494447ebcee17c1. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/stream-chemistry-reactivated-channel"

        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_relcnutr.csv":
        import_map = {
            "Location Code": str,
            "Date": str,
            "Method": str,
            "NO2 (mg/l)": float,
            "NO2 Comments": str,
            "NO3 (mg/l)": float,
            "NO3 Comments": str,
            "NH4 (mg/l)": float,
            "NH4 Comments": str,
            "DIC (mg C/l)": float,
            "DIC Comments": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["Date"],
            encoding="ISO-8859-14",
        )

        # combine comment columns so dataframe can be restructured
        comments_list = list(filter(lambda x: "Comments" in x, import_map.keys()))
        values_list = list(filter(lambda x: "mg" in x, import_map.keys()))

        for comment, col in zip(comments_list, values_list):
            df_temp[col] = (
                df_temp[col].astype(str) + "---" + df_temp[comment].astype(str)
            )

        df_temp = df_temp.drop(comments_list, axis="columns")

        # format - dataframe structure
        df_temp = restructure.df(
            df_temp,
            [
                "Location Code",
                "Date",
                "Method",
            ],
        )
        df_temp = df_temp[~df_temp["ResultValue"].isnull()]

        rename_map = {
            "Location Code": "MonitoringLocationID",
            "Date": "ActivityStartDate",
            "Method": "ResultAnalyticalMethodName",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Lyons, W. 2015. Stream Nutrients for Reactivated Channel. Environmental Data Initiative. DOI: 10.6073/pasta/b3d212996e5e4cb7f91b82090b4f550d. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/stream-nutrients-reactivated-channel"

        # extract comments from values
        df_temp["ResultComment"] = df_temp["ResultValue"].str.split("---").str[1]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.replace(" nan", "")
        df_temp["ResultValue"] = df_temp["ResultValue"].str.split("---").str[0]
        df_temp["ResultComment"] = df_temp["ResultComment"].str.strip()
        df_temp = df_temp[~df_temp["ResultValue"].str.contains("nan")]

        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)

    if file == "raw_mcmurdo_samples_strmdic.csv":
        import_map = {
            "strmgageid": str,
            "Date": str,
            "Method": str,
            "DIC (mg C/l)": float,
            "DIC Comments": str,
        }
        df_temp = pd.read_csv(
            file,
            usecols=import_map.keys(),
            dtype=import_map,
            parse_dates=["Date"],
        )

        rename_map = {
            "strmgageid": "MonitoringLocationID",
            "Date": "ActivityStartDate",
            "Method": "ResultAnalyticalMethodName",
            "DIC (mg C/l)": "ResultValue",
            "DIC Comments": "ResultComment",
        }
        df_temp = df_temp.rename(rename_map, axis="columns")
        df_temp["CharacteristicName"] = "DIC (mg C/l)"
        df_temp[
            "DatasetName"
        ] = "McMurdo Dry Valleys LTER: Lyons, W. 2015. Dissolved Inorganic Carbon in Streams. Environmental Data Initiative. DOI: 10.6073/pasta/4d64208bd91fc6a336c9c388436b1634. Dataset accessed 4 April 2022."
        df_temp[
            "ResultAnalyticalMethodName"
        ] = "variable; described here: https://mcm.lternet.edu/content/dissolved-inorganic-carbon-streams"

        df_samples = pd.concat([df_samples, df_temp], sort=True, ignore_index=True)


# remove sample duplicates
df_samples = df_samples.drop_duplicates(
    subset=[
        "MonitoringLocationID",
        "ActivityStartDate",
        "ResultValue",
    ],
    keep="first",
)

# merge site and sample data
df = pd.merge(df_samples, df_sites, on="MonitoringLocationID", how="inner")


print("Extracting data...")

# extract locations with sites info
df = df.dropna(
    axis="index",
    how="any",
    subset=[
        "MonitoringLocationLatitude",
        "MonitoringLocationLongitude",
    ],
)
df["MonitoringLocationID"] = np.where(
    df["MonitoringLocationID"].isnull(),
    df["MonitoringLocationName"],
    df["MonitoringLocationID"],
)


# format - dates and times
df["ActivityStartTime"] = df["ActivityStartDate"].dt.time
df["ActivityStartDate"] = df["ActivityStartDate"].dt.date


# format - parameter naming
df["ResultSampleFraction"] = df["CharacteristicName"]
df["ResultSampleFraction"] = df["ResultSampleFraction"].replace(parameters_fractions)

df["MethodSpeciation"] = df["CharacteristicName"]
df["MethodSpeciation"] = df["MethodSpeciation"].replace(parameters_speciation)

df["ResultUnit"] = df["CharacteristicName"]
df["ResultUnit"] = df["ResultUnit"].replace(parameters_unit)

df["CharacteristicName"] = df["CharacteristicName"].replace(parameters_name)


# format - add missing required columns
df["ActivityMediaName"] = "Surface Water"
df["ActivityType"] = np.where(
    df["CharacteristicName"] == "pH",
    "Field Msr/Obs",  # assume measured in the field
    "Sample-Routine",
)
df["LaboratoryName"] = "Unspecified"
df["MonitoringLocationHorizontalCoordinateReferenceSystem"] = "WGS84"
df["ResultAnalyticalMethodContext"] = np.nan
df["ResultAnalyticalMethodID"] = np.nan
df["ResultValueType"] = "Actual"  # assumed
df["ResultStatusID"] = "Preliminary"  # not feasible to go through all of the comments


# format - below detection limit notation
detect_limit_map = {
    "MCM: below detection 0.1": 0.1,
    "below detection 0.4": 0.4,
    "below detection 0.3": 0.3,
    "below detection 0.1": 0.1,
    "below detection 0.2": 0.2,
    '"Lachat: no sample;  MCM: below detection 0.1"': 0.1,
    "Lachat: below detection 0.3": 0.3,
    "no detect; DOC detection limit = 0.503 mg C/L": 0.503,
    "no detect; DOC detection limit = 1 mg C/L": 1,
    "ND - DOC detection limit = 0.20 mg/L": 0.20,
    "ND - detection limit 0.2mg/L": 0.2,
    "no detect; DOC detection limit = 0.503 mg C/L; changed sampling time from 15:30 to 12:25 - this was time ions and nutrients were sampled - also 1/8/03 at 15:30 was time Mariah sampled and same people sampled both Mariah and Green and can't be in two places at one time": 0.503,
    "changed sampling time from no time noted to 16:25 - this matches time for nutrient sample and time that field note says samples were collected; no detect; DOC detection limit = 0.503 mg C/L": 0.503,
    "< 0.1mg/L below detections": 0.1,
    "NH4 detection limit = 0.14 umol/L": 0.14,
    "no detect; DOC detection limit = 0.503 mg C/L; changed sampling time from no sampling time noted to 11:50 - this was time noted on field sheet that water quality sample collected": 0.503,
    "no detect; DOC detection limit = 0.503 mg C/L.  Original date was 2003 - changed to 2002": 0.503,
    "no detect; DOC detection limit = 0.503 mg C/L; changed sampling date from 1/5/03 to 1/2/03 - field note calendar showed that on 1/2/03 did Wright Valley and Lost Seal not Andersen and sampling time exactly same for the two different dates": 0.503,
    "no detect; DOC detection limit = 0.503 mg C/L; changed sampling time from 21:00 to 13:00 - this matches times for other samples and time noted for water quality collection on field notes - also same people collecting water quality sample from Lost Seal on 1/2/03 at 21:00 so couldn't have been in Wright Valley as well": 0.503,
    "no detect; DOC detection limit = 0.503 mg C/L; changed sampling date from 12/20/02 to 12/30/02 - sampling time exactly the same as 12/30 Bohner ion and nutrient and field note flow dates and field calendar shows that at McMurdo Station on 12/20/02 so couldn't have sampled Bohner on that date": 0.503,
    "Not detected": np.nan,
    "NH4 below detection limit": np.nan,
    "(Not detected)USARP Crary Lab Analytical group (Jack Sheets et al.)": np.nan,
    "(not-detected)USARP Crary Lab Analytical group (Jack Sheets et al.)": np.nan,
}

# create detection limit column
df["ResultDetectionCondition"] = np.where(
    df["ResultComment"].str.contains("detect"),
    "Below Detection/Quantification Limit",
    "",
)

df["ResultDetectionQuantitationLimitMeasure"] = df["ResultComment"]

df["ResultDetectionQuantitationLimitMeasure"] = np.where(
    df["ResultComment"].str.contains("detect"),
    df["ResultDetectionQuantitationLimitMeasure"],
    np.nan,
)

df["ResultDetectionQuantitationLimitMeasure"] = df[
    "ResultDetectionQuantitationLimitMeasure"
].replace(detect_limit_map)


# create detection limit unit column
df["ResultDetectionQuantitationLimitMeasure"] = np.where(
    (df["ResultDetectionCondition"] == "Below Detection/Quantification Limit")
    & (df["ResultDetectionQuantitationLimitMeasure"].isnull()),
    df["ResultValue"],
    df["ResultDetectionQuantitationLimitMeasure"],
)

df["ResultDetectionQuantitationLimitUnit"] = np.where(
    df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
    df["ResultUnit"],
    "",
)


# create detection limit type column
df["ResultDetectionQuantitationLimitType"] = np.where(
    df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
    "Reporting Limit",
    "",
)


# remove result info when values are below detection limit
df["ResultValue"] = np.where(
    df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
    np.nan,
    df["ResultValue"],
)

df["ResultUnit"] = np.where(
    df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
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

df.to_csv("cleaned_mcmurdo.csv", index=False, encoding="utf-8")

print("Done!")
