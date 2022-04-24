""" Merge and harmonize cleaned data

Approach:
    * Import and merge data
    * Harmonize data
    * Remove duplicates
    * Flag dubious values
    * Flag outliers
    * Correct wrong coordinates
    * Create datasets for mapping and publication

Input:
    * cleaned_[database name].csv
    * sites_wrong_coordinates_checkedbyLilyBaraclough.csv

Output:
    * SWatCh Sites with GEMStat.csv - use for analysis
    * SWatCh with GEMStat.csv - use for mapping
    * SWatCh.csv - use for publication
    * SWatCh Sites.csv - use to create shapefile for publication
"""

import os
import warnings
import pandas as pd
import numpy as np
from tqdm import tqdm
from pyproj import Transformer
from swatch_utils import convert
from statsmodels.robust import scale as scale


""" Transform coordinates into WGS 1984

Input: pandas dataframe grouped by coordinate reference system
"""
coordinate_espg_map = {
    "NAD27": "epsg:4267",
    "NAD83": "epsg:4269",
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


""" Funtion to flag outliers

Use median average deviation (MAD) is suitable for data with large outliers
and/or skewed distributions.

Input: dataframe grouped by site, parameter, fraction, and speciation
"""


def flag_outlier(group):
    # Runtime warning due to nans in data
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    stats_group = group[group["ResultStatusID"] != "Rejected"]
    median = np.median(stats_group["ResultValue"])
    MAD = scale.mad(stats_group["ResultValue"], center=median)
    outlier_high = median + 4 * MAD
    outlier_low = median - 4 * MAD
    group["ResultComment"] = np.where(
        group["ResultValue"] > outlier_high,
        group["ResultComment"].astype(str)
        + "---potential outlier, value greater than four times the median average deviation",
        group["ResultComment"],
    )
    group["ResultComment"] = np.where(
        group["ResultValue"] < outlier_low,
        group["ResultComment"].astype(str)
        + "---potential outlier, value less than four times the median average deviation",
        group["ResultComment"],
    )
    return group


print("Harmonizing data...")

path = r"/home/lobke/Boggart/Files/Manuscripts/SWatCh/Data and Scripts/scripts"
directory = os.listdir(path)

df_final = pd.DataFrame()
for file in tqdm(directory):
    if "cleaned_" in file:
        df = pd.read_csv(
            file,
            dtype={
                "ActivityDepthHeightMeasure": float,
                "ActivityDepthHeightUnit": str,
                "ActivityMediaName": str,
                "ActivityStartDate": str,
                "ActivityStartTime": str,
                "ActivityType": str,
                "CharacteristicName": str,
                "DatasetName": str,
                "LaboratoryName": str,
                "MethodSpeciation": str,
                "MonitoringLocationHorizontalCoordinateReferenceSystem": str,
                "MonitoringLocationID": str,
                "MonitoringLocationLatitude": float,
                "MonitoringLocationLongitude": float,
                "MonitoringLocationName": str,
                "MonitoringLocationType": str,
                "ResultAnalyticalMethodContext": str,
                "ResultAnalyticalMethodID": str,
                "ResultAnalyticalMethodName": str,
                "ResultComment": str,
                "ResultDetectionCondition": str,
                "ResultDetectionQuantitationLimitMeasure": float,
                "ResultDetectionQuantitationLimitType": str,
                "ResultDetectionQuantitationLimitUnit": str,
                "ResultSampleFraction": str,
                "ResultStatusID": str,
                "ResultUnit": str,
                "ResultValue": float,
                "ResultValueType": str,
            },
            parse_dates=["ActivityStartDate"],
        )

        # harmonize - coordinate reference system (into WGS84)
        df = df.groupby(
            ["MonitoringLocationHorizontalCoordinateReferenceSystem"]
        ).apply(convert_coordinates)
        df["MonitoringLocationHorizontalCoordinateReferenceSystem"] = "WGS84"

        # harmonize - speciation

        # NO2 as N to NO2 ion
        df["ResultValue"] = np.where(
            (df["MethodSpeciation"] == "as N")
            & (df["CharacteristicName"] == "Nitrite"),
            df["ResultValue"] * 3.2845,
            df["ResultValue"],
        )
        df["MethodSpeciation"] = np.where(
            (df["MethodSpeciation"] == "as N")
            & (df["CharacteristicName"] == "Nitrite"),
            "as NO2",
            df["MethodSpeciation"],
        )

        # NO3 as N to NO3 ion
        df["ResultValue"] = np.where(
            (df["MethodSpeciation"] == "as N")
            & (df["CharacteristicName"] == "Nitrate"),
            df["ResultValue"] * 4.4268,
            df["ResultValue"],
        )
        df["MethodSpeciation"] = np.where(
            (df["MethodSpeciation"] == "as N")
            & (df["CharacteristicName"] == "Nitrate"),
            "as NO3",
            df["MethodSpeciation"],
        )

        # NH4 as N to NH4 ion
        df["ResultValue"] = np.where(
            (df["MethodSpeciation"] == "as N")
            & (df["CharacteristicName"] == "Ammonium"),
            df["ResultValue"] * 1.2878,
            df["ResultValue"],
        )
        df["MethodSpeciation"] = np.where(
            (df["MethodSpeciation"] == "as N")
            & (df["CharacteristicName"] == "Ammonium"),
            "as NH4",
            df["MethodSpeciation"],
        )

        # PO4 as P to PO4 ion
        df["ResultValue"] = np.where(
            df["MethodSpeciation"] == "as P",
            df["ResultValue"] * 3.06,
            df["ResultValue"],
        )
        df["MethodSpeciation"] = np.where(
            df["MethodSpeciation"] == "as P",
            "as PO4",
            df["MethodSpeciation"],
        )

        # CaCO3 to CO3
        df["ResultValue"] = np.where(
            (df["MethodSpeciation"] == "as CaCO3")
            & (
                df["CharacteristicName"].isin(
                    [
                        "Alkalinity, Phenolphthalein (total hydroxide+1/2 carbonate)",
                        "Alkalinity, carbonate",
                        "Hardness, carbonate",
                        "Total hardness",
                        "Hardness, Calcium",
                        "Hardness, non-carbonate",
                    ]
                )
            ),
            df["ResultValue"] * 0.60,
            df["ResultValue"],
        )
        df["MethodSpeciation"] = np.where(
            (df["MethodSpeciation"] == "as CaCO3")
            & (
                df["CharacteristicName"].isin(
                    [
                        "Alkalinity, Phenolphthalein (total hydroxide+1/2 carbonate)",
                        "Alkalinity, carbonate",
                        "Hardness, carbonate",
                        "Total hardness",
                        "Hardness, Calcium",
                        "Hardness, non-carbonate",
                    ]
                )
            ),
            "as CO3",
            df["MethodSpeciation"],
        )

        # harmonize - units

        # add temporary placeholder so that empty cells are not dropped
        df["ResultUnit"] = np.where(
            df["ResultUnit"].isnull(),
            "temp",
            df["ResultUnit"],
        )
        df["ResultDetectionQuantitationLimitUnit"] = np.where(
            df["ResultDetectionQuantitationLimitUnit"].isnull(),
            "temp",
            df["ResultDetectionQuantitationLimitUnit"],
        )

        df = df.groupby(["CharacteristicName", "ResultUnit"]).apply(
            convert.chemistry,
            param_col="CharacteristicName",
            unit_col="ResultUnit",
            value_col="ResultValue",
        )

        df = df.groupby(
            ["CharacteristicName", "ResultDetectionQuantitationLimitUnit"]
        ).apply(
            convert.chemistry,
            param_col="CharacteristicName",
            unit_col="ResultDetectionQuantitationLimitUnit",
            value_col="ResultDetectionQuantitationLimitMeasure",
        )

        # remove temporary placeholder
        df["ResultUnit"] = np.where(
            df["ResultUnit"] == "temp",
            np.nan,
            df["ResultUnit"],
        )
        df["ResultDetectionQuantitationLimitUnit"] = np.where(
            df["ResultDetectionQuantitationLimitUnit"] == "temp",
            np.nan,
            df["ResultDetectionQuantitationLimitUnit"],
        )

        # fill missing values so empty cells are not dropped
        df["MethodSpeciation"] = np.where(
            df["MethodSpeciation"].isnull(),
            "as Unspecified",
            df["MethodSpeciation"],
        )
        df["ResultSampleFraction"] = np.where(
            df["ResultSampleFraction"].isnull(),
            "Unspecified",
            df["ResultSampleFraction"],
        )

        # some pH values are listed as (un)filtered
        df["ResultSampleFraction"] = np.where(
            df["CharacteristicName"] == "pH",
            "Unspecified",
            df["ResultSampleFraction"],
        )

        """ total recoverable is concidered the same as total:
        https://www.svl.net/2021/03/the-argument-on-total-metals/
        """
        df["ResultSampleFraction"] = np.where(
            df["ResultSampleFraction"] == "Total Recoverable",
            "Filtered",
            df["ResultSampleFraction"],
        )

        # reject impossible values
        df["ResultStatusID"] = np.where(
            (df["ResultValue"] <= 0)
            & (
                ~df["CharacteristicName"].isin(
                    [
                        "Alkalinity, Phenolphthalein (total hydroxide+1/2 carbonate)",
                        "Alkalinity, carbonate",
                        "Alkalinity, total",
                        "Temperature, water",
                        "Gran acid neutralizing capacity",
                    ]
                )
            ),
            "Rejected",
            df["ResultStatusID"],
        )

        # flag potential outliers
        df = df.groupby(
            [
                "MonitoringLocationID",
                "CharacteristicName",
                "MethodSpeciation",
                "ResultSampleFraction",
            ]
        ).apply(flag_outlier)

        # add temporary column for processing; remove after
        df["DatasetName_short"] = df["DatasetName"].str.split(";").str[0]

        df_final = pd.concat([df, df_final], sort=True, ignore_index=True)

# remove duplicates
df_final = df_final.drop_duplicates(
    subset=[
        "MonitoringLocationID",
        "CharacteristicName",
        "ResultSampleFraction",
        "MethodSpeciation",
        "ResultValue",
        "ActivityStartDate",
    ],
    keep="first",
)

# correct wrong coordinates
correct_coordinates = pd.read_csv(
    "SWatCh Sites with GEMStat Corrected Coordinates Mapping Output.csv",
    usecols=[
        "MonitoringLocationID",
        "MonitoringLocationLatitude",
        "MonitoringLocationLongitude",
    ],
)
df_final = pd.merge(
    df_final, correct_coordinates, on="MonitoringLocationID", how="right"
)

df_final = df_final.drop(
    ["MonitoringLocationLatitude_x", "MonitoringLocationLongitude_x"], axis="columns"
)

df_final = df_final.rename(
    {
        "MonitoringLocationLatitude_y": "MonitoringLocationLatitude",
        "MonitoringLocationLongitude_y": "MonitoringLocationLongitude",
    },
    axis="columns",
)

df_final = df_final[df_final["ResultSampleFraction"] != "Free Available"]

# create sites dataset for mapping
df_sites = df_final
df_sites = df_sites[
    [
        "MonitoringLocationID",
        "MonitoringLocationName",
        "MonitoringLocationType",
        "MonitoringLocationHorizontalCoordinateReferenceSystem",
        "MonitoringLocationLatitude",
        "MonitoringLocationLongitude",
        "DatasetName_short",
    ]
].reset_index()

# sort dataset by order; national datasets first
dataset_order = {
    "ECCC National Long-Term Water Quality Monitoring Data": 0,
    "Water Quality Portal": 1,
    "European Environment Agency (EEA) Waterbase": 2,
    "McMurdo Dry Valleys LTER: Priscu, J. 2022. Nitrogen and phosphorus concentrations in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1993-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/5cba7e25aa687c1e989c72c3ee0a0f69. Dataset accessed 4 April 2022.": 3,
    "McMurdo Dry Valleys LTER: Priscu, J., Welch, K.A., Lyons, W. 2022. Ion concentrations in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1991-2019, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/31f7354d1a05679eb3ce7c384c6e2b22. Dataset accessed 4 April 2022.": 4,
    "McMurdo Dry Valleys LTER: Priscu, J. 2019. Hydrogen ion concentrations (pH) in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1993-2018, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/a0c17e313c63f6b5e5e5e071e5ba6b4a. Dataset accessed 4 April 2022.": 5,
    "McMurdo Dry Valleys LTER: Lyons, W. 2015. Dissolved Inorganic Carbon in Streams. Environmental Data Initiative. DOI: 10.6073/pasta/4d64208bd91fc6a336c9c388436b1634. Dataset accessed 4 April 2022.": 6,
    "McMurdo Dry Valleys LTER: Gooseff, M.N., Lyons, W. 2022. Nitrogen and phosphorus concentrations in glacial meltwater streams, McMurdo Dry Valleys, Antarctica (1993-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/f6131f5ef67901bc98027e9df55ec364. Dataset accessed 4 April 2022.": 7,
    "McMurdo Dry Valleys LTER: Gooseff, M.N., Lyons, W. 2022. Ion concentrations in glacial meltwater streams, McMurdo Dry Valleys, Antarctica (1993-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/275ee580f3c93f077dd7ddcce1f2ecdd. Dataset accessed 4 April 2022.": 8,
    "GloRiCh": 9,
    "GEMStat": 10,
}
df_sites.iloc[df_sites["DatasetName_short"].map(dataset_order).sort_values().index]
df_sites = df_sites.drop_duplicates(subset="MonitoringLocationID", keep="last")


# create datasets for publication (no GEMStat)
df_no_gemstat = df_final
df_no_gemstat = df_no_gemstat[df_no_gemstat["DatasetName_short"] != "GEMStat"]

df_sites_no_gemstat = df_no_gemstat[
    [
        "MonitoringLocationID",
        "MonitoringLocationName",
        "MonitoringLocationType",
        "MonitoringLocationHorizontalCoordinateReferenceSystem",
        "MonitoringLocationLatitude",
        "MonitoringLocationLongitude",
        "DatasetName_short",
    ]
].reset_index()

# sort dataset by order; national datasets first
dataset_order = {
    "ECCC National Long-Term Water Quality Monitoring Data": 0,
    "Water Quality Portal": 1,
    "European Environment Agency (EEA) Waterbase": 2,
    "McMurdo Dry Valleys LTER: Priscu, J. 2022. Nitrogen and phosphorus concentrations in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1993-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/5cba7e25aa687c1e989c72c3ee0a0f69. Dataset accessed 4 April 2022.": 3,
    "McMurdo Dry Valleys LTER: Priscu, J., Welch, K.A., Lyons, W. 2022. Ion concentrations in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1991-2019, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/31f7354d1a05679eb3ce7c384c6e2b22. Dataset accessed 4 April 2022.": 4,
    "McMurdo Dry Valleys LTER: Priscu, J. 2019. Hydrogen ion concentrations (pH) in discrete water column samples collected from lakes in the McMurdo Dry Valleys, Antarctica (1993-2018, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/a0c17e313c63f6b5e5e5e071e5ba6b4a. Dataset accessed 4 April 2022.": 5,
    "McMurdo Dry Valleys LTER: Lyons, W. 2015. Dissolved Inorganic Carbon in Streams. Environmental Data Initiative. DOI: 10.6073/pasta/4d64208bd91fc6a336c9c388436b1634. Dataset accessed 4 April 2022.": 6,
    "McMurdo Dry Valleys LTER: Gooseff, M.N., Lyons, W. 2022. Nitrogen and phosphorus concentrations in glacial meltwater streams, McMurdo Dry Valleys, Antarctica (1993-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/f6131f5ef67901bc98027e9df55ec364. Dataset accessed 4 April 2022.": 7,
    "McMurdo Dry Valleys LTER: Gooseff, M.N., Lyons, W. 2022. Ion concentrations in glacial meltwater streams, McMurdo Dry Valleys, Antarctica (1993-2020, ongoing). Environmental Data Initiative. DOI: 10.6073/pasta/275ee580f3c93f077dd7ddcce1f2ecdd. Dataset accessed 4 April 2022.": 8,
    "GloRiCh": 9,
    # "GEMStat": 10,
}

# use loc if still not working

df_sites_no_gemstat.iloc[
    df_sites_no_gemstat["DatasetName_short"].map(dataset_order).sort_values().index
]

df_sites_no_gemstat = df_sites_no_gemstat.drop_duplicates(
    subset="MonitoringLocationID", keep="last"
)

df_no_gemstat = df_no_gemstat.drop("DatasetName_short", axis="columns")
df_sites_no_gemstat = df_sites_no_gemstat.drop("DatasetName_short", axis="columns")
df_final = df_final.drop("DatasetName_short", axis="columns")
df_sites = df_sites.drop("DatasetName_short", axis="columns")

# print SWatCh summary stats
print("Number of sites: {}".format(len(df_sites)))
print("Number of sites without GEMStat {}".format(len(df_sites_no_gemstat)))
print("Number of samples: {}".format(len(df_final)))
print(
    "Number of rejected samples: {}".format(
        len(df_final[df_final["ResultStatusID"] == "Rejected"])
    )
)
print(
    "Number of non-rejected samples: {}".format(
        len(df_final[df_final["ResultStatusID"] != "Rejected"])
    )
)
print(
    "Number of non-rejected outlier samples: {}".format(
        len(
            df_final[
                (df_final["ResultStatusID"] == "Rejected")
                & df_final["ResultComment"].str.contains("potential outlier")
            ]
        )
    )
)


print("Exporting data...")

df_sites.to_csv("SWatCh Sites with GEMStat.csv", index=False, encoding="utf-8")
df_final.to_csv("SWatCh with GEMStat.csv", index=False, encoding="utf-8")
df_no_gemstat.to_csv("SWatCh.csv", index=False, encoding="utf-8")
df_sites_no_gemstat.to_csv("SWatCh Sites.csv", index=False, encoding="utf-8")

print("Done!")
