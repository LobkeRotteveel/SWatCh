""" Clean sample data from NWQMC

Script to clean sample data obtained from the National Water Quality
Monitoring Council Water Quality Database.
Data source: https://www.waterqualitydata.us/portal/

Approach:
    * Merge datasets - due to the large size of this dataset, desired sites
      are extracted prior to merging the datasets
    * Extract desired data
    * Format dataset structure and contents

Notes:
    * Water in mixing zones is not included; this is and area where water
      from an effluent point source, and pollutant levels are permitted to be
      higher than in the rest of the water body.

Input:
    * raw_wqp_samples.csv
    * raw_wqp_sites.csc

Output:
    * cleaned_wqp.csv
"""

import re
import pandas as pd
import numpy as np
from tqdm import tqdm
from swatch_utils import set_type
from swatch_utils import convert
from swatch_utils import extract
from swatch_utils import check


print("Importing data...")

df_sites = pd.read_csv(
    "raw_wqp_sites.csv",
    usecols=[
        "MonitoringLocationDescriptionText",
        "OrganizationFormalName",
        "MonitoringLocationIdentifier",
        "MonitoringLocationName",
        "MonitoringLocationTypeName",
        "LatitudeMeasure",
        "LongitudeMeasure",
        "HorizontalCoordinateReferenceSystemDatumName",
        "ProviderName",
    ],
    dtype=str,
    encoding="utf-8",
    engine="python",
    on_bad_lines="skip",
)

chunked = pd.read_csv(
    "raw_wqp_samples.csv",
    usecols=[
        "ActivityTypeCode",
        "ActivityMediaName",
        "ActivityMediaSubdivisionName",
        "ActivityStartDate",
        "ActivityStartTime/Time",
        "MonitoringLocationIdentifier",
        "ActivityCommentText",
        "ResultDetectionConditionText",
        "CharacteristicName",
        "ResultSampleFractionText",
        "ResultMeasureValue",
        "ResultMeasure/MeasureUnitCode",
        "MeasureQualifierCode",
        "LaboratoryName",
        "ResultStatusIdentifier",
        "ResultValueTypeName",
        "ResultCommentText",
        "ResultDepthHeightMeasure/MeasureValue",
        "ResultDepthHeightMeasure/MeasureUnitCode",
        "ResultAnalyticalMethod/MethodIdentifierContext",
        "ResultAnalyticalMethod/MethodIdentifier",
        "ResultAnalyticalMethod/MethodName",
        "DetectionQuantitationLimitMeasure/MeasureValue",
        "DetectionQuantitationLimitMeasure/MeasureUnitCode",
    ],
    dtype=str,
    parse_dates=["ActivityStartDate"],
    encoding="utf-8",
    engine="python",
    on_bad_lines="skip",
    chunksize=1000000,
)


print("Extracting site data...")

# extract sites - good quality sites
notes_list = df_sites["MonitoringLocationDescriptionText"].unique()
poor_pattern = r"error|guess"
poor_list = []

for note in notes_list:
    if re.findall(poor_pattern, str(note)):
        poor_list.append(note)

df_sites = df_sites[~df_sites["MonitoringLocationDescriptionText"].isin(poor_list)]
df_sites = df_sites.drop("MonitoringLocationDescriptionText", axis="columns")


# extract sites - monitoring location type
sites_map = {
    "BEACH Program Site-Channelized stream": "River/Stream",
    "BEACH Program Site-Great Lake": "Lake/Pond",
    "BEACH Program Site-Lake": "Lake/Pond",
    "BEACH Program Site-River/Stream": "River/Stream",
    "Canal Drainage": "River/Stream",
    "Canal Irrigation": "River/Stream",
    "Canal Transport": "River/Stream",
    "Channelized Stream": "River/Stream",
    "Great Lake": "Lake/Pond",
    "Lake": "Lake/Pond",
    # "Reservoir", - already in correct format
    # "River/Stream", - already in correct format
    "River/Stream Ephemeral": "River/Stream",
    "River/Stream Intermittent": "River/Stream",
    "River/Stream Perennial": "River/Stream",
    "Riverine Impoundment": "Reservoir",
}
df_sites = df_sites[df_sites["MonitoringLocationTypeName"].isin(sites_map.keys())]


# extract sites - locations with lat and long information
df_sites = df_sites.dropna(
    axis="index",
    how="any",
    subset=[
        "LatitudeMeasure",
        "LongitudeMeasure",
        "MonitoringLocationIdentifier",
    ],
)

# extract sites - remove locations with too long names
df_sites = df_sites[df_sites["MonitoringLocationIdentifier"].str.len() < 34]


# extract sites - sites with known coordinate reference systems
df_sites = df_sites[df_sites["HorizontalCoordinateReferenceSystemDatumName"] != "OTHER"]


print("Cleaning site data...")

# format - combine columns where required
df_sites["ProviderName"] = (
    "Water Quality Portal; "
    + df_sites["ProviderName"].astype(str)
    + " Database; Data collected by "
    + df_sites["OrganizationFormalName"].astype(str)
)
df_sites = df_sites.drop("OrganizationFormalName", axis="columns")


# format - column names
df_sites = df_sites.rename(
    {
        "MonitoringLocationIdentifier": "MonitoringLocationID",
        # "MonitoringLocationName" - do not rename, alreaady correct format
        "MonitoringLocationTypeName": "MonitoringLocationType",
        "LatitudeMeasure": "MonitoringLocationLatitude",
        "LongitudeMeasure": "MonitoringLocationLongitude",
        "HorizontalCoordinateReferenceSystemDatumName": "MonitoringLocationHorizontalCoordinateReferenceSystem",
        "ProviderName": "DatasetName",
    },
    axis="columns",
)


# format - site type
df_sites = df_sites.replace(sites_map)


# format - coordinate reference system notation
df_sites["MonitoringLocationHorizontalCoordinateReferenceSystem"] = df_sites[
    "MonitoringLocationHorizontalCoordinateReferenceSystem"
].replace(
    {"UNKWN": "WGS84"}  # assumed
)


print("Extracting and cleaning sample data...")

for i, df in tqdm(enumerate(chunked)):

    # extract and format data - media
    df = df[df["ActivityMediaName"] == "Water"]
    df = df[df["ActivityMediaSubdivisionName"] == "Surface Water"]
    df = df.drop("ActivityMediaSubdivisionName", axis="columns")
    df["ActivityMediaName"] = "Surface Water"
    df = df[~df["ResultSampleFractionText"].isin(["Bed Sediment", "Free Available"])]

    # extract and format data - sample type
    activity_map = {
        "Field Msr/Obs": "Field Msr/Obs",
        "Field Msr/Obs-Habitat Assessment": "Field Msr/Obs",
        "Field Msr/Obs-Incidental": "Field Msr/Obs",
        "Field Msr/Obs-Portable Data Logger": "Field Msr/Obs",
        "Sample-Routine": "Sample-Routine",
        "Sample-Routine Resample": "Sample-Routine",
        np.nan: "Sample-Routine",
    }
    df = df[df["ActivityTypeCode"].isin(activity_map.keys())]
    df["ActivityTypeCode"] = df["ActivityTypeCode"].replace(activity_map)

    df = df[df["ResultValueTypeName"].isin(["Actual", "Calculated", np.nan])]
    df["ResultValueTypeName"] = df["ResultValueTypeName"].replace(
        {np.nan: "Actual"}
    )  # assumed

    bdl_list = [
        "BMDL",  # below method detection limit
        "BQL",  # below quantitation limit
        "BRL",  # below reporting limit
        "DL",  # below method detection limit
        "IDL",  # below instrument detection limit
        "K",  # below detection limit
        "KRMDL",  # below method detection limit
        "KRPQL",  # below reporting limit
        "L",  # lowest reporting limit used
        "U",  # below detection limit
        "UH",  # below detection limit
    ]
    df["ResultDetectionConditionText"] = np.where(
        df["MeasureQualifierCode"].isin(bdl_list),
        "Below Detection/Quantification Limit",
        df["ResultDetectionConditionText"],
    )

    quantitation_list = [
        "DU",  # below quantitation limit
        "GG",  # between detection and quantitation limits
    ]
    df["ResultDetectionConditionText"] = np.where(
        df["MeasureQualifierCode"].isin(quantitation_list),
        "Present Below Quantification Limit",
        df["ResultDetectionConditionText"],
    )

    status_list = [
        "CON",  # confirmed value
        "RNAF",  # result not affected by QAQC issue
        "RSM",  # sample verified by re-run
        "Z",  # value verified by re-run
    ]
    df["ResultStatusIdentifier"] = np.where(
        df["MeasureQualifierCode"].isin(status_list),
        "Validated",
        df["ResultStatusIdentifier"],
    )

    comment_map = {
        "*": "sample was warm",
        "OA3": "--- flagged as outlier in WQP",
        "OS3": "--- flagged as outlier in WQP",
        "OUT": "--- flagged as outlier in WQP",
    }
    df["ResultCommentText"] = np.where(
        df["MeasureQualifierCode"].isin(comment_map.keys()),
        df["MeasureQualifierCode"],
        df["ResultCommentText"],
    )
    df["ResultCommentText"] = df["ResultCommentText"].replace(comment_map)

    no_issue_list = [
        "NA",  # not applicable
        np.nan,
    ]

    df = df[
        (df["MeasureQualifierCode"].isin(bdl_list))
        | (df["MeasureQualifierCode"].isin(quantitation_list))
        | (df["MeasureQualifierCode"].isin(status_list))
        | (df["MeasureQualifierCode"].isin(comment_map.keys()))
        | (df["MeasureQualifierCode"].isin(no_issue_list))
    ]
    df = df.drop("MeasureQualifierCode", axis="columns")

    # format data - QAQC'ed data
    qaqc_map = {
        # "Accepted",
        # "Final",
        # "Validated",
        # "Preliminary",
        # "Rejected"
        "Provisional": "Preliminary",
        "Raw": "Preliminary",
        "Unreviewed": "Preliminary",
        "Supervised": "Preliminary",
    }
    df["ResultStatusIdentifier"] = df["ResultStatusIdentifier"].replace(qaqc_map)

    # format data - combine comments
    df["ResultCommentText"] = np.where(
        ~df["ActivityCommentText"].isnull(),
        df["ResultCommentText"].astype(str)
        + "; "
        + df["ActivityCommentText"].astype(str),
        df["ResultCommentText"],
    )
    df = df.drop("ActivityCommentText", axis="columns")

    # format - method context
    method_map = {
        "MNPCA": "OTHER",
        # "USEPA",
        # "APHA",
        "EPA_GLNPO": "OTHER",
        "21IOWA_WQX": "OTHER",
        "21ARIZ_WQX": "OTHER",
        # "ASTM",
        "MASSDEP": "OTHER",
        "MDEQ_WQ_WQX": "OTHER",
        "21DELAWQ_WQX": "OTHER",
        "MTWTRSHD_WQX": "OTHER",
        "MBMG_WQX": "OTHER",
        "OREGONDEQ": "OTHER",
        # "USDOI/USGS",
        "21NC03WQ": "OTHER",
        "11NPSWRD_WQX": "OTHER",
        "R10BUNKER": "OTHER",
        "MEDEP_WQX": "OTHER",
        "MNPCA_BIO": "OTHER",
        "AZDEQ_SW": "OTHER",
        # "NIOSH",
        "21FLSFWM_WQX": "OTHER",
        "21FLACEP_WQX": "OTHER",
        # "HACH",
        "21NYDECA_WQX": "OTHER",
        "21FLCOLL_WQX": "OTHER",
        "21FLGW_WQX": "OTHER",
        "21FLA_WQX": "OTHER",
        "21FLEECO_WQX": "OTHER",
        # "AOAC",
    }
    df["ResultCommentText"] = np.where(
        df["ResultAnalyticalMethod/MethodIdentifierContext"].isin(method_map.keys()),
        df["ResultCommentText"].astype(str)
        + "; analysis method context: "
        + df["ResultAnalyticalMethod/MethodIdentifierContext"].astype(str),
        df["ResultCommentText"],
    )
    df["ResultAnalyticalMethod/MethodIdentifierContext"] = df[
        "ResultAnalyticalMethod/MethodIdentifierContext"
    ].replace(method_map)

    df["ResultCommentText"] = df["ResultCommentText"].replace({"nan; ", ""})

    # format - column names
    df = df.rename(
        {
            "ResultAnalyticalMethod/MethodIdentifierContext": "ResultAnalyticalMethodContext",
            "ResultAnalyticalMethod/MethodIdentifier": "ResultAnalyticalMethodID",
            "ResultAnalyticalMethod/MethodName": "ResultAnalyticalMethodName",
            # "ActivityStartDate" - already correct format
            "ActivityStartTime/Time": "ActivityStartTime",
            "MonitoringLocationIdentifier": "MonitoringLocationID",
            "ResultDetectionConditionText": "ResultDetectionCondition",
            # "CharacteristicName" - already correct format
            "ResultSampleFractionText": "ResultSampleFraction",
            "ResultMeasureValue": "ResultValue",
            "ResultMeasure/MeasureUnitCode": "ResultUnit",
            "ResultDepthHeightMeasure/MeasureValue": "ActivityDepthHeightMeasure",
            "ResultDepthHeightMeasure/MeasureUnitCode": "ActivityDepthHeightUnit",
            "DetectionQuantitationLimitMeasure/MeasureValue": "ResultDetectionQuantitationLimitMeasure",
            "DetectionQuantitationLimitMeasure/MeasureUnitCode": "ResultDetectionQuantitationLimitUnit",
            "ResultStatusIdentifier": "ResultStatusID",
            "ActivityTypeCode": "ActivityType",
            "ResultValueTypeName": "ResultValueType",
            "ResultCommentText": "ResultComment",
            # "ActivityMediaName" - already correct format
        },
        axis="columns",
    )

    # format - add missing columns
    df = pd.merge(df, df_sites, on="MonitoringLocationID", how="inner")
    df["ResultDetectionQuantitationLimitType"] = "Reporting Limit"

    # format - add missing values for lab name
    df["LaboratoryName"] = np.where(
        df["LaboratoryName"].isnull(), "Unspecified", df["LaboratoryName"]
    )

    # format - dates
    df["ActivityStartDate"] = df["ActivityStartDate"].dt.date

    # format - parameter names
    df["CharacteristicName"] = df["CharacteristicName"].replace(
        {"Phosphorus": "Total Phosphorus, mixed forms"}
    )

    frac_map = {
        # "Total Recoverable"
        "None": "Unspecified",
        "Filtered, lab": "Filtered",
        "Filtered, field": "Filtered",
        "Filterable": "Filtered",
        "Unfiltered, field": "Unfiltered",
        # "Unfiltered"
        "Dissolved": "Filtered",
        # "Extractable"
        "Field": "Unspecified",
        "Total": "Unfiltered",
        np.nan: "Unspecified",
    }
    df = df[
        ~df["ResultSampleFraction"].isin(
            [
                "Suspended",
                "Inorganic",
                "Non-Filterable (Particle)",
                "Organic",
            ]
        )
    ]
    df["ResultSampleFraction"] = df["ResultSampleFraction"].replace(frac_map)

    df["ResultSampleFraction"] = np.where(
        df["CharacteristicName"] == "Temperature, water",
        "Unspecified",
        df["ResultSampleFraction"],
    )  # temperature is falsely recorded "Filtered" in some cases

    speciation_map = {
        "mg/l CaCO3": "as CaCO3",
        "mg/l as P": "as P",
        "mg/l as N": "as N",
        "mg/l as PO4": "as PO4",
    }

    df["MethodSpeciation"] = np.where(
        df["ResultUnit"].isin(speciation_map.keys()),
        df["ResultUnit"],
        "as Unspecified",
    )
    df["MethodSpeciation"] = df["MethodSpeciation"].replace(speciation_map)

    # format - method speciation
    df["ResultAnalyticalMethodName"] = np.where(
        df["ResultAnalyticalMethodName"].isnull(),
        "Undefined",
        df["ResultAnalyticalMethodName"],
    )

    # format - unit notation
    df = df[df["ResultUnit"] != "mg/kg"]
    unit_map = {
        "std units": "None",
        "mg/l as P": "mg/l",
        "mg/l as N": "mg/l",
        "Mole/l": "mol/l",
        "mg/l PO4": "mg/l",
        "mg/l CaCO3": "mg/l",
    }
    unit_cols = [
        "ResultUnit",
        "ResultDetectionQuantitationLimitUnit",
    ]
    for col in unit_cols:
        df[col] = df[col].replace(unit_map)

    df["ResultValue"] = pd.to_numeric(df["ResultValue"], errors="coerce")
    df["ResultDetectionQuantitationLimitMeasure"] = pd.to_numeric(
        df["ResultDetectionQuantitationLimitMeasure"], errors="coerce"
    )
    df["ActivityDepthHeightMeasure"] = pd.to_numeric(
        df["ActivityDepthHeightMeasure"], errors="coerce"
    )

    df["ActivityDepthHeightMeasure"] = np.where(
        df["ActivityDepthHeightUnit"] == "ft",
        df["ActivityDepthHeightMeasure"] / 3.28084,
        df["ActivityDepthHeightMeasure"],
    )
    df["ActivityDepthHeightUnit"] = np.where(
        df["ActivityDepthHeightUnit"] == "ft", "m", df["ActivityDepthHeightUnit"]
    )

    # format - below detection limit notation
    condition_list = [
        "Above Operating Range",  # above instrument opperating range
        "Detected Not Quantified",  # measured value not reported
        "High Moisture",  # fermetation allowed to occur
        "Not Present",  # not detected and no detection limit reported
        "Not Reported",  # no value recorded
        "Present Above Quantification Limit",  # value too high to quanify
        "Reported in Raw Data (attached)",  # attached time series data
        "Value Decensored",  # recommended to remove value
    ]
    df = df[~df["ResultDetectionCondition"].isin(condition_list)]

    df["ResultDetectionCondition"] = np.where(
        df["ResultDetectionCondition"].isnull(),
        "",
        "Below Detection/Quantification Limit",
    )

    df["ResultDetectionQuantitationLimitMeasure"] = np.where(
        df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
        df["ResultDetectionQuantitationLimitMeasure"],
        np.nan,
    )
    df["ResultDetectionQuantitationLimitUnit"] = np.where(
        df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
        df["ResultUnit"],
        np.nan,
    )
    df["ResultDetectionQuantitationLimitType"] = np.where(
        df["ResultDetectionCondition"] == "Below Detection/Quantification Limit",
        "Reporting Limit",
        "",
    )
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
    df = df.dropna(
        axis="index",
        how="all",
        subset=["ResultDetectionQuantitationLimitUnit", "ResultUnit"],
    )

    # export sample data
    if i == 0:
        df.to_csv(
            "cleaned_wqp.csv",
            index=False,
            encoding="utf-8",
            mode="w",
            header=True,
        )
    else:
        df.to_csv(
            "cleaned_wqp.csv",
            index=False,
            encoding="utf-8",
            mode="a",
            header=False,
        )

print("Done!")
