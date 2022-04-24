""" Clean site data from Waterbase

Script to clean site data obtained from the European Environment Agency
Waterbase.
Data source: https://www.eea.europa.eu/data-and-maps/data/waterbase-
             water-quality-icm-1


Approach:
    * Merge datasets
    * Extract desired data
    * Format dataset structure and contents

Assumptions:
    * detection limits are in the same units as measured values

Input:
    * raw_waterbase_samples_v2020_1_T_WISE6_DisaggregatedData.csv

Output:
    * cleaned_waterbase.csv
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
from collections import namedtuple
from swatch_utils import set_type
from swatch_utils import convert
from swatch_utils import extract
from swatch_utils import check


# prepare dictionaries for re-naming parameters
parameters = namedtuple("parameters", "name")
parameters = {
    "CAS_7429-90-5": parameters(
        "Aluminum",
    ),
    "CAS_7439-89-6": parameters(
        "Iron",
    ),
    "CAS_7440-70-2": parameters(
        "Calcium",
    ),
    "CAS_7439-95-4": parameters(
        "Magnesium",
    ),
    "CAS_7440-09-7": parameters(
        "Potassium",
    ),
    "CAS_7440-23-5": parameters(
        "Sodium",
    ),
    "CAS_16887-00-6": parameters(
        "Chloride",
    ),
    "CAS_18785-72-3": parameters(
        "Sulfate",
    ),
    "CAS_14797-65-0": parameters(
        "Nitrite",
    ),
    "CAS_14797-55-8": parameters(
        "Nitrate",
    ),
    "CAS_14265-44-2": parameters(
        "Orthophosphate",
    ),
    "CAS_7723-14-0": parameters(
        "Total Phosphorus, mixed forms",
    ),
    "CAS_16984-48-8": parameters(
        "Fluoride",
    ),
    "CAS_14798-03-9": parameters(
        "Ammonium",
    ),
    "EEA_3133-05-9": parameters(
        "Organic carbon",  # DOC
    ),
    "EEA_3133-06-0": parameters(
        "Organic carbon",  # TOC
    ),
    "EEA_3152-01-0": parameters(
        "pH",
    ),
    "EEA_3121-01-5": parameters(
        "Temperature, water",
    ),
    "CAS_3812-32-6": parameters(
        "Carbonate",
    ),
    "CAS_71-52-3": parameters(
        "Bicarbonate",
    ),
    "EEA_31-01-6": parameters(
        "Hardness, carbonate",  # assumed - not defined
    ),
    "EEA_3153-02-4": parameters(
        "Alkalinity, total",  # assumed - not defined
    ),
    "EEA_3151-01-7": parameters(
        "Gran acid neutralizing capacity",  # Other types of ANC not in WQX
    ),
    "EEA_3153-01-3": parameters(
        "Gran acid neutralizing capacity",  # Other types of ANC not in WQX
    ),
}
parameters_name = dict()
for i in parameters:
    parameters_name[i] = parameters[i].name


print("Importing data...")

import_map = {
    "monitoringSiteIdentifier": str,
    "parameterWaterBodyCategory": str,
    "observedPropertyDeterminandCode": str,
    "procedureAnalysedMatrix": str,
    "resultUom": str,
    "phenomenonTimeSamplingDate": str,
    "resultObservedValue": float,
    "resultQualityObservedValueBelowLOQ": str,
    "procedureLOQValue": str,
    "parameterSampleDepth": float,
    "resultObservationStatus": str,
    "Remarks": str,
    "metadata_observationStatus": str,
}
chunked = pd.read_csv(
    "raw_waterbase_samples_v2020_1_T_WISE6_DisaggregatedData.csv",
    usecols=import_map.keys(),
    dtype=import_map,
    parse_dates=["phenomenonTimeSamplingDate"],
    chunksize=1000000,
)

df_sites = pd.read_csv(
    "raw_waterbase_selected_sites.csv",
    dtype={
        "MonitoringLocationID": str,
        "MonitoringLocationLongitude": float,
        "MonitoringLocationLatitude": float,
    },
)


print("Cleaning data...")

for i, df in tqdm(enumerate(chunked)):

    # extract - waterbody type
    type_map = {
        "LW": "Lake/Pond",
        "RW": "River/Stream",
    }
    df = df[df["parameterWaterBodyCategory"].isin(type_map.keys())]

    # extract - media type
    fraction_map = {
        "W": "Unfiltered",
        "W-DIS": "Filtered",
    }
    df = df[df["procedureAnalysedMatrix"].isin(fraction_map.keys())]

    # extract - parameter type
    df = df[df["observedPropertyDeterminandCode"].isin(parameters.keys())]

    # extract - sample type
    df = df[df["resultObservationStatus"].isin(["A", "X", "Y"])]
    df = df.drop("resultObservationStatus", axis="columns")

    # format - column names
    df = df.rename(
        columns={
            "monitoringSiteIdentifier": "MonitoringLocationID",
            "parameterWaterBodyCategory": "MonitoringLocationType",
            "observedPropertyDeterminandCode": "CharacteristicName",
            "procedureAnalysedMatrix": "ResultSampleFraction",
            "resultUom": "ResultUnit",
            "phenomenonTimeSamplingDate": "ActivityStartDate",
            "resultObservedValue": "ResultValue",
            "resultQualityObservedValueBelowLOQ": "ResultDetectionCondition",
            "procedureLOQValue": "ResultDetectionQuantitationLimitMeasure",
            "parameterSampleDepth": "ActivityDepthHeightMeasure",
            "Remarks": "ResultComment",
            "metadata_observationStatus": "ResultStatusID",
        }
    )

    # format - add missing required columns
    df = pd.merge(df, df_sites, on="MonitoringLocationID", how="inner")

    df["ActivityMediaName"] = "Surface Water"
    df["DatasetName"] = "European Environment Agency (EEA) Waterbase"
    df["LaboratoryName"] = "Unspecified"
    df["ResultAnalyticalMethodName"] = "Undefined"
    df["ActivityDepthHeightUnit"] = "m"
    df["ResultValueType"] = "Actual"  # assumed - not defined

    # format - parameter naming
    df["CharacteristicName"] = df["CharacteristicName"].replace(parameters_name)

    df["ResultSampleFraction"] = df["ResultSampleFraction"].replace(fraction_map)
    df["ResultSampleFraction"] = np.where(
        df["CharacteristicName"].isin(["pH", "Temperature, water"]),
        "",  # listed as total or dissolved
        df["ResultSampleFraction"],
    )
    df["ActivityType"] = np.where(
        df["CharacteristicName"].isin(["pH", "Temperature, water"]),
        "Field Msr/Obs",  # assumed
        "Sample-Routine",
    )

    df["MonitoringLocationType"] = df["MonitoringLocationType"].replace(type_map)

    # format - status
    status_map = {
        "A": "Accepted",
        "U": "Rejected",
        "V": "Preliminary",
    }
    df["ResultStatusID"] = df["ResultStatusID"].replace(status_map)

    # format - speciation
    speciation_map = {
        "mg{CaCO3}.L-1": "as CaCO3",
        "mg{Ca}.L-1": "as Ca",
        "mg{Cl}.L-1": "as Cl",
        "mg{Mg}.L-1": "as Mg",
        "mg{NH4}.L-1": "as NH4",
        "mg{NO2}.L-1": "as NO2",
        "mg{NO3}.L-1": "as NO3",
        "mg{N}.L-1": "as N",
        "mg{PO4}.L-1": "as PO4",
        "mg{P}.L-1": "as P",
        "mg{SO4}.L-1": "as SO4",
        "mg{S}.L-1": "as S",
        "ug{N}.L-1": "as N",
        "ug{P}.L-1": "as P",
        "umol{N}.L-1": "as N",
        "umol{P}.L-1": "as P",
        "umol{S}.L-1": "as S",
        "mg{CaCO3}/L": "as CaCO3",
        "mg{NH4}/L": "as NH4",
        "mg{NO2}/L": "as NO2",
        "mg{NO3}/L": "as NO3",
        "mg{P}/L": "as P",
    }

    df["MethodSpeciation"] = np.where(
        df["ResultUnit"].isin(speciation_map.keys()),
        df["ResultUnit"],
        "",
    )
    df["MethodSpeciation"] = df["MethodSpeciation"].replace(speciation_map)
    df["MethodSpeciation"] = np.where(
        (
            df["CharacteristicName"].isin(
                ["Ammonium", "Nitrate", "Nitrite", "Total Phosphorus, mixed forms"]
            )
            & (df["MethodSpeciation"] == "")
        ),
        "as Unspecified",
        df["MethodSpeciation"],
    )

    # format - units
    unit_map = {
        "Cel": "deg C",
        "[pH]": "None",
        "eq.L-1": "eq/L",
        "meq.L-1": "meq/L",
        "mg.L-1": "mg/l",
        "mg{CaCO3}.L-1": "mg/l",
        "mg{Ca}.L-1": "mg/l",
        "mg{Cl}.L-1": "mg/l",
        "mg{C}.L-1": "mg/l",
        "mg{Mg}.L-1": "mg/l",
        "mg{NH4}.L-1": "mg/l",
        "mg{NO2}.L-1": "mg/l",
        "mg{NO3}.L-1": "mg/l",
        "mg{N}.L-1": "mg/l",
        "mg{PO4}.L-1": "mg/l",
        "mg{P}.L-1": "mg/l",
        "mg{SO4}.L-1": "mg/l",
        "mg{S}.L-1": "mg/l",
        "mmol.L-1": "mmol/L",
        "mol.L-1": "Mole/l",
        "ng.L-1": "ng/l",
        "ueq.L-1": "ueq/L",
        "ug.L-1": "ug/l",
        "ug{N}.L-1": "ug/l",
        "ug{P}.L-1": "ug/l",
        "umol.L-1": "umol/L",
        "umol{C}.L-1": "umol/L",
        "umol{N}.L-1": "umol/L",
        "umol{P}.L-1": "umol/L",
        "umol{S}.L-1": "umol/L",
        "mg{CaCO3}/L": "mg/l",
        "mg{NH4}/L": "mg/l",
        "mg{NO2}/L": "mg/l",
        "mg{NO3}/L": "mg/l",
        "mg{P}/L": "mg/l",
        "mg{C}/L": "mg/l",
        "ug/L": "ug/l",
        "mg/L": "mg/l",
    }
    df["ResultUnit"] = df["ResultUnit"].replace(unit_map)

    # format - below detection limit notation
    detect_map = {
        "1": "Below Detection/Quantification Limit",
        "0": "",
    }
    df["ResultDetectionCondition"] = df["ResultDetectionCondition"].replace(detect_map)

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

    if i == 0:
        df.to_csv(
            "cleaned_waterbase.csv",
            index=False,
            encoding="utf-8",
            mode="w",
            header=True,
        )
    else:
        df.to_csv(
            "cleaned_waterbase.csv",
            index=False,
            encoding="utf-8",
            mode="a",
            header=False,
        )

print("Done!")
