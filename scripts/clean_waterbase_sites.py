""" Clean site data from Waterbase

Script to clean site data obtained from the European Environment Agency
Waterbase.
Data source: https://www.eea.europa.eu/data-and-maps/data/waterbase-water-quality-2

Approach:
    * Extract data
    * Standardize column names

Input:
    * raw_sites_waterbase.csv

Output:
    * intermediate_sites_waterbase.csv
"""

import os
import pandas as pd
import numpy as np
from tqdm import tqdm


print("Importing data...")

# import data
import_map = {
    "monitoringSiteIdentifier": str,
    "confidentialityStatus": str,
    "waterBodyIdentifierScheme": str,
    "lon": float,
    "lat": float,
}
chunked = pd.read_csv(
    "raw_waterbase_sites.csv",
    usecols=import_map.keys(),
    dtype=import_map,
    chunksize=1000000,
)


print("Cleaning data...")

for i, df in tqdm(enumerate(chunked)):

    # extract sites
    df = df[df["confidentialityStatus"] == "F"]
    df = df.drop("confidentialityStatus", axis="columns")

    df = df[
        (df["waterBodyIdentifierScheme"] != "euGroundWaterBodyCode")
        & (df["waterBodyIdentifierScheme"] != "eionetGroundWaterBodyCode")
    ]
    df = df.drop("waterBodyIdentifierScheme", axis="columns")

    df = df.dropna(axis="index", how="any", subset=["lat", "lon"])

    # format - column names
    df = df.rename(
        {
            "monitoringSiteIdentifier": "MonitoringLocationID",
            "lon": "MonitoringLocationLongitude",
            "lat": "MonitoringLocationLatitude",
        },
        axis="columns",
    )

    # format - missing columns
    df["MonitoringLocationHorizontalCoordinateReferenceSystem"] = "WGS84"  # assumed
    df["MonitoringLocationName"] = df["MonitoringLocationID"]

    # export sample data
    if i == 0:
        df.to_csv(
            "raw_waterbase_selected_sites.csv",
            index=False,
            encoding="utf-8",
            mode="w",
            header=True,
        )
    else:
        df.to_csv(
            "raw_waterbase_selected_sites.csv",
            index=False,
            encoding="utf-8",
            mode="a",
            header=False,
        )

df = pd.read_csv("raw_waterbase_selected_sites.csv")

# remove duplicates
df = df.drop_duplicates(subset="MonitoringLocationID", keep="first")


print("Exporting data...")

df.to_csv("raw_waterbase_selected_sites.csv", index=False, encoding="utf-8")

print("Done!")
