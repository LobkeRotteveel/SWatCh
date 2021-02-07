""" Function to standardize data
"""

import pandas as pd
import numpy as np
import datetime as dt


def samples(df):
    """ Standardize sample data

    Standardize sample data as specified in the metadata.xlsx file

    Args:
        df (pandas.DataFrame): samples dataframe where:
            * date and time are noted in one date-time column
            * dataframe structure has been standardized
            * naming conventions have been standardized

    Returns:
        df (pandas.DataFrame): samples dataframe with:
            * standardized data types
            * standardized missing value denotation
            * no missing values in required columns
            * standardized string column specifications

    Example:
        >>> df = set_type.samples(df)
    """

    str_cols = [
        'site_id',
        'date',
        'timezone',
        'parameter_name',
        'parameter_fraction',
        'unit',
        'bdl_flag',
        'database',
        'method_id',
        'status',
        ]

    float_cols = [
        'value',
        'depth',
        ]

    unk_cols = [
        'timezone',
        'parameter_fraction',
        'method_id',
        'status',
        ]

    nan_cols = [
        'bdl_flag',
        'depth',
        ]

    req_cols = [
        'site_id',
        'date',
        'parameter_name',
        'value',
        'unit',
        'database',
        ]

    # set data types
    for col in str_cols:
        df[col] = df[col].astype(str)
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # fill missing values
    for col in unk_cols:
        df[col] = np.where(df[col] == '', 'unknown', df[col])
    for col in nan_cols:
        df[col] = np.where(df[col].isnull(), np.NaN, df[col])

    # remove rows with missing required values
    for col in req_cols:
        df = df[~df[col].isnull()]

    # standardize string column format
    str_cols.remove('date')
    str_cols.remove('parameter_name')
    for col in str_cols:
        df[col] = df[col].str.lower()
    df['time'] = df['date']
    df['date'] = df['date'].dt.date
    df['time'] = df['time'].dt.time

    return df


def sites(df):
    """ Standardize site data

    Standardize site data as specified in the metadata.xlsx file

    Args:
        df (pandas.DataFrame): samples dataframe where:
            * dataframe structure has been standardized
            * naming conventions have been standardized

    Returns:
        df (pandas.DataFrame): samples dataframe with:
            * standardized data types
            * standardized missing value denotation
            * no missing values in required columns
            * standardized column specifications

    Example:
        >>> df = set_type.sites(df)
    """

    str_cols = [
        'site_id',
        'site_name',
        'site_type',
        'coordinate_system',
        'state_province',
        'country',
        'water_id',
        'water_name',
        'catchment_id',
        'catchment_name',
        'agency',
        'database',
        ]

    float_cols = [
        'latitude',
        'longitude',
        'elevation',
        'water_mrt',
        'water_volume',
        'catchment_area',
        ]

    unk_cols = [
        'site_name',
        'site_type',
        'coordinate_system',
        'state_province',
        'country',
        'water_id',
        'water_name',
        'catchment_id',
        'catchment_name',
        'agency',
        ]

    req_cols = [
        'site_id',
        'database',
        ]

    # set data types
    for col in str_cols:
        df[col] = df[col].astype(str)
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # fill missing values
    for col in unk_cols:
        df[col] = np.where(df[col] == '', 'unknown', df[col])
    for col in float_cols:
        df[col] = np.where(df[col].isnull(), np.NaN, df[col])

    # remove rows with missing required values
    for col in req_cols:
        df = df[~df[col].isnull()]

    # standardize column formats
    lower_case = [
        'site_id',
        'water_id',
        'catchment_id',
        'site_type',
        'coordinate_system',
        'database',
        ]
    title_case = [
        'site_name',
        'water_name',
        'catchment_name',
        'state_province',
        'country',
        'agency',
        ]
    three_decimals = [
        'elevation',
        'water_mrt',
        'water_volume',
        'catchment_area',
        ]
    five_decimals = [
        'latitude',
        'longitude',
        ]

    for col in lower_case:
        df[col] = df[col].str.lower()
    for col in title_case:
        df[col] = df[col].str.title()
    for col in three_decimals:
        df[col] = df[col].round(decimals=3)
    for col in five_decimals:
        df[col] = df[col].round(decimals=5)

    return df


def methods(df):
    """ Standardize method data

    Standardize method data as specified in the metadata.xlsx file

    Args:
        df (pandas.DataFrame): samples dataframe where:
            * dataframe structure has been standardized
            * naming conventions have been standardized

    Returns:
        df (pandas.DataFrame): samples dataframe with:
            * standardized data types
            * standardized missing value denotation
            * no missing values in required columns
            * standardized column specifications

    Example:
        >>> df = set_type.methods(df)
    """

    req_cols = [
        'site_id',
        'database',
        'date_start',
        'date_end',
        ]

    # set data types
    for col in list(df):
        df[col] = df[col].astype(str)
    df['date_start'] = pd.to_datetime(df['date_start'])
    df['date_end'] = pd.to_datetime(df['date_end'])

    # fill missing values
    for col in list(df):
        if (col != 'date_start') & (col != 'date_end'):
            df[col] = np.where(df[col] == '', 'unknown', df[col])

    # remove rows with missing required values
    for col in req_cols:
        df = df[~df[col].isnull()]


    # standardize string column specifications
    lower_case = [
        'site_id',
        'database',
        'parameter_fraction',
        'method_name',
        'method_type',
        'method_description',
        'method_reference',
        'method_id',
        'method_agency_number',
        ]

    for col in lower_case:
        df[col] = df[col].str.lower()

    df['date_start'] = df['date_start'].dt.date
    df['date_end'] = df['date_end'].dt.date

    df['method_agency'] = df['method_agency'].str.title()

    return df
