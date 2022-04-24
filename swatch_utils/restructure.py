""" Function to standardize dataframe strcuture
"""

import pandas as pd


def df(df, info_cols):
    """Standardize dataframe structure

    Changes dataframe structure from wide (seperate columns for each
    parameter) to long (parameter column and value column).

    Args:
        df (pandas.DataFrame): dataframe with seperate columns for each
        parameter
        info_cols (list): list of columns which do not contain measured
        values; i.e. columns which should be replicated in every row in
        the restuctured dataframe

    Returns:
        df (pandas.DataFrame): dataframe with standardized format:
            * information columns (variable): e.g. site, date, time
            * parameter columns (str): measured parameter name
            * value columns (float): measured values

    Example:
        >>> df = restructure.df(df, ['site_id','date','time'])
    """

    restruct_df = pd.DataFrame()
    restruct_list = []

    # isolate parameter columns
    info_cols = info_cols
    all_cols = df.columns
    param_cols = all_cols.difference(info_cols)

    # restructure dataframe
    for i, col in enumerate(param_cols):
        temp_dict = {}
        temp_dict.update({"ResultValue": df[col], "CharacteristicName": col})

        for col in info_cols:
            col = df[col]
            temp_dict.update({col.name: col})

        temp_df = pd.DataFrame.from_dict(temp_dict)
        restruct_df = pd.concat([restruct_df, temp_df], sort=True, ignore_index=True)

    return restruct_df
