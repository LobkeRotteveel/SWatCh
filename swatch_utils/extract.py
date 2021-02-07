""" Function to extract sampling method data
"""

import pandas as pd


def methods(df, site_col, date_col, param_col, frac_col, method_col):
    """ Extract method data

    Get start and end date for each method used for each parameter at each
    site.

    Args:
        df (pandas.DataFrame): cleaned samples dataframe
        site_col (str): unique site identifier column
        param_col (str): parameter name column
        frac_col (str): parameter fraction column
        date_col (pandas.datetime): date of sample collection column
        method_col (str): methodology information column

    Returns:
        methods (pandas.DataFrame): dataframe with the start and end dates for
        each method used for each parameter at each site.

    Example:
        >>> methods = extract.methods(df,
                      site_col='site_id',
                      date_col='date'
                      param_col='parameter_name',
                      frac_col='parameter_fraction',
                      method_col='method_id')
    """

    methods = df.groupby([
        site_col,
        param_col,
        frac_col,
        method_col,
        ]).agg({date_col: ['min', 'max']})
    methods = methods.reset_index()
    methods.columns = [' '.join(col).strip() for col in methods.columns.values]
    methods = methods.rename({'date min':'date_start', 'date max':'date_end'},
        axis='columns')

    return methods
