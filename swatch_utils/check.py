""" Function to check units
"""

import re
import numpy as np


def units(group):
    """Check if parameters are in acceptable units

    Check if units are as expected for each parameter.

    Args:
        group (pandas.DataFrame.groupby): dataframe grouped by parameter name
            and unit, in that order, where
            * parameter names are standardized
            * unit names are standardized

    Returns:
        group (pandas.DataFrame.groupby): groups where groups with
            unacceptable parameter-unit combinations removed.

    Example:
        >>> df = df.groupby(['parameter_name','unit']).filter(check.units)
    """

    anion_cation_units = r"^pp|mol/l|eq/l|g/l"
    ph_units = ["unit", np.nan]
    temp_units = r"deg_"

    anion_cation_list = r"Al|Fe|Ca|Mg|K|Na|SO4|NO3|NO2|NH4|Cl|F$|^P$|PO4|OC|IC|CO3|Carbon Dioxide|Alkalinity|Hardness"

    param = group.name[0]
    unit = group.name[1]

    if re.findall(anion_cation_list, param):
        if re.findall(anion_cation_units, unit):
            return True

    if param == "pH":
        if unit in ph_units:
            return True

    if param == "temperature":
        if re.findall(temp_units, unit):
            return True

    else:
        return False
