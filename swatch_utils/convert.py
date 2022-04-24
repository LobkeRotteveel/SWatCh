""" Function to standardize units
"""

import re
from collections import namedtuple


# lists of units types to convert
pp_list = r"^pp"
mol_list = r"umol/L|mmol/L|Mole/l"
eq_list = r"ueq/L|meq/L|eq/L"

# lists of desired units for parameters
ug_list = r"Aluminum|Iron"
mg_list = r"Alkalinity, Phenolphthalein (total hydroxide+1/2 carbonate)|Alkalinity, carbonate|Bicarbonate|Calcium|Carbon Dioxide, free CO2|Organic carbon|Carbonate|Chloride|Fluoride|Hardness, non-carbonate|Hardness, carbonate|Magnesium|Total Phosphorus, mixed forms|Potassium|Sodium|Sulfate|Nitrate|Nitrite|Inorganic carbon|Orthophosphate|Alkalinity, total|Total hardness|Hardness, Calcium|Ammonium"
degc_list = r"Temperature, water"


def chemistry(group, param_col, unit_col, value_col):
    """Standardize units

    Standardize parameter units as specified in metadata.xlsx.

    Args:
        param_col (str): parameter name column
        unit_col (str): unit information column
        value_col (float): measured value column
        group (pandas.DataFrame.groupby): dataframe grouped by parameters
            and units, in that order

    Returns:
        group (pandas.DataFrame.groupby): dataframe grouped by parameters
            and units (in that order) with standardized parameter units

    Example:
        >>> df = df.groupby(['parameter_name','unit']).apply(convert.chemistry,
                param_col='parameter_name',
                unit_col='unit',
                value_col='value')
    """

    param = group.name[0]
    unit = group.name[1]

    # convert to g/L or any SI unit thereof
    if re.findall(pp_list, unit):
        if unit == "ppm":
            group[unit_col] = "mg/l"
        if unit == "ppb":
            group[unit_col] = "ug/l"

    """ Parameter properties

    First item is molar mass, second is valency.

    Assumptions:
        * Fe: speciation assumed to be half Fe2+ and Fe3+
        * P: speciation assumed to be one third P3-, P3+, and P5+
        * OC and IC: molar mass assumed to be that of C
    """

    Properties = namedtuple("Properties", "mol_mass valency")
    param_tuples = {
        "Aluminum": Properties(26.9815, 3),
        "Iron": Properties(55.8450, 2.5),
        "Calcium": Properties(40.0780, 2),
        "Magnesium": Properties(24.3050, 2),
        "Potassium": Properties(39.0983, 1),
        "Sodium": Properties(22.9898, 1),
        "Sulfate": Properties(96.0626, 2),
        "Nitrate": Properties(62.0049, 1),
        "Nitrite": Properties(46.0055, 4),
        "Ammonium": Properties(18.0385, 1),
        "Chloride": Properties(35.4530, 1),
        "Fluoride": Properties(18.9984, 1),
        "Total Phosphorus, mixed forms": Properties(30.9738, 3.7),
        "Orthophosphate": Properties(94.9714, 3),
        "Organic carbon": Properties(12.0107, 4),
        "Inorganic carbon": Properties(12.0107, 4),
        "Bicarbonate": Properties(61.017, 1),
        "Carbonate": Properties(60.009, 2),  # reported as CO3
        "Alkalinity, Phenolphthalein (total hydroxide+1/2 carbonate)": Properties(
            60.009, 2
        ),  # reported as CO3
        "Alkalinity, carbonate": Properties(60.009, 2),  # reported as CO3
        "Alkalinity, total": Properties(60.009, 2),  # reported as CO3
        "Hardness, non-carbonate": Properties(60.009, 2),  # reported as CO3
        "Hardness, carbonate": Properties(60.009, 2),  # reported as CO3
        "Total hardness": Properties(60.009, 2),  # reported as CO3
        "Hardness, Calcium": Properties(60.009, 2),  # reported as CO3
        "Carbon Dioxide, free CO2": Properties(44.0095, 1),
    }

    if re.findall(mol_list, unit):
        if (
            param != "Gran acid neutralizing capacity"
        ):  # unable to convert because constituent ions are unknown
            group[value_col] = group[value_col] * param_tuples[param].mol_mass
            if unit == "Mole/l":
                group[unit_col] = "g/l"
            if unit == "mmol/L":
                group[unit_col] = "mg/l"
            if unit == "umol/L":
                group[unit_col] = "ug/l"

    if re.findall(eq_list, unit):
        group[value_col] = (
            group[value_col]
            * param_tuples[param].mol_mass
            / param_tuples[param].valency
        )
        if unit == "eq/L":
            group[unit_col] = "g/l"
        if unit == "meq/L":
            group[unit_col] = "mg/l"
        if unit == "ueq/L":
            group[unit_col] = "ug/l"

    # standardize SI units
    if re.findall(ug_list, param):
        if unit == "g/l":
            group[value_col] = group[value_col] * 10**6
            group[unit_col] = "ug/l"
        if unit == "mg/l":
            group[value_col] = group[value_col] * 10**3
            group[unit_col] = "ug/l"
        if unit == "ng/l":
            group[value_col] = group[value_col] / 10**3
            group[unit_col] = "ug/l"

    if re.findall(mg_list, param):
        if unit == "g/l":
            group[value_col] = group[value_col] * 10**3
            group[unit_col] = "mg/l"
        if unit == "ug/l":
            group[value_col] = group[value_col] / 10**3
            group[unit_col] = "mg/l"
        if unit == "ng/l":
            group[value_col] = group[value_col] / 10**6
            group[unit_col] = "mg/l"

    if re.findall(degc_list, param):
        if unit == "deg F":
            group[value_col] = (group[value_col] - 32) / 1.8
            group[unit_col] = "deg C"
        if unit == "Deg":
            group[value_col] = group[value_col] + 273.15
            group[unit_col] = "deg C"

    return group
