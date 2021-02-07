""" Function to standardize units
"""

import re
from collections import namedtuple


# lists of units types to convert
pp_list = r'^pp'
mol_list = r'mol/l'
eq_list = r'eq/l'

# lists of desired units for parameters
ug_list = r'Al|Fe'
mg_list = r'Ca|Mg|K|Na|SO4|NO3|NO2|NH4|Cl|F$|^P$|PO4|OC'
degc_list = r'temperature'


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
        if unit == 'ppm':
            group[unit_col] = 'mg/l'
        if unit == 'ppb':
            group[unit_col] = 'ug/l'

    """ Parameter properties

    First item is molar mass, second is valency.

    Assumptions:
        * Fe: speciation assumed to be half Fe2+ and Fe3+
        * P: speciation assumed to be one third P3-, P3+, and P5+
        * OC: molar mass assumed to be that of C
    """

    Properties = namedtuple('Properties', 'mol_mass valency')
    param_tuples = {
        'Al': Properties(26.9815, 3),
        'Fe': Properties(55.8450, 2.5),
        'Ca': Properties(40.0780, 2),
        'Mg': Properties(24.3050, 2),
        'K': Properties(39.0983, 1),
        'Na': Properties(22.9898, 1),
        'SO4': Properties(96.0626, 2),
        'NO3': Properties(62.0049, 1),
        'NO2': Properties(46.0055, 4),
        'NH4': Properties(18.0385, 1),
        'Cl': Properties(35.4530, 1),
        'F': Properties(18.9984, 1),
        'P': Properties(30.9738, 3.7),
        'PO4': Properties(94.9714, 3),
        'OC': Properties(12.0107, 4),
    }

    if re.findall(mol_list, unit):
        group[value_col] = group[value_col] * param_tuples[param].mol_mass
        if unit == 'mol/l':
            group[unit_col] = 'g/l'
        if unit == 'mmol/l':
            group[unit_col] = 'mg/l'
        if unit == 'umol/l':
            group[unit_col] = 'ug/l'
        if unit == 'nmol/l':
            group[unit_col] = 'ng/l'

    if re.findall(eq_list, unit):
        group[value_col] = group[value_col] * param_tuples[param].mol_mass / param_tuples[param].valency
        if unit == 'eq/l':
            group[unit_col] = 'g/l'
        if unit == 'meq/l':
            group[unit_col] = 'mg/l'
        if unit == 'ueq/l':
            group[unit_col] = 'ug/l'
        if unit == 'neq/l':
            group[unit_col] = 'ng/l'


    # standardize SI units
    if re.findall(ug_list, param):
        if group[unit_col].all() == 'g/l':
            group[value_col] = group[value_col] * 10**6
            group[unit_col] = 'ug/l'
        if group[unit_col].all() == 'mg/l':
            group[value_col] = group[value_col] * 10**3
            group[unit_col] = 'ug/l'
        if group[unit_col].all() == 'ng/l':
            group[value_col] = group[value_col] / 10**3
            group[unit_col] = 'ug/l'

    if re.findall(mg_list, param):
        if group[unit_col].all() == 'g/l':
            group[value_col] = group[value_col] * 10**3
            group[unit_col] = 'mg/l'
        if group[unit_col].all() == 'ug/l':
            group[value_col] = group[value_col] / 10**3
            group[unit_col] = 'mg/l'
        if group[unit_col].all() == 'ng/l':
            group[value_col] = group[value_col] / 10**6
            group[unit_col] = 'mg/l'

    if re.findall(degc_list, param):
        if group[unit_col].all() == 'deg_f':
            group[value_col] = (group[value_col] - 32) / 1.8
            group[unit_col] = 'deg_c'
        if group[unit_col].all() == 'deg_k':
            group[value_col] = group[value_col] + 273.15
            group[unit_col] = 'deg_c'

    return group
