import pytest
import pandas as pd
import numpy as np
import json
import sys
from pprint import pprint
import re


@pytest.fixture
def df(schema):
    dtype_map = dict()
    for prop_name, prop in schema["properties"].items():
        if prop["type"] == "string":
            dtype_map[prop_name] = str
        if prop["type"] == "number":
            dtype_map[prop_name] = np.float64
    return pd.read_csv("test.csv", dtype=dtype_map)


@pytest.fixture
def schema():
    with open("schema.json") as f:
        return json.load(f)


def test_print_dtype_of_string(df):
    print(df.dtypes)


def test_print_out_all_existing_property_fields(schema):
    keys = list()
    for name, prop in schema["properties"].items():
        keys.extend(prop.keys())
    pprint(set(keys))


def test_print_out_all_existing_property_field_types(schema):
    types = list()
    for name, prop in schema["properties"].items():
        types.append(prop["type"])
    pprint(set(types))


def test_print_out_all_fields_that_occur_for_string_types(schema):
    fields = list()
    for name, prop in schema["properties"].items():
        if prop["type"] == "string":
            fields.extend(prop.keys())
    print("String Type Constraints:", set(fields))


def test_print_out_all_fields_that_occur_for_number_types(schema):
    fields = list()
    for name, prop in schema["properties"].items():
        if prop["type"] == "number":
            fields.extend(prop.keys())
    print("Number Type Constraints:", set(fields))


def test_required_properties_constraint(df, schema):
    required_properties = schema["required"]
    existing_properties = df.columns

    for prop in required_properties:
        assert prop in existing_properties


def test_property_constraints(df, schema):
    for prop_name, prop in schema["properties"].items():
        data = df.get(prop_name)
        if data is not None:
            if prop["type"] == "string":
                # Type Constraint
                data.astype(str)

                # Enum Constraint
                enum_constraint = prop.get("enum")
                if enum_constraint is not None:
                    for datum in data:
                        assert (
                            datum in enum_constraint
                        ), f"Enum Error: {prop_name} property: value '{datum}' not in {enum_constraint}."

                # Max String Length Constraint
                max_length_constraint = prop.get("maxLength")
                if max_length_constraint is not None:
                    for datum in data:
                        assert (
                            len(datum) <= max_length_constraint
                        ), f"Max Length Error: {prop_name} property: value '{datum}' > {max_length_constraint}"

                # Min String Length Constraint
                min_length_constraint = prop.get("minLength")
                if min_length_constraint is not None:
                    for datum in data:
                        assert (
                            len(datum) >= min_length_constraint
                        ), f"Min Length Error: {prop_name} property: value '{datum}' < {min_length_constraint}"

                # Pattern Constraint
                pattern_constraint = prop.get("pattern")
                if pattern_constraint is not None:
                    p = re.compile(pattern_constraint)
                    for datum in data:
                        constraint_met = p.match(datum) is not None
                        assert (
                            constraint_met
                        ), f"Pattern Error: {prop_name} property: value {datum} does not conform to regex r'{pattern_constraint}'"

                # Format Constraint
                format_constraint = prop.get("format")
                if format_constraint is not None:
                    if format_constraint == "date":
                        pd.to_datetime(data)

            elif prop["type"] == "number":
                # Maximum Value Constraint
                max_constraint = prop.get("maximum")
                if max_constraint is not None:
                    for datum in data:
                        assert (
                            datum <= max_constraint
                        ), f"Maximum Value Error: {prop_name} property: value '{datum}' > {max_constraint}"

                # Minimum Value Constraint
                min_constraint = prop.get("minimum")
                if min_constraint is not None:
                    for datum in data:
                        assert (
                            datum >= min_constraint
                        ), f"Minimum Value Error: {prop_name} property: value '{datum}' < {min_constraint}"
