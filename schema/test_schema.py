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


def test_only_defined_properties_in_data(df, schema):
    valid_properties = schema["properties"].keys()
    data_properties = df.columns
    for prop in data_properties:
        assert (
            prop in valid_properties
        ), f"Header Not in Schema Error: property {prop} is not a valid column header"


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
                        ), f"Enum Error: {prop_name} property: value '{datum}' not in {enum_constraint}"

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


def test_required_properties_constraint(df, schema):
    required_properties = schema["required"]
    existing_properties = df.columns
    for prop in required_properties:
        assert (
            prop in existing_properties
        ), f"Required Property Error: property {prop} is missing as a column header"


def test_property_dependencies(df, schema):
    data_prop_names = df.columns
    for prop_name, dependency_props in schema["dependencies"].items():
        if prop_name in data_prop_names:
            for required_prop_name in dependency_props:
                assert (
                    required_prop_name in data_prop_names
                ), f"Missing Dependency Property: property {prop_name} requires {dependency_props}"


def recursive_key_search(collection, key_counts):
    if isinstance(collection, dict):
        for key in collection.keys():
            key_counts[key] = True
            key_counts.update(recursive_key_search(collection[key], key_counts))
    elif isinstance(collection, list):
        for item in collection:
            key_counts.update(recursive_key_search(item, key_counts))
    return key_counts


def test_print_out_keys_in_all_of_section(schema):
    constraints = schema["allOf"]
    key_counts = recursive_key_search(constraints, dict())
    print(key_counts.keys())


def test_complex_constraints_are_met(df, schema):
    complex_constraints = schema["allOf"]
    for constraint in complex_constraints:
        print(f"Constraint {constraint['title']} is being evaluated...")
        conditional = constraint.get("if")
        if conditional is not None:
            # Determine if the constraint applies based on the datas headers (properties)
            requirements_to_evaluate_conditional = conditional.get("required")
            if requirements_to_evaluate_conditional is not None:
                requirements_to_check_conditional_met = all(
                    [
                        prop in df.columns
                        for prop in requirements_to_evaluate_conditional
                    ]
                )
                if not requirements_to_check_conditional_met:
                    print("Constraint Skipped: Required properties not met")
                    continue

            # Determine if the constraint applies on the datas values (row wise)
            for i, row in df.iterrows():
                constraint_applies = True
                properties = conditional.get("properties")
                if properties is not None:
                    for prop_name, prop in properties.items():
                        # Handle regular value
                        if not isinstance(prop, dict) and not isinstance(prop, list):
                            if row[prop_name] != prop:
                                print("Constraint Skipped: Property value mismatch")
                                constraint_applies = False

                        # Handle the enumeration
                        enum = prop.get("enum")
                        if enum is not None:
                            if row[prop_name] not in enum:
                                print("Constraint Skipped: Enum mismatch")
                                constraint_applies = False

                        # Handle the negated enumeration
                        not_clause = prop.get("not")
                        if not_clause is not None:
                            not_enum = not_clause.get("enum")
                            if not_enum is not None:
                                if row[prop_name] in not_enum:
                                    print("Constraint Skipped: Negated enum mismatch")
                                    constraint_applies = False

                        # Handle regex pattern
                        pattern = prop.get("pattern")
                        if pattern is not None:
                            p = re.compile(pattern)
                            if p.match(row[prop_name]) is None:
                                print("Constraint Skipped: Pattern did not match")
                                constraint_applies = False

                if constraint_applies:
                    then_clause = constraint.get("then")
                    if then_clause is not None:
                        # Handle required
                        required_properties = then_clause.get("required")
                        if required_properties:
                            for prop in required_properties:
                                assert (
                                    prop in df.columns
                                ), f"Complex Constraint Error: {constraint['title']} constraint: {prop} required"

                        # Handle anyOf
                        constraint_met = False
                        any_of_clause = then_clause.get("anyOf")
                        if any_of_clause is not None:
                            for clause in any_of_clause:
                                dependencies = clause.get("dependencies")
                                if dependencies is not None:
                                    dependencies_met = True
                                    for (
                                        prop_name,
                                        prop_dependencies,
                                    ) in dependencies.items():
                                        if prop_name in df.columns:
                                            dependencies_met = all(
                                                [
                                                    prop in df.columns
                                                    for prop in prop_dependencies
                                                ]
                                            )
                                    if dependencies_met:
                                        constraint_met = True
                            assert (
                                constraint_met
                            ), f"Complex Constraint Error: {constraint['title']} constraint: anyOf clause failed on row '{row}'"

        # Handle oneof condition
        one_of_conditional = constraint.get("oneOf")
        if one_of_conditional is not None:
            number_true = 0
            for conditional in one_of_conditional:
                # raise NotImplementedError
                pass
