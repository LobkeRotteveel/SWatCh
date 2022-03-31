#!/usr/bin/env python

from jsonschema import validate
import jsonschema
import pandas as pd
import json
from pprint import pprint
import argparse


def init_cli():
    parser = argparse.ArgumentParser(
        description="Validate .csv records against JSON Schema"
    )
    parser.add_argument(
        "--schema-path",
        required=True,
        type=str,
        help="The path to the JSON Schema file",
    )
    parser.add_argument(
        "--max-fails",
        type=int,
        default=10,
        help="Maximum number of failed records before exiting",
    )
    parser.add_argument(
        "--start-row", type=int, default=0, help="The row index from which to start"
    )
    parser.add_argument(
        "csv_path",
        type=str,
        metavar="CSV_PATH",
        help="The path to the .csv file",
    )
    return parser


dtype_map = {
    "string": str,
    "number": float,
}


def get_column_dtypes_from_schema(schema):
    column_dtypes = dict()
    properties = schema["properties"]
    for col_name, col_details in properties.items():
        column_dtypes[col_name] = dtype_map[col_details["type"]]
    return column_dtypes


def main():
    cli = init_cli()
    args = cli.parse_args()

    with open(args.schema_path) as file:
        schema = json.load(file)

    dtypes = get_column_dtypes_from_schema(schema)
    df = pd.read_csv(
        args.csv_path, dtype=dtypes, skiprows=lambda x: x != 0 and x < args.start_row
    )
    json_string = df.to_json(orient="records")
    data = json.loads(json_string)

    fails = 0
    for i, record in enumerate(data):
        if fails >= args.max_fails:
            break
        try:
            validate(instance=record, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            print(f"row: {i}")
            print(f"{e.validator} validator failed because: {e.message}")
            print(f"offending json element:")
            pprint(e.instance)
            print(f"json path: {e.json_path}")
            print(f"applicable schema:")
            print(e.schema)
            print()
            fails += 1


if __name__ == "__main__":
    main()
