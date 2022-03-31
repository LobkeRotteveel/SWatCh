#!/usr/bin/env python

from jsonschema import validate
import jsonschema
import pandas as pd
import json
from pprint import pprint
import argparse
from tqdm import tqdm


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


def lines_in_file(file_path):
    with open(file_path) as f:
        return sum(1 for _ in f)


def validation_error_report(e, row):
    return "\n".join(
        [
            f"row (0-index, including header): {row}",
            f"{e.validator} validator failed because: {e.message}",
            f"offending json element:",
            str(e.instance),
            f"json path: {e.json_path}",
            f"applicable schema:",
            str(e.schema),
        ]
    )


def main():
    cli = init_cli()
    args = cli.parse_args()

    with open(args.schema_path) as file:
        schema = json.load(file)

    dtypes = get_column_dtypes_from_schema(schema)
    csv_rows = lines_in_file(args.csv_path)
    header_rows = 1
    row_offset = csv_rows - header_rows
    start_data_row = args.start_row - header_rows
    df = pd.read_csv(
        args.csv_path,
        dtype=dtypes,
        skiprows=lambda x: x != 0 and x < args.start_row,
    )
    json_string = df.to_json(orient="records")
    data = json.loads(json_string)

    fails = 0
    with tqdm(total=row_offset) as pbar:
        pbar.n = start_data_row
        pbar.refresh()
        for start_relative_row, record in enumerate(data):
            if fails >= args.max_fails:
                break
            try:
                validate(instance=record, schema=schema)
            except jsonschema.exceptions.ValidationError as e:
                absolute_row = args.start_row + start_relative_row
                print(validation_error_report(e, absolute_row))
                print()
                fails += 1
            pbar.update(1)


if __name__ == "__main__":
    main()
