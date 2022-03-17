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
        "csv_path",
        type=str,
        metavar="CSV_PATH",
        help="The path to the .csv file",
    )
    return parser


def main():
    cli = init_cli()
    args = cli.parse_args()

    with open(args.schema_path) as file:
        schema = json.load(file)

    df = pd.read_csv(args.csv_path)
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
