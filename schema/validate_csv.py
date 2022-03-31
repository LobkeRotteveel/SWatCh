#!/usr/bin/env python

from jsonschema import validate
import jsonschema
import pandas as pd
import json
from pprint import pformat
import argparse
from tqdm import tqdm
import multiprocessing as mp
import queue
import sys
import math


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
        default=1,
        help="Maximum number of failed records before exiting",
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=0,
        help="The row index from which to start",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Number of processes to use for validation",
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
            f"row: {row}",
            f"{e.validator} validator failed because: {e.message}",
            f"offending json element:",
            str(e.instance),
            f"json path: {e.json_path}",
            f"applicable schema:",
            str(e.schema),
        ]
    )


def worker(records, schema, start_row, validation_error_queue, row_progress_queue):
    reports = list()
    for relative_row, record in enumerate(records):
        try:
            validate(instance=record, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            absolute_row = start_row + relative_row
            report = validation_error_report(e, absolute_row)
            validation_error_queue.put(report)
            reports.append(report)
        row_progress_queue.put(1)
    return (len(records), reports)


def get_row_progress_update(progress_queue):
    total_rows = 0
    while True:
        try:
            update = progress_queue.get(False)
            total_rows += update
        except queue.Empty:
            break
    return total_rows


def get_validation_reports(report_queue):
    reports = list()
    while True:
        try:
            report = report_queue.get(False)
            reports.append(report)
        except queue.Empty:
            break
    return reports


def chunk_records(records, n):
    length = len(records)
    chunk_size = math.ceil(length / n)
    for chunk_num in range(n):
        start = chunk_num * chunk_size
        stop = (chunk_num + 1) * chunk_size
        if stop >= length:
            stop = length
        yield records[start:stop], start, stop


def dispatch_workers(
    records, start_row, schema, manager, pool, n, validation_error_queue
):
    progress_queues = list()
    results = list()
    for chunk, chunk_start_row, _ in chunk_records(records, n):
        file_relative_worker_start_row = start_row + chunk_start_row
        row_progress_queue = manager.Queue()
        progress_queues.append(row_progress_queue)
        result = pool.apply_async(
            worker,
            (
                chunk,
                schema,
                file_relative_worker_start_row,
                validation_error_queue,
                row_progress_queue,
            ),
        )
        results.append(result)
    return results, progress_queues


def are_results_ready(results):
    return all([result.ready() for result in results])


def get_results(results):
    return [result.get() for result in results]


def update_progress_bar(pbar, progress_queues):
    for progress_queue in progress_queues:
        rows_done = get_row_progress_update(progress_queue)
        if rows_done:
            pbar.update(rows_done)


def print_validation_reports(validation_error_queue, max_reports):
    reports = get_validation_reports(validation_error_queue)
    report_num = len(reports)
    if report_num > max_reports:
        reports = reports[:max_reports]
    for report in reports:
        tqdm.write(report)
        tqdm.write("")
    return report_num


def load_records(csv_path, start_row, schema):
    # convert start_row to 0-index
    start_row -= 1
    dtypes = get_column_dtypes_from_schema(schema)
    df = pd.read_csv(
        csv_path,
        dtype=dtypes,
        skiprows=lambda x: x != 0 and x < start_row,
    )
    json_string = df.to_json(orient="records")
    records = json.loads(json_string)
    return records


def main():
    cli = init_cli()
    args = cli.parse_args()

    manager = mp.Manager()
    validation_error_queue = manager.Queue()
    pool = mp.Pool(processes=args.processes)

    with open(args.schema_path) as file:
        schema = json.load(file)

    csv_rows = lines_in_file(args.csv_path)
    header_rows = 1
    data_rows = csv_rows - header_rows

    records = load_records(args.csv_path, args.start_row, schema)

    total_fails = 0
    with tqdm(total=csv_rows) as pbar:
        pbar.n = args.start_row - 1
        pbar.refresh()

        results, progress_queues = dispatch_workers(
            records,
            args.start_row,
            schema,
            manager,
            pool,
            args.processes,
            validation_error_queue,
        )
        pool.close()

        rows_counted = 0
        while not are_results_ready(results):
            if total_fails > args.max_fails:
                tqdm.write("Exit Condition: Max failures reached")
                sys.exit(1)
            update_progress_bar(pbar, progress_queues)
            max_reports = args.max_fails - total_fails
            fails = print_validation_reports(validation_error_queue, max_reports)
            total_fails += fails

        pbar.n = csv_rows
        pbar.refresh()


if __name__ == "__main__":
    main()
