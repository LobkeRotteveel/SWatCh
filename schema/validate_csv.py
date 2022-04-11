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


def main():
    """
    Validate CSV file records against JSON schema.

    The CSV file must have a single header row. This is used to validate
    the records against the JSON schema.
     * The row at which to start the validation within the CSV file can
       be specified.
     * The maximum failures / validation error reports before the CLI
       terminates can be set.
     * The validation error reports are printed to screen with a
       reference to the CSV file relative row where it occurred.
     * The validation error reports are printed to screen with
       contextual information about what caused the validation error.
     * Multiple processes can be used to validate the CSV file records
       in parallel.
     * Overall validation progress is shown using a progress bar.

    One must be careful when using start row and multiprocessing
    together because not all rows before the first failure are
    validated. This is because the records are chunked and given to
    separate workers. Each worker validates a single chunk. Validation
    errors can be found by any worker, anywhere in the file.
    """
    cli = init_cli()
    args = cli.parse_args()

    csv_rows = lines_in_file(args.csv_path)
    header_rows = 1
    data_rows = csv_rows - header_rows

    # Make sure we use a chunk size no larger than what would create n
    # processes number of chunks.
    max_chunk_size = maximum_chunk_size(csv_rows, args.processes)
    chunk_size = min(max_chunk_size, args.chunk_size)

    schema = load_schema(args.schema_path)
    record_chunks = records_chunk_loader(
        args.csv_path,
        args.start_row,
        schema,
        chunk_size,
    )

    manager = mp.Manager()
    validation_error_queue = manager.Queue()
    pool = mp.Pool(processes=args.processes)

    total_fails = 0
    with tqdm(total=csv_rows) as pbar:
        pbar.n = args.start_row - 1
        pbar.refresh()

        results, progress_queues = dispatch_workers(
            record_chunks,
            args.start_row,
            schema,
            manager,
            pool,
            args.processes,
            validation_error_queue,
        )
        pool.close()

        while not are_results_ready(results):
            if total_fails > args.max_fails:
                tqdm.write("Exit Condition: Max failures reached")
                sys.exit(1)
            update_progress_bar(pbar, progress_queues)
            fails = print_validation_reports(
                validation_error_queue,
                total_fails,
                args.max_fails,
            )
            total_fails += fails

        pbar.n = csv_rows
        pbar.refresh()


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
        default=2,
        help="The row 1-index from which to start",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="Number of processes to use for validation",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Number of rows in a CSV records chunk",
    )
    parser.add_argument(
        "csv_path",
        type=str,
        metavar="CSV_PATH",
        help="The path to the .csv file",
    )
    return parser


def lines_in_file(file_path):
    """
    Count the number of lines in a file.

    :param      file_path:  The file path
    :type       file_path:  str
    """
    with open(file_path) as f:
        return sum(1 for _ in f)


def maximum_chunk_size(rows, processes):
    """
    Determine the maximum chunk size that allows use to make full use of
    the specified number of processes.

    :param      rows:       The number of rows that are to be chunked.
    :type       rows:       int
    :param      processes:  The number of processes we have available.
    :type       processes:  int
    """
    return int(rows / processes)


def load_schema(schema_path):
    """
    Loads the jsonschema from a file path.

    This is the schema that is used to validate the csv file.

    :param      schema_path:  The schema file path
    :type       schema_path:  str
    """
    with open(schema_path) as file:
        schema = json.load(file)
    return schema


def records_chunk_loader(csv_path, start_row, schema, chunk_size=None):
    """
    Loads the records from a CSV file path.

    Each row in the CSV file is converted into an individual JSON record
    that can be validated against the schema. The jsonschema is used by
    this function to set the data types of the loaded in records.

    :param      csv_path:    The path to the CSV file
    :type       csv_path:    str
    :param      start_row:   The file relative row at which to start.
                             All rows before this row will be skipped,
                             excluding the header row.
    :type       start_row:   int
    :param      schema:      The jsonschema used to validate the CSV
                             file.
    :type       schema:      json object
    :param      chunk_size:  The number of rows within a chunk.
    :type       chunk_size:  int

    :returns:   A list of individual records for every row in the CSV
                file. Each record is ready to be validated against the
                jsonschema.
    :rtype:     list of json object
    """
    # convert start_row to 0-index
    start_row -= 1
    dtypes = get_column_dtypes_from_schema(schema)
    df_chunks = pd.read_csv(
        csv_path,
        dtype=dtypes,
        skiprows=lambda x: x != 0 and x < start_row,
        iterator=True,
        chunksize=chunk_size,
    )
    for df in df_chunks:
        json_string = df.to_json(orient="records")
        records = json.loads(json_string)
        yield records


def get_column_dtypes_from_schema(schema):
    """
    Gets the column data types from the jsonschema.

    Specifying data types when reading the CSV file using pandas speeds
    up loading, and makes sure columns are read in as the correct data
    types.

    :param      schema:  The json schema
    :type       schema:  json object

    :returns:   A map from column name to python data type. Ready to be
                given to pandas.read_csv.
    :rtype:     dictionary
    """
    column_dtypes = dict()
    properties = schema["properties"]
    for col_name, col_details in properties.items():
        column_dtypes[col_name] = dtype_map[col_details["type"]]
    return column_dtypes


"""
A map from jsonschema data types to python data types.
"""
dtype_map = {
    "string": str,
    "number": float,
}


def dispatch_workers(
    record_chunks, start_row, schema, manager, pool, n, validation_error_queue
):
    """
    Dispatch the validation workers using a pool.

    The records are lazy loaded from the CSV file. This function expects
    the chunked records from read_csv with chunking enabled. The chunks
    are distributed to a multiprocessing pool with n workers. Each
    worker reports its progress using an individual progress queue, and
    reports its encountered validation errors using the validation error
    queue. The individual progress queue are returned by the function
    for monitoring by another process.

    :param      record_chunks:           An iterator containing record
                                         chunks. The records are from
                                         the CSV file, converted to
                                         their JSON object form.
    :type       record_chunks:           Iterator[List[Json Record]]
    :param      start_row:               The row at which the user
                                         requested the validation start.
                                         1-index based and relative to
                                         the original CSV file.
    :type       start_row:               int
    :param      schema:                  The json schema to validate
                                         against.
    :type       schema:                  json object
    :param      manager:                 The multiprocessing manager.
                                         Used to create the individual
                                         progress queues that are
                                         returned to the caller.
    :type       manager:                 mp.Manager
    :param      pool:                    The worker pool to dispatch
                                         workers to.
    :type       pool:                    mp.Pool
    :param      n:                       The number of processes,
                                         workers, and chunks.
    :type       n:                       int
    :param      validation_error_queue:  The queue used to return
                                         validation error reports back
                                         to the calling process.
    :type       validation_error_queue:  mp.Queue

    :returns:   A tuple containing the async results return from
                pool.apply_async for each worker, and the individual
                progress queues for each worker.
    :rtype:     (list[mp.pool.AsyncResult], list[mp.Queue])
    """
    progress_queues = list()
    results = list()
    file_relative_chunk_start_row = start_row
    for chunk in record_chunks:
        row_progress_queue = manager.Queue()
        progress_queues.append(row_progress_queue)
        result = pool.apply_async(
            worker,
            (
                chunk,
                schema,
                file_relative_chunk_start_row,
                validation_error_queue,
                row_progress_queue,
            ),
        )
        results.append(result)
        file_relative_chunk_start_row += len(chunk)
    return results, progress_queues


def worker(records, schema, start_row, validation_error_queue, row_progress_queue):
    """
    The worker that performs the validation of a chunk of records.

    The worker reports on validation failures as they happen using the
    validation error queue. The worker also reports its progress using
    the progress queue.

    :param      records:                 The records from the CSV file.
                                         In their json object form.
    :type       records:                 list[json object]
    :param      schema:                  The json schema to validate
                                         against.
    :type       schema:                  jsonschema
    :param      start_row:               The CSV file relative row at
                                         which the chunk this work is
                                         responsible for starts.
    :type       start_row:               int
    :param      validation_error_queue:  The queue used to send
                                         validation error reports back
                                         to the caller.
    :type       validation_error_queue:  mp.Queue
    :param      row_progress_queue:      The queue used to send progress
                                         back to the caller.
    :type       row_progress_queue:      mp.Queue

    :returns:   Returns a tuple of the number of records processed, and
                reports on the validation errors encountered.
    :rtype:     (int, list[str])
    """
    reports = list()
    for relative_row, record in enumerate(records):
        try:
            record = {k: v for k, v in record.items() if v is not None}
            validate(instance=record, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            absolute_row = start_row + relative_row
            report = validation_error_report(e, absolute_row)
            validation_error_queue.put(report)
            reports.append(report)
        row_progress_queue.put(1)
    return (len(records), reports)


def validation_error_report(e, row):
    """
    The function responsible for generating the validation error report.

    Uses the validation error and the file relative row number to
    generate a validation error report.

    :param      e:    The validation error.
    :type       e:    jsonschema.exceptions.ValidationError
    :param      row:  The CSV file relative row at which the validation
                      error occurred.
    :type       row:  int

    :returns:   The validation error report
    :rtype:     str
    """
    return "\n".join(
        [
            f"row: {row}",
            f"{e.validator} validator failed because: {e.message}",
            f"offending json element:",
            pformat(e.instance),
            f"json path: {e.json_path}",
            f"applicable schema:",
            pformat(e.schema),
        ]
    )


def are_results_ready(results):
    """
    Returns if all async results from workers are ready.

    :param      results:  The results from workers.
    :type       results:  list[mp.pool.AsyncResult]

    :returns:   If all results are ready
    :rtype:     bool
    """
    return all([result.ready() for result in results])


def update_progress_bar(pbar, progress_queues):
    """
    Updates the tqdm progress bar using progress queue.

    :param      pbar:             The tqdm progress bar.
    :type       pbar:             tqdm.tqdm
    :param      progress_queues:  A list of the worker progress queues.
    :type       progress_queues:  list[mp.Queue]
    """
    for progress_queue in progress_queues:
        rows_done = get_row_progress_update(progress_queue)
        if rows_done:
            pbar.update(rows_done)


def get_row_progress_update(progress_queue):
    """
    Retrieves all the progress updates currently on a queue.

    This function expects the queue to contain only integers that
    represent how many rows have been processed.

    :param      progress_queue:  The queue to receives the worker's row
                                 progress updates.
    :type       progress_queue:  mp.Queue

    :returns:   The total amount of rows completed since last update.
    :rtype:     int
    """
    total_rows = 0
    while True:
        try:
            update = progress_queue.get(False)
            total_rows += update
        except queue.Empty:
            break
    return total_rows


def print_validation_reports(validation_error_queue, printed_reports, report_quota):
    """
    Prints all the validation error reports to the screen.

    Uses tqdm.write to keep the progress bar intact. Will only print out
    as many reports as the user asked for using max fails option, even
    if it received more. This is because when exiting due to max fails
    reached there is no guarantee that more will not arrive while each
    process is exiting. This provides the user with a consistent
    experience.

    :param      validation_error_queue:  The queue used to receive
                                         validation error reports from
                                         the validation workers.
    :type       validation_error_queue:  mp.Queue
    :param      printed_reports:         The number of reports that have
                                         been printed for the user.
    :type       printed_reports:         int
    :param      report_quota:            The number of reports that the
                                         user asked for with max fails
                                         options.
    :type       report_quota:            int

    :returns:   The number of reports printed to the screen.
    :rtype:     int
    """

    max_reports = report_quota - printed_reports
    reports = get_validation_reports(validation_error_queue)
    report_num = len(reports)
    if report_num > max_reports:
        reports = reports[:max_reports]
    for report in reports:
        tqdm.write(report)
        tqdm.write("")
    return report_num


def get_validation_reports(report_queue):
    """
    Retrieve the validation reports from the validation worker using the
    report queue.

    :param      report_queue:  The queue used by the worker to send
                               validation error reports.
    :type       report_queue:  mp.Queue

    :returns:   The validation error reports received on the queue.
    :rtype:     list[str]
    """
    reports = list()
    while True:
        try:
            report = report_queue.get(False)
            reports.append(report)
        except queue.Empty:
            break
    return reports


if __name__ == "__main__":
    main()
