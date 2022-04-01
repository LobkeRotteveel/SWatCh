import pytest
from validate_csv import chunk_records
from validate_csv import load_records
from tqdm import tqdm
import json
from pprint import pprint


@pytest.fixture
def schema():
    with open("schema.json") as file:
        return json.load(file)


@pytest.fixture
def records():
    return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_chunk_records(records):
    n = 2
    chunks = list(chunk_records(records, n))
    assert len(chunks) == n
    assert len(chunks[0][0]) == 5
    assert len(chunks[1][0]) == 5

    n = 3
    chunks = list(chunk_records(records, n))
    assert len(chunks) == n
    assert len(chunks[0][0]) == 4
    assert len(chunks[1][0]) == 4
    assert len(chunks[2][0]) == 2


def test_progress_bar():
    with tqdm(total=23) as pbar:
        for i in range(24):
            pbar.update(1)
        pbar.n = 10


def test_load_records_with_start_row(schema):
    file = "test_load_records_with_start_row.csv"
    start_row = 2
    records = load_records(file, start_row, schema)
    assert records[0]["DatasetName"] == "row2"

    start_row = 4
    records = load_records(file, start_row, schema)
    assert records[0]["DatasetName"] == "row4"
