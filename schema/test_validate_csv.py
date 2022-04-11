import pytest
from validate_csv import records_chunk_loader
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


def test_progress_bar():
    with tqdm(total=23) as pbar:
        for i in range(24):
            pbar.update(1)
        pbar.n = 10


def test_load_records_with_start_row(schema):
    file = "test_load_records_with_start_row.csv"
    start_row = 2
    loader = records_chunk_loader(file, start_row, schema)
    records = next(loader)
    assert records[0]["DatasetName"] == "row2"

    start_row = 4
    loader = records_chunk_loader(file, start_row, schema)
    records = next(loader)
    assert records[0]["DatasetName"] == "row4"
