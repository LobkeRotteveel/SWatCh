import pytest
from validate_csv import chunk_records


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
