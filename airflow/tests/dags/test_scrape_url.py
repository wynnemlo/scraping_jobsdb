import os

import pytest
import requests
from dags.scrape_url import _prepare_data_lake_dir_for_html, _prepare_file_dir_for_csv

TEST_RAW_DATA_DIR = "./tests/dags/test_staging"
TEST_DATA_LAKE_DIR = "./tests/dags/test_data_lake"


def test_prepare_file_dir_for_csv(monkeypatch):
    monkeypatch.setenv("RAW_DATA_DIR", TEST_RAW_DATA_DIR)
    # expected CSV file path
    csv_filepath = f"{TEST_RAW_DATA_DIR}/data_analyst_10000_20000_2022-12-01.csv"

    # intermediate directory doesn't exist yet
    assert os.path.exists(TEST_RAW_DATA_DIR) == False

    result = _prepare_file_dir_for_csv("data_analyst", "10000", "20000", "2022-12-01")

    # intermediate directory should now exist
    assert os.path.exists(TEST_RAW_DATA_DIR)
    assert result == csv_filepath

    # cleanup
    os.rmdir(TEST_RAW_DATA_DIR)


def test_prepare_data_lake_dir_for_html(monkeypatch):
    monkeypatch.setenv("DATA_LAKE_DIR", TEST_DATA_LAKE_DIR)

    # expected HTML file path
    html_filepath = f"{TEST_DATA_LAKE_DIR}/2022/12/01/abcd.html"

    # intermediate directory doesn't exist yet
    assert os.path.exists(f"{TEST_DATA_LAKE_DIR}/2022") == False
    assert os.path.exists(f"{TEST_DATA_LAKE_DIR}/2022/12") == False
    assert os.path.exists(f"{TEST_DATA_LAKE_DIR}/2022/12/01") == False
    assert os.path.exists(f"{TEST_DATA_LAKE_DIR}/2022/01") == False

    result = _prepare_data_lake_dir_for_html("abcd", "2022-12-01")

    # intermediate directory should now exist
    assert os.path.exists(f"{TEST_DATA_LAKE_DIR}/2022")
    assert os.path.exists(f"{TEST_DATA_LAKE_DIR}/2022/12")
    assert os.path.exists(f"{TEST_DATA_LAKE_DIR}/2022/12/01")
    assert os.path.exists(f"{TEST_DATA_LAKE_DIR}/2022/01") == False
    assert result == html_filepath

    # cleanup
    os.rmdir(f"{TEST_DATA_LAKE_DIR}/2022/12/01")
    os.rmdir(f"{TEST_DATA_LAKE_DIR}/2022/12")
    os.rmdir(f"{TEST_DATA_LAKE_DIR}/2022")
    os.rmdir(f"{TEST_DATA_LAKE_DIR}")
