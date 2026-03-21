import gzip
import json
from unittest.mock import MagicMock, patch

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

from bdi_api.app import app

client = TestClient(app)

BUCKET = "test-bdi-aircraft"
TEST_DATA = {
    "now": 1698796800.0,
    "aircraft": [
        {"hex": "abc123", "lat": 41.3, "lon": 2.1, "alt_baro": 35000, "gs": 450},
        {"hex": "def456", "lat": 40.0, "lon": 3.0, "alt_baro": 28000, "gs": 380},
    ],
}


def test_prepare_empty_bucket(tmp_path):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        with patch("bdi_api.s4.exercise.settings") as m:
            m.s3_bucket = BUCKET
            m.prepared_dir = str(tmp_path)
            response = client.post("/api/s4/aircraft/prepare")
    assert response.status_code == 200
    assert response.json()["processed"] == 0


def test_prepare_processes_s3_files(tmp_path):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        compressed = gzip.compress(json.dumps(TEST_DATA).encode())
        s3.put_object(Bucket=BUCKET, Key="raw/day=20231101/000000Z.json.gz", Body=compressed)
        with patch("bdi_api.s4.exercise.settings") as m:
            m.s3_bucket = BUCKET
            m.prepared_dir = str(tmp_path)
            response = client.post("/api/s4/aircraft/prepare")
    assert response.status_code == 200
    assert response.json()["processed"] == 1
    assert response.json()["aircraft_records"] == 2


def test_prepare_writes_local_file(tmp_path):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        compressed = gzip.compress(json.dumps(TEST_DATA).encode())
        s3.put_object(Bucket=BUCKET, Key="raw/day=20231101/000000Z.json.gz", Body=compressed)
        with patch("bdi_api.s4.exercise.settings") as m:
            m.s3_bucket = BUCKET
            m.prepared_dir = str(tmp_path)
            client.post("/api/s4/aircraft/prepare")
    output = tmp_path / "aircraft.json"
    assert output.exists()
    data = json.loads(output.read_text())
    assert len(data) == 2
    assert data[0]["icao"] == "ABC123"


def test_download_uploads_to_s3():
    mock_file = gzip.compress(json.dumps(TEST_DATA).encode())
    with patch("bdi_api.s4.exercise.httpx.get") as mock_get, \
         patch("bdi_api.s4.exercise.get_s3_client") as mock_s3_client, \
         patch("bdi_api.s4.exercise.settings") as mock_settings:
        mock_settings.s3_bucket = BUCKET
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = mock_file
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        mock_s3 = MagicMock()
        mock_s3_client.return_value = mock_s3
        response = client.post("/api/s4/aircraft/download?file_limit=1")
    assert response.status_code == 200
    assert response.json()["uploaded"] == 1


def test_download_file_limit():
    mock_file = gzip.compress(json.dumps(TEST_DATA).encode())
    with patch("bdi_api.s4.exercise.httpx.get") as mock_get, \
         patch("bdi_api.s4.exercise.get_s3_client") as mock_s3_client, \
         patch("bdi_api.s4.exercise.settings") as mock_settings:
        mock_settings.s3_bucket = BUCKET
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = mock_file
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        mock_s3 = MagicMock()
        mock_s3_client.return_value = mock_s3
        response = client.post("/api/s4/aircraft/download?file_limit=3")
    assert response.status_code == 200
    assert response.json()["total_attempted"] == 3