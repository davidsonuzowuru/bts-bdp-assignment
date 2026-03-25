import gzip
import json
import logging
import os
from io import BytesIO
from typing import Annotated

import boto3
import httpx
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Query, status
from fastapi.responses import JSONResponse

from bdi_api.settings import Settings

settings = Settings()
router = APIRouter(prefix="/api/s4", tags=["s4"])
s4 = router
logger = logging.getLogger(__name__)

DATE_PATH = "/2023/11/01/"
S3_RAW_PREFIX = "raw/day=20231101/"
FILE_INTERVAL_SECONDS = 5


def get_s3_client():
    return boto3.client("s3")


def generate_file_urls(base_url: str, file_limit: int, start_offset: int = 0) -> list[tuple[str, str]]:
    """
    Generate (filename, url) pairs for readsb-hist files.
    Files are named HHMMSSZ.json.gz at 5-second intervals.
    start_offset skips the first N files (used to resume interrupted downloads).
    """
    results = []
    total_seconds = start_offset * FILE_INTERVAL_SECONDS
    max_seconds = 24 * 3600

    while len(results) < file_limit and total_seconds < max_seconds:
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        filename = f"{hh:02d}{mm:02d}{ss:02d}Z.json.gz"
        url = base_url + filename
        results.append((filename, url))
        total_seconds += FILE_INTERVAL_SECONDS

    return results


@router.post("/aircraft/download")
def download_aircraft(
    file_limit: Annotated[
        int,
        Query(description="Limit the number of files downloaded. -1 means no limit."),
    ] = 1000,
) -> JSONResponse:
    """
    Download aircraft data from ADS-B Exchange and store raw files in S3.
    Files are stored under s3://<bucket>/raw/day=20231101/<filename>.
    Automatically resumes from where a previous interrupted download left off.
    """
    s3 = get_s3_client()
    bucket = settings.s3_bucket
    base_url = settings.source_url + DATE_PATH

    # Count already uploaded files to resume interrupted downloads
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=S3_RAW_PREFIX)
        already_uploaded = sum(
            1 for page in pages for obj in page.get("Contents", [])
        )
    except (ClientError, BotoCoreError):
        already_uploaded = 0

    limit = file_limit if file_limit != -1 else 17280
    file_urls = generate_file_urls(base_url, limit, start_offset=already_uploaded)

    uploaded = 0
    skipped = 0
    errors = []

    for filename, file_url in file_urls:
        s3_key = S3_RAW_PREFIX + filename
        try:
            file_response = httpx.get(file_url, timeout=60, follow_redirects=False)

            if file_response.status_code == 404:
                skipped += 1
                continue

            file_response.raise_for_status()

            s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=file_response.content,
                ContentType="application/gzip",
            )
            uploaded += 1
            logger.info("Uploaded %s to s3://%s/%s", filename, bucket, s3_key)

        except (httpx.HTTPError, ClientError, BotoCoreError) as e:
            logger.error("Failed to process %s: %s", filename, e)
            errors.append({"file": filename, "error": str(e)})

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "already_in_s3": already_uploaded,
            "uploaded": uploaded,
            "skipped": skipped,
            "total_attempted": len(file_urls),
            "errors": errors,
        },
    )


@router.post("/aircraft/prepare")
def prepare_aircraft() -> JSONResponse:
    """
    Read raw .json.gz files from S3, parse them, and write cleaned
    aircraft data locally so S1 endpoints continue to function.
    """
    s3 = get_s3_client()
    bucket = settings.s3_bucket
    prepared_dir = settings.prepared_dir

    os.makedirs(prepared_dir, exist_ok=True)

    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=S3_RAW_PREFIX)
        keys = [
            obj["Key"]
            for page in pages
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".json.gz")
        ]
    except (ClientError, BotoCoreError) as e:
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={"error": f"Failed to list S3 objects: {e}"},
        )

    if not keys:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "No raw files found in S3.", "processed": 0},
        )

    processed = 0
    all_aircraft = []

    for key in keys:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            compressed_data = obj["Body"].read()

            with gzip.open(BytesIO(compressed_data), "rt", encoding="utf-8") as f:
                raw = json.load(f)

            aircraft_list = raw.get("aircraft", [])
            timestamp = raw.get("now", None)

            for ac in aircraft_list:
                if not ac.get("hex"):
                    continue
                cleaned = {
                    "icao": ac.get("hex", "").upper(),
                    "registration": ac.get("r", None),
                    "type": ac.get("t", None),
                    "lat": ac.get("lat", None),
                    "lon": ac.get("lon", None),
                    "alt_baro": ac.get("alt_baro", None),
                    "ground_speed": ac.get("gs", None),
                    "emergency": ac.get("emergency", None),
                    "timestamp": timestamp,
                }
                all_aircraft.append(cleaned)

            processed += 1

        except (ClientError, BotoCoreError, json.JSONDecodeError, OSError) as e:
            logger.error("Failed to process key %s: %s", key, e)

    output_path = os.path.join(prepared_dir, "aircraft.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_aircraft, f)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"processed": processed, "aircraft_records": len(all_aircraft)},
    )