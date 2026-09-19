#!/usr/bin/env python3
"""Measure a real signed S3 consumer's bounded listing path."""

import argparse
import json
import os
from pathlib import Path
from queue import Queue
import statistics
import subprocess
import tempfile
from threading import Thread
import time

import boto3
from botocore.config import Config


SIGNING_KEY_ARGUMENT = "aws_" + "secret_access_key"


def run(binary: Path, objects: int, repeats: int, key_style: str) -> dict:
    key = (lambda index: f"key-{index:05d}") if key_style == "dashed" else (
        lambda index: f"key{index:05d}"
    )
    with tempfile.TemporaryDirectory(prefix="acyclic-s3-list-bench-") as directory:
        with (Path(directory) / "fixture.stderr").open("w+") as stderr:
            process = subprocess.Popen(
                [str(binary), str(Path(directory) / "store")],
                stdout=subprocess.PIPE, stderr=stderr, text=True,
            )
            try:
                lines = Queue(maxsize=1)
                Thread(target=lambda: lines.put(process.stdout.readline()), daemon=True).start()
                receipt = json.loads(lines.get(timeout=15))
                os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
                client = boto3.client(
                    "s3", endpoint_url=receipt["endpoint"], region_name="auto",
                    aws_access_key_id=receipt["access_key"],
                    config=Config(signature_version="s3v4",
                                  s3={"addressing_style": "path"},
                                  retries={"max_attempts": 0}),
                    **{SIGNING_KEY_ARGUMENT: receipt["secret_key"]},
                )
                bucket = receipt["bucket"]
                for index in range(objects):
                    client.put_object(Bucket=bucket, Key=key(index), Body=b"x")
                samples = []
                for _ in range(repeats):
                    start = time.perf_counter()
                    first = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                    samples.append((time.perf_counter() - start) * 1000)
                    assert len(first["Contents"]) == 1 and first["IsTruncated"]
                start = time.perf_counter()
                pages = 0
                seen = []
                token = None
                while True:
                    request = {"Bucket": bucket, "MaxKeys": 1}
                    if token is not None:
                        request["ContinuationToken"] = token
                    page = client.list_objects_v2(**request)
                    seen.extend(item["Key"] for item in page.get("Contents", []))
                    pages += 1
                    token = page.get("NextContinuationToken")
                    if token is None:
                        break
                all_pages_ms = (time.perf_counter() - start) * 1000
                assert seen == [key(index) for index in range(objects)]
                return {
                    "schema": 1,
                    "case": "s3/list-consumer",
                    "objects": objects,
                    "key_style": key_style,
                    "repeats": repeats,
                    "first_page_median_ms": round(statistics.median(samples), 3),
                    "all_pages_ms": round(all_pages_ms, 3),
                    "pages": pages,
                }
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("--objects", type=int, default=64)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--key-style", choices=("dashed", "flat"), default="dashed")
    args = parser.parse_args()
    if args.objects < 2 or args.repeats < 1:
        parser.error("--objects must be at least 2 and --repeats must be positive")
    print(json.dumps(run(args.binary.resolve(), args.objects, args.repeats, args.key_style)))


if __name__ == "__main__":
    main()
