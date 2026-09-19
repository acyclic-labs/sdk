#!/usr/bin/env python3
"""Exercise the public local S3 adapter with a real SigV4 client."""

import argparse
import base64
import json
import os
from pathlib import Path
from queue import Queue
import subprocess
import tempfile
from threading import Thread
import zlib

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


SIGNING_KEY_ARGUMENT = "aws_" + "secret_access_key"


def exercise(binary: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="acyclic-s3-") as directory:
        with (Path(directory) / "fixture.stderr").open("w+") as stderr:
            process = subprocess.Popen(
                [str(binary), str(Path(directory) / "store")],
                stdout=subprocess.PIPE, stderr=stderr, text=True,
            )
            try:
                lines = Queue(maxsize=1)
                Thread(target=lambda: lines.put(process.stdout.readline()), daemon=True).start()
                receipt = json.loads(lines.get(timeout=15))
                assert receipt["schema"] == 1
                os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
                settings = {
                    "endpoint_url": receipt["endpoint"],
                    "region_name": "auto",
                    "aws_access_key_id": receipt["access_key"],
                    SIGNING_KEY_ARGUMENT: receipt["secret_key"],
                    "config": Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                     retries={"max_attempts": 0}),
                }
                client = boto3.client("s3", **settings)
                bucket = receipt["bucket"]
                client.head_bucket(Bucket=bucket)
                assert bucket in {item["Name"] for item in client.list_buckets()["Buckets"]}
                temporary_bucket = "s3-consumer-lifecycle"
                client.create_bucket(Bucket=temporary_bucket)
                client.head_bucket(Bucket=temporary_bucket)
                assert temporary_bucket in {item["Name"] for item in client.list_buckets()["Buckets"]}
                alternate = boto3.client("s3", **{
                    **settings, "aws_access_key_id": "alt",
                    SIGNING_KEY_ARGUMENT: "alt-secret",
                })
                assert temporary_bucket not in {item["Name"] for item in alternate.list_buckets()["Buckets"]}
                alternate.create_bucket(Bucket="s3-consumer-alternate")
                assert "s3-consumer-alternate" in {
                    item["Name"] for item in alternate.list_buckets()["Buckets"]
                }
                alternate.delete_bucket(Bucket="s3-consumer-alternate")
                client.delete_bucket(Bucket=temporary_bucket)
                try:
                    client.head_bucket(Bucket=temporary_bucket)
                except ClientError as error:
                    assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
                else:
                    raise AssertionError("deleted bucket remained readable")
                client.create_bucket(Bucket=temporary_bucket)
                client.head_bucket(Bucket=temporary_bucket)
                client.delete_bucket(Bucket=temporary_bucket)
                try:
                    client.put_object(Bucket=bucket, Key="bad-checksum", Body=b"abcdef",
                                      ChecksumCRC32="AAAAAA==")
                except ClientError as error:
                    assert error.response["Error"]["Code"] == "BadDigest"
                else:
                    raise AssertionError("incorrect CRC32 was accepted")
                checksum = base64.b64encode(zlib.crc32(b"abcdef").to_bytes(4, "big")).decode()
                put = client.put_object(Bucket=bucket, Key="folder/unicode-雪.txt",
                                        Body=b"abcdef", ChecksumCRC32=checksum)
                assert put["ChecksumCRC32"] == checksum
                upload = client.create_multipart_upload(Bucket=bucket, Key="crc32-part")
                try:
                    try:
                        client.upload_part(Bucket=bucket, Key="crc32-part",
                                           UploadId=upload["UploadId"], PartNumber=1,
                                           Body=b"abcdef", ChecksumCRC32="AAAAAA==")
                    except ClientError as error:
                        assert error.response["Error"]["Code"] == "BadDigest"
                    else:
                        raise AssertionError("incorrect multipart CRC32 was accepted")
                    part = client.upload_part(Bucket=bucket, Key="crc32-part",
                                              UploadId=upload["UploadId"], PartNumber=1,
                                              Body=b"abcdef", ChecksumCRC32=checksum)
                    assert part["ChecksumCRC32"] == checksum
                finally:
                    client.abort_multipart_upload(Bucket=bucket, Key="crc32-part",
                                                  UploadId=upload["UploadId"])
                response = client.get_object(Bucket=bucket, Key="folder/unicode-雪.txt")
                assert response["Body"].read() == b"abcdef"
                assert client.get_object(Bucket=bucket, Key="folder/unicode-雪.txt",
                                         VersionId="null")["Body"].read() == b"abcdef"
                for operation in (client.get_object, client.head_object):
                    try:
                        operation(Bucket=bucket, Key="folder/unicode-雪.txt",
                                  VersionId="unknown-version")
                    except ClientError as error:
                        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
                    else:
                        raise AssertionError("unknown version returned the latest object")
                response = client.get_object(Bucket=bucket, Key="folder/unicode-雪.txt",
                                             Range="bytes=2-4")
                assert response["Body"].read() == b"cde"
                page = client.list_objects_v2(Bucket=bucket, Prefix="folder/")
                assert [item["Key"] for item in page["Contents"]] == ["folder/unicode-雪.txt"]
                client.put_object(Bucket=bucket, Key="z-version", Body=b"second")
                versions = client.list_object_versions(Bucket=bucket, MaxKeys=1)
                assert versions["IsTruncated"]
                assert versions["NextVersionIdMarker"] == "null"
                assert len(versions["Versions"]) == 1
                next_versions = client.list_object_versions(
                    Bucket=bucket, MaxKeys=1,
                    KeyMarker=versions["NextKeyMarker"],
                    VersionIdMarker=versions["NextVersionIdMarker"],
                )
                assert [item["Key"] for item in next_versions["Versions"]] == ["z-version"]
                deletion = client.delete_object(Bucket=bucket, Key="folder/unicode-雪.txt",
                                                VersionId="null")
                assert deletion["VersionId"] == "null"
                deletion = client.delete_objects(Bucket=bucket, Delete={"Objects": [
                    {"Key": "z-version", "VersionId": "null"},
                ]})
                assert deletion["Deleted"] == [{"Key": "z-version", "VersionId": "null"}]
                client.delete_object(Bucket=bucket, Key="z-version", VersionId="null")
                try:
                    client.get_object(Bucket=bucket, Key="folder/unicode-雪.txt")
                except ClientError as error:
                    assert error.response["Error"]["Code"] == "NoSuchKey"
                else:
                    raise AssertionError("deleted object remained readable")
                bad_client = boto3.client("s3", **{**settings, SIGNING_KEY_ARGUMENT: "wrong"})
                try:
                    bad_client.head_bucket(Bucket=bucket)
                except ClientError as error:
                    assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 403
                else:
                    raise AssertionError("invalid signature was accepted")
            finally:
                process.terminate()
                try:
                    process.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    exercise(parser.parse_args().binary)
    print(json.dumps({"schema": 1, "case": "s3/boto3-smoke", "passed": True}))
