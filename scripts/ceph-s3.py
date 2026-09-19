#!/usr/bin/env python3
"""Run a pinned Ceph S3 consumer case against the public filesystem adapter."""

import argparse
import configparser
import json
import os
from pathlib import Path
from queue import Queue
import subprocess
import tempfile
from threading import Thread


CEPH_REVISION = "5522d1c351f75bc00ae0f64f742f3f095f5939d9"
CASES = (
    "test_bucket_create_delete",
    "test_bucket_create_naming_bad_short_one",
    "test_bucket_create_naming_bad_short_two",
    "test_bucket_create_naming_bad_starts_nonalpha",
    "test_bucket_create_naming_bad_ip",
    "test_bucket_create_naming_dns_underscore",
    "test_bucket_create_naming_dns_dash_at_end",
    "test_bucket_create_naming_dns_dot_dot",
    "test_bucket_create_naming_dns_dot_dash",
    "test_bucket_create_naming_dns_dash_dot",
    "test_bucket_create_naming_good_long_63",
    "test_bucket_create_naming_good_contains_period",
    "test_bucket_create_naming_good_contains_hyphen",
    "test_bucket_recreate_not_overriding",
    "test_bucket_list_empty",
    "test_bucket_list_distinct",
    "test_bucket_list_many",
    "test_bucket_listv2_many",
    "test_basic_key_count",
    "test_bucket_listv2_encoding_basic",
    "test_bucket_list_prefix_basic",
    "test_bucket_listv2_prefix_basic",
    "test_bucket_list_maxkeys_one",
    "test_bucket_listv2_maxkeys_one",
    "test_object_read_not_exist",
    "test_multi_object_delete",
    "test_multi_objectv2_delete",
    "test_object_head_zero_bytes",
    "test_object_write_read_update_read_delete",
    "test_object_copy_zero_size",
    "test_object_copy_same_bucket",
    "test_multipart_upload_complete_without_create",
    "test_multipart_upload_small",
    "test_ranged_request_response_code",
    "test_ranged_request_skip_leading_bytes_response_code",
    "test_ranged_request_invalid_range",
)
KNOWN_GAPS = (
    "test_bucket_list_delimiter_basic",   # An S3 key can prefix another key.
    "test_bucket_listv2_delimiter_basic",
    "test_object_write_check_etag",        # S3 single-part ETags require MD5.
)


def run(binary: Path, checkout: Path, python: Path, cases: tuple[str, ...]) -> None:
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=checkout, text=True,
    ).strip()
    if revision != CEPH_REVISION:
        raise RuntimeError(f"unexpected Ceph s3-tests revision: {revision}")
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=checkout, text=True,
    )
    if dirty.strip():
        raise RuntimeError(f"modified Ceph s3-tests cache: {checkout}")
    with tempfile.TemporaryDirectory(prefix="acyclic-ceph-s3-") as directory:
        work = Path(directory)
        with (work / "fixture.stderr").open("w+") as stderr:
            fixture = subprocess.Popen(
                [str(binary), str(work / "store")],
                stdout=subprocess.PIPE, stderr=stderr, text=True,
            )
            try:
                lines = Queue(maxsize=1)
                Thread(target=lambda: lines.put(fixture.stdout.readline()), daemon=True).start()
                receipt = json.loads(lines.get(timeout=15))
                endpoint = receipt["endpoint"].split("://", 1)[1]
                host, port = endpoint.rsplit(":", 1)
                config = configparser.RawConfigParser()
                config["DEFAULT"] = {
                    "host": host, "port": port, "is_secure": "false",
                    "ssl_verify": "false",
                }
                config["fixtures"] = {"bucket prefix": "acyclic-{random}-"}
                for section, access, secret in (
                    ("s3 main", "access", "secret"),
                    ("s3 alt", "alt", "alt-secret"),
                    ("s3 tenant", "tenant", "tenant-secret"),
                    ("iam", "access", "secret"),
                    ("iam root", "access", "secret"),
                    ("iam alt root", "alt", "alt-secret"),
                ):
                    config[section] = {
                        "access_key": access, "secret_key": secret,
                        "display_name": section, "user_id": access,
                        "email": f"{access}@example.invalid",
                    }
                config["s3 tenant"]["tenant"] = "test"
                config_path = work / "s3tests.conf"
                with config_path.open("w") as output:
                    config.write(output)
                environment = os.environ.copy()
                environment.update(S3TEST_CONF=str(config_path), S3_USE_SIGV4="true",
                                   AWS_EC2_METADATA_DISABLED="true")
                command = [str(python), "-m", "pytest", "-q", "--tb=line",
                           "--disable-warnings",
                           *(f"s3tests/functional/test_s3.py::{case}" for case in cases)]
                result = subprocess.run(command, cwd=checkout, env=environment,
                                        capture_output=True, text=True, timeout=90)
                if result.returncode:
                    raise RuntimeError((result.stdout + result.stderr)[-4000:])
                print(json.dumps({"schema": 1, "case": "s3/ceph-selected",
                                  "revision": CEPH_REVISION, "cases": list(cases),
                                  "passed": True}))
            finally:
                fixture.terminate()
                try:
                    fixture.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    fixture.kill()
                    fixture.communicate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("checkout", type=Path)
    parser.add_argument("python", type=Path)
    parser.add_argument("--known-gaps", action="store_true")
    arguments = parser.parse_args()
    run(arguments.binary, arguments.checkout, arguments.python,
        KNOWN_GAPS if arguments.known_gaps else CASES)
