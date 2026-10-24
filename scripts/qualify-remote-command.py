#!/usr/bin/env python3
"""Qualify remote commands independently of an SSH channel's exit status."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def runner_hash():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def verify(record, nonce, command):
    if (record.get("schema") != 1 or record.get("state") != "completed"
            or record.get("nonce") != nonce or record.get("command") != command
            or record.get("runner_sha256") != runner_hash()
            or type(record.get("exit_code")) is not int):
        raise ValueError("remote result does not match the admitted command")
    return record["exit_code"]


def run(receipt, nonce, command):
    record = {"schema": 1, "state": "running", "nonce": nonce,
              "command": command, "runner_sha256": runner_hash()}
    # Exclusive admission prevents an old or concurrent command from satisfying
    # this run. A killed producer leaves a nonterminal receipt, rejected below.
    with receipt.open("x", encoding="utf-8") as output:
        json.dump(record, output)
    start = time.monotonic()
    try:
        result = subprocess.run(command, check=False).returncode
    except OSError:
        result = 127
    record.update(state="completed", exit_code=result,
                  seconds=time.monotonic() - start)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                         dir=receipt.parent, delete=False) as output:
            temporary = Path(output.name)
            json.dump(record, output)
        os.replace(temporary, receipt)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "verify"))
    parser.add_argument("receipt", type=Path)
    parser.add_argument("nonce")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.nonce or not args.command:
        parser.error("an admitted nonce and command are required")
    if args.mode == "run":
        return run(args.receipt, args.nonce, args.command)
    with args.receipt.open(encoding="utf-8") as source:
        return verify(json.load(source), args.nonce, args.command)


if __name__ == "__main__":
    raise SystemExit(main())
