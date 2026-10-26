#!/usr/bin/env python3
"""Produce captured archive mutations once, then check the entire selection.

--prepare ARCHIVE FIXTURES retains four mutations and their provenance.
--check FIXTURES RECORDS [ACTUAL_OLD_API] runs every unchanged rejection oracle.
No Cargo, provider, host or executable output is fabricated by this fixture.
"""

import hashlib
import inspect
import io
import json
from pathlib import Path
import sys
import tarfile
import time
import types


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def prepare(archive, fixtures):
    fixtures.mkdir(parents=True)
    with tarfile.open(archive) as tar:
        manifest = json.load(tar.extractfile("manifest.json"))
        executable = manifest["artifacts"][0]
        source_name = "scripts/graphcoder-e2e/artifacts.mjs"
        replacements = {
            "source/" + source_name: tar.extractfile("source/scripts/windows-runner.py").read(),
            "bin/" + executable["name"]: tar.extractfile("bin/" + executable["name"]).read(4096),
        }
    requirements = {"source/" + source_name: manifest["source"][source_name],
                    "bin/" + executable["name"]: executable["sha256"]}
    cases = []
    for selected in requirements:
        for change in ("missing", "stale"):
            name = ("source" if selected.startswith("source/") else "companion") + "-" + change
            mutant = fixtures / (name + ".tar.gz")
            changed = json.loads(json.dumps(manifest))
            if selected.startswith("source/"):
                if change == "missing":
                    del changed["source"][source_name]
                else:
                    changed["source"][source_name] = hashlib.sha256(replacements[selected]).hexdigest()
            elif change == "missing":
                changed["artifacts"] = [row for row in changed["artifacts"] if "bin/" + row["name"] != selected]
            else:
                changed["artifacts"][0]["sha256"] = hashlib.sha256(replacements[selected]).hexdigest()
            with tarfile.open(archive) as original, tarfile.open(mutant, "w:gz", compresslevel=1) as output:
                for member in original:
                    if member.name == selected and change == "missing":
                        continue
                    if member.name == "manifest.json":
                        data = json.dumps(changed).encode()
                    elif member.name == selected and change == "stale":
                        data = replacements[selected]
                    else:
                        output.addfile(member, original.extractfile(member))
                        continue
                    entry = tarfile.TarInfo(member.name)
                    entry.size = len(data)
                    output.addfile(entry, io.BytesIO(data))
            cases.append(dict(name=name, selected=selected, archive=mutant.name, sha256=digest(mutant)))
    plan = dict(captured_archive=str(archive), archive_sha256=digest(archive),
                requirements=requirements, cases=cases)
    (fixtures / "fixtures.json").write_text(json.dumps(plan, indent=2), encoding="utf-8")
    print(json.dumps(plan))


def check(fixtures, records, path):
    started = time.monotonic()
    plan = json.loads((fixtures / "fixtures.json").read_text(encoding="utf-8"))
    # Exactly four independently captured mutations are admitted before tests.
    assert {case["name"] for case in plan["cases"]} == {
        "source-missing", "source-stale", "companion-missing", "companion-stale"}
    assert len(plan["cases"]) == 4
    records.mkdir(parents=True)
    api = types.ModuleType("captured_export_admission")
    api.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), api.__dict__)
    observations, failed = [], []
    for case in plan["cases"]:
        destination = records / case["name"]
        destination.mkdir()
        original_capacity = api.require_capacity
        def payload_barrier(*args, **kwargs):
            raise AssertionError("selected inputs reached payload admission before missing/stale rejection")
        api.require_capacity = payload_barrier
        options = {"required_members": plan["requirements"]} if "required_members" in inspect.signature(api.admit_archive).parameters else {}
        before = time.monotonic()
        try:
            # The canonical API checks the captured archive digest itself.
            api.admit_archive(destination, fixtures / case["archive"], case["sha256"], **options)
        except ValueError as error:
            assert "step=selected-input-admission" in str(error) and case["selected"] in str(error), error
            observations.append(dict(case=case["name"], rejected=True, seconds=time.monotonic() - before))
        except AssertionError as error:
            failed.append(dict(case=case["name"], error=str(error), seconds=time.monotonic() - before))
        else:
            failed.append(dict(case=case["name"], error="selected incomplete export was admitted"))
        finally:
            api.require_capacity = original_capacity
        assert list(destination.iterdir()) == [], "rejection wrote fixture bytes"
    elapsed = time.monotonic() - started
    result = dict(archive_sha256=plan["archive_sha256"], api_sha256=digest(path),
                  requirements=plan["requirements"], observations=observations, failed=failed,
                  seconds=elapsed, under_10s=elapsed < 10)
    (records / "qualification.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    assert not failed, failed
    assert result["under_10s"], result
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        if len(sys.argv) == 4 and sys.argv[1] == "--prepare":
            prepare(Path(sys.argv[2]), Path(sys.argv[3]))
        elif len(sys.argv) in (4, 5) and sys.argv[1] == "--check":
            check(Path(sys.argv[2]), Path(sys.argv[3]),
                  Path(sys.argv[4]) if len(sys.argv) == 5 else Path(__file__).with_name("windows-artifacts.py"))
        else:
            raise ValueError("expected --prepare ARCHIVE FIXTURES or --check FIXTURES RECORDS [ACTUAL_OLD_API]")
    except Exception as error:
        raise RuntimeError("Export step=selected-input-controls failed: " + str(error)
                           + "; recovery=python3 scripts/windows-artifacts-required-test.py "
                           "--check CAPTURED_FIXTURES NEW_RECORDS") from error
