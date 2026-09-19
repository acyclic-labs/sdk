#!/usr/bin/env python3
"""Run one bounded SDK and plugin gate with a per-platform Cargo cache."""

import argparse
import json
import os
from pathlib import Path
import platform as host_platform
import shutil
import subprocess
import sys
import tempfile
import time


def run(command, root, environment, deadline):
    started = time.monotonic()
    remaining = deadline - started
    if remaining <= 0:
        return None, {"elapsed_ms": 0, "passed": False,
                      "error": "local qualification time budget exhausted"}
    try:
        result = subprocess.run(
            command, cwd=root, env=environment, capture_output=True, text=True,
            check=False, timeout=remaining,
        )
        error = None if result.returncode == 0 else (result.stderr + result.stdout)[-2000:]
        return result, {"elapsed_ms": int((time.monotonic() - started) * 1000),
                        "passed": result.returncode == 0, "error": error}
    except (OSError, subprocess.TimeoutExpired) as failure:
        return None, {"elapsed_ms": int((time.monotonic() - started) * 1000),
                      "passed": False, "error": str(failure)[-2000:]}


def s3_client_python(target, environment, deadline):
    """Reuse one pinned S3 client environment; never cache mutable S3 state."""
    packages = (
        "boto3==1.40.0", "botocore==1.40.76", "jmespath==1.1.0",
        "s3transfer==0.13.1", "python-dateutil==2.9.0.post0",
        "urllib3==2.8.0", "six==1.17.0",
        "pytest==8.4.2", "munch==4.0.0", "isodate==0.7.2",
        "pytz==2025.2", "requests==2.32.5", "certifi==2026.7.22",
        "charset-normalizer==3.5.1", "colorama==0.4.6", "idna==3.20",
        "iniconfig==2.3.0", "packaging==26.3", "pluggy==1.6.0",
        "Pygments==2.21.0", "exceptiongroup==1.3.1", "tomli==2.2.1",
        "typing_extensions==4.15.0",
    )
    location = target / "tools/s3-python-v1"
    python = location / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    created = False
    if not python.is_file():
        _, setup = run([sys.executable, "-m", "venv", str(location)],
                       target, environment, deadline)
        if not setup["passed"]:
            return None, setup
        created = True
    versions = {name: version for name, version in
                (package.split("==", 1) for package in packages)}
    check_script = (
        "import importlib.metadata as m, platform, sys; "
        f"assert sys.version_info[:2] == {sys.version_info[:2]!r}; "
        f"assert platform.machine() == {host_platform.machine()!r}; "
        f"assert all(m.version(name) == version for name, version in {versions!r}.items())"
    )
    _, check = run([str(python), "-c", check_script], target, environment, deadline)
    if not check["passed"]:
        if not created:
            _, upgrade = run([sys.executable, "-m", "venv", "--upgrade", str(location)],
                             target, environment, deadline)
            if not upgrade["passed"]:
                return None, upgrade
        _, install = run([str(python), "-m", "pip", "install",
                          "--disable-pip-version-check", "--no-deps", "--force-reinstall",
                          *packages],
                         target, environment, deadline)
        if not install["passed"]:
            return None, install
        _, check = run([str(python), "-c", check_script], target, environment, deadline)
        if not check["passed"]:
            return None, check
    return python, None


def ceph_s3_checkout(target, environment, deadline):
    """Cache immutable upstream suite sources at one reviewed revision."""
    revision = "5522d1c351f75bc00ae0f64f742f3f095f5939d9"
    checkout = target / "external/s3-tests"
    if checkout.is_dir():
        result, check = run(["git", "rev-parse", "HEAD"], checkout, environment, deadline)
        if check["passed"]:
            if result.stdout.strip() != revision:
                return None, {"passed": False, "elapsed_ms": check["elapsed_ms"],
                              "error": f"Ceph s3-tests cache has unexpected revision: {checkout}"}
            status, clean = run(["git", "status", "--porcelain=v1", "--untracked-files=all"],
                                checkout, environment, deadline)
            if not clean["passed"] or status.stdout.strip():
                return None, {"passed": False, "elapsed_ms": clean["elapsed_ms"],
                              "error": f"Ceph s3-tests cache is modified: {checkout}"}
            return checkout, None
        # A partial clone has no usable HEAD. Preserve it for diagnosis and retry cold.
        checkout.rename(checkout.with_name(f"s3-tests.invalid-{os.getpid()}"))
    elif checkout.exists():
        return None, {"passed": False, "error": f"Ceph s3-tests cache path is not a directory: {checkout}"}
    checkout.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="s3-tests-", dir=checkout.parent) as staging:
        staged = Path(staging) / "source"
        for command in (
            ["git", "init", "--quiet", str(staged)],
            ["git", "remote", "add", "origin", "https://github.com/ceph/s3-tests.git"],
            ["git", "fetch", "--quiet", "--depth", "1", "--filter=blob:none",
             "origin", revision],
            ["git", "checkout", "--quiet", "--detach", "FETCH_HEAD"],
        ):
            _, step = run(command, staged if staged.is_dir() else checkout.parent,
                          environment, deadline)
            if not step["passed"]:
                return None, step
        try:
            staged.rename(checkout)
        except OSError:
            # Another local gate may have populated the same immutable cache.
            result, check = run(["git", "rev-parse", "HEAD"], checkout, environment, deadline)
            if not check["passed"] or result.stdout.strip() != revision:
                raise
            status, clean = run(["git", "status", "--porcelain=v1", "--untracked-files=all"],
                                checkout, environment, deadline)
            if not clean["passed"] or status.stdout.strip():
                raise
    return checkout, None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int)
    parser.add_argument("--max-seconds", type=int)
    parser.add_argument("--plugin-root", type=Path)
    parser.add_argument("--sdk-only", action="store_true")
    args = parser.parse_args()
    sdk = Path(__file__).resolve().parent.parent
    plugin = (args.plugin_root or sdk / "plugins/acyclic-agent-workspaces").resolve()
    plugin_manifest = plugin / "Cargo.toml"
    environment = os.environ.copy()
    platform = {"win32": "windows", "darwin": "macos"}.get(sys.platform, sys.platform)
    if platform == "linux" and "microsoft" in Path("/proc/sys/kernel/osrelease").read_text().lower():
        default_target = Path.home() / ".cache/acyclic-lab/target"
    elif platform == "macos":
        default_target = sdk.parent / "target"
    else:
        default_target = sdk / f"target-{platform}"
    environment.setdefault("CARGO_TARGET_DIR", str(default_target))
    target = Path(environment["CARGO_TARGET_DIR"]).resolve()
    environment["CARGO_TARGET_DIR"] = str(target)
    executable = ".exe" if sys.platform == "win32" else ""
    warm = (target / "release" / f"qualify{executable}").is_file()
    if args.max_seconds is None:
        args.max_seconds = 300 if warm else 900
    if args.max_seconds < 1:
        parser.error("--max-seconds must be positive")
    if args.seed is None:
        args.seed = int(time.strftime("%Y%m%d", time.gmtime()))
    if args.seed < 0:
        parser.error("--seed must be nonnegative")
    environment["ACYCLIC_QUAL_SEED"] = str(args.seed)
    started = time.monotonic()
    deadline = started + args.max_seconds
    qualifier = target / "release" / f"qualify{executable}"
    object_bench = target / "release" / f"bench-objects{executable}"
    filesystem_bench = target / "release" / f"bench-fs{executable}"
    _, build_case = run(
        ["cargo", "build", "--quiet", "--locked", "--release", "-p", "acyclic-conformance",
         "--features", "local-runner", "--bin", "qualify", "--bin", "bench-objects",
         "--bin", "bench-fs"],
        sdk / "rust", environment, deadline,
    )
    if build_case["passed"]:
        result, sdk_case = run(
            [str(qualifier), f"--max-seconds={args.max_seconds}"],
            sdk / "rust", environment, deadline,
        )
        sdk_case["elapsed_ms"] += build_case["elapsed_ms"]
    else:
        result, sdk_case = None, build_case
    if result is not None and result.returncode == 0:
        try:
            report = json.loads(result.stdout)
        except ValueError as failure:
            sdk_case.update(passed=False, error=f"invalid SDK report: {failure}")
    if not sdk_case["passed"]:
        report = {"schema": 1, "os": platform, "arch": "unknown", "seed": str(args.seed),
                  "budget_seconds": args.max_seconds, "cases": []}
        if result is not None:
            print(result.stderr, file=sys.stderr, end="")
        report["cases"].append({"name": "sdk/qualify", **sdk_case})

    if sdk_case["passed"]:
        bun = environment.get("BUN") or shutil.which("bun", path=environment.get("PATH"))
        if bun is None:
            bindings_case = {"elapsed_ms": 0, "passed": False,
                             "error": "bun is required for the public binding consumer"}
            result = None
        else:
            result, bindings_case = run(
                [sys.executable, str(sdk / "scripts/typescript-qualification.py"),
                 "consumer", bun], sdk, environment, deadline,
            )
        if result is not None and bindings_case["passed"]:
            try:
                evidence = json.loads(result.stdout)
                if evidence.get("passed") is not True:
                    bindings_case.update(passed=False, error="binding consumer returned a failed result")
            except (ValueError, TypeError) as failure:
                bindings_case.update(passed=False, error=f"invalid binding consumer report: {failure}")
        report["cases"].append({"name": "bindings/public-consumer", **bindings_case})

    if sdk_case["passed"]:
        with tempfile.TemporaryDirectory(prefix="acyclic-roundtrip-") as work:
            fixture = Path(work) / "fixture"
            restored = Path(work) / "roundtrip"
            _, fixture_case = run([str(qualifier), "fixture", str(fixture)],
                                  sdk, environment, deadline)
            if fixture_case["passed"]:
                _, roundtrip_case = run([str(qualifier), "roundtrip", str(fixture),
                                         str(restored)], sdk, environment, deadline)
            else:
                roundtrip_case = fixture_case
            report["cases"].append({"name": "filesystem/public-roundtrip",
                                    **roundtrip_case})

        _, s3_build = run(
            ["cargo", "build", "--quiet", "--locked", "--release",
             "-p", "acyclic-conformance", "--features", "s3-fixture",
             "--bin", "s3-fixture"], sdk, environment, deadline)
        if s3_build["passed"]:
            client_python, client_setup = s3_client_python(target, environment, deadline)
            if client_setup is None:
                fixture_binary = target / "release" / f"s3-fixture{executable}"
                _, s3_case = run([str(client_python), str(sdk / "scripts/s3-smoke.py"),
                                  str(fixture_binary)], sdk, environment, deadline)
            else:
                s3_case = client_setup
        else:
            s3_case = s3_build
        report["cases"].append({"name": "s3/boto3-smoke", **s3_case})
        if s3_case["passed"]:
            for key_style in ("dashed", "flat"):
                result, listing_case = run(
                    [str(client_python), str(sdk / "scripts/bench-s3-list.py"),
                     str(fixture_binary), "--key-style", key_style],
                    sdk, environment, deadline)
                if listing_case["passed"]:
                    try:
                        report.setdefault("benchmarks", []).append(json.loads(result.stdout))
                    except (ValueError, TypeError) as failure:
                        listing_case.update(passed=False,
                                            error=f"invalid S3 listing benchmark: {failure}")
                report["cases"].append({"name": f"s3/bench-list-{key_style}",
                                        **listing_case})
                if not listing_case["passed"]:
                    break
        if s3_build["passed"] and client_setup is None:
            ceph_checkout, ceph_setup = ceph_s3_checkout(target, environment, deadline)
            if ceph_setup is None:
                ceph_result, ceph_case = run(
                    [sys.executable, str(sdk / "scripts/ceph-s3.py"),
                     str(fixture_binary), str(ceph_checkout), str(client_python)],
                    sdk, environment, deadline)
                if ceph_case["passed"]:
                    try:
                        evidence = json.loads(ceph_result.stdout)
                        ceph_case["suite_revision"] = evidence["revision"]
                        ceph_case["selected_count"] = len(evidence["cases"])
                    except (ValueError, KeyError, TypeError) as failure:
                        ceph_case.update(passed=False,
                                         error=f"invalid Ceph report: {failure}")
            else:
                ceph_case = ceph_setup
            report["cases"].append({"name": "s3/ceph-selected", **ceph_case})

    if sdk_case["passed"] and sys.platform == "win32":
        debug_qualifier = target / "debug/qualify.exe"
        if debug_qualifier.is_file():
            with tempfile.TemporaryDirectory(prefix="acyclic-debug-capture-") as work:
                source = Path(work) / "source"
                source.mkdir()
                (source / "one-byte.txt").write_bytes(b"x")
                _, debug_case = run([str(debug_qualifier), "roundtrip", str(source),
                                     str(Path(work) / "restored")],
                                    sdk, environment, deadline)
        else:
            debug_case = {"elapsed_ms": 0, "passed": True,
                          "skipped": "debug qualifier was not prebuilt"}
        report["cases"].append({"name": "filesystem/windows-debug-capture", **debug_case})

    if not args.sdk_only and sdk_case["passed"]:
        if not plugin_manifest.is_file():
            report["cases"].append({"name": "plugin/workspace", "elapsed_ms": 0,
                                    "passed": False,
                                    "error": f"plugin manifest missing: {plugin_manifest}"})
        else:
            _, case = run(
                ["cargo", "test", "--locked", "--release", "--quiet",
                 "--manifest-path", str(plugin_manifest)],
                sdk, environment, deadline,
            )
            report["cases"].append({"name": "plugin/tests", **case})
            if case["passed"]:
                _, lint = run(
                    ["cargo", "clippy", "--locked", "--all-targets",
                     "--manifest-path", str(plugin_manifest), "--", "-D", "warnings"],
                    sdk, environment, deadline,
                )
                report["cases"].append({"name": "plugin/clippy", **lint})

    report["cache"] = "warm" if warm else "cold"
    report.setdefault("benchmarks", [])
    if sdk_case["passed"]:
        for mode, page_size, extra in (("keys-single", 1, []),
                                       ("keys-batch", 1000, []),
                                       ("versions-single", 1, ["--versions"])):
            result, case = run(
                [str(object_bench), "20000", str(page_size), *extra],
                sdk / "rust", environment, deadline)
            if result is not None and case["passed"]:
                try:
                    report["benchmarks"].append(json.loads(result.stdout))
                except ValueError as failure:
                    case.update(passed=False, error=f"invalid benchmark report: {failure}")
            report["cases"].append({"name": f"objects/bench-{mode}", **case})
            if not case["passed"]:
                break
        barrier_supported = sys.platform == "darwin"
        for batch, barrier in ((False, False), (True, False), (False, True), (True, True)):
            mode = "batch" if batch else "individual"
            durability = "barrier" if barrier else "full-flush"
            command = [str(filesystem_bench), "--writes=64"]
            if batch:
                command.append("--batch")
            if barrier:
                command.append("--barrier")
            if barrier and not barrier_supported:
                report["cases"].append({
                    "name": f"filesystem/bench-{mode}-{durability}",
                    "elapsed_ms": 0,
                    "passed": True,
                    "unsupported": "ordered durability barriers require macOS F_BARRIERFSYNC",
                })
                continue
            result, case = run(command, sdk / "rust", environment, deadline)
            if result is not None and case["passed"]:
                try:
                    report["benchmarks"].append(json.loads(result.stdout))
                except ValueError as failure:
                    case.update(passed=False, error=f"invalid benchmark report: {failure}")
            report["cases"].append({"name": f"filesystem/bench-{mode}-{durability}", **case})
            if not case["passed"]:
                break
        for paths in (64, 256):
            result, case = run(
                [str(filesystem_bench), "--capture", f"--writes={paths}"],
                sdk / "rust", environment, deadline)
            if result is not None and case["passed"]:
                try:
                    report["benchmarks"].append(json.loads(result.stdout))
                except ValueError as failure:
                    case.update(passed=False, error=f"invalid benchmark report: {failure}")
            report["cases"].append({"name": f"filesystem/bench-capture-{paths}", **case})
            if not case["passed"]:
                break

    report["passed"] = all(case["passed"] for case in report["cases"])
    report["total_elapsed_ms"] = int((time.monotonic() - started) * 1000)
    print(json.dumps(report, separators=(",", ":")))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
