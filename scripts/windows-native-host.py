#!/usr/bin/env python3
"""The sole native process-host producer, composed inside an admitted bb2 job.

Call produce() inside the existing with-xwin-cache workspace, with independent
expected_source from the selected source graph. Product and driver profiles
remain caller-owned; this function builds and stages only the native host.
Attribution: Codex <noreply@openai.com>.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess

TARGET = "x86_64-pc-windows-msvc"
RECOVERY = "bash scripts/windows-test.sh acyclic-native-runtime --lib process_tree::external_host::tests"


def refuse(step, detail):
    raise RuntimeError(f"step=native-host-{step}: {detail}; recovery={RECOVERY}")


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def source_catalog(root):
    root = Path(root).resolve()
    names = subprocess.check_output(["git", "ls-files", "-z"], cwd=root, text=True).split("\0")
    catalog = {}
    for name in names:
        if not name or not (name.startswith(("rust/crates/native-runtime/", "rust/crates/host-platform/"))
                            or name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo/config.toml")):
            continue
        path = root / name
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            refuse("source", "source escapes the selected graph: " + name)
        catalog[name] = digest(path)
    return catalog


def host_flags(cargo_args):
    """Select build-profile/parallelism options; never inherit test features."""
    result = []
    arguments = iter(cargo_args)
    for argument in arguments:
        key = argument.split("=", 1)[0]
        if argument in ("--release", "--offline", "--frozen"):
            result.append(argument)
        elif key in ("--profile", "--jobs", "-j"):
            result.append(argument)
            if "=" not in argument:
                value = next(arguments, None)
                if value is None:
                    refuse("arguments", "missing value for " + argument)
                result.append(value)
    return result


def _produce(root, output, *, expected_source, cargo_args=()):
    """Build/stage one source-bound host; return row, admission, receipt and log.

    output is a fresh source-relative directory retained by the existing job
    export. No target/cache/workspace or compiler is created here. Use the
    returned staged row and receipt unchanged in any same-source driver export,
    including a dev driver paired with a host built using --profile dist.
    """
    root = Path(root).resolve()
    output = Path(output).resolve()
    if not output.is_relative_to(root) or output == root:
        refuse("output", "output must be a fresh directory inside the job source")
    if (os.environ.get("CARGO_INCREMENTAL") != "0"
            or not os.environ.get("CARGO_TARGET_DIR")
            or not os.environ.get("GATE_XWIN_CACHE_ROOT")):
        refuse("custody", "enter the admitted bs.ps1/with-xwin-cache workspace first")
    actual = source_catalog(root)
    if not expected_source or actual != expected_source:
        refuse("source", "complete independently selected native source catalog differs")
    flags = host_flags(cargo_args)
    output.mkdir()
    command = ["cargo", "xwin", "build", "--locked", "--target", TARGET,
               "-p", "acyclic-native-runtime", "--bin", "acyclic-process-host",
               "--message-format=json-render-diagnostics", *flags]
    log = output / "cargo.jsonl"
    print("WIN_TEST_BUILD", json.dumps(command), flush=True)
    with log.open("x") as stream:
        result = subprocess.run(command, cwd=root, stdout=stream)
    if result.returncode:
        refuse("build", f"Cargo exit {result.returncode}; log={log}")
    if source_catalog(root) != expected_source:
        refuse("source", "native source changed during Cargo execution")
    name = "acyclic-process-host-cli.exe"
    specification = output / "specification.json"
    specification.write_text(json.dumps([dict(name=name, target="acyclic-process-host",
                                              test=False, cargo_json=str(log))]))
    bundle = output / "artifacts"
    selector = ["node", "scripts/graphcoder-e2e/artifacts.mjs", str(specification), str(bundle)]
    selected = subprocess.run(selector, cwd=root, stdout=subprocess.DEVNULL)
    if selected.returncode:
        refuse("artifact-selection", f"selector exit {selected.returncode}; log={log}")
    admission = json.loads((bundle / "artifacts.json").read_text())["artifacts"]
    if set(admission) != {name}:
        refuse("artifact-selection", "expected exactly one Cargo-selected native host")
    value = admission[name]
    if Path(value["source"]).suffix.lower() != ".exe":
        refuse("artifact-selection", "Cargo-selected host is not an MSVC executable")
    row = dict(remote=value["path"], cargo_path=value["source"], name=name,
               target="acyclic-process-host", test=False, sha256=value["sha256"])
    receipt = dict(exit=0, target=TARGET, executable_sha256=row["sha256"],
                   source=actual, cargo_admission=value,
                   producer=dict(argv=command, cargo_json_sha256=digest(log),
                                 script_sha256=digest(Path(__file__)),
                                 selector_sha256=digest(root / "scripts/graphcoder-e2e/artifacts.mjs")))
    receipt_path = output / "host-receipt.json"
    receipt_path.write_text(json.dumps(receipt))
    print("WIN_TEST_REMOTE_EXE", json.dumps(row), flush=True)
    return dict(row=row, admission=admission, receipt=receipt,
                receipt_path=receipt_path, log=log)


def produce(root, output, *, expected_source, cargo_args=()):
    """Typed public entry point for the sole host producer above."""
    try:
        return _produce(root, output, expected_source=expected_source, cargo_args=cargo_args)
    except Exception as error:
        if str(error).startswith("step=native-host-"):
            raise
        refuse("admission-or-export", str(error))
