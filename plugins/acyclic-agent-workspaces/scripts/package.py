#!/usr/bin/env python3
"""Build a target-specific, self-contained Codex plugin artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import zipfile


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PLUGIN_ROOT.parents[1]
CONTROL_ROOT = PLUGIN_ROOT / "control"
BINARY_NAME = "acyclic-agent-workspaces-control"


def run(*args: str, cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    completed = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=None,
    )
    return completed.stdout.strip()


def host_target() -> str:
    for line in run("rustc", "-vV").splitlines():
        if line.startswith("host: "):
            return line.removeprefix("host: ")
    raise RuntimeError("rustc -vV did not report a host target")


def deterministic_zip(source: Path, destination: Path) -> None:
    timestamp = (2020, 1, 1, 0, 0, 0)
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(source.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(source).as_posix()
            info = zipfile.ZipInfo(relative, timestamp)
            mode = path.stat().st_mode
            info.external_attr = (stat.S_IMODE(mode) & 0xFFFF) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes(), compresslevel=9)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", default=None, help="Rust target triple (defaults to the host)")
    parser.add_argument(
        "--acyclic-binary",
        type=Path,
        default=None,
        help="use an already-built acyclic CLI (integration/testing override)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PLUGIN_ROOT / "dist",
        help="artifact output directory",
    )
    args = parser.parse_args()

    target = args.target or host_target()
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", target) is None:
        parser.error(f"invalid Rust target triple: {target!r}")
    manifest = json.loads((PLUGIN_ROOT / ".codex-plugin" / "plugin.json").read_text(encoding="utf-8"))
    version = manifest["version"]
    artifact_name = f"acyclic-agent-workspaces-{version}-{target}"
    output_root = args.output.resolve()
    stage = output_root / "marketplace"
    archive = output_root / f"{artifact_name}.zip"
    checksum = output_root / f"{artifact_name}.zip.sha256"

    repository_marketplace = json.loads(
        (REPOSITORY_ROOT / ".agents" / "plugins" / "marketplace.json").read_text(
            encoding="utf-8"
        )
    )
    repository_source = (
        REPOSITORY_ROOT / repository_marketplace["plugins"][0]["source"]["path"]
    ).resolve()
    expected_repository_source = (
        PLUGIN_ROOT / "dist" / "marketplace" / "plugins" / "acyclic-agent-workspaces"
    ).resolve()
    if repository_source != expected_repository_source:
        raise RuntimeError("repository marketplace must point only to the staged dist marketplace")

    output_root.mkdir(parents=True, exist_ok=True)
    build_environment = os.environ.copy()
    if "windows-msvc" in target:
        existing_rustflags = build_environment.get("RUSTFLAGS", "").strip()
        static_crt = "-C target-feature=+crt-static"
        build_environment["RUSTFLAGS"] = f"{existing_rustflags} {static_crt}".strip()

    control_target_dir = CONTROL_ROOT / "target"
    cargo_args = [
        "cargo",
        "build",
        "--locked",
        "--release",
        "--target",
        target,
        "--target-dir",
        str(control_target_dir),
        "--manifest-path",
        str(CONTROL_ROOT / "Cargo.toml"),
    ]
    run(*cargo_args, cwd=REPOSITORY_ROOT, env=build_environment)

    suffix = ".exe" if "windows" in target else ""
    built_binary = control_target_dir / target / "release" / f"{BINARY_NAME}{suffix}"
    if not built_binary.is_file():
        raise FileNotFoundError(f"Cargo did not produce {built_binary}")

    if args.acyclic_binary is None:
        cli_target_dir = CONTROL_ROOT / "target" / "acyclic-cli"
        run(
            "cargo",
            "build",
            "--locked",
            "--release",
            "--target",
            target,
            "--target-dir",
            str(cli_target_dir),
            "--package",
            "acyclic-cli",
            "--bin",
            "acyclic",
            cwd=REPOSITORY_ROOT,
            env=build_environment,
        )
        acyclic_binary = cli_target_dir / target / "release" / f"acyclic{suffix}"
    else:
        acyclic_binary = args.acyclic_binary.resolve()
    if not acyclic_binary.is_file():
        raise FileNotFoundError(f"acyclic CLI was not produced: {acyclic_binary}")

    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir()
    plugin_stage = stage / "plugins" / "acyclic-agent-workspaces"
    plugin_stage.mkdir(parents=True)

    shutil.copytree(PLUGIN_ROOT / ".codex-plugin", plugin_stage / ".codex-plugin")
    shutil.copytree(PLUGIN_ROOT / "hooks", plugin_stage / "hooks")
    shutil.copy2(PLUGIN_ROOT / "README.md", plugin_stage / "README.md")
    shutil.copy2(REPOSITORY_ROOT / "LICENSE", plugin_stage / "LICENSE")
    marketplace_dir = stage / ".agents" / "plugins"
    marketplace_dir.mkdir(parents=True)
    marketplace = {
        "name": "acyclic-sdk",
        "interface": {"displayName": "Acyclic SDK"},
        "plugins": [
            {
                "name": "acyclic-agent-workspaces",
                "source": {
                    "source": "local",
                    "path": "./plugins/acyclic-agent-workspaces",
                },
                "policy": {
                    "installation": "AVAILABLE",
                    "authentication": "ON_INSTALL",
                },
                "category": "Developer Tools",
            }
        ],
    }
    (marketplace_dir / "marketplace.json").write_text(
        json.dumps(marketplace, indent=2) + "\n", encoding="utf-8"
    )
    libexec_dir = plugin_stage / "libexec"
    libexec_dir.mkdir()
    packaged_binary = libexec_dir / built_binary.name
    shutil.copy2(built_binary, packaged_binary)
    binary_dir = plugin_stage / "bin"
    binary_dir.mkdir()
    packaged_cli = binary_dir / f"acyclic{suffix}"
    shutil.copy2(acyclic_binary, packaged_cli)
    if not suffix:
        packaged_binary.chmod(packaged_binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        packaged_cli.chmod(packaged_cli.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    command = f"${{PLUGIN_ROOT}}/libexec/{built_binary.name}"
    runtime_config = {
        "mcpServers": {
            "acyclic_agent_workspaces": {
                "command": command,
                "args": [],
                "env_vars": ["PLUGIN_ROOT", "PLUGIN_DATA", "PATH"],
                "startup_timeout_sec": 30,
                "tool_timeout_sec": 3600,
                "default_tools_approval_mode": "approve",
            }
        }
    }
    (plugin_stage / ".mcp.json").write_text(
        json.dumps(runtime_config, indent=2) + "\n", encoding="utf-8"
    )

    for old_output in (archive, checksum):
        old_output.unlink(missing_ok=True)
    deterministic_zip(stage, archive)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    checksum.write_text(f"{digest}  {archive.name}\n", encoding="ascii")
    print(stage)
    print(archive)
    print(checksum)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, subprocess.CalledProcessError, RuntimeError) as error:
        print(f"package.py: {error}", file=sys.stderr)
        raise SystemExit(1) from error
