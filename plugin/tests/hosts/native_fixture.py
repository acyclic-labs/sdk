"""Thin JSON bridge to the shared, credential-free fixture profile factory."""
import json
import os
from pathlib import Path
import subprocess


def fixture_environment(root, variables=None):
    bridge = Path(__file__).resolve().parents[3] / "scripts/graphcoder-e2e/scratch.mjs"
    command = ["node", str(bridge), "profile", str(Path(root).resolve())]
    recovery = "node scripts/graphcoder-e2e/scratch.mjs profile ABSOLUTE_SETTINGS"
    if not bridge.is_file():
        raise RuntimeError(f"step=admit shared fixture profile bridge {bridge}; recovery: restore the admitted scratch.mjs, then {recovery}")
    result = subprocess.run(command, env={"PATH": os.environ["PATH"]},
                            capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(f"step=create shared fixture profile: {result.stderr}; recovery: {recovery}")
    try:
        profile = json.loads(result.stdout)
        if not isinstance(profile, dict):
            raise ValueError("profile is not an environment object")
    except (ValueError, TypeError) as error:
        raise RuntimeError(f"step=decode shared fixture profile: {error}; recovery: {recovery}") from error
    variables = variables or {}
    if profile.keys() & variables.keys():
        raise RuntimeError(f"step=admit native fixture variables; recovery: remove profile overrides and use {recovery}")
    return {"PATH": os.environ["PATH"], **profile, **variables}
