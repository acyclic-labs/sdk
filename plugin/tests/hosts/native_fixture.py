"""Thin JSON bridge to the shared, credential-free fixture profile factory."""
import json
import os
from pathlib import Path
import shlex
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


def start_workspace_owner(binary, workspace, state, root, env, log):
    bridge = Path(__file__).resolve().with_name("workspace-owner.mjs")
    command = ["node", str(bridge), str(binary), str(workspace), str(state), str(root)]
    recovery = shlex.join(command)
    try:
        owner = subprocess.Popen(command, cwd=workspace, env=env, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=log, text=True)
    except OSError as error:
        raise RuntimeError(f"step=start shared workspace owner: {error}; recovery command: {recovery}") from error
    try:
        # The shared bridge arms its original filesystem watch before launch.
        # Its stdout closes if that watch or the original owner fails.
        event = json.loads(owner.stdout.readline())
        if event != {"event": "workspace-owner-ready"}:
            raise ValueError(f"unexpected readiness event: {event}")
    except BaseException as error:
        try:
            stop_workspace_owner(owner)
        finally:
            if not isinstance(error, (ValueError, OSError)):
                raise
            raise RuntimeError(f"step=observe shared workspace owner: {error}; recovery command: {recovery}") from error
    return owner


def stop_workspace_owner(owner):
    try:
        try:
            owner.stdin.close()
        finally:
            status = owner.wait()
            owner.stdout.close()
    except OSError as error:
        raise RuntimeError(f"step=join shared workspace owner: {error}; recovery command: {shlex.join(owner.args)}") from error
    if status:
        raise RuntimeError(f"step=retire shared workspace owner: exit {status}; recovery command: {shlex.join(owner.args)}")
