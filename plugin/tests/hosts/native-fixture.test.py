"""Capture actual child temp-directory selection; reject the previous env shape."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from native_fixture import fixture_environment


with tempfile.TemporaryDirectory(prefix="plugins-fixture-environment-") as temporary:
    root = Path(temporary)
    os.environ["E2E_PROCESS_HOST"] = os.environ.get("E2E_PROCESS_HOST", sys.executable)
    os.environ["PROFILE_BRIDGE_SENTINEL"] = "must-never-be-returned"
    variables = {"PATH": os.environ["PATH"]}
    env = fixture_environment(root, variables)
    assert variables == {"PATH": os.environ["PATH"]}
    try:
        fixture_environment(root, {"HOME": str(root / "escape")})
    except RuntimeError as error:
        assert "step=admit native fixture variables" in str(error)
    else:
        raise AssertionError("step=reject profile override; recovery: restore the shared fixture profile adapter")
    original = fixture_environment.__globals__["__file__"]
    try:
        fixture_environment.__globals__["__file__"] = str(root / "missing/plugin/tests/hosts/native_fixture.py")
        try:
            fixture_environment(root)
        except RuntimeError as error:
            assert "step=admit shared fixture profile bridge" in str(error)
        else:
            raise AssertionError("step=reject absent shared bridge; recovery: restore the shared fixture profile adapter")
    finally:
        fixture_environment.__globals__["__file__"] = original
    command = [sys.executable, "-c", "import json,os,tempfile; print(json.dumps({'paths':[tempfile.gettempdir(),os.environ.get('XDG_RUNTIME_DIR')],'owner':os.environ.get('E2E_PROCESS_HOST'),'unrelated':os.environ.get('PROFILE_BRIDGE_SENTINEL')}))"]

    def capture(environment):
        return json.loads(subprocess.check_output(command, env=environment, text=True))

    actual = capture(env)
    assert actual['owner'] == os.environ['E2E_PROCESS_HOST'], (
        "step=retain native process owner; recovery: use nativeEnvironment in the shared profile bridge", actual)
    assert actual['unrelated'] is None, actual
    assert all(path and Path(path).is_relative_to(root) for path in actual['paths']), (
        "step=admit private fixture paths; recovery: python3 plugin/tests/hosts/native-fixture.test.py", actual)
    previous = {key: value for key, value in env.items() if key not in ("TMPDIR", "XDG_RUNTIME_DIR")}
    outside = capture(previous)
    assert not all(path and Path(path).is_relative_to(root) for path in outside['paths']), (
        "step=reject previous shared temporary paths; recovery: python3 plugin/tests/hosts/native-fixture.test.py", outside)
    print(json.dumps({"actual_child_paths": actual, "previous_child_paths": outside,
                      "previous_shape_rejected": True}))
