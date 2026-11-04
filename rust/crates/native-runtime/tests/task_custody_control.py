"""Check actual task-custody defects against the unchanged conformance oracle.

Run from the repository root with Python 3. Each invocation qualifies one named
defect, restores the original source, then requires the original test to pass.
"""

import argparse
from pathlib import Path
import shlex
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("defect", choices=["panic", "output"])
    defect = parser.parse_args().defect
    if defect == "panic":
        path = Path("rust/crates/native-runtime/src/task/custody.rs")
        anchor = b"JoinFailure::Native(error).into_error()"
        replacement = (
            b"if error.is_cancelled() { JoinFailure::Native(error).into_error() } "
            b"else { io::Error::other(error) }"
        )
        count = 1
        test = "task::custody::tests::original_tasks_conform_to_scoped_and_shared_completion_custody"
        expected = 'right: "TaskPanicked"'
        features = []
    else:
        path = Path("rust/crates/native-runtime/src/testing/executor.rs")
        anchor = (
            b"let _ = sender.send(result.map(|value| Accounted {\n"
            b"                value,\n"
            b"                _owner: owner,\n"
            b"            }));"
        )
        replacement = anchor.replace(b"_owner: owner,", b"_owner: None,")
        count = 1
        test = "testing::executor::tests::unjoined_original_output_keeps_its_task_accounted_through_release"
        expected = "task accounting retired before its original cleanup"
        features = ["--features", "test-support"]
    command = [
        "cargo", "test", "--locked", "-p", "acyclic-native-runtime",
        *features, "--lib", "--", test, "--exact", "--nocapture",
    ]
    recovery = shlex.join(["python3", __file__, defect])
    source = path.read_bytes()
    if b"\r\n" in source:
        anchor = anchor.replace(b"\n", b"\r\n")
        replacement = replacement.replace(b"\n", b"\r\n")
    step = "source-anchor"
    try:
        if source.count(anchor) != count:
            raise ValueError("original source anchor changed")
        step = "planted-production-defect"
        path.write_bytes(source.replace(anchor, replacement, 1))
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(result.stdout, flush=True)
        if not (
            result.returncode == 101
            and test + " ... FAILED" in result.stdout
            and "test result: FAILED." in result.stdout
            and expected in result.stdout
        ):
            raise ValueError("unchanged semantic oracle did not reject the defect")
    except Exception as error:
        raise SystemExit(f"REFUSED step={step}: {error}; recovery={recovery}") from error
    finally:
        path.write_bytes(source)
    if path.read_bytes() != source:
        raise SystemExit(f"REFUSED step=source-restoration; recovery=git restore -- {path}")
    result = subprocess.run(command)
    if result.returncode:
        raise SystemExit(f"REFUSED step=restored-original; recovery={shlex.join(command)}")
    print(f"PASS {defect}: actual defect rejected; original source restored and oracle passed", flush=True)


if __name__ == "__main__":
    main()
