"""Qualify the original compiled-prefix properties against production defects.

Run from the repository root with Python 3. Source is restored before the
original properties run; no test assertion is changed for a negative control.
"""

from pathlib import Path
import shlex
import subprocess


def main():
    path = Path("rust/crates/collections/src/compiled_prefix.rs")
    source = path.read_bytes()
    newline = b"\r\n" if b"\r\n" in source else b"\n"
    order = b"result.nodes[cursor].key = position(result.nodes[cursor].key);"
    refund = newline.join([b"drop(order);", b"        drop(allocation);"])
    controls = [
        ("original-position", order, b"", 1,
         "compiled_prefix_matches_original_keys_and_refuses_original_read_credit", "Test failed:"),
        ("sort-funding", refund,
         newline.join([b"drop(allocation);", b"        drop(order);"]), 1,
         "compiled_prefix_cost_and_backing_do_not_grow_with_unrelated_keys", "early_refund"),
        ("read-refusal", b"work.examine(1)?;", b"let _ = work.examine(1);", 2,
         "compiled_prefix_matches_original_keys_and_refuses_original_read_credit", "items refused"),
    ]
    recovery = shlex.join(["python3", __file__])
    command = ["cargo", "nextest", "run", "--locked", "-p", "acyclic-collections", "--lib", "-E"]
    step = "source-anchor"
    try:
        for name, anchor, replacement, count, test, expected in controls:
            step = name + ":source-anchor"
            if source.count(anchor) != count:
                raise ValueError("original source anchor changed")
            step = name + ":negative"
            path.write_bytes(source.replace(anchor, replacement))
            result = subprocess.run(command + [f"test({test})"], text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            print(f"{step}: exit={result.returncode}\n{result.stdout}", flush=True)
            if not (result.returncode == 100 and "0 passed, 1 failed" in result.stdout
                    and expected in result.stdout):
                raise ValueError("unchanged semantic oracle did not reject the defect")
            path.write_bytes(source)
    except Exception as error:
        raise SystemExit(f"REFUSED step={step}: {error}; recovery={recovery}") from error
    finally:
        path.write_bytes(source)
    if path.read_bytes() != source:
        raise SystemExit(f"REFUSED step=source-restoration; recovery=git restore -- {path}")
    step = "restored-original"
    positive = command + ["test(compiled_prefix_)"]
    result = subprocess.run(positive)
    if result.returncode:
        raise SystemExit(f"REFUSED step={step}; recovery={shlex.join(positive)}")
    print("PASS: three actual defects rejected; exact source restored; original properties passed", flush=True)


if __name__ == "__main__":
    main()
