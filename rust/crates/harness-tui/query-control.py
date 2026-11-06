"""Check visible query navigation against planted production defects."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "rust/crates/harness-tui/src/query_controller.rs"
RECOVERY = "python3 rust/crates/harness-tui/query-control.py"
COMMAND = [
    "cargo", "test", "--locked", "-p", "acyclic-harness-tui", "--features", "tui",
    "--lib", "query_page::tests::wrapped_query_pages_reveal_every_row_before_the_source_continuation",
    "--", "--exact", "--nocapture",
]
GUARD = b"if !page.page_boundary(top, rows, height) || pending.as_ref().get_ref().is_some() {"
CONTROLS = [
    (
        "progress",
        GUARD,
        b"if pending.as_ref().get_ref().is_some() || !page.page_boundary(top, rows, height) {",
        b"pending refresh must leave every admitted row reachable",
    ),
    (
        "safety",
        GUARD,
        b"if !page.page_boundary(top, rows, height) {",
        b"pending navigation must preserve the admitted page",
    ),
    (
        "cost",
        GUARD,
        b"std::hint::black_box(page.lines.clone());\n        " + GUARD,
        b"local query scroll must not allocate",
    ),
]


def require(condition, step):
    if not condition:
        raise RuntimeError(step)


def run():
    result = subprocess.run(COMMAND, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    sys.stdout.buffer.write(result.stdout)
    sys.stdout.flush()
    return result


def qualify():
    original = SOURCE.read_bytes()
    for name, guard, replacement, witness in CONTROLS:
        require(original.count(guard) == 1, f"locate {name} production boundary")
        try:
            SOURCE.write_bytes(original.replace(guard, replacement))
            result = run()
            require(result.returncode != 0 and witness in result.stdout,
                    f"detect planted {name} defect in the original renderer check")
            print(f"{name}: production defect detected", flush=True)
        finally:
            SOURCE.write_bytes(original)
        require(SOURCE.read_bytes() == original, f"restore source after {name} control")
    require(run().returncode == 0, "qualify restored production navigation")
    print("visible query navigation: all controls and restored source PASS", flush=True)


if __name__ == "__main__":
    try:
        qualify()
    except (OSError, RuntimeError) as error:
        print(f"step=qualify visible query navigation: {error}; recovery: {RECOVERY}", file=sys.stderr)
        sys.exit(1)
