#!/usr/bin/env python3
"""Run every DST test and the cross-process controls within one nine-second bound."""
import itertools
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

executable, = sys.argv[1:]
second_test = "digest::cross_process_seeded_replay_digest"
started = time.monotonic()
# Each process saves the canonical trace its digest hashed, so a mismatch can
# name the first differing poll, boundary, write or 32-byte chunk.
trace_directory = tempfile.mkdtemp(prefix="dst-trace-")
environment = {**os.environ, "DST_TRACE_DIR": trace_directory}

# Exercise the same in-process concurrency as the main gate before splitting
# independent trace peers. Shared registries must not make another runtime's
# wake delivery look like elapsed simulated time.
parallel = subprocess.run([executable, "--nocapture"], env=environment,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=max(0, 9 - (time.monotonic() - started)))
sys.stdout.buffer.write(parallel.stdout)
sys.stderr.buffer.write(parallel.stderr)
sys.stdout.flush()
sys.stderr.flush()
parallel.check_returncode()
if not re.search(rb"test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;", parallel.stdout):
    sys.exit("Default parallel execution must run every DST case without ignored tests")


def run_pair():
    commands = [
        ["--skip", second_test, "--skip", "sources::", "--nocapture"],
        [second_test, "sources::", "--nocapture"],
    ]
    processes = []
    try:
        # Independent cases retain the same test concurrency as the main gate.
        # Each replay polls its own original tasks on one seeded runtime thread;
        # serializing unrelated crash cases would consume the shared deadline
        # without increasing schedule coverage. Both peers keep the same bound.
        for arguments in commands:
            processes.append(subprocess.Popen([executable, *arguments], env=environment,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        outputs = []
        for process in processes:
            remaining = max(0, 9 - (time.monotonic() - started))
            output, error = process.communicate(timeout=remaining)
            sys.stdout.buffer.write(output)
            sys.stderr.buffer.write(error)
            sys.stdout.flush()
            sys.stderr.flush()
            if process.returncode:
                raise subprocess.CalledProcessError(process.returncode, process.args)
            outputs.append(output)
        return outputs
    except subprocess.TimeoutExpired as error:
        sys.stdout.buffer.write(error.stdout or b"")
        sys.stderr.buffer.write(error.stderr or b"")
        raise
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
        for process in processes:
            process.wait()
            process.stdout.close()
            process.stderr.close()


def equal(left, right, locate=lambda: ""):
    if left != right:
        raise AssertionError(f"cross-process trace diverged: {left!r} != {right!r}{locate()}")


def digest(output, label, pattern=rb"[0-9a-f]{64}"):
    values = re.findall(rb"^" + label + rb"=(" + pattern + rb")$", output, re.MULTILINE)
    if len(values) != 1:
        sys.exit(f"Expected exactly one {label!r}, found {len(values)}")
    return values[0]


SECTION = re.compile(rb"^(polls|boundaries|writes|requests|dispatches|files|jsonl|durable) ")


def first_difference(left, right):
    """Return the first differing canonical line, its section and durable file."""
    section = file = b""
    for number, (a, b) in enumerate(itertools.zip_longest(left, right), 1):
        if a != b:
            where = b" ".join(part for part in (section, file) if part)
            return number, where.decode(errors="replace"), a, b
        if SECTION.match(a):
            section, file = a, b""
        elif a.startswith(b"file "):
            file = a
    return None


def describe(difference):
    if difference is None:
        return "\n  canonical traces are identical; only the digest lines differ"
    number, where, a, b = difference
    return (f"\n  first difference at canonical line {number} ({where}):"
            f"\n  - {a!r}\n  + {b!r}")


# The two command filters partition all tests: one has the turn/process/cut
# scenarios, the other has the replay peer, erasure control and source scans.
# No test is omitted or repeated, including tests added in future modules.
first, second = run_pair()
for output in (first, second):
    if not re.search(rb"test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored;", output):
        sys.exit("Each process must execute its selected tests without ignored cases")
traces = []
for output in (first, second):
    with open(digest(output, b"DST_REPLAY_TRACE", rb".+"), "rb") as trace:
        traces.append(trace.read().splitlines())
# The locator must name a planted divergence in the real trace, in every run.
middle = len(traces[0]) // 2
planted = [*traces[0][:middle], traces[0][middle] + b"!", *traces[0][middle + 1:]]
if (first_difference(traces[0], planted) or (0,))[0] != middle + 1:
    sys.exit("First-divergence locator missed a planted canonical-line change")
equal(digest(first, b"DST_REPLAY_DIGEST"), digest(second, b"DST_REPLAY_DIGEST"),
      lambda: describe(first_difference(*traces)) + f"\n  traces kept in {trace_directory}")
shutil.rmtree(trace_directory)
print("Cross-process seeded trace: identical digest bytes", flush=True)

# These digests add one host-random startup value, cached for the entire process.
# The same-process comparison passes, but this exact comparison must fail.
try:
    equal(digest(first, b"DST_STARTUP_MUTANT_DIGEST"),
          digest(second, b"DST_STARTUP_MUTANT_DIGEST"))
except AssertionError as error:
    print(f"Startup-entropy negative control caught: {error}", flush=True)
else:
    sys.exit("Startup-entropy negative control escaped the cross-process oracle")
print(f"DST suite and cross-process proof elapsed: {time.monotonic() - started:.3f}s",
      flush=True)
