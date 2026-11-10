"""Displace only the lifecycle observer's clock; run the real session unchanged.

Usage: python3 plugin/tests/hosts/proofs/session-clock.py ACYCLIC GRAPHCODER CODEX
The recorded elapsed time increases by sixty seconds without delaying any event.
"""
import pathlib
import runpy
import subprocess  # Bind subprocess's own wait clock before the observation fault.
import sys
import time

fixture = pathlib.Path(__file__).resolve().parents[1] / 'managed-session.py'
recovery = 'python3 plugin/tests/hosts/proofs/session-clock.py ACYCLIC GRAPHCODER CODEX'
if len(sys.argv) != 4:
    raise SystemExit('step=admit lifecycle clock control; recovery command: ' + recovery)
original = time.monotonic
sampled = False


def displaced():
    global sampled
    now = original()
    if sys._getframe(1).f_code.co_filename == str(fixture):
        if sampled:
            return now + 60
        sampled = True
    return now


sys.path.insert(0, str(fixture.parent))
sys.argv = [str(fixture), *sys.argv[1:], 'lifecycle']
try:
    time.monotonic = displaced
    runpy.run_path(str(fixture), run_name='__main__')
finally:
    time.monotonic = original
if not sampled:
    raise SystemExit('step=observe lifecycle clock control: clock unused; recovery command: ' + recovery)
