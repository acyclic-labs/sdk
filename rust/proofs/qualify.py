#!/usr/bin/env python3
"""Qualify a production oracle and its retained mutation."""
import argparse
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


class ClaimParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(2, f"step=select claim; cause={message}; recovery: python3 rust/proofs/qualify.py --help\n")


parser = ClaimParser(description=__doc__)
parser.add_argument("claim", choices=["retained-credit", "settlement-terminal", "admission-rebind", "objects-empty-dispatch", "auth-producer-loss", "work-arithmetic", "work-mixed", "borrowed-budget", "native-charge", "budget-errors", "ready-source-allocation", "view-demand", "checkpoint-policy", "handler-policy", "guest-failure"])
claim = parser.parse_args().claim
target = ["--lib"]
if claim == "retained-credit":
    source = "rust/crates/filesystem/src/kernel/allocation.rs"
    crate = "acyclic-fs"
    test = "kernel::allocation::retained::tests::retained_credit_conserves_full_width_ownership_transitions"
    diagnostic = "scratch retirement accepted retained credit"
    features = []
elif claim == "settlement-terminal":
    source = "rust/crates/harness/src/local/tests/agents.rs"
    crate = "acyclic-harness"
    test = "local::tests::agents::lost_model_producer_closes_local_session_without_settling_input"
    diagnostic = "settlement bypassed the original runtime terminal event"
    features = ["--features", "filesystem-local,native-execution,test-support,grpc,search-http"]
elif claim == "admission-rebind":
    source = "rust/crates/stream/src/session/client/admissions.rs"
    crate = "acyclic-stream"
    test = "session::client::admissions::tests::home_rebinding_preserves_pending_memberships_without_allocating"
    diagnostic = "home rebinding allocated per pending membership"
    features = ["--no-default-features", "--features", "session"]
elif claim == "objects-empty-dispatch":
    source = "rust/crates/objects/src/local.rs"
    crate = "acyclic-objects"
    test = "local::outcome_tests::empty_ranges_preserve_native_presence_without_dispatch"
    diagnostic = "empty range dispatched a physical job for key"
    features = ["--no-default-features", "--features", "local,test-support"]
elif claim == "auth-producer-loss":
    source = "rust/crates/auth/src/trust/follower.rs"
    crate = "acyclic-auth"
    test = "trust::follower::tests::account_events_preserve_exact_dependencies_without_unrelated_wakes"
    diagnostic = "account wait retained its lost global producer"
    features = ["--no-default-features"]
elif claim in ("work-arithmetic", "work-mixed"):
    source = "rust/crates/filesystem/src/performance.rs"
    crate = "acyclic-fs"
    test = "performance::tests::full_width_work_arithmetic_preserves_exact_sums_and_refusals"
    diagnostic = "full-width work sum disagreed with wider arithmetic"
    features = []
elif claim == "ready-source-allocation":
    source = "rust/crates/stream/src/runtime/sources.rs"
    crate = "acyclic-stream"
    test = "runtime::sources::tests::ready_records_reuse_the_registered_source_node"
    diagnostic = "ready record scheduling allocated physical heap storage"
    features = ["--no-default-features"]
elif claim == "budget-errors":
    source = "rust/crates/filesystem/src/performance.rs"
    crate = "acyclic-fs"
    test = "performance::tests::native::borrowed::borrowed_verification_preserves_exact_errors_and_priority"
    diagnostic = "budget refusal lost the exact counter or priority"
    features = []
elif claim == "borrowed-budget":
    source = "rust/crates/filesystem/src/kernel/allocation/accounting.rs"
    crate = "acyclic-fs"
    test = "performance::tests::native::borrowed::completed_kernel_work_borrows_the_original_budget_without_allocating"
    diagnostic = "verification copied the original budget"
    features = []
elif claim == "checkpoint-policy":
    source = "rust/crates/actors/src/local/runtime.rs"
    crate = "acyclic-actors"
    test = "local::runtime::tests::checkpoint::checkpoint_policy_settles_original_invocation_and_reopens"
    diagnostic = "checkpoint policy lost original terminal:"
    features = []
elif claim == "handler-policy":
    source = "rust/crates/actors/src/local/runtime.rs"
    crate = "acyclic-actors"
    test = "local::runtime::tests::checkpoint::handler_deadline_settles_original_invocation_and_reopens"
    diagnostic = "handler deadline lost original terminal:"
    features = []
elif claim == "guest-failure":
    source = "rust/crates/actors/src/guest/authoring.rs"
    crate = "acyclic-actors"
    test = "local::runtime::tests::checkpoint::checkpoint_policy_settles_original_invocation_and_reopens"
    diagnostic = "invocation failure codec lost original typed policy"
    features = []
elif claim == "view-demand":
    source = "rust/crates/harness-client/src/demand.rs"
    crate = "acyclic-harness-client"
    test = "view_capacity_depends_only_on_distinct_interest"
    diagnostic = "distinct-interest admission:"
    features = []
    target = ["--test", "demand"]
else:
    source = "rust/crates/filesystem/src/kernel/allocation/accounting.rs"
    crate = "acyclic-fs"
    test = "kernel::allocation::accounting::charge_tests::native_charge_matches_the_old_path_at_every_completed_prefix"
    diagnostic = "kernel charge allocated physical heap storage"
    features = []
SOURCE = ROOT / source
COMMAND = ["cargo", "test", "--locked", "-p", crate, *features, *target, test,
           "--", "--exact", "--nocapture"]
RECOVERY = f"git diff -- {source}; inspect target/{claim}/*.log; correct source or command, then python3 rust/proofs/qualify.py {claim}"


def require(condition, step):
    if not condition:
        raise RuntimeError(f"step={step}; recovery: {RECOVERY}")


def run(label):
    environment = os.environ.copy()
    environment.setdefault("PROPTEST_RNG_SEED", "1697")
    result = subprocess.run(COMMAND, cwd=ROOT, capture_output=True, text=True, env=environment)
    output = result.stdout + result.stderr
    directory = ROOT / "target" / claim
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{label}.log").write_text(output, encoding="utf-8")
    print(f"step={label}; command={COMMAND!r}; exit={result.returncode}", flush=True)
    for line in result.stdout.splitlines():
        if line.startswith("test result:"):
            print(line, flush=True)
    print("\n".join(output.splitlines()[-35:]), flush=True)
    return result.returncode, output


def main():
    original = SOURCE.read_bytes()
    mutation = ("work-arithmetic", "mixed-mutation.patch") if claim == "work-mixed" else (claim, "mutation.patch")
    patch = str(ROOT / "rust/proofs" / mutation[0] / mutation[1])
    require(subprocess.run(["git", "apply", "--check", patch], cwd=ROOT).returncode == 0,
            "check mutation anchors")
    try:
        require(subprocess.run(["git", "apply", patch], cwd=ROOT).returncode == 0,
                "apply production mutation")
        code, output = run("mutated")
        require(code == 101 and diagnostic in output and "0 passed; 1 failed" in output,
                "unchanged oracle rejects production mutation")
    finally:
        SOURCE.write_bytes(original)
    require(SOURCE.read_bytes() == original, "restore exact original source")
    code, output = run("restored")
    require(code == 0 and "1 passed; 0 failed" in output,
            "restored original oracle completes")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"step={claim} qualification; cause={error}; recovery: {RECOVERY}") from error
