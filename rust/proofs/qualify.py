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
parser.add_argument("claim", choices=['retained-credit', 'settlement-terminal', 'admission-rebind', 'objects-empty-dispatch', 'auth-producer-loss', 'auth-generation', 'work-arithmetic', 'work-mixed', 'borrowed-budget', 'native-charge', 'budget-errors', 'ready-source-allocation', 'scalar-charge-cost', 'guest-refusal', 'trust-catchup', 'ready-source-turn', 'ready-source-race', 'projection-cost', 'changes-decode-cost', 'change-marker-cost', 'partial-projection-cost', 'projection-fuzz', 'default-projection-fuzz', 'follow-digest-cost', 'children-selection'])
parser.add_argument("--instrumented", action="store_true",
                    help="challenge the actual projection fuzz target and explore 1000 inputs")
arguments = parser.parse_args()
claim = arguments.claim
if arguments.instrumented and claim != "projection-fuzz":
    parser.error("--instrumented requires projection-fuzz")
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
elif claim == "projection-cost":
    source = "rust/crates/stream/src/changes.rs"
    crate = "acyclic-stream"
    test = "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding"
    diagnostic = "unrestricted projection must transfer the original owners"
    features = []
elif claim == "partial-projection-cost":
    source = "rust/crates/stream/src/changes.rs"
    crate = "acyclic-stream"
    test = "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding"
    diagnostic = "discarded change payload must not be copied"
    features = []
elif claim in ("projection-fuzz", "default-projection-fuzz"):
    source = "rust/crates/stream/src/changes.rs"
    crate = "acyclic-stream"
    test = "follow_filter::tests::projection_reuses_validation_and_skips_unrestricted_decoding"
    diagnostic = "projection fuzz oracle"
    features = ["--features", "test-support"] if claim == "projection-fuzz" else []
elif claim == "follow-digest-cost":
    source = "rust/crates/stream/src/follow_filter.rs"
    crate = "acyclic-stream"
    test = "follow_filter::tests::provider_hops_preserve_requested_any_keys_and_all_authority_ceilings"
    diagnostic = "digest scratch storage grew with repeated clauses"
    features = []
elif claim == "change-marker-cost":
    source = "rust/crates/stream/src/changes.rs"
    crate = "acyclic-stream"
    test = "changes::golden::retained_and_unknown_change_versions_keep_identity_and_one_marker_pass"
    diagnostic = "reserved marker classification repeated an attribute probe"
    features = []
elif claim == "changes-decode-cost":
    source = "rust/crates/stream/src/changes.rs"
    crate = "acyclic-stream"
    test = "changes::tests::admitted_decode_reserves_once_without_trusting_opaque_or_forged_keys"
    diagnostic = "validated decode must not allocate discarded storage"
    features = []
elif claim == "auth-producer-loss":
    source = "rust/crates/auth/src/trust/follower.rs"
    crate = "acyclic-auth"
    test = "trust::follower::tests::account_events_preserve_exact_dependencies_without_unrelated_wakes"
    diagnostic = "account wait retained its lost global producer"
    features = ["--no-default-features"]
elif claim == "auth-generation":
    source = "rust/crates/auth/src/trust/follower.rs"
    crate = "acyclic-auth"
    test = "trust::follower::tests::swap_precedes_wake_and_lost_follow_fences"
    diagnostic = "generation exhaustion retained a live publication owner"
    features = ["--no-default-features"]
elif claim in ("work-arithmetic", "work-mixed"):
    source = "rust/crates/filesystem/src/performance.rs"
    crate = "acyclic-fs"
    test = "performance::tests::full_width_work_arithmetic_preserves_exact_sums_and_refusals"
    diagnostic = "full-width work sum disagreed with wider arithmetic"
    features = []
elif claim == "trust-catchup":
    source = "rust/crates/stream/src/trust_feed.rs"
    crate = "acyclic-stream"
    test = "trust_feed::tests::losing_one_account_follow_fences_everything"
    diagnostic = "reconnect served trust before its original observed tail"
    features = ["--no-default-features"]
elif claim == "ready-source-race":
    source = "rust/crates/stream/src/runtime/sources.rs"
    crate = "acyclic-stream"
    test = "client_pool_loom::provider_source_publication_cannot_lose_its_wake"
    diagnostic = "provider source lost its publication wake"
    features = ["--no-default-features", "--features", "grpc,model-checking"]
elif claim == "children-selection":
    source = "rust/crates/stream/src/lib/head.rs"
    crate = "acyclic-stream"
    test = "head_tests::trusted_heads_use_the_result_budget_and_wire_heads_enforce_the_byte_bound"
    diagnostic = "child page escaped its original selection"
    features = ["--features", "local"]
elif claim in ("ready-source-allocation", "ready-source-turn"):
    source = "rust/crates/stream/src/runtime/sources.rs"
    crate = "acyclic-stream"
    test = "runtime::sources::tests::ready_records_reuse_the_registered_source_node"
    diagnostic = ("ready record scheduling allocated physical heap storage" if claim == "ready-source-allocation"
                  else "ready record delivery drained unrelated provider hints")
    features = ["--no-default-features"]
elif claim == "guest-refusal":
    source = "rust/crates/actors/src/guest/state.rs"
    crate = "acyclic-actors"
    test = "guest_loom::state::tests::lock_order_checker_rejects_same_and_different_state_nesting"
    diagnostic = "nested mutation lost its typed refusal"
    features = ["--features", "model-checking"]
    target = ["--test", "main"]
elif claim == "scalar-charge-cost":
    source = "rust/crates/filesystem/src/performance.rs"
    crate = "acyclic-fs"
    test = "performance::tests::native::borrowed::item_only_kernel_charge_preserves_exact_errors_receipts_and_cost"
    diagnostic = "scalar charge performed unrelated counter arithmetic"
    features = ["--no-default-features", "--features", "distributed"]
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
else:
    source = "rust/crates/filesystem/src/kernel/allocation/accounting.rs"
    crate = "acyclic-fs"
    test = "kernel::allocation::accounting::charge_tests::native_charge_matches_the_old_path_at_every_completed_prefix"
    diagnostic = "kernel charge allocated physical heap storage"
    features = []
SOURCE = ROOT / source
if claim == "ready-source-race":
    target = ["--test", "main"]
COMMAND = ["cargo", "test", "--locked", "-p", crate, *features, *target, test,
           "--", "--exact", "--nocapture"]
RECOVERY = f"git diff -- {source}; inspect target/{claim}/*.log; correct source or command, then python3 rust/proofs/qualify.py {claim}"
if arguments.instrumented:
    RECOVERY += " --instrumented"


def require(condition, step):
    if not condition:
        raise RuntimeError(f"step={step}; recovery: {RECOVERY}")


def run(label):
    environment = os.environ.copy()
    environment.setdefault("PROPTEST_RNG_SEED", "1697")
    if claim in ("guest-refusal", "ready-source-race"):
        wrapper = "loom-rustc.cmd" if os.name == "nt" else "loom-rustc.sh"
        environment["RUSTC_WORKSPACE_WRAPPER"] = str(ROOT / "scripts" / wrapper)
    command = COMMAND
    directory = ROOT / "target" / claim
    directory.mkdir(parents=True, exist_ok=True)
    if arguments.instrumented:
        corpus = "corpus/stream_persistence"
        # Challenge an original captured writer packet before exploring the
        # restored decoder. Fresh discoveries belong to the output directory.
        inputs = ([f"{corpus}/projection-v1-append"] if label == "mutated" else
                  [str(directory / "corpus"), corpus])
        (directory / "corpus").mkdir(exist_ok=True)
        command = ["cargo", "+nightly", "fuzz", "run", "--fuzz-dir", ".",
                   "stream_persistence", *inputs, "--", "-runs=1000", "-seed=1697"]
    result = subprocess.run(command,
                            cwd=ROOT / "rust/codec-fuzz" if arguments.instrumented else ROOT,
                            capture_output=True, text=True, env=environment)
    output = result.stdout + result.stderr
    (directory / f"{label}.log").write_text(output, encoding="utf-8")
    print(f"step={label}; command={command!r}; exit={result.returncode}", flush=True)
    for line in result.stdout.splitlines():
        if line.startswith("test result:"):
            print(line, flush=True)
    print("\n".join(output.splitlines()[-35:]), flush=True)
    if arguments.instrumented:
        print("\n".join(line for line in output.splitlines()
                        if "assertion failed:" in line or "ERROR: libFuzzer:" in line), flush=True)
    return result.returncode, output


def main():
    original = SOURCE.read_bytes()
    mutation = (("work-arithmetic", "mixed-mutation.patch") if claim == "work-mixed" else
                ("ready-source-parking", "mutation.patch") if claim == "ready-source-race" else
                ("projection-fuzz", "mutation.patch") if claim == "default-projection-fuzz" else
                (claim, "mutation.patch"))
    mutations = [(mutation, diagnostic)]
    if claim == "ready-source-turn":
        mutations += [
            (("ready-source-pending", "mutation.patch"), "owner turn polled unrelated pending providers"),
            (("ready-source-parking", "mutation.patch"), "a late cancelled wake must not disarm the live provider"),
            (("ready-source-delivery", "mutation.patch"), "the live provider's committed record must be ready"),
        ]
    try:
        for index, ((directory, filename), expected) in enumerate(mutations):
            SOURCE.write_bytes(original)
            patch = str(ROOT / "rust/proofs" / directory / filename)
            require(subprocess.run(["git", "apply", "--check", patch], cwd=ROOT).returncode == 0,
                    f"check {directory} mutation anchors")
            require(subprocess.run(["git", "apply", patch], cwd=ROOT).returncode == 0,
                    f"apply {directory} production mutation")
            code, output = run("mutated" if index == 0 else f"mutated-{directory}")
            rejected = (code != 0 and "assertion failed: acyclic_stream::codec_fuzz::check(bytes)" in output
                        and "ERROR: libFuzzer:" in output if arguments.instrumented else
                        code == 101 and expected in output and "0 passed; 1 failed" in output)
            require(rejected,
                    f"unchanged oracle rejects {directory} production mutation")
    finally:
        SOURCE.write_bytes(original)
    require(SOURCE.read_bytes() == original, "restore exact original source")
    code, output = run("restored")
    completed = (code == 0 and "DONE" in output and "1000 runs" in output
                 and "cov:" in output if arguments.instrumented else
                 code == 0 and "1 passed; 0 failed" in output)
    require(completed,
            "restored original oracle completes")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"step={claim} qualification; cause={error}; recovery: {RECOVERY}") from error
