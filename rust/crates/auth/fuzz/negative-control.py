"""Require an unchanged authority fuzz oracle to catch a production defect."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parent
choice = sys.argv[1] if len(sys.argv) == 2 else "delimiter"
if choice.startswith("filesystem-"):
    root = root.parent.parent / "harness-fuzz"
    target = "filesystem_access"
    source = root.parent / "harness-core/src/core/authority/workload/admission.rs"
    oracle = source.with_name("verification.rs")
elif choice.startswith("addressing-"):
    target = "addressing"
    source = root.parent / "src" / {
        "addressing-fragment": "addressing/routes.rs",
        "addressing-resource": "addressing.rs",
        "addressing-account-empty": "addressing.rs",
        "addressing-id-empty": "addressing.rs",
        "addressing-field-endian": "addressing.rs",
        "addressing-resource-tail": "addressing.rs",
        "addressing-resource-version": "format.rs",
        "addressing-resource-domain": "format.rs",
        "addressing-resource-admission": "addressing.rs",
        "addressing-actor": "addressing/actor.rs",
        "addressing-actor-tail": "addressing/actor.rs",
        "addressing-actor-version": "format.rs",
        "addressing-actor-domain": "format.rs",
        "addressing-actor-admission": "addressing/actor.rs",
        "addressing-name-length": "addressing/actor.rs",
        "addressing-name-character": "addressing/actor.rs",
    }.get(choice, "addressing/dns.rs")
    oracle = root.parent / "tests/common/addressing.rs"
elif choice.startswith("secret-"):
    target = "secret_ref"
    source = root.parent / "src/secrets.rs"
    oracle = root.parent / "tests/common/secret_ref.rs"
elif choice.startswith("issuance-"):
    target = "capability"
    source = root.parent / "src/trust" / ("issuance/wire.rs" if choice == "issuance-reply" else "issuance.rs")
    oracle = root / "fuzz_targets/capability.rs"
elif choice == "trust-target":
    target = "trust_record"
    source = root.parent / "src/trust/follower.rs"
    oracle = root / "fuzz_targets/trust_record.rs"
else:
    target = "credential"
    source = root.parent / "src/credential.rs"
    oracle = root.parent / "tests/common/credential.rs"
step = "read original Auth source and oracle"
try:
    original = source.read_bytes()
    oracles = [oracle, root / "fuzz_targets" / (target + ".rs")]
    if choice.startswith("filesystem-"):
        oracles.append(root.parent / "harness-core/src/filesystem/access/verification.rs")
    oracle_hashes = {path: hashlib.sha256(path.read_bytes()).digest() for path in oracles}
    newline = b"\r\n" if b"\r\n" in original else b"\n"
    mutations = {
        "addressing-account-empty": (
            b"return Err(IdentityError::EmptyAccount);", b"return Ok(());",
            "empty account must retain typed refusal", "resource",
        ),
        "addressing-id-empty": (
            b"return Err(IdentityError::EmptyId);", b"return Ok(());",
            "empty resource id must retain typed refusal", "resource",
        ),
        "addressing-resource-domain": (
            b".strip_prefix(domain)", b".get(domain.len()..)",
            "identity domain must retain typed refusal", "resource",
        ),
        "addressing-actor-domain": (
            b".strip_prefix(domain)", b".get(domain.len()..)",
            "identity domain must retain typed refusal", "actor",
        ),
        "addressing-host-numeric": (
            b"return Err(DnsError::NumericHost);", b"()",
            "numeric DNS host must retain typed refusal", "resource-hostname",
        ),
        "addressing-host-length": (
            b"if host.len() > 253 {", b"if false && host.len() > 253 {",
            "oversized DNS host must refuse", "resource-hostname",
        ),
        "addressing-name-length": (
            b"(1..=128).contains(&bytes.len())", b"true",
            "namespace name aliases must refuse", "actor",
        ),
        "addressing-name-character": (
            b'b"-_.".contains(byte)', b'b"-_./".contains(byte)',
            "namespace name aliases must refuse", "actor",
        ),
        "addressing-field-endian": (
            b"u64::from_be_bytes(*prefix)", b"u64::from_le_bytes(*prefix)",
            "resource writer output must admit", "resource",
        ),
        "addressing-resource-admission": (
            b"            RESOURCE_VERSION,", b"            b\"\\x02\",",
            "resource writer output must admit", "resource",
        ),
        "addressing-actor-admission": (
            b"            b\"\\x01\",", b"            b\"\\x02\",",
            "actor writer output must admit", "actor",
        ),
        "addressing-resource-version": (
            b".strip_prefix(version)", b".get(version.len()..)",
            "identity version must refuse before fields", "resource",
        ),
        "addressing-actor-version": (
            b".strip_prefix(version)", b".get(version.len()..)",
            "identity version must refuse before fields", "actor",
        ),
        "addressing-resource-tail": (
            b"if !bytes.is_empty() {", b"if false && !bytes.is_empty() {",
            "identity framing must refuse trailing bytes", "resource",
        ),
        "addressing-actor-tail": (
            b"if !bytes.is_empty() {", b"if false && !bytes.is_empty() {",
            "identity framing must refuse trailing bytes", "actor",
        ),
        "addressing-resource": (
            b"Self::new(account, id)", b"Self::new(id, account)",
            "resource bytes must roundtrip exactly", "resource",
        ),
        "addressing-actor": (
            b"Self::new(account, namespace, id)", b"Self::new(account, id, namespace)",
            "actor bytes must roundtrip exactly", "actor",
        ),
        "addressing-case": (
            b"b.is_ascii_lowercase()", b"b.is_ascii_alphabetic()",
            "DNS label aliases must refuse", "resource-hostname",
        ),
        "addressing-length": (
            b"if label.len() > 63 {", b"if false && label.len() > 63 {",
            "oversized DNS label must refuse", "resource-hostname",
        ),
        "addressing-fragment": (
            b"            || url.fragment().is_some()", b"",
            "routing must refuse credentials, fragments and unsupported schemes", "route-0",
        ),
        "filesystem-ancestor": (
            b".is_some_and(|parent| !capabilities.is_subset_of(&parent.capabilities))",
            b".is_some_and(|parent| false && !capabilities.is_subset_of(&parent.capabilities))",
            "signed refinement removed its ancestor ceiling", "ceiling-set",
        ),
        "filesystem-certified": (
            b"if !refined.is_subset_of(&capabilities) {",
            b"if false && !refined.is_subset_of(&capabilities) {",
            "filesystem label gained uncertified authority", "ceiling-set",
        ),
        "delimiter": (
            b"        && !value.contains(['\\r', '\\n', '\\0'])", b"",
            "credential transport delimiter must refuse", "v2-opaque-bearer",
        ),
        "text-decoder": (
            b"base64::engine::general_purpose::URL_SAFE_NO_PAD" + newline + b"        .decode_vec",
            b"base64::engine::general_purpose::STANDARD_NO_PAD" + newline + b"        .decode_vec",
            "canonical capability text must decode original bytes", "v2-signed-capability",
        ),
        "json-field": (
            b"super::capability_from_text(&encoded).map_err(serde::de::Error::custom)",
            b"Ok(Vec::new())",
            "capability JSON must retain original bytes", "v2-signed-capability",
        ),
        "trust-target": (
            b"if !matches {", b"if false && !matches {",
            "trust record targets must be disjoint", "foreign-keyring",
        ),
        "issuance-request": (
            b"if request.wire() != bytes {", b"if false && request.wire() != bytes {",
            "duplicate issuance request version must refuse", "issuance-request",
        ),
        "issuance-reply": (
            b"if reply.encode_to_vec() != bytes {", b"if false && reply.encode_to_vec() != bytes {",
            "duplicate issuance reply version must refuse", "issuance-unknown",
        ),
        "secret-json": (
            b"        #[serde(deny_unknown_fields)]", b"",
            "secret reference must refuse unknown fields", "json-max-version",
        ),
        "secret-wire": (
            b"if canonical.as_slice() != input {", b"if false && canonical.as_slice() != input {",
            "secret reference must refuse duplicate wire fields", "wire-max-version",
        ),
    }
    step = "select production Auth mutation"
    if len(sys.argv) > 2 or choice not in mutations:
        raise RuntimeError("select one of: " + ", ".join(mutations))
    check, replacement, message, seed = mutations[choice]
    try:
        if original.count(check) != 1:
            raise RuntimeError("expected exactly one production mutation site")
        source.write_bytes(original.replace(check, replacement))
        command = ["cargo", "+nightly", "fuzz", "run", "--dev", "--fuzz-dir", str(root)]
        if cargo_target := os.environ.get("CARGO_TARGET_DIR"):
            command += ["--target-dir", cargo_target]
        if choice.startswith("filesystem-"):
            command += ["--features", "filesystem-access"]
        command += [target, "corpus/" + target + "/" + seed, "--", "-runs=1"]
        step = "detect " + choice + " through the " + target + " fuzzer"
        result = subprocess.run(
            command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        print(result.stdout, end="")
        panic = result.stdout.partition("panicked at ")[2]
        if (result.returncode == 0 or message not in panic
                or "ERROR: libFuzzer: deadly signal" not in panic):
            raise RuntimeError("the unchanged semantic oracle did not detect the production defect")
    finally:
        source.write_bytes(original)
    step = "verify unchanged Auth oracle"
    if any(hashlib.sha256(path.read_bytes()).digest() != digest
           for path, digest in oracle_hashes.items()):
        raise RuntimeError("Auth oracle changed during the negative control")
except (OSError, RuntimeError) as error:
    print(
        f"STEP {step} failed: {error}; inspect the original output and correct the source or command; "
        f"recovery from the repository root: python3 rust/crates/auth/fuzz/negative-control.py {choice}",
        file=sys.stderr,
    )
    sys.exit(1)
print("Auth negative control: unchanged " + choice + " oracle detected the production defect")
