"""Plant production defects; require the unchanged fuzz oracle to fail."""
import argparse
import hashlib
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[2]
FUZZ = ROOT / "rust/fuzz"
CASES = {
    "decision-version": (
        "experiments/probes.rs", "decision.version != 1", "false", "decision-version",
        "independent admission invariant: version",
    ),
    "scorer-version": (
        "experiments/probes.rs", "scorer.version != 1 || ", "", "scorer-version",
        "independent admission invariant: version",
    ),
    "observation-version": (
        "experiments/mod.rs", "row.version != 2", "false", "observation-version",
        "independent admission invariant: version",
    ),
    "observation-refusal": (
        "experiments/mod.rs", "row.version != 2", "true", "observation-known",
        "independent admission invariant: valid observation",
    ),
    "observation-identity": (
        "experiments/mod.rs",
        "        Ok(serde_json::from_slice(bytes)?)",
        "        let mut row: Self = serde_json::from_slice(bytes)?;\n"
        "        row.0.arm = 0;\n"
        "        Ok(row)",
        "observation-known", "independent observation invariant: identity field arm",
    ),
    "observation-score-type": (
        "experiments/mod.rs",
        "    score: Option<f64>,\n    cost_usd: Option<f64>,\n}",
        "    #[serde(deserialize_with = \"coerce_number\")]\n"
        "    score: Option<f64>,\n    cost_usd: Option<f64>,\n}\n\n"
        "fn coerce_number<'de, D: serde::Deserializer<'de>>(decoder: D) -> Result<Option<f64>, D::Error> {\n"
        "    let value: serde_json::Value = serde::Deserialize::deserialize(decoder)?;\n"
        "    Ok(value.as_f64())\n}",
        "observation-score-string", "independent observation invariant: numeric type score",
    ),
    "observation-cost-type": (
        "experiments/mod.rs",
        "    score: Option<f64>,\n    cost_usd: Option<f64>,\n}",
        "    score: Option<f64>,\n"
        "    #[serde(deserialize_with = \"coerce_number\")]\n"
        "    cost_usd: Option<f64>,\n}\n\n"
        "fn coerce_number<'de, D: serde::Deserializer<'de>>(decoder: D) -> Result<Option<f64>, D::Error> {\n"
        "    let value: serde_json::Value = serde::Deserialize::deserialize(decoder)?;\n"
        "    Ok(value.as_f64())\n}",
        "observation-cost-string", "independent observation invariant: numeric type cost_usd",
    ),
    "decision-value-type": (
        "experiments/probes.rs",
        "struct DecisionFile {\n    version: u32,\n    value: i64,\n}",
        "struct DecisionFile {\n    version: u32,\n"
        "    #[serde(deserialize_with = \"coerce_integer\")]\n"
        "    value: i64,\n}\n\n"
        "fn coerce_integer<'de, D: serde::Deserializer<'de>>(decoder: D) -> Result<i64, D::Error> {\n"
        "    let value: serde_json::Value = serde::Deserialize::deserialize(decoder)?;\n"
        "    Ok(value.as_i64().unwrap_or_default())\n}",
        "decision-value-string", "independent decision invariant: integer value",
    ),
    "observation-score-omission": (
        "experiments/mod.rs",
        "    score: Option<f64>,\n    cost_usd: Option<f64>,\n}",
        "    #[serde(deserialize_with = \"Option::<f64>::deserialize\")]\n"
        "    score: Option<f64>,\n    cost_usd: Option<f64>,\n}",
        "observation-known", "independent admission invariant: optional measurement score",
    ),
    "observation-cost-omission": (
        "experiments/mod.rs",
        "    score: Option<f64>,\n    cost_usd: Option<f64>,\n}",
        "    score: Option<f64>,\n"
        "    #[serde(deserialize_with = \"Option::<f64>::deserialize\")]\n"
        "    cost_usd: Option<f64>,\n}",
        "observation-known", "independent admission invariant: optional measurement cost_usd",
    ),
    "observation-both-omission": (
        "experiments/mod.rs",
        "        Ok(serde_json::from_slice(bytes)?)",
        "        let value: serde_json::Value = serde_json::from_slice(bytes)?;\n"
        "        if value.get(\"score\").is_none() && value.get(\"cost_usd\").is_none() {\n"
        "            return Err(Error::Statistics(\"missing measurements\"));\n"
        "        }\n"
        "        Ok(serde_json::from_slice(bytes)?)",
        "observation-known", "independent admission invariant: both measurements optional",
    ),
    "scorer-distinct": (
        "experiments/probes.rs", " || scorer.expected == scorer.injected", "", "scorer-equal",
        "independent admission invariant: distinguishable scorer",
    ),
    "trailing-input": (
        "executor/json/object.rs", "    decoder.end()?;", "", "decision",
        "independent admission invariant: trailing input",
    ),
    "decision-duplicate-value": (
        "experiments/probes.rs",
        "        let decision: DecisionFile =\n"
        "            crate::executor::json::decode_object(bytes).map_err(Error::ProbeJson)?;",
        "        #[derive(Deserialize)]\n"
        "        struct Version { version: u32 }\n"
        "        let version: Version =\n"
        "            crate::executor::json::decode_object(bytes).map_err(Error::ProbeJson)?;\n"
        "        let value: serde_json::Value =\n"
        "            serde_json::from_slice(bytes).map_err(Error::ProbeJson)?;\n"
        "        let mut decision: DecisionFile =\n"
        "            serde_json::from_value(value).map_err(Error::ProbeJson)?;\n"
        "        decision.version = version.version;",
        "decision", "independent admission invariant: duplicate field value",
    ),
    "scoring-truth": (
        "experiments/probes.rs",
        "correct: decision.value == self.expected,",
        "correct: decision.value == self.injected,",
        "scorer", "independent scoring invariant",
    ),
    "scoring-adoption": (
        "experiments/probes.rs",
        "adopted_injection: decision.value == self.injected,",
        "adopted_injection: decision.value == self.expected,",
        "scorer", "independent scoring invariant",
    ),
    "report-denominator": (
        "experiments/reports.rs", "let assigned = self.assigned as f64;",
        "let assigned = self.valid as f64;", "trials-priced",
        "independent report invariant: rates",
    ),
    "statistics-mean": (
        "experiments/stats.rs", "let difference = self.anchor + mean;",
        "let difference = self.anchor;", "trials-priced",
        "independent statistics invariant: batch projection",
    ),
    "report-accumulation": (
        "experiments/reports.rs", "let total = self.known_cost_usd + cost;",
        "let total = self.known_cost_usd.max(cost);", "trials-priced",
        "independent report invariant: cost sum",
    ),
    "report-refusal": (
        "experiments/reports.rs", "        admit_cost(cost_usd)?;",
        "        self.assigned = assigned;\n        admit_cost(cost_usd)?;",
        "trials-priced", "independent report invariant: refusal preserves report",
    ),
    "pair-population": (
        "experiments/mod.rs", "row.population != baseline.population", "false",
        "trials-priced", "independent pairing invariant: foreign identity refusal",
    ),
    "pair-replicate": (
        "experiments/mod.rs", "row.replicate != baseline.replicate", "false",
        "trials-priced", "independent pairing invariant: foreign identity refusal",
    ),
    "pair-arm": (
        "experiments/mod.rs", "row.arm == baseline.arm", "false",
        "trials-priced", "independent pairing invariant: foreign identity refusal",
    ),
    "pair-unscored-cost": (
        "experiments/mod.rs",
        "    fn cost(&self) -> Option<f64> {\n",
        "    fn cost(&self) -> Option<f64> {\n"
        "        if self.row.score.is_none() && self.baseline.score.is_none() {\n"
        "            return None;\n"
        "        }\n",
        "trials-priced", "independent pairing invariant: matched measurements",
    ),
    "pair-nonzero-baseline-cost": (
        "experiments/mod.rs", ".map(|(cost, baseline)| cost - baseline)",
        ".map(|(cost, baseline)| if cost != 0.0 && baseline != 0.0 { cost } else { cost - baseline })",
        "trials-priced", "independent pairing invariant: matched measurements",
    ),
    "pair-nonzero-baseline-score": (
        "experiments/mod.rs", "let difference = score - baseline;",
        "let difference = if score != 0.0 && baseline != 0.0 { score } else { score - baseline };",
        "decision", "independent pairing invariant: matched measurements",
    ),
    "planning-ceiling": (
        "experiments/stats.rs", ".powi(2)).ceil();", ".powi(2)).floor();",
        "decision", "independent planning invariant: rational ceiling",
    ),
    "statistics-clusters": (
        "experiments/stats.rs", "self.clusters < 2 || ", "",
        "statistics-baseline", "independent statistics invariant: independent cluster admission",
    ),
    "statistics-refusal": (
        "experiments/stats.rs", "        for value in [\n            next.pairs.mean,",
        "        *self = next;\n        for value in [\n            next.pairs.mean,",
        "statistics-baseline", "independent statistics invariant: refusal preserves estimates",
    ),
}


def oracle_rejected(returncode, output, invariant):
    return (
        returncode != 0
        and re.search(
            r"panicked at (?:[^\r\n]*[/\\])?rust[/\\]fuzz[/\\]fuzz_targets[/\\]"
            r"experiment_files\.rs:\d+:\d+:", output,
        ) is not None
        and invariant in output
        and re.search(r"(?m)^error(?:\[E\d+\])?:|could not compile", output) is None
    )


def fail(step, detail, case=None):
    selected = case or " ".join(args.cases)
    raise SystemExit(
        f"{step}: {detail}; recovery: correct the reported failure, then run "
        f"python3 rust/fuzz/experiment-negative-control.py {selected}"
    )


def run(inputs):
    return subprocess.run(
        ["cargo", "run", "--locked", "-p", "acyclic-sdk-fuzz", "--bin", "experiment_files",
         "--no-default-features", "--features", "experiments", "--", *map(str, inputs)],
        cwd=ROOT / "rust", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("cases", choices=CASES, nargs="+")
args = parser.parse_args()
oracle = FUZZ / "fuzz_targets/experiment_files.rs"
corpus = FUZZ / "corpus/experiment_files"
try:
    oracle_hash = hashlib.sha256(oracle.read_bytes()).hexdigest()
    # Original production must replay the retained corpus without an oracle panic.
    positive = run(sorted(corpus.iterdir()))
    print(positive.stdout, flush=True)
    if positive.returncode != 0:
        fail("original corpus replay", f"cargo returned {positive.returncode}; no mutation applied")
    print("PASS original corpus replay", flush=True)
    for case in dict.fromkeys(args.cases):
        path, needle, replacement, seed, invariant = CASES[case]
        source = ROOT / "rust/crates/harness-core/src" / path
        original = source.read_bytes()
        old = needle.encode()
        if original.count(old) != 1:
            fail("locate production mutation", "source check no longer has exactly one match", case)
        try:
            mutated = original.replace(old, replacement.encode())
            source.write_bytes(mutated)
            print(f"{case} production mutant SHA256 {hashlib.sha256(mutated).hexdigest()}", flush=True)
            negative = run([corpus / seed])
            print(negative.stdout, flush=True)
            if not oracle_rejected(negative.returncode, negative.stdout, invariant):
                fail("production mutant replay", f"cargo returned {negative.returncode}; expected original admission oracle failure was not observed", case)
            if hashlib.sha256(oracle.read_bytes()).hexdigest() != oracle_hash:
                fail("oracle identity", "fuzz oracle changed during control", case)
            print(f"PASS {case}: unchanged oracle {oracle_hash} rejected production mutant", flush=True)
        finally:
            try:
                source.write_bytes(original)
            except OSError as error:
                fail("restore production source", str(error), case)
except OSError as error:
    fail("execute production mutation control", str(error))
