"""Exercise the original writer corpus against unchanged production-code oracles."""
import hashlib
from pathlib import Path
import subprocess
import sys

package = Path(__file__).resolve().parents[1]
root = package.parents[1]
oracle = package / 'src/harness_resources.rs'
oracle_digest = hashlib.sha256(oracle.read_bytes()).digest()
command = ['cargo', 'run', '--locked', '--example', 'replay', '--',
           'harness_codecs/resource-']


def run(step, diagnostic=None):
    if hashlib.sha256(oracle.read_bytes()).digest() != oracle_digest:
        raise RuntimeError(f'{step}: fuzz oracle changed; recovery: git diff -- src/harness_resources.rs')
    result = subprocess.run(command, cwd=package, capture_output=True, text=True)
    print(result.stdout, end='', flush=True)
    print(result.stderr, end='', flush=True)
    failed_oracle = (
        'panicked at' in result.stderr
        and 'src/harness_resources.rs:' in result.stderr.replace('\\', '/')
        and diagnostic is not None and diagnostic in result.stderr
    )
    if (diagnostic is not None and (result.returncode == 0 or not failed_oracle)) or (
        diagnostic is None and result.returncode != 0
    ):
        raise RuntimeError(f'{step}: expected {diagnostic or "successful writer replay"}, exit={result.returncode}; recovery: cargo run --locked --example replay -- harness_codecs/resource-')
    print(f'{step}: {"REJECTED" if diagnostic is not None else "PASS"}', flush=True)


def main():
    mutations = [
        ('cache-version', 'executor/model_cache.rs',
         '(observation.version == Self::VERSION).then_some(observation)', 'Some(observation)',
         'unsupported cache observation version admitted'),
        ('cache-key-scope', 'executor/model_cache.rs',
         'credential_scope: self.credential_scope?,',
         'credential_scope: self.breakpoint?.digest,',
         'cache observation key identity changed'),
        ('cache-key-prefix', 'executor/model_cache.rs',
         'prefix_digest: self.breakpoint?.digest,',
         'prefix_digest: self.breakpoint?.parent_digest,',
         'cache observation key identity changed'),
        ('cache-key-version', 'executor/model_cache.rs',
         '(self.version == Self::VERSION).then_some(ModelCacheKey {',
         'Some(ModelCacheKey {', 'cache observation key identity changed'),
        ('residency-clock-query', 'executor/model_cache.rs',
         '            || self.clock_domain != clock_domain\n', '',
         'foreign clock cache query admitted'),
        ('residency-clock-update', 'executor/model_cache.rs',
         'if previous.clock_domain != clock_domain {', 'if false {',
         'foreign clock cache update admitted'),
        ('residency-key', 'executor/model_cache.rs',
         'if key != previous.key {', 'if false {', 'foreign cache key admitted'),
        ('residency-expired-hit', 'executor/model_cache.rs',
         '} else if next.expires_at_ns.is_some_and(|expiry| expiry <= at_ns) {',
         '} else if false {', 'cache residency expiry changed'),
        ('residency-deadline', 'executor/model_cache.rs',
         '.and_then(|duration| at_ns.checked_add(duration))',
         '.map(|duration| at_ns.wrapping_add(duration))', 'cache lifetime overflow admitted'),
        ('residency-version-query', 'executor/model_cache.rs',
         'if self.version != Self::VERSION\n', 'if false\n',
         'unsupported cache residency query admitted'),
        ('residency-version-update', 'executor/model_cache.rs',
         'if previous.version != Self::VERSION {', 'if false {',
         'unsupported cache residency update admitted'),
        ('residency-time-query', 'executor/model_cache.rs',
         '            || at_ns < self.observed_at_ns\n', '',
         'cache query original time contract changed'),
        ('residency-time-update', 'executor/model_cache.rs',
         'if at_ns < previous.observed_at_ns {', 'if false {',
         'reversed cache update admitted'),
        ('residency-multiply', 'executor/model_cache.rs',
         'seconds\n            .checked_mul(1_000_000_000)',
         'Some(seconds.wrapping_mul(1_000_000_000))', 'cache lifetime overflow admitted'),
        ('summary-version', 'conversation/epochs/summary.rs',
         'if wire.version != 1 {', 'if false {', 'unsupported summary version admitted'),
        ('summary-portable', 'conversation/epochs/summary.rs',
         'if wire.portable.descriptor().media_type() != "text/plain" {',
         'if false {', 'nonplaintext portable summary admitted'),
        ('summary-provenance', 'conversation/epochs/summary.rs',
         'native.provenance.validate()?;', 'let _ = &native.provenance;',
         'incomplete native summary provenance admitted'),
        ('epoch-source', 'conversation/epochs/mod.rs',
         'if &self.key.source != source {', 'if false {',
         'epoch admission contract changed'),
        ('epoch-prefix', 'conversation/epochs/mod.rs',
         'if !inherited(self.cursor(), common_prefix) {', 'if false {',
         'epoch admission contract changed'),
        ('epoch-coverage', 'conversation/epochs/mod.rs',
         'if (self.cursor() == 0) != self.summary.is_none() {', 'if false {',
         'epoch admission contract changed'),
        ('snapshot-source', 'conversation/epochs/snapshot.rs',
         'wire.epoch.validate(source, head)?;', 'let _ = (source, head);',
         'snapshot foreign source admission changed'),
        ('snapshot-parity', 'conversation/epochs/snapshot.rs',
         'if (wire.generation == 0) != (wire.epoch.cursor() == 0) {', 'if false {',
         'snapshot generation parity admission changed'),
        ('snapshot-version', 'conversation/epochs/snapshot.rs',
         '#[serde(rename = "2")]', '#[serde(rename = "2", alias = "1")]',
         'unsupported snapshot version admitted'),
        ('snapshot-confirm-replay', 'conversation/epochs/snapshot.rs',
         'if self.anchor == Some((sequence, commit_id)) {', 'if false {',
         'snapshot receipt admission changed'),
        ('snapshot-confirm-lineage', 'conversation/epochs/snapshot.rs',
         'Some((previous, original)) if original != commit_id => previous.checked_add(1),',
         'Some((previous, _)) => previous.checked_add(1),',
         'snapshot receipt admission changed'),
        ('snapshot-confirm-overflow', 'conversation/epochs/snapshot.rs',
         'previous.checked_add(1)', 'Some(previous.wrapping_add(1))',
         'snapshot receipt admission changed'),
        ('snapshot-confirm-atomicity', 'conversation/epochs/snapshot.rs',
         'if expected != Some(sequence) {',
         'self.anchor = Some((sequence, commit_id));\n        if expected != Some(sequence) {',
         'refused confirmation changed snapshot custody'),
        ('snapshot-identity', 'conversation/epochs/snapshot.rs',
         'if wire.epoch.key.digest_through(pending.through) != pending.summary {', 'if false {',
         'foreign snapshot preparation identity admitted'),
        ('snapshot-completion', 'conversation/epochs/snapshot.rs',
         'completion: pending.completion.as_ref().map(SummaryOutcome::completion),',
         'completion: None,', 'writer snapshot lost epoch or producer custody'),
        ('snapshot-failed-completion', 'conversation/epochs/snapshot.rs',
         'Self::Failed => Completion::Failed,', 'Self::Failed => Completion::Ready,',
         'completion reducer lost producer custody'),
        ('producer-failed-demand', 'conversation/epochs/protocol.rs',
         '}) => Demand::Failed(summary),', '}) => Demand::Await(summary),',
         'snapshot terminal demand changed'),
        ('producer-completion-contradiction', 'conversation/epochs/protocol.rs',
         'if pending.completion.is_some_and(|prior| prior != completion) {',
         'if false {', 'producer completion admission changed'),
        ('producer-completion-identity', 'conversation/epochs/protocol.rs',
         '.filter(|pending| pending.summary == summary)', '.filter(|_| true)',
         'producer completion admission changed'),
        ('producer-swap-readiness', 'conversation/epochs/protocol.rs',
         'if pending.completion != Some(Completion::Ready) {', 'if false {',
         'producer swap admission changed'),
        ('snapshot-result-completion', 'conversation/epochs/snapshot.rs',
         'if self.outcome.as_ref().is_some_and(|prior| prior != &outcome) {',
         'if false {', 'different terminal snapshot reference admitted'),
        ('snapshot-result-swap', 'conversation/epochs/snapshot.rs',
         '(Some(SummaryOutcome::Ready(body)), Some(summary)) if body == summary)',
         '(Some(SummaryOutcome::Ready(_)), Some(_)))',
         'different swap result reference admitted'),
        ('snapshot-swap-atomicity', 'conversation/epochs/snapshot.rs',
         'state.swap(pending.summary, expected, head)?;\n        if !matches!',
         'state.swap(pending.summary, expected, head)?;\n        self.state = state;\n        if !matches!',
         'refused swap changed snapshot custody'),
        ('snapshot-generation', 'conversation/epochs/protocol.rs',
         'if prepared.generation != generation {', 'if false {',
         'foreign pending epoch generation admitted'),
        ('snapshot-range-start', 'conversation/epochs/protocol.rs',
         'prepared.through <= cursor || prepared.through > head',
         'prepared.through > head', 'already covered pending epoch range admitted'),
        ('snapshot-range-end', 'conversation/epochs/protocol.rs',
         'prepared.through <= cursor || prepared.through > head',
         'prepared.through <= cursor', 'pending epoch range beyond source head admitted'),
    ]
    arguments = sys.argv[1:]
    selection = arguments[1:]
    if arguments and (arguments[0] != '--only' or not selection
                      or len(set(selection)) != len(selection)
                      or not set(selection) <= {row[0] for row in mutations}):
        raise RuntimeError('select mutation: expected --only GUARD ...; recovery: python3 mutations/resource-codecs.py --only snapshot-range-start snapshot-range-end')
    run('original')
    for step, relative, before, after, diagnostic in mutations:
        if selection and step not in selection:
            continue
        production = root / 'rust/crates/harness-core/src' / relative
        original = production.read_bytes()
        source = original.decode()
        if source.count(before) != 1:
            raise RuntimeError(f'{step}: expected one production guard; recovery: git diff -- {production}')
        try:
            production.write_bytes(source.replace(before, after).encode())
            run(step, diagnostic)
        finally:
            production.write_bytes(original)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'resource-codecs qualification failed: {error}; recovery: python3 mutations/resource-codecs.py after inspecting the original output and restoring source guards', file=sys.stderr)
        sys.exit(1)
