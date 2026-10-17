import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {models, rules, violations} from './check-loom-models.mjs';

const path = 'rust/crates/example/src/model.rs';
// A healthy real model: it drives a production component and asserts that
// the crate shim is the loom lock, which is not a model-owned lock.
const healthy = [
  'const _: fn(crate::sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;',
  'fn production_component() {} fn check() { loom::model(production_component); }',
].join('\n');
const bounded = 'fn bounded(builder: &mut Builder) {\nbuilder.preemption_bound = Some(2);\n}\n';
const constant = 'const CAP: usize = 2;\nfn bounded(builder: &mut Builder) {\nbuilder.preemption_bound = Some(CAP);\n}\n';
// A healthy model whose extra function runs `body`; fixtures are whole Rust files.
const owning = (body, prefix = '') => `${prefix}${healthy}\nfn extra() { ${body} }`;
const real = {[path]: {grade: 'real'}};

// Each mutant must be reported by exactly its named rule: with every rule the
// gate reports it, and with only that rule deleted the same fixture is clean.
const mutants = [
  ['unregistered', {[path]: healthy}, {}],
  ['grade', {[path]: healthy}, {[path]: {grade: 'substitute'}}],
  ['scope', {[path]: healthy}, {[path]: {grade: 'abstract', scope: ' '}}],
  // The deleted local admission model: provider head state behind a loom lock.
  ['locks', {[path]: 'use loom::{sync::Mutex as DataMutex, thread};\n' + healthy}, real],
  // The deleted refresh model: a shim lock owning a hand-advanced generation.
  ['locks', {[path]: owning('let index = Arc::new(Mutex::new(Generation { position: 1 }));')}, real],
  ['locks', {[path]: healthy}, {[path]: {grade: 'real', constructs: 'production lock'}}],
  // Aliases of a lock type still construct a lock.
  ['locks', {[path]: owning('let head = DataMutex::new(Head::default());', 'use crate::sync::SyncMutex as DataMutex;\n')}, real],
  ['locks', {[path]: owning('let head = Cell::new(0);', 'type Lock<T> = crate::sync::Mutex<T>;\ntype Cell<T> = Lock<T>;\n')}, real],
  // Nested, unit, tuple, array and function-type turbofish payloads.
  ['locks', {[path]: owning('let state = SyncMutex::<Vec<u8>>::new(Vec::new());')}, real],
  ['locks', {[path]: owning('let state = SyncMutex::<()>::new(());')}, real],
  ['locks', {[path]: owning('let state = SyncMutex::<(u8, Vec<u8>)>::new((0, Vec::new()));')}, real],
  ['locks', {[path]: owning('let state = Mutex::<[u8; 4]>::default();')}, real],
  ['locks', {[path]: owning('let state = Mutex::<Box<dyn Fn() -> u8>>::new(Box::new(|| 0));')}, real],
  // The five forms that escaped the text matcher.
  ['locks', {[path]: owning('let state = SyncMutex::<[u8; { 4 }]>::new([0; 4]);')}, real],
  ['locks', {[path]: owning('let state = SyncMutex::<[u8; (8 >> 1)]>::new([0; 4]);')}, real],
  ['locks', {[path]: owning('let state = SyncMutex::<Vec</* > */ u8>>::new(Vec::new());')}, real],
  ['locks', {[path]: owning('let state = <SyncMutex<()>>::new(());')}, real],
  ['locks', {[path]: owning('let state = SyncMutex /* x */ ::<u8>::new(0);')}, real],
  // A real model that does not parse cannot be judged lock-free.
  ['locks', {[path]: healthy + '\nfn broken( {'}, real],
  ['bound', {[path]: bounded + healthy}, real],
  ['bound', {[path]: healthy}, {[path]: {grade: 'real', bound: 'two preemptions'}}],
  // A constant or forwarded bound is still a bound.
  ['bound', {[path]: constant + healthy}, real],
  ['bound', {[path]: healthy + '\nfn check_with_bound(_: Option<usize>, _: ()) {} fn run(bound: Option<usize>) { check_with_bound(bound, ()); }'}, real],
  ['exhaustive', {[path]: '// Every interleaving of the race.\n' + constant + healthy}, {[path]: {grade: 'real', bound: 'CAP preemptions'}}],
  ['exhaustive', {[path]: '// Exhaustive schedules of the race.\n' + bounded + healthy}, {[path]: {grade: 'real', bound: 'two preemptions'}}],
  ['missing', {}, real],
];

test('every repository Loom model is classified and clean', () => {
  const files = execFileSync('git', ['ls-files', '-z', '--', '*.rs'], {encoding: 'utf8'}).split('\0').filter(Boolean);
  const sources = new Map(files.map(file => [file, readFileSync(file, 'utf8').replaceAll('\r\n', '\n')]));
  assert.deepEqual(violations(sources), []);
  assert.ok(Object.keys(models).length > 0);
  for (const model of Object.values(models)) assert.notEqual(model.grade, 'substitute');
});

test('the healthy fixture passes every rule', () => {
  assert.deepEqual(violations(new Map([[path, healthy]]), {[path]: {grade: 'real'}}), []);
  assert.deepEqual(violations(new Map([[path, bounded + healthy]]), {[path]: {grade: 'real', bound: 'two preemptions'}}), []);
  // Defining the bounded runner or disabling the bound is not itself a bound.
  const runner = 'pub fn check_with_bound(\n    preemptions: Option<usize>,\n) {}\nfn unbounded() { builder.preemption_bound = None; }\n';
  assert.deepEqual(violations(new Map([[path, runner + healthy]]), {[path]: {grade: 'real'}}), []);
  // A lock named inside another type's generic list constructs that type.
  for (const container of [
    'let locks = Vec::<SyncMutex::<Vec<u8>>>::new();',
    'let lock = Option::<SyncMutex::<Vec<u8>>>::default();',
    'let locks = Vec::<SyncMutex<u8>>::new();',
    '/* SyncMutex::new(0) */',
    'let text = "SyncMutex::new(0)";',
  ]) {
    assert.deepEqual(violations(new Map([[path, owning(container)]]), real), [], container);
  }
});

for (const [index, [rule, sources, registry]] of mutants.entries()) {
  test(`mutant ${index} is reported by rule ${rule} alone`, () => {
    const files = new Map(Object.entries(sources));
    const found = violations(files, registry);
    assert.ok(found.length > 0, `${rule} mutant passed the gate`);
    assert.deepEqual(found, violations(files, registry, {[rule]: rules[rule]}));
    const {[rule]: _deleted, ...others} = rules;
    assert.deepEqual(violations(files, registry, others), [], `${rule} mutant failed another rule`);
  });
}

test('every rule has a mutant', () => {
  assert.deepEqual(new Set(mutants.map(([rule]) => rule)), new Set(Object.keys(rules)));
});
