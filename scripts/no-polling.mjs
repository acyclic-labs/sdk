import {createHash} from 'node:crypto';
import ts from 'typescript';

// This is a conservative source rule, not a liveness proof. Loops require an
// exact finite-work or producer-event disposition. In particular, changing a
// helper's spelling does not exempt the loop or its underlying timer reference.
const temporal = /^(?:sleep(?:_until)?|usleep|nanosleep|Sleep(?:Ex)?|delay|interval(?:_at)?|timeout(?:_at)?|wait_timeout(?:_while)?|pthread_cond_(?:timedwait|clockwait)|cnd_timedwait|sem_timedwait|recv_timeout|park_timeout|try_wait|yield_now|yieldNow|spin_loop|setTimeout|setInterval|setImmediate|nextTick|queueMicrotask|requestAnimationFrame|requestIdleCallback|waitAsync|waitForTimeout|waitForFunction|waitUntil)$/;
const policyName = /(?:retry|retries|backoff|poll[_A-Z]|polling|[_A-Z]INTERVAL|wait[_A-Z].*Ready)/i;
const loop = new Set(['for', 'while', 'loop', 'do']);
const literalKinds = new Set([ts.SyntaxKind.StringLiteral, ts.SyntaxKind.NoSubstitutionTemplateLiteral, ts.SyntaxKind.TemplateHead, ts.SyntaxKind.TemplateMiddle, ts.SyntaxKind.TemplateTail]);
const hostObjects = new Set(['globalThis', 'window', 'global', 'Bun', 'process']);
const opening = new Set(['(', '[', '{']);
const closing = new Set([')', ']', '}']);
const bindingEnds = new Set([';', ',', ')', ']', '}']);
const statementStarts = new Set(['let', 'const', 'fn', 'return']);
const classes = new Set([
 'caller-deadline', 'transient-transport-backoff', 'retryable-tool', 'retryable-model',
 'idempotent-identity', 'event-wait', 'finite-work', 'injected-child-stall',
]);

export const temporalModule = name => /^(?:node:)?timers(?:\/promises)?$/.test(name);

export function sourceInScope(file) {
 // This declarative file is validated as exact dispositions, never executed.
 if (file === 'scripts/no-polling-dispositions.json') return false;
 return file === 'package.json' || /^(?:rust\/crates\/|typescript\/packages\/|scripts\/)[\s\S]+\.(?:rs|[cm]?[jt]s|[jt]sx|[ch]|cc|cpp|sh|ps1|cmd|bat|proto|toml|json|ya?ml)$/.test(file);
}

/** @param {import('typescript').Expression | undefined} expression @returns {string | undefined} */
function constantString(expression) {
 if (!expression) return undefined;
 const stack = [expression];
 const pieces = [];
 while (stack.length) {
  const node = /** @type {import('typescript').Expression} */ (stack.pop());
  if (ts.isStringLiteralLike(node)) pieces.push(node.text);
  else if (ts.isParenthesizedExpression(node)) stack.push(node.expression);
  else if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.PlusToken) stack.push(node.right, node.left);
  else return undefined;
 }
 return pieces.join('');
}

function scriptTokens(file, source) {
 // Both snapshots share a virtual root; no host filesystem lookup participates
 // in resolving a source binding, and the dependency pass reuses this AST.
 const parsed = ts.createSourceFile('/' + file, source, ts.ScriptTarget.Latest, true);
 const diagnostics = /** @type {typeof parsed & {parseDiagnostics: readonly import('typescript').Diagnostic[]}} */ (parsed).parseDiagnostics;
 if (diagnostics.length) throw new Error(file + ': ' + ts.flattenDiagnosticMessageText(diagnostics[0].messageText, '\n'));
 /** @type {import('typescript').Node[]} */
 const stack = [parsed];
 const out = [];
 while (stack.length) {
  const node = /** @type {import('typescript').Node} */ (stack.pop());
  const start = node.getStart(parsed);
  const line = parsed.getLineAndCharacterOfPosition(start).line + 1;
  if ((ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) && node.moduleSpecifier
   && ts.isStringLiteralLike(node.moduleSpecifier) && temporalModule(node.moduleSpecifier.text)) {
   out.push({value: node.moduleSpecifier.text, kind: 'indirection', line, start});
  }
  if (ts.isElementAccessExpression(node)) {
   const property = constantString(node.argumentExpression);
   if (property !== undefined && (temporal.test(property) || policyName.test(property))) out.push({value: property, kind: 'word', line, start});
   else if (property === 'wait' || property === 'yield') out.push({value: property, kind: 'indirection', line, start});
   else if (property === undefined && ts.isIdentifier(node.expression) && hostObjects.has(node.expression.text)) out.push({value: node.getText(parsed), kind: 'indirection', line, start});
  }
  if (ts.isPropertyAccessExpression(node) && (node.name.text === 'wait' || node.name.text === 'yield')) out.push({value: node.name.text, kind: 'indirection', line, start});
  if (ts.isCallExpression(node) || ts.isNewExpression(node)) {
   const callee = node.expression;
   if (ts.isElementAccessExpression(callee) || callee.kind === ts.SyntaxKind.ImportKeyword
    || (ts.isIdentifier(callee) && (callee.text === 'eval' || callee.text === 'Function' || callee.text === 'require'))
    || (ts.isPropertyAccessExpression(callee) && ts.isIdentifier(callee.expression) && callee.expression.text === 'Reflect')) out.push({value: callee.getText(parsed), kind: 'indirection', line, start});
  }
  const children = node.getChildren(parsed);
  if (children.length) {
   for (let i = children.length - 1; i >= 0; i--) stack.push(children[i]);
   continue;
  }
  if (node.kind === ts.SyntaxKind.EndOfFileToken) continue;
  const kind = literalKinds.has(node.kind) ? 'literal'
   : node.kind === ts.SyntaxKind.Identifier || (node.kind >= ts.SyntaxKind.FirstKeyword && node.kind <= ts.SyntaxKind.LastKeyword) ? 'word'
   : 'punctuation';
  // The parser preserves executable template substitutions and regex boundaries.
  const value = kind === 'literal' ? /** @type {import('typescript').LiteralLikeNode} */ (node).text
   : ts.isIdentifier(node) ? node.text : node.getText(parsed);
  out.push({value, kind, line, start});
 }
 return {stream: out, parsed};
}

// Nested comments, raw strings and lifetimes have Rust lexical boundaries.
// Quoted child programs and computed property names remain visible as literals.
function nativeTokens(source, rust) {
 const out = [];
 let at = 0;
 let line = 1;
 const emit = (end, kind) => {
  const value = source.slice(at, end);
  if (kind) out.push({value, kind, line, start: at});
  line += (value.match(/\n/g) ?? []).length;
  at = end;
 };
 while (at < source.length) {
  const tail = source.slice(at);
  if (/^\s/.test(tail)) { emit(at + tail.match(/^\s+/)[0].length); continue; }
  if (tail.startsWith('//')) { emit(source.indexOf('\n', at) < 0 ? source.length : source.indexOf('\n', at)); continue; }
  if (tail.startsWith('/*')) {
   let depth = 1;
   let end = at + 2;
   while (end < source.length && depth) {
    if (source.startsWith('/*', end)) { depth++; end += 2; }
    else if (source.startsWith('*/', end)) { depth--; end += 2; }
    else end++;
   }
   if (depth) throw new Error('unterminated block comment');
   emit(end); continue;
  }
  const raw = rust && tail.match(/^(?:b|c)?r(#*)"/);
  if (raw) {
   const close = '"' + raw[1];
   const end = source.indexOf(close, at + raw[0].length);
   if (end < 0) throw new Error('unterminated raw string');
   emit(end + close.length, 'literal'); continue;
  }
  const quote = tail[0];
  const character = rust && quote === "'" && tail.match(/^'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^'\\\r\n])'/u);
  if (character) { emit(at + character[0].length, 'literal'); continue; }
  if (quote === '"' || (!rust && quote === "'")) {
   let end = at + 1;
   while (end < source.length && source[end] !== quote) end += source[end] === '\\' ? 2 : 1;
   if (end >= source.length) throw new Error('unterminated quoted string');
   emit(end + 1, 'literal'); continue;
  }
  const word = tail.match(/^[A-Za-z_$][A-Za-z0-9_$]*/);
  emit(at + (word ? word[0].length : 1), word ? 'word' : 'punctuation');
 }
 return out;
}

export function pollingUnit(file, source) {
 const parsed = /\.(?:[cm]?[jt]s|[jt]sx)$/.test(file) ? scriptTokens(file, source) : undefined;
 const stream = parsed?.stream ?? nativeTokens(source, file.endsWith('.rs'));
 // A changed producer/condition elsewhere in the source unit must reopen the
 // disposition; a short context hash could preserve approval of a changed loop.
 const unit = createHash('sha256').update(JSON.stringify(stream.map(t => [t.kind, t.value]))).digest('hex');
 const aliases = new Map();
 const forbidden = new Set(stream.filter(t => t.kind === 'word' && (temporal.test(t.value) || policyName.test(t.value))).map(t => t.value));
 const edge = (from, to) => {
  if (!from || !to) return;
  if (!aliases.has(from)) aliases.set(from, new Set());
  aliases.get(from).add(to);
 };
 const bindings = [];
 let depth = 0;
 for (let i = 0; i < stream.length; i++) {
  const t = stream[i];
  // Only the innermost initializer owns a reference. Its binding name already
  // belongs to its outer initializer, so dependency closure handles nesting.
  while (bindings.length) {
   const active = bindings[bindings.length - 1];
   if (depth > active.depth || (!(t.kind === 'punctuation' && bindingEnds.has(t.value))
    && !(t.kind === 'word' && t.line > active.line && statementStarts.has(t.value)))) break;
   bindings.pop();
  }
  if (t.kind === 'word') {
   if (bindings.length) edge(t.value, bindings[bindings.length - 1].name);
   if (stream[i + 1]?.value === 'as') edge(t.value, stream[i + 2]?.value);
   if (stream[i + 1]?.value === ':' && stream[i + 2]?.kind === 'word') edge(t.value, stream[i + 2].value);
   if (stream[i + 1]?.value === '=' && stream[i + 2]?.value !== '=' && stream[i + 2]?.value !== '>') bindings.push({name: t.value, depth, line: t.line});
  }
  if (t.kind === 'punctuation' && opening.has(t.value)) depth++;
  if (t.kind === 'punctuation' && closing.has(t.value)) depth--;
 }
 const queue = [...forbidden];
 for (let i = 0; i < queue.length; i++) {
  for (const alias of aliases.get(queue[i]) ?? []) {
   if (!forbidden.has(alias)) { forbidden.add(alias); queue.push(alias); }
  }
 }
 const sites = [];
 for (let i = 0; i < stream.length; i++) {
  const t = stream[i];
  const kind = t.kind === 'indirection' ? 'indirect-timer-or-call'
   : t.kind === 'word' && loop.has(t.value) ? 'loop'
   : t.kind === 'word' && forbidden.has(t.value) ? 'timer-or-readiness'
   : t.kind === 'literal' && (/\b(?:sleep|usleep|nanosleep|setTimeout|setInterval|Bun\.sleep|retry|backoff)\b/.test(t.value) || policyName.test(t.value)) ? 'embedded-timer-or-retry'
   : undefined;
  if (!kind) continue;
  const fingerprint = createHash('sha256').update(JSON.stringify([kind, i, unit])).digest('hex');
  sites.push({file, line: t.line, kind, fingerprint, start: t.start});
 }
 return {stream, fingerprint: unit, sites, parsed: parsed?.parsed};
}

export function pollingSites(file, source) {
 return pollingUnit(file, source).sites;
}

export function validateDispositions(dispositions) {
 if (!Array.isArray(dispositions)) throw new Error('no-polling dispositions must be an array');
 const seen = new Set();
 const fields = new Set(['file', 'fingerprint', 'classification', 'producer', 'terminal', 'negativeControl']);
 for (const item of dispositions) {
  if (!item || typeof item !== 'object' || Object.keys(item).length !== fields.size || Object.keys(item).some(key => !fields.has(key))
   || typeof item.file !== 'string' || !sourceInScope(item.file) || /[\r\n]/.test(item.file)
   || !/^[a-f0-9]{64}$/.test(item.fingerprint) || !classes.has(item.classification)
   || typeof item.producer !== 'string' || !item.producer.trim()
   || typeof item.terminal !== 'string' || !item.terminal.trim()
   || typeof item.negativeControl !== 'string' || !item.negativeControl.trim()) {
   throw new Error('invalid exact no-polling disposition: ' + JSON.stringify(item));
  }
  const key = item.file + ':' + item.fingerprint;
  if (seen.has(key)) throw new Error('duplicate no-polling disposition: ' + key);
  seen.add(key);
 }
}

export function newPollingSites(file, before, after, dispositions = []) {
 validateDispositions(dispositions);
 const retained = new Map();
 for (const site of pollingSites(file, before)) retained.set(site.fingerprint, (retained.get(site.fingerprint) ?? 0) + 1);
 for (const item of dispositions) {
  if (item.file !== file) continue;
  retained.set(item.fingerprint, Math.max(1, retained.get(item.fingerprint) ?? 0));
 }
 return pollingSites(file, after).filter(site => {
  const count = retained.get(site.fingerprint) ?? 0;
  if (count) { retained.set(site.fingerprint, count - 1); return false; }
  return true;
 });
}
