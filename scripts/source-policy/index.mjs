import {createHash} from 'node:crypto';
import ts from 'typescript-parser';

// This is a conservative source rule, not a liveness proof. Loops require an
// exact finite-work or producer-event disposition. In particular, changing a
// helper's spelling does not exempt the loop or its underlying timer reference.
// Case-insensitive candidates cover PowerShell in both sources and child programs.
const temporal = /^(?:sleep(?:_(?:for|until)|Ex|ConditionVariable(?:CS|SRW))?|thrd_sleep|usleep|(?:clock_)?nanosleep|delay|interval(?:_at)?|timeout(?:_at)?|wait_(?:for|until|timeout(?:_while|_ms)?)|pthread_cond_(?:timedwait|clockwait)|cnd_timedwait|sem_timedwait|recv_(?:timeout|deadline)|park_timeout(?:_ms)?|poll|select|epoll_(?:wait|pwait2?)|ppoll|pselect|kevent(?:64)?|try_wait|yield_now|yieldNow|spin_loop|setTimeout|setInterval|setImmediate|nextTick|queueMicrotask|requestAnimationFrame|requestIdleCallback|waitAsync|waitForTimeout|waitForFunction|waitUntil|WaitForSingleObject(?:Ex)?|WaitForMultipleObjects(?:Ex)?|WaitOnAddress|WaitOne|WaitAny|WaitAll|WaitForExit(?:Async)?|WaitForInputIdle|SpinWait|SpinUntil|(?:sqlite3_)?busy_(?:timeout|handler)|PeriodicReader|with_batch_exporter|with_scheduled_delay|(?:http2_)?keep_?alive_interval|set_tcp_keepalive)$/i;
const embeddedTemporal = new RegExp('\\b(?:' + temporal.source.slice(1, -1) + ')\\b', temporal.flags);
const policyName = /(?:retry|retries|backoff|poll[_A-Z]|polling|[_A-Z]INTERVAL|wait[_A-Z].*Ready)/i;
const embeddedCandidate = spelling => embeddedTemporal.test(spelling) || policyName.test(spelling);
const loop = new Set(['for', 'while', 'loop', 'do']);
const embeddedLoop = new RegExp('\\b(?:' + [...loop].join('|') + ')\\b', 'i');
const embeddedKind = spelling => embeddedCandidate(spelling) ? 'embedded-timer-or-retry'
 : embeddedLoop.test(spelling) ? 'embedded-loop' : undefined;
const literalKinds = new Set([ts.SyntaxKind.StringLiteral, ts.SyntaxKind.NoSubstitutionTemplateLiteral, ts.SyntaxKind.TemplateHead, ts.SyntaxKind.TemplateMiddle, ts.SyntaxKind.TemplateTail]);
const hostObjects = new Set(['globalThis', 'window', 'global', 'Bun', 'process']);
const opening = new Set(['(', '[', '{']);
const closing = new Set([')', ']', '}']);
const bindingEnds = new Set([';', ',', ')', ']', '}']);
const statementStarts = new Set(['let', 'const', 'fn', 'return']);
const cFamily = /\.(?:[ch](?:pp|xx)?|cc|hh|cs)$/;
const classes = new Set([
 'caller-deadline', 'transient-transport-backoff', 'retryable-tool', 'retryable-model',
 'idempotent-identity', 'event-wait', 'finite-work', 'negative-control',
]);

export class SourceSyntaxError extends Error {
 constructor(message) {
  super(message);
  this.name = 'SourceSyntaxError';
 }
}

export const temporalModule = name => /^(?:node:)?timers(?:\/promises)?$/.test(name);

// Host identifiers and quoted child programs can spell the same operation with
// Unicode, shell or PowerShell escapes. Decode across wrapper quoting: a
// wrapper must not turn a readiness timer into an approved ordinary identifier.
function escapedSpelling(source) {
 // Consume a backslash run once even when no Unicode escape follows it.
 return source.replace(/`(.)|\\+(?:u\{([0-9a-fA-F_]+)\}|u([0-9a-fA-F]{4})|U([0-9a-fA-F]{8})|x([0-9a-fA-F]{2})|(\r?\n|.))?/g,
  (escape, powershell, braced, short, long, byte, shell) => {
   if (powershell !== undefined) return powershell;
   if (shell !== undefined) return shell === '\n' || shell === '\r\n' ? '' : shell;
   const digits = braced ?? short ?? long ?? byte;
   if (digits === undefined) return escape;
   const point = Number.parseInt(digits.replaceAll('_', ''), 16);
   return point <= 0x10ffff ? String.fromCodePoint(point) : escape;
  });
}

export function supportedSource(file) {
 return cFamily.test(file) || /\.(?:rs|[cm]?[jt]s|[jt]sx|py|sh|bash|ps1|cmd|bat|proto|toml|json|ya?ml)$/.test(file);
}

/** @param {import('typescript-parser').Expression | undefined} expression @returns {string | undefined} */
function constantString(expression) {
 if (!expression) return undefined;
 const stack = [expression];
 const pieces = [];
 while (stack.length) {
  const node = /** @type {import('typescript-parser').Expression} */ (stack.pop());
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
 const diagnostics = /** @type {typeof parsed & {parseDiagnostics: readonly import('typescript-parser').Diagnostic[]}} */ (parsed).parseDiagnostics;
 if (diagnostics.length) {
  const diagnostic = diagnostics[0];
  const {line, character} = parsed.getLineAndCharacterOfPosition(diagnostic.start ?? 0);
  throw new SourceSyntaxError(`${file}:${line + 1}:${character + 1}: ${ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n')}`);
 }
 /** @type {import('typescript-parser').Node[]} */
 const stack = [parsed];
 const out = [];
 while (stack.length) {
  const node = /** @type {import('typescript-parser').Node} */ (stack.pop());
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
  const value = kind === 'literal' ? /** @type {import('typescript-parser').LiteralLikeNode} */ (node).text
   : ts.isIdentifier(node) ? node.text : node.getText(parsed);
  out.push({value, kind, line, start});
 }
 return {stream: out, parsed};
}

// Nested comments, raw strings and lifetimes have Rust lexical boundaries.
// Quoted child programs and computed property names remain visible as literals.
function nativeTokens(source, file) {
 const rust = file.endsWith('.rs');
 const python = file.endsWith('.py');
 const powershell = file.endsWith('.ps1');
 const shell = /\.(?:sh|bash)$/.test(file);
 const yaml = /\.ya?ml$/.test(file);
 const whitespace = yaml ? /^[ \t\r\n]+/ : /^\s+/;
 const wordComments = powershell || shell;
 const escape = powershell ? '`' : shell ? '\\' : undefined;
 const continuation = powershell ? '`\n' : '\\\n';
 const separator = powershell ? /^[;|&(){},]$/ : /^[;|&()<>]$/;
 const hashComments = /\.(?:py|sh|bash|ps1|toml|ya?ml)$/.test(file);
 const escapedIdentifiers = cFamily.test(file);
 const slashComments = escapedIdentifiers || /\.(?:rs|proto)$/.test(file);
 const blockOpen = powershell ? '<#' : '/*';
 const blockClose = powershell ? '#>' : '*/';
 const wordPattern = escapedIdentifiers
  ? /^(?:[A-Za-z_$]|\\u[0-9a-fA-F]{4}|\\U[0-9a-fA-F]{8})(?:[A-Za-z0-9_$]|\\u[0-9a-fA-F]{4}|\\U[0-9a-fA-F]{8})*/
  : powershell ? /^`?[A-Za-z_$](?:`?[A-Za-z0-9_$])*/
  : shell ? /^(?:[A-Za-z_$]|\\[A-Za-z_$])(?:[A-Za-z0-9_$]|\\[A-Za-z0-9_$]|\\\n)*/
  : /^[A-Za-z_$][A-Za-z0-9_$]*/;
 const out = [];
 let at = 0;
 let line = 1;
 let yamlFlow = 0;
 let wordBoundary = true;
 const malformed = message => new SourceSyntaxError(`${file}:${line}: ${message}`);
 const emit = (end, kind, spelling) => {
  const value = source.slice(at, end);
  if (kind) out.push({value: spelling ?? value, kind, line, start: at});
  // A hash inside an unquoted argument is data. Escaped separators stay in
  // that argument; a continued line preserves the preceding boundary.
  if (wordComments && value !== continuation && !(powershell && value === '`\r\n')) {
   wordBoundary = !kind || (kind === 'literal' ? powershell && wordBoundary : kind === 'punctuation' && separator.test(value));
  }
  line += (value.match(/\n/g) ?? []).length;
  at = end;
 };
 while (at < source.length) {
  const tail = source.slice(at);
  const blank = tail.match(whitespace);
  if (blank) { emit(at + blank[0].length); continue; }
  if (hashComments && tail.startsWith('#') && (!wordComments || wordBoundary)) {
   emit(source.indexOf('\n', at) < 0 ? source.length : source.indexOf('\n', at)); continue;
  }
  if (slashComments && tail.startsWith('//')) { emit(source.indexOf('\n', at) < 0 ? source.length : source.indexOf('\n', at)); continue; }
  if ((slashComments || (powershell && wordBoundary)) && tail.startsWith(blockOpen)) {
   let depth = 1;
   let end = at + 2;
   while (end < source.length && depth) {
    if (rust && source.startsWith(blockOpen, end)) { depth++; end += 2; }
    else if (source.startsWith(blockClose, end)) { depth--; end += 2; }
    else end++;
   }
   if (depth) throw malformed('unterminated block comment');
   emit(end); continue;
  }
  const raw = rust && tail.match(/^(?:b|c)?r(#*)"/);
  if (raw) {
   const close = '"' + raw[1];
   const end = source.indexOf(close, at + raw[0].length);
   if (end < 0) throw malformed('unterminated raw string');
   emit(end + close.length, 'literal'); continue;
  }
  const here = powershell && tail.match(/^@(['"])\r?\n/);
  if (here) {
   const terminator = new RegExp('^' + here[1] + '@', 'gm');
   terminator.lastIndex = at + here[0].length;
   const end = terminator.exec(source);
   if (!end) throw malformed('unterminated PowerShell here-string');
   emit(end.index + end[0].length, 'literal'); continue;
  }
  const quote = tail[0];
  if (yaml && !'[]{} ,#&*!|>\'"%@`'.includes(quote) && !/^[-?:](?:[ \t\r\n]|$)/.test(tail)) {
   // Quotes and unseparated hashes inside plain scalars are data. Only YAML
   // separation or a flow delimiter returns control to the quoted scanner.
   let end = at;
   while (end < source.length) {
    const character = source[end];
    const next = source[end + 1];
    if (character === '\n' || character === '\r' || (yamlFlow && ',[]{}'.includes(character))
     || (character === '#' && end > at && /[ \t]/.test(source[end - 1]))
     || (character === ':' && (next === undefined || /[ \t\r\n]/.test(next) || (yamlFlow && ',[]{}'.includes(next))))) break;
    end++;
   }
   if (end > at) { emit(end, 'literal'); continue; }
  }
  if (yaml && (quote === '[' || quote === '{')) yamlFlow++;
  if (yaml && (quote === ']' || quote === '}')) yamlFlow--;
  if (python && (tail.startsWith('"""') || tail.startsWith("'''"))) {
   const delimiter = tail.slice(0, 3);
   let end = at + 3;
   while (end < source.length && !source.startsWith(delimiter, end)) end += source[end] === '\\' ? 2 : 1;
   if (end >= source.length) throw malformed('unterminated Python triple-quoted string');
   emit(end + 3, 'literal'); continue;
  }
  const character = rust && quote === "'" && tail.match(/^'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^'\\\r\n])'/u);
  if (character) { emit(at + character[0].length, 'literal'); continue; }
  if (quote === '"' || (!rust && quote === "'")) {
   let end = at + 1;
   while (end < source.length) {
    if ((powershell || (yaml && quote === "'")) && source[end] === quote && source[end + 1] === quote) { end += 2; continue; }
    if (source[end] === quote) break;
    end += powershell ? (quote === '"' && source[end] === '`' ? 2 : 1)
     : yaml && quote === "'" ? 1 : (source[end] === '\\' ? 2 : 1);
   }
   if (end >= source.length) throw malformed('unterminated quoted string');
   emit(end + 1, 'literal'); continue;
  }
  const word = tail.match(wordPattern);
  let spelling;
  if (word && (powershell || shell || escapedIdentifiers)) {
   spelling = escapedSpelling(word[0]);
   if (powershell) spelling = spelling.toLowerCase();
  }
  const width = word ? word[0].length : escape && tail[0] === escape
   ? powershell && tail.startsWith('`\r\n') ? 3 : Math.min(2, tail.length) : 1;
  emit(at + width, word ? 'word' : 'punctuation', spelling);
 }
 return out;
}

export function pollingUnit(file, source) {
 const parsed = /\.(?:[cm]?[jt]s|[jt]sx)$/.test(file) ? scriptTokens(file, source) : undefined;
 const stream = parsed?.stream ?? nativeTokens(source, file);
 // A direct Rust Future::poll advances one state machine. Other poll/select
 // spellings remain conservative candidates, including native imports/aliases.
 const rust = file.endsWith('.rs');
 const futurePoll = i => rust && stream[i]?.value === 'poll'
  && stream[i - 1]?.value === ':' && stream[i - 2]?.value === ':'
  && stream[i - 3]?.value === 'Future';
 // A changed producer/condition elsewhere in the source unit must reopen the
 // disposition; a short context hash could preserve approval of a changed loop.
 const unit = createHash('sha256').update(JSON.stringify(stream.map(t => [t.kind, t.value]))).digest('hex');
 const aliases = new Map();
 const forbidden = new Set(stream.filter((t, i) => t.kind === 'word' && !futurePoll(i) && (temporal.test(t.value) || policyName.test(t.value))).map(t => t.value));
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
   if (bindings.length && !futurePoll(i)) edge(t.value, bindings[bindings.length - 1].name);
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
   : t.kind === 'word' && !futurePoll(i) && forbidden.has(t.value) ? 'timer-or-readiness'
   : t.kind === 'literal' ? embeddedKind(t.value) ?? embeddedKind(escapedSpelling(t.value))
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

export function validateDispositions(dispositions, {inScope = supportedSource} = {}) {
 if (!Array.isArray(dispositions)) throw new Error('no-polling dispositions must be an array');
 const seen = new Set();
 const fields = new Set(['file', 'fingerprint', 'classification', 'producer', 'terminal', 'negativeControl']);
 for (const item of dispositions) {
  if (!item || typeof item !== 'object' || Object.keys(item).length !== fields.size || Object.keys(item).some(key => !fields.has(key))
   || typeof item.file !== 'string' || !inScope(item.file) || /[\r\n]/.test(item.file)
   || typeof item.fingerprint !== 'string' || !/^[a-f0-9]{64}$/.test(item.fingerprint) || !classes.has(item.classification)
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

export function newPollingSites(file, before, after, dispositions = [], policy = {}) {
 validateDispositions(dispositions, policy);
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
