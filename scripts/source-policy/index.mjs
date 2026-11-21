import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
const require = createRequire(import.meta.url);
let ts;

// This is a conservative source rule, not a liveness proof. A loop is polling
// when its condition or body reaches a clock or a timer wait (a sleep, interval,
// timeout, readiness recheck, clock read), or when it spins on an empty body:
// iteration and blocking reads on an fd or channel are not polling (2026-11-21:
// 411 of 538 Cloud findings were `for queue in &self.queues`). The loop is the
// one finding, naming the timer that made it polling. Changing a helper's
// spelling does not exempt the loop or its underlying timer reference.
// Case-insensitive candidates cover PowerShell in both sources and child programs.
const temporal = /^(?:sleep(?:_(?:for|until)|Ex|ConditionVariable(?:CS|SRW))?|thrd_sleep|usleep|(?:clock_)?nanosleep|delay|interval(?:_at)?|timeout(?:_at)?|wait_(?:for|until|timeout(?:_while|_ms)?)|pthread_cond_(?:timedwait|clockwait)|cnd_timedwait|sem_timedwait|recv_(?:timeout|deadline)|park_timeout(?:_ms)?|poll|select|epoll_(?:wait|pwait2?)|ppoll|pselect|kevent(?:64)?|try_wait|yield_now|yieldNow|spin_loop|setTimeout|setInterval|setImmediate|nextTick|queueMicrotask|requestAnimationFrame|requestIdleCallback|waitAsync|waitForTimeout|waitForFunction|waitUntil|WaitForSingleObject(?:Ex)?|WaitForMultipleObjects(?:Ex)?|WaitOnAddress|WaitOne|WaitAny|WaitAll|WaitForExit(?:Async)?|WaitForInputIdle|SpinWait|SpinUntil|(?:sqlite3_)?busy_(?:timeout|handler)|PeriodicReader|with_batch_exporter|with_scheduled_delay|(?:http2_)?keep_?alive_interval|set_tcp_keepalive)$/i;
// In text, the words that are also English or SQL (`select a committed cell`, `SELECT a.*`) are a timer only as a call.
const embeddedTemporal = new RegExp('\\b(?:(?:select|poll|delay|interval)(?=\\s*\\()|(?:' + temporal.source.slice(1, -1) + ')(?<!\\b(?:select|poll|delay|interval)))\\b', temporal.flags);
const policyName = /(?:retry|retries|backoff|poll[_A-Z]|polling|[_A-Z]INTERVAL|wait[_A-Z].*Ready)/i;
// Inside text a policy name configures something only as a flag, key or call (`--retry 5`, `retries=3`,
// `retry(`); prose ('retry the identity request'), a path (`polling-policy.mjs`) or a longer word (the Kubernetes
// reason 'BackoffLimitExceeded') is data (2026-11-21).
const embeddedPolicy = /(?:^|[^\w-])--?(?:retry|retries|backoff|polling)\b|\b(?:retry|retries|backoff|polling)\s*[=:(]/i;
const embeddedName = /poll_|POLL_|poll[A-Z]|[_A-Z]INTERVAL|wait[_A-Z].*Ready/;
const embeddedCandidate = spelling => embeddedTemporal.test(spelling) || embeddedPolicy.test(spelling) || embeddedName.test(spelling);
// A timeout bounds one wait (a call's option, `--timeout=`, `timeout(d, f)`): a deadline on an event, not a timer
// wait. Inside a loop it is the loop's timer, as a clock is; outside every loop it is no finding (2026-11-21: a
// `kubectl wait --for=... --timeout=` watch and every `{timeout}` option made Cloud's local-cell helpers polling).
const deadlineWord = /^timeout(?:_at)?$/i;
const embeddedDeadlines = /\btimeout(?:_at)?\b/gi;
const embeddedTimer = spelling => embeddedCandidate(spelling.replace(embeddedDeadlines, ''));
const loop = new Set(['for', 'while', 'loop', 'do']);
// A clock read makes a loop around it a deadline or recheck loop; alone it is a measurement.
// A loop that subscribes (a watch, follow or subscription, list-then-watch resync included) waits on events: a clock
// or deadline in it bounds that wait. A sleep or other timer in it still makes it polling (2026-11-21: Cloud's
// waitKubernetes resync loop made every readiness caller a finding).
const subscription = /^(?:watch|follow|subscribe)(?:[A-Z_]\w*)?$/;
const clock = /^(?:now|elapsed|Instant|SystemTime|monotonic|perf_counter|hrtime|Stopwatch|GetTickCount(?:64)?|QueryPerformanceCounter|clock_gettime|deadline)$/;
const hostObjects = new Set(['globalThis', 'window', 'global', 'Bun', 'process']);

// A host object's unknown member is a function only where its value is used as one: called, or escaping as a value
// (an argument, a binding, a return, an element). A read of a member of it (`process[stream].write`), arithmetic or a
// comparison uses it as data (2026-11-21: Cloud's `totals[key] += process[key]` made every perf caller a finding).
function escapes(node) {
 let at = node;
 for (;;) {
  const parent = at.parent;
  if (!parent) return false;
  if (ts.isParenthesizedExpression(parent) || ts.isAsExpression?.(parent) || ts.isNonNullExpression?.(parent)
   || (ts.isConditionalExpression(parent) && parent.condition !== at)
   || (ts.isBinaryExpression(parent) && parent.right === at && [ts.SyntaxKind.QuestionQuestionToken, ts.SyntaxKind.BarBarToken,
    ts.SyntaxKind.AmpersandAmpersandToken, ts.SyntaxKind.CommaToken].includes(parent.operatorToken.kind))) { at = parent; continue; }
  if ((ts.isPropertyAccessExpression(parent) || ts.isElementAccessExpression(parent)) && parent.expression === at) return false;
  if (ts.isBinaryExpression(parent)) return parent.operatorToken.kind === ts.SyntaxKind.EqualsToken && parent.right === at;
  return !(ts.isPrefixUnaryExpression(parent) || ts.isPostfixUnaryExpression(parent) || ts.isTypeOfExpression(parent)
   || ts.isTemplateSpan(parent) || ts.isExpressionStatement(parent) || ts.isIfStatement(parent) || ts.isSwitchStatement(parent)
   || (ts.isConditionalExpression(parent) && parent.condition === at));
 }
}

// Reflect calls a function only through apply and construct, and only an unknown one is indirect: a named target
// (`Reflect.construct(Uint8Array, ...)`) is its own word. Reflect.get returns a function only from a host object by an
// unknown key (a temporal constant key is a timer); its other methods call nothing (2026-11-21: Cloud's
// lib/seal/bytes.mjs `Reflect.get(Uint8Array.prototype, 'byteLength', value)`).
function reflectIndirect(node) {
 const callee = node.expression;
 if (!ts.isPropertyAccessExpression(callee) || !ts.isIdentifier(callee.expression) || callee.expression.text !== 'Reflect') return false;
 const [target, key] = node.arguments ?? [];
 const named = x => !!x && (ts.isIdentifier(x) || (ts.isPropertyAccessExpression(x) && named(x.expression)));
 const method = callee.name.text;
 if (method === 'apply' || method === 'construct') return !named(target) || (ts.isIdentifier(target) && temporal.test(target.text));
 if (method !== 'get') return false;
 const constant = key && constantString(key);
 if (constant !== undefined && constant !== null && constant !== '') return temporal.test(constant) || policyName.test(constant);
 return !named(target) || (ts.isIdentifier(target) && hostObjects.has(target.text));
}
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

function scriptTokens(file, source, parser) {
 ts ??= parser ?? require('typescript-parser');
 const literalKinds = new Set([ts.SyntaxKind.StringLiteral, ts.SyntaxKind.NoSubstitutionTemplateLiteral, ts.SyntaxKind.TemplateHead, ts.SyntaxKind.TemplateMiddle, ts.SyntaxKind.TemplateTail]);
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
 if (parsed.isDeclarationFile) return {stream: out, parsed};
 while (stack.length) {
  const node = /** @type {import('typescript-parser').Node} */ (stack.pop());
  // The TypeScript type syntax range leaves class heritage expressions and
  // executable keywords such as `void` in the runtime token stream.
  if ((node.kind >= ts.SyntaxKind.FirstTypeNode && node.kind <= ts.SyntaxKind.LastTypeNode)
   || ts.isTypeAliasDeclaration(node) || ts.isInterfaceDeclaration(node)
   || (ts.isImportDeclaration(node) && node.importClause?.isTypeOnly)
   || ((ts.isExportDeclaration(node) || ts.isImportEqualsDeclaration(node)) && node.isTypeOnly)) {
   continue;
  }
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
   else if (property === undefined && ts.isIdentifier(node.expression) && hostObjects.has(node.expression.text) && escapes(node)) out.push({value: node.getText(parsed), kind: 'indirection', line, start});
  }
  if (ts.isPropertyAccessExpression(node) && (node.name.text === 'wait' || node.name.text === 'yield')) out.push({value: node.name.text, kind: 'indirection', line, start});
  if (ts.isCallExpression(node) || ts.isNewExpression(node)) {
   const callee = node.expression;
   const specifier = callee.kind === ts.SyntaxKind.ImportKeyword && node.arguments?.length === 1 ? node.arguments[0] : undefined;
   if (specifier && ts.isStringLiteralLike(specifier) && /^\.{1,2}\//.test(specifier.text))
    out.push({value: specifier.text, kind: 'module-load', line, start});
   // A computed callee with a static key (`object['close']()`, `source[Symbol.asyncDispose]()`) names its method like
   // a member call; a temporal key is already a word above. Only an unknown key can be any function
   // (2026-11-21: Cloud's `source[Symbol.asyncDispose]()` made every process command caller a finding).
   else if ((ts.isElementAccessExpression(callee) && constantString(callee.argumentExpression) === undefined
     && !(ts.isPropertyAccessExpression(callee.argumentExpression) && ts.isIdentifier(callee.argumentExpression.expression)
      && callee.argumentExpression.expression.text === 'Symbol'))
    || callee.kind === ts.SyntaxKind.ImportKeyword
    || (ts.isIdentifier(callee) && (callee.text === 'eval' || callee.text === 'Function' || callee.text === 'require'))
    || reflectIndirect(node)) out.push({value: callee.getText(parsed), kind: 'indirection', line, start});
  }
  const children = node.getChildren(parsed);
  if (children.length) {
   for (let i = children.length - 1; i >= 0; i--) stack.push(children[i]);
   continue;
  }
  if (node.kind === ts.SyntaxKind.EndOfFileToken) continue;
  const isLiteral = literalKinds.has(node.kind);
  const parent = node.parent;
  const isModuleSpecifier = isLiteral && ((ts.isImportDeclaration(parent) || ts.isExportDeclaration(parent))
   && parent.moduleSpecifier === node || ts.isCallExpression(parent)
   && parent.expression.kind === ts.SyntaxKind.ImportKeyword && parent.arguments[0] === node);
  const kind = isLiteral ? isModuleSpecifier ? 'module-specifier' : 'literal'
   : node.kind === ts.SyntaxKind.Identifier || (node.kind >= ts.SyntaxKind.FirstKeyword && node.kind <= ts.SyntaxKind.LastKeyword) ? 'word'
   : 'punctuation';
  // The parser preserves executable template substitutions and regex boundaries.
  const value = isLiteral ? /** @type {import('typescript-parser').LiteralLikeNode} */ (node).text
   : ts.isIdentifier(node) ? node.text : node.getText(parsed);
  out.push({value, kind, line, start});
 }
 return {stream: out, parsed};
}

function shellHereRedirect(source, start) {
 const match = /^<<(-?)[ \t]*((?:'[^'\r\n]*'|"[^"\r\n]*"|\\[^\r\n]|[^ \t\r\n;&|()<>'"\\])+)/.exec(source.slice(start));
 if (!match) return undefined;
 const delimiter = match[2].replace(/'([^']*)'|"([^"]*)"|\\(.)/g, (_, single, double, escaped) => single ?? double ?? escaped);
 return {delimiter, tabs: match[1] === '-', end: start + match[0].length};
}

function shellHereEnd(source, body, {delimiter, tabs}) {
 while (body < source.length) {
  const newline = source.indexOf('\n', body);
  const last = newline < 0 ? source.length : newline;
  const text = source.slice(body, last).replace(/\r$/, '');
  const end = newline < 0 ? last : last + 1;
  if ((tabs ? text.replace(/^\t+/, '') : text) === delimiter) return {body, end};
  body = end;
 }
 return undefined;
}

function shellQuoteEnd(source, end, quote, escaped, malformed) {
 const scopes = [{quote, escaped}];
 while (end < source.length) {
  const scope = scopes[scopes.length - 1];
  const character = source[end++];
  if (scope.quote !== undefined) {
   if (character === scope.quote) {
    scopes.pop();
    if (!scopes.length) return end;
   } else if (scope.escaped && character === '\\') end++;
   else if (scope.quote === '"' && character === '$' && source[end] === '(') {
    scopes.push({depth: 1, arithmetic: source[end + 1] === '(', hereDocuments: [], wordBoundary: true});
    end++;
   }
  } else if (character === '#' && scope.wordBoundary && !scope.arithmetic) {
   const newline = source.indexOf('\n', end);
   end = newline < 0 ? source.length : newline;
   continue;
  } else if (character === '<' && !scope.arithmetic) {
   if (source.startsWith('<<', end)) { end += 2; continue; }
   const redirect = shellHereRedirect(source, end - 1);
   if (redirect) {
    scope.hereDocuments.push(redirect);
    end = redirect.end;
   }
  } else if (character === '\n') {
   for (const redirect of scope.hereDocuments.splice(0)) {
    const result = shellHereEnd(source, end, redirect);
    if (!result) throw malformed('unterminated shell here-document');
    end = result.end;
   }
  } else if (character === "'" || character === '"')
   scopes.push({quote: character, escaped: character === '"' || source[end - 2] === '$'});
  else if (character === '\\') {
   if (source[end++] === '\n') continue;
  }
  else if (character === '(') scope.depth++;
  else if (character === ')' && --scope.depth === 0) scopes.pop();
  scope.wordBoundary = /^[ \t\r\n;|&()<>]$/.test(character);
 }
 return source.length + 1;
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
 const hereDocuments = [];
 let arithmeticDepth = 0;
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
  if (shell && !arithmeticDepth && tail.startsWith('((')) {
   arithmeticDepth = 2;
   emit(at + 1, 'punctuation');
   emit(at + 1, 'punctuation');
   continue;
  }
  if (arithmeticDepth && tail[0] === '(') arithmeticDepth++;
  if (arithmeticDepth && tail[0] === ')') arithmeticDepth--;
  if (shell && !arithmeticDepth && tail.startsWith('<<<')) { emit(at + 3, 'punctuation'); continue; }
  const redirect = shell && !arithmeticDepth && tail.startsWith('<<') && shellHereRedirect(source, at);
  if (redirect) {
   hereDocuments.push(redirect);
   emit(redirect.end, 'punctuation');
   continue;
  }
  if (hereDocuments.length && (tail.startsWith('\n') || tail.startsWith('\r\n'))) {
   emit(at + (tail.startsWith('\r') ? 2 : 1));
   for (const pending of hereDocuments.splice(0)) {
    const result = shellHereEnd(source, at, pending);
    if (!result) throw malformed('unterminated shell here-document');
    emit(result.body, 'literal');
    emit(result.end);
   }
   continue;
  }
  const blank = tail.match(whitespace);
  if (blank) {
   const width = hereDocuments.length ? blank[0].search(/\r?\n/) : -1;
   emit(at + (width >= 0 ? width : blank[0].length));
   continue;
  }
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
  const verbatim = file.endsWith('.cs') && tail.match(/^@\$?"/);
  const quote = verbatim ? '"' : tail[0];
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
  if (shell && (quote === '"' || quote === "'")) {
   const end = shellQuoteEnd(source, at + 1, quote, quote === '"' || source[at - 1] === '$', malformed);
   if (end > source.length) throw malformed('unterminated quoted string');
   emit(end, 'literal');
   continue;
  }
  if (quote === '"' || (!rust && quote === "'")) {
   let end = at + (verbatim ? verbatim[0].length : 1);
   while (end < source.length) {
    if ((powershell || verbatim || (yaml && quote === "'")) && source[end] === quote && source[end + 1] === quote) { end += 2; continue; }
    if (source[end] === quote) break;
    end += powershell ? (quote === '"' && source[end] === '`' ? 2 : 1)
     : verbatim || yaml && quote === "'" ? 1 : (source[end] === '\\' ? 2 : 1);
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
 if (hereDocuments.length) throw malformed('unterminated shell here-document');
 return out;
}

// Rustfmt can reorder an unadorned file-level import block and remove the
// optional comma before a use-tree's closing brace. Keep everything else,
// including attributes, visibility, aliases, and executable tokens, exact.
// Recognize casts, typed bindings and `for` in types in Rust syntax. Macro
// and attribute bodies may use other grammars, so their candidates stay intact.
function rustSyntax(stream) {
 const typed = new Set(), casts = new Set(), typeFor = new Set();
 const syntax = {typed, casts, typeFor};
 const ends = new Map(), stack = [];
 const pairs = new Map([['(', ')'], ['[', ']'], ['{', '}']]);
 for (let i = 0; i < stream.length; i++) {
  if (stream[i].kind !== 'punctuation') continue;
  const value = stream[i].value;
  if (pairs.has(value)) stack.push(i);
  if (closing.has(value)) {
   const start = stack.pop();
   if (start === undefined || pairs.get(stream[start].value) !== value) return syntax;
   ends.set(start, i);
  }
 }
 if (stack.length) return syntax;
 const afterGenerics = start => {
  if (stream[start]?.value !== '<') return start;
  let depth = 1;
  for (let at = start + 1; at < stream.length; at++) {
   const token = stream[at];
   if (token.kind !== 'punctuation') continue;
   const end = ends.get(at);
   if (end !== undefined) { at = end; continue; }
   if (token.value === '<') depth++;
   if (token.value === '>' && stream[at - 1]?.value !== '-' && --depth === 0) return at + 1;
  }
 };
 const pathSeparator = at => stream[at]?.value === ':' && stream[at + 1]?.value === ':';
 const traitFor = start => {
  let at = afterGenerics(start + 1);
  if (at === undefined) return;
  if (stream[at]?.value === '!') at++;
  if (pathSeparator(at)) at += 2;
  while (stream[at]?.kind === 'word') {
   const next = afterGenerics(at + 1);
   if (next === undefined) return;
   if (stream[next]?.value === 'for') return next;
   if (!pathSeparator(next)) return;
   at = next + 2;
  }
 };
 let importing = false;
 for (let i = 0; i < stream.length; i++) {
  const end = ends.get(i), previous = stream[i - 1]?.value;
  if (end !== undefined && (previous === '!' || previous === '#'
   || (stream[i - 2]?.value === '!' && stream[i - 3]?.value === 'macro_rules'))) {
   i = end;
   continue;
  }
  if (stream[i].kind === 'punctuation' && stream[i].value === ';') importing = false;
  if (stream[i].kind !== 'word') continue;
  const value = stream[i].value;
  if (value === 'impl') {
   const target = traitFor(i);
   if (target !== undefined) typeFor.add(target);
  }
  if (value === 'use' && stream[i + 1]?.value !== '<'
   || value === 'extern' && stream[i + 1]?.value === 'crate') importing = true;
  if (!importing && stream[i + 1]?.value === 'as') casts.add(i);
  if (stream[i + 1]?.value === ':' && stream[i + 2]?.kind === 'word') {
   let declaration = i - 1;
   if (stream[declaration]?.value === 'mut') declaration--;
   if (stream[declaration]?.value === 'ref') declaration--;
   if (['let', 'const', 'static'].includes(stream[declaration]?.value)) typed.add(i);
  }
  if (value !== 'for') continue;
  if (stream[i + 1]?.value === '<' && stream[i + 2]?.value === "'") typeFor.add(i);
 }
 return syntax;
}

// Each JavaScript loop statement's keyword start, its end offset and whether its
// body is empty (a spin). A do-while's `while` starts no loop of its own.
function scriptLoops(parsed) {
 const loops = new Map();
 const visit = node => {
  if (ts.isIterationStatement(node, false)) {
   const body = node.statement;
   loops.set(node.getStart(parsed), {end: node.end, empty: ts.isEmptyStatement(body) || (ts.isBlock(body) && !body.statements.length),
    iteration: ts.isForOfStatement(node) || ts.isForInStatement(node)});
  }
  ts.forEachChild(node, visit);
 };
 visit(parsed);
 return loops;
}

// Each loop keyword's extent in the token stream: index -> {end, empty}, the last
// token of its header and body and whether the body is empty. A keyword that
// starts no loop (a shell `do`, a do-while's `while`) has none. An iteration (`for x of`, `for x in`) visits each
// element once: it re-observes no state, so a clock or deadline in it bounds one element's work.
function loopExtents(file, source, stream, scriptForms, typeFor) {
 const out = new Map();
 const rust = file.endsWith('.rs');
 if (scriptForms) {
  const at = new Map(stream.map((t, i) => [t.start, i]));
  for (const [start, {end, empty, iteration}] of scriptForms) {
   const i = at.get(start);
   if (i === undefined) continue;
   let last = i;
   while (last + 1 < stream.length && stream[last + 1].start < end) last++;
   out.set(i, {end: last, empty, iteration});
  }
  return out;
 }
 const close = new Map(), stack = [], enclosing = new Array(stream.length);
 const pairs = {'(': ')', '[': ']', '{': '}'};
 for (let i = 0; i < stream.length; i++) {
  const t = stream[i];
  enclosing[i] = stack[stack.length - 1];
  if (t.kind !== 'punctuation') continue;
  if (pairs[t.value]) stack.push(i);
  else if (closing.has(t.value) && stack.length && pairs[stream[stack[stack.length - 1]].value] === t.value) close.set(stack.pop(), i);
 }
 const word = (i, ...values) => stream[i]?.kind === 'word' && values.includes(stream[i].value);
 const lineEnd = i => { let j = i; while (j + 1 < stream.length && stream[j + 1].line === stream[i].line) j++; return j; };
 if (/\.(?:sh|bash)$/.test(file)) {
  for (let i = 0; i < stream.length; i++) {
   if (!word(i, 'while', 'until', 'for', 'select')) continue;
   let j = i + 1;
   while (j < stream.length && !word(j, 'do')) j++;
   let depth = 0, end = lineEnd(i);
   for (; j < stream.length; j++) {
    if (word(j, 'do')) depth++;
    else if (word(j, 'done') && --depth === 0) { end = j; break; }
   }
   out.set(i, {end, empty: false, iteration: stream[i].value === 'for'});
  }
  return out;
 }
 if (file.endsWith('.py')) {
  const column = i => stream[i].start - source.lastIndexOf('\n', stream[i].start - 1) - 1;
  for (let i = 0; i < stream.length; i++) {
   if (!word(i, 'for', 'while')) continue;
   if (enclosing[i] !== undefined) { out.set(i, {end: close.get(enclosing[i]) ?? lineEnd(i), empty: false, iteration: true}); continue; } // a comprehension
   let end = lineEnd(i);
   while (end + 1 < stream.length && column(end + 1) > column(i)) end = lineEnd(end + 1);
   out.set(i, {end, empty: false, iteration: stream[i].value === 'for'});
  }
  return out;
 }
 const doWhile = new Set();
 for (let i = 0; i < stream.length; i++) {
  if (!word(i, ...loop) || typeFor?.has(i) || doWhile.has(i)) continue;
  let end = lineEnd(i), empty = false;
  for (let j = i + 1; j < stream.length; j++) {
   const t = stream[j];
   if (t.kind !== 'punctuation') continue;
   if (t.value === '(' || t.value === '[') { j = close.get(j) ?? j; continue; }
   if (t.value === '{' && close.has(j)) {
    end = close.get(j);
    empty = end === j + 1 && stream[i].value !== 'for';
    if (stream[i].value === 'do' && word(end + 1, 'while')) {
     doWhile.add(end + 1);
     if (stream[end + 2]?.value === '(' && close.has(end + 2)) end = close.get(end + 2);
    }
    break;
   }
   if (t.value === ';' || t.value === '}') { end = j; break; }
  }
  out.set(i, {end, empty, iteration: rust && stream[i].value === 'for'});
 }
 return out;
}

const futureTrait = new Set(['Future']);
const futureCrates = new Set(['futures', 'futures_util', 'futures_lite']);
const declarations = new Set(['let', 'const', 'var']);

// Each word's alias identity. A name declared by `let`/`const`/`var` is its own
// binding from its declaration to the end of the enclosing block (a later
// declaration of the same name shadows it); any other word is the file-wide
// name. Two bindings that share a spelling in different scopes, such as `x` in
// two test functions, are then two names, so a timer alias of one cannot taint
// the other.
function lexicalNames(stream, rust) {
 const ids = new Array(stream.length);
 const scopes = [new Map()];
 let next = 0;
 for (let i = 0; i < stream.length; i++) {
  const t = stream[i];
  if (t.kind === 'punctuation' && t.value === '{') { scopes.push(new Map()); continue; }
  if (t.kind === 'punctuation' && t.value === '}') { if (scopes.length > 1) scopes.pop(); continue; }
  if (t.kind !== 'word') continue;
  const before = stream[i - 1];
  const member = before?.value === '.' || (before?.value === ':' && stream[i - 2]?.value === ':');
  const declared = before?.kind === 'word' && (before.value === 'mut' ? stream[i - 2]?.value === 'let' : declarations.has(before.value));
  if (!member && declared) {
   const id = `${t.value}#${next++}`;
   scopes[scopes.length - 1].set(t.value, id);
   ids[i] = id;
   continue;
  }
  if (!member) {
   for (let s = scopes.length - 1; s >= 0; s--) {
    const id = scopes[s].get(t.value);
    if (id) { ids[i] = id; break; }
   }
  }
  ids[i] ??= t.value;
 }
 return i => ids[i] ?? stream[i]?.value;
}

export function pollingUnit(file, source, parser = undefined) {
 const parsed = /\.(?:[cm]?[jt]s|[jt]sx)$/.test(file) ? scriptTokens(file, source, parser) : undefined;
 const stream = parsed?.stream ?? nativeTokens(source, file);
 // A direct Rust Future::poll, or the futures crate's poll! macro (one
 // Future::poll of its operand), advances one state machine. Other poll/select
 // spellings remain conservative candidates, including native imports/aliases.
 const rust = file.endsWith('.rs');
 const path = (i, crate) => stream[i - 1]?.value === ':' && stream[i - 2]?.value === ':' && crate.has(stream[i - 3]?.value);
 const directPoll = i => rust && stream[i]?.value === 'poll' && (path(i, futureTrait)
  || (stream[i + 1]?.value === '!' && (path(i, futureCrates) || stream[i - 1]?.value !== ':')));
 // A method `.poll(cx)` whose one argument is a task Context (a `Context` path, or a name declared of type Context)
 // is Future::poll too; an event loop's `poll.poll(&mut events, timeout)` takes two. A local `fn` whose body polls
 // nothing but a future that way is a step of that future, and so is each call of it: a loop that steps a future
 // and waits only on an awaited event (`wakes.notified().await`) is no timer loop (2026-11-21: Cloud's gateway
 // metering test `fn poll(future, wakes)` made its wake-driven step loop a finding).
 const closeOf = new Map(), open = [];
 for (let i = 0; i < stream.length; i++) {
  if (stream[i].kind !== 'punctuation') continue;
  if (opening.has(stream[i].value)) open.push(i);
  else if (closing.has(stream[i].value) && open.length) closeOf.set(open.pop(), i);
 }
 const contexts = new Set(), steps = new Set();
 if (rust) {
  for (let i = 0; i + 1 < stream.length; i++) {
   if (stream[i].kind !== 'word' || stream[i + 1].value !== ':' || stream[i + 2]?.value === ':') continue;
   for (let j = i + 2; j < Math.min(stream.length, i + 10) && stream[j].value !== ',' && stream[j].value !== ')'; j++)
    if (stream[j].value === 'Context') { contexts.add(stream[i].value); break; }
  }
  for (let i = 1; i + 1 < stream.length; i++) {
   if (stream[i].value !== 'poll' || stream[i - 1].value !== '.' || stream[i + 1].value !== '(') continue;
   const end = closeOf.get(i + 1);
   if (end === undefined) continue;
   let depth = 0, commas = 0, context = false;
   for (let j = i + 2; j < end; j++) {
    const t = stream[j];
    if (t.kind === 'punctuation' && opening.has(t.value)) depth++;
    else if (t.kind === 'punctuation' && closing.has(t.value)) depth--;
    else if (depth === 0 && t.value === ',') commas++;
    if (t.kind === 'word' && (t.value === 'Context' || contexts.has(t.value))) context = true;
   }
   if (context && commas === 0) steps.add(i);
  }
 }
 const stepping = new Set();
 if (rust) {
  for (let i = 0; i + 2 < stream.length; i++) {
   if (stream[i].kind !== 'word' || stream[i].value !== 'fn' || stream[i + 1].kind !== 'word') continue;
   let b = i + 2;
   while (b < stream.length && stream[b].value !== '{' && stream[b].value !== ';') b++;
   const end = closeOf.get(b);
   if (stream[b]?.value !== '{' || end === undefined) continue;
   let polls = 0, other = false;
   for (let j = b + 1; j < end; j++) {
    const t = stream[j];
    if (t.kind !== 'word' && t.kind !== 'indirection' && t.kind !== 'module-load') continue;
    if (directPoll(j) || steps.has(j)) polls++;
    else if (t.kind !== 'word' || temporal.test(t.value) || policyName.test(t.value) || clock.test(t.value)) other = true;
   }
   if (polls && !other) stepping.add(stream[i + 1].value);
  }
 }
 // A type is no timer: `Poll` named from `task` (`std::task::Poll`, `task::{Context, Poll}`) or spelled as its
 // variants is a future's result, unless the file uses mio, whose `Poll` is an event-loop poller.
 const taskPoll = rust && !stream.some(t => t.value === 'mio') && stream.some((t, i) => t.value === 'Poll'
  && (stream[i + 1]?.value === ':' && ['Ready', 'Pending'].includes(stream[i + 3]?.value)
   || stream.slice(Math.max(0, i - 12), i).some((u, k, a) => u.value === 'task' && a[k + 1]?.value === ':')));
 // A predicate wait (`rx.wait_for(|value| ...)`, a watch receiver's) wakes on each published value: an event, no
 // clock. A wait with a duration stays a timer.
 const predicateWait = i => rust && /^wait_(?:for|until)$/.test(stream[i]?.value ?? '') && stream[i - 1]?.value === '.'
  && stream[i + 1]?.value === '(' && (stream[i + 2]?.value === '|' || stream[i + 2]?.value === '||' || stream[i + 2]?.value === 'move');
 const futurePoll = i => directPoll(i) || steps.has(i) || predicateWait(i) || (taskPoll && stream[i]?.value === 'Poll')
  || (rust && stepping.has(stream[i]?.value) && stream[i - 1]?.value !== '.'
  && !(stream[i - 1]?.value === ':' && stream[i - 2]?.value === ':'));
 // A changed producer/condition elsewhere in the source unit must reopen the
 // disposition; a short context hash could preserve approval of a changed loop.
 const unit = createHash('sha256').update(JSON.stringify(stream.map(t => [t.kind, t.value]))).digest('hex');
 // A timer's origin is its spelling in its enclosing named function (`fn`, `function`; the file for a top-level one),
 // never its position: an edit elsewhere in the file leaves every origin, and so every chain, as it was.
 const within = new Array(stream.length);
 {
  const scopes = [''];
  let pending, nesting = 0, at = 0;
  for (let i = 0; i < stream.length; i++) {
   const t = stream[i];
   within[i] = scopes[scopes.length - 1];
   if (t.kind === 'word' && (t.value === 'fn' || t.value === 'function') && stream[i + 1]?.kind === 'word') { pending = stream[i + 1].value; at = nesting; }
   else if (t.kind !== 'punctuation') continue;
   else if (t.value === '(' || t.value === '[') nesting++;
   else if (t.value === ')' || t.value === ']') nesting--;
   else if (t.value === '{') { scopes.push(pending ?? scopes[scopes.length - 1]); pending = undefined; }
   else if (t.value === '}') { if (scopes.length > 1) scopes.pop(); }
   else if (t.value === ';' && nesting === at) pending = undefined;
  }
 }
 const origin = i => file + ':' + within[i] + ':' + (stream[i].kind === 'word' ? '' : stream[i].kind + ':') + stream[i].value;
 const aliases = new Map();
 const name = lexicalNames(stream, rust);
 // Names resolve before they match. A local binding (`const select = selector => ...`, `const elapsed = field(...)`)
 // is its own code: it is a timer only when its initializer reaches one, not by its spelling. A policy name that is
 // a data field, a member or key that is not called (a Job's `backoffLimit`), configures no retry here
 // (2026-11-21: each was a Cloud finding).
 const spans = new Map();
 for (let i = 0; i < stream.length; i++) {
  if (stream[i].kind !== 'word' || stream[i + 1]?.value !== '=' || stream[i + 2]?.value === '=' || name(i) === stream[i].value) continue;
  let depth = 0, j = i + 2;
  for (; j < stream.length; j++) {
   const t = stream[j];
   if (t.kind === 'punctuation' && opening.has(t.value)) depth++;
   else if (t.kind === 'punctuation' && closing.has(t.value) && --depth < 0) break;
   else if (depth === 0 && t.kind === 'punctuation' && (t.value === ';' || t.value === ',')) break;
   else if (depth === 0 && t.kind === 'word' && t.line > stream[i].line && statementStarts.has(t.value) && stream[j - 1]?.value !== '=>') break;
  }
  spans.set(name(i), [i + 2, j]);
 }
 // Inside its own initializer a spelling is the outer name (`let interval = interval(d)`). A binding with no
 // initializer of its own (`let delay: later` in a macro's tokens, a declared-then-assigned name) has no code to
 // judge, so it keeps its spelling's class: an alias of a timer is a timer.
 const local = i => { const span = spans.get(name(i)); return name(i) !== stream[i].value && span !== undefined && !(i >= span[0] && i < span[1]); };
 const called = i => stream[i + 1]?.value === '(' || (rust && stream[i + 1]?.value === '!');
 const dataField = i => policyName.test(stream[i].value) && !temporal.test(stream[i].value) && !called(i)
  && (stream[i - 1]?.value === '.' || stream[i - 1]?.value === '?.' || (stream[i + 1]?.value === ':' && stream[i + 2]?.value !== ':'
   && (stream[i - 1]?.value === '{' || stream[i - 1]?.value === ',')));
 // A policy name is a retry only where it repeats work: a local binding whose initializer has no loop, no timer or
 // clock, no other policy name and no call of itself runs its action once, whatever it is named; a predicate
 // (`retryable`) decides, it repeats nothing (2026-11-21: Cloud's upgrade-cell fixture `const retry = async (action,
 // {retryable}) => { try { return await action(); } ... }`).
 const predicate = /^(?:is_?)?retr[iy]able$/i;
 const inert = new Map();
 const inertPolicy = i => {
  const value = stream[i].value;
  if (temporal.test(value)) return false;
  if (predicate.test(value)) return true;
  const span = spans.get(name(i));
  if (!span || name(i) === value) return false;
  if (inert.has(name(i))) return inert.get(name(i));
  let quiet = true;
  for (let j = span[0]; j < span[1] && quiet; j++) {
   const t = stream[j];
   if (t.kind === 'indirection' || t.kind === 'module-load') quiet = false;
   else if (t.kind === 'literal') quiet = !embeddedTimer(t.value);
   else if (t.kind === 'word') quiet = !loop.has(t.value) && name(j) !== name(i) && !(temporal.test(t.value) && !local(j))
    && !clock.test(t.value) && (!policyName.test(t.value) || predicate.test(t.value));
  }
  inert.set(name(i), quiet);
  return quiet;
 };
 const candidate = (t, i) => t.kind === 'word' && !futurePoll(i) && !dataField(i)
  && ((temporal.test(t.value) && !local(i)) || (policyName.test(t.value) && !inertPolicy(i)));
 // A timer spelling is its own origin; a name bound to or aliasing one reaches each origin it can hold.
 const timers = stream.map((t, i) => candidate(t, i) && !deadlineWord.test(t.value));
 const forbidden = new Set(), reaches = new Map();
 const seed = (binding, i) => {
  forbidden.add(binding);
  if (!reaches.has(binding)) reaches.set(binding, new Set());
  reaches.get(binding).add(origin(i));
 };
 for (let i = 0; i < stream.length; i++) if (timers[i]) seed(name(i), i);
 for (const [binding, [from, to]] of spans) for (let j = from; j < to; j++) if (timers[j]) seed(binding, j);
 const bounds = new Set(stream.flatMap((t, i) => candidate(t, i) && deadlineWord.test(t.value) ? [name(i)] : []));
 const edge = (from, to) => {
  if (!from || !to) return;
  if (!aliases.has(from)) aliases.set(from, new Set());
  aliases.get(from).add(to);
 };
 const rustForms = rust ? rustSyntax(stream) : undefined;
 const scriptForms = parsed?.parsed ? scriptLoops(parsed.parsed) : undefined;
 const bindings = [];
 let depth = 0;
 let braceDepth = 0;
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
   if (bindings.length && braceDepth === bindings[bindings.length - 1].braceDepth && !futurePoll(i)) edge(name(i), bindings[bindings.length - 1].name);
   if (!rustForms?.casts.has(i) && stream[i + 1]?.value === 'as' && stream[i + 2]) edge(name(i), name(i + 2));
   // A declared type is independent of its binding's value. Destructuring
   // fields and other ambiguous colons still propagate alias candidates.
   // A field initialized by an expression (`retry: Mutex::new(..)`, `wait: make()`) binds no alias.
   if (!rustForms?.typed.has(i) && stream[i + 1]?.value === ':' && stream[i + 2]?.kind === 'word'
    && !['(', ':', '.', '<', '!'].includes(stream[i + 3]?.value))
    edge(name(i), name(i + 2));
   // A binding of a timer's type is that timer: a loop waiting on a passed `tick: Interval` reaches it.
   if (stream[i + 1]?.value === ':' && stream[i + 2]?.kind === 'word' && temporal.test(stream[i + 2].value))
    edge(name(i + 2), name(i));
   if (stream[i + 1]?.value === '=' && stream[i + 2]?.value !== '=' && stream[i + 2]?.value !== '>') bindings.push({name: name(i), depth, braceDepth, line: t.line});
  }
  if (t.kind === 'punctuation' && opening.has(t.value)) depth++;
  if (t.kind === 'punctuation' && closing.has(t.value)) depth--;
  if (t.kind === 'punctuation' && t.value === '{') braceDepth++;
  if (t.kind === 'punctuation' && t.value === '}') braceDepth--;
 }
 const queue = [...forbidden];
 for (let k = 0; k < queue.length; k++) {
  const from = reaches.get(queue[k]);
  for (const alias of aliases.get(queue[k]) ?? []) {
   forbidden.add(alias);
   if (!reaches.has(alias)) reaches.set(alias, new Set());
   const to = reaches.get(alias), size = to.size;
   for (const item of from) to.add(item);
   if (to.size > size) queue.push(alias);
  }
 }
 const kindAt = i => {
  const t = stream[i];
  return t.kind === 'indirection' || t.kind === 'module-load' ? 'indirect-timer-or-call'
   : t.kind === 'word' && !futurePoll(i) && forbidden.has(name(i)) ? 'timer-or-readiness'
   : t.kind === 'word' && !futurePoll(i) && bounds.has(name(i)) ? 'deadline'
   : t.kind === 'literal' && (embeddedTimer(t.value) || embeddedTimer(escapedSpelling(t.value))) ? 'embedded-timer-or-retry'
   : t.kind === 'literal' && (embeddedCandidate(t.value) || embeddedCandidate(escapedSpelling(t.value))) ? 'deadline'
   : undefined;
 };
 // Each timer or clock belongs to its innermost loop, which it makes polling: that loop is the one finding, and names
 // it. A timer outside every loop is its own finding; a clock outside every loop is a measurement.
 const extents = loopExtents(file, source, stream, scriptForms, rustForms?.typeFor);
 const reasons = new Map(), active = [], found = [];
 const subscribes = new Set();
 for (const [i, {end}] of extents) {
  for (let j = i + 1; j <= end; j++) if (stream[j].kind === 'word' && subscription.test(stream[j].value)) { subscribes.add(i); break; }
 }
 const directTimer = i => {
  const token = stream[i];
  return token.kind === 'literal' || temporal.test(token.value) || clock.test(token.value);
 };
 for (let i = 0; i < stream.length; i++) {
  while (active.length && extents.get(active[active.length - 1]).end < i) active.pop();
  const kind = kindAt(i), owner = active[active.length - 1];
  const bound = kind === 'deadline' || (!kind && stream[i].kind === 'word' && clock.test(stream[i].value) && !local(i));
  const timer = kind === 'timer-or-readiness' || kind === 'embedded-timer-or-retry'
   || (bound && owner !== undefined && !subscribes.has(owner) && !extents.get(owner).iteration);
  if (timer && owner !== undefined) {
   const previous = reasons.get(owner);
   if (previous === undefined || previous < 0 || (!directTimer(previous) && directTimer(i))) reasons.set(owner, i);
  }
  else if (kind && kind !== 'deadline') found.push([i, kind]);
  if (extents.has(i)) { active.push(i); if (extents.get(i).empty) reasons.set(i, -1); }
 }
 for (const i of reasons.keys()) found.push([i, 'loop']);
 // A site's origins are the timers it reaches; its own are those spelled at it (a loop's, each in its body).
 const reached = j => {
  const kind = kindAt(j);
  return kind === 'timer-or-readiness' ? reaches.get(name(j)) ?? [] : kind === 'embedded-timer-or-retry' || kind === 'indirect-timer-or-call' ? [origin(j)] : [];
 };
 const sites = [];
 for (const [i, kind] of found.sort((x, y) => x[0] - y[0])) {
  const t = stream[i];
  const fingerprint = createHash('sha256').update(JSON.stringify([kind, i, unit])).digest('hex');
  const site = {file, line: t.line, kind, fingerprint, start: t.start, token: t.value};
  const origins = new Set(), own = new Set();
  if (kind === 'loop') {
   const by = reasons.get(i);
   site.timer = by < 0 ? 'an empty body (spin)' : stream[by].value;
   site.timerLine = by < 0 ? t.line : stream[by].line;
   if (by < 0) { origins.add(origin(i)); own.add(origin(i)); }
   else if (!kindAt(by) || kindAt(by) === 'deadline') { origins.add(origin(by)); own.add(origin(by)); }
   for (let j = i + 1; j <= extents.get(i).end; j++) {
    for (const item of reached(j)) origins.add(item);
    if (timers[j] || (stream[j].kind !== 'word' && kindAt(j))) own.add(origin(j));
   }
  } else {
   for (const item of reached(i)) origins.add(item);
   if (timers[i] || t.kind !== 'word') own.add(origin(i));
  }
  site.origins = [...origins].sort();
  site.own = [...own].sort();
  if (t.kind === 'word' && (stream[i - 1]?.value === 'fn' || stream[i - 1]?.value === 'function')) site.function = t.value;
  site.scope = within[i];
  sites.push(site);
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

/** The sites of `current` that are new against `previous`. Sites of one file, kind and spelling form a group. A site is
 * new where it reaches an origin (a timer or readiness wait, by file, enclosing function and spelling) that no site of
 * its group reached before, so a new timer and every chain newly reaching a timer are new; and its group's sites that
 * outnumber the predecessor's are new, so a second identical timer is. Nothing else is: not a position, not the text of
 * a module on the chain, not a chain that lost an origin (2026-11-21: an edit to one shared Cloud test module
 * re-reported 375 unchanged helper references in its import cone). Of a grown group, the sites reported are those whose
 * source line (`text`) the predecessor's sites lack, so the finding names the added line.
 * @param {{file: string, kind: string, token: string, origins: string[]}[]} previous @param {typeof previous} current
 * @param {(site: previous[number]) => string} before @param {(site: previous[number]) => string} after */
export function newSites(previous, current, before = () => '', after = () => '') {
 const group = site => JSON.stringify([site.file, site.kind, site.token]);
 const reached = new Map(), remaining = new Map(), lines = new Map();
 const count = (map, key, step) => map.set(key, (map.get(key) ?? 0) + step).get(key);
 for (const site of previous) {
  const key = group(site);
  count(remaining, key, 1);
  count(lines, JSON.stringify([key, before(site)]), 1);
  if (!reached.has(key)) reached.set(key, new Set());
  for (const item of site.origins) reached.get(key).add(item);
 }
 const fresh = new Set(), unmatched = [];
 for (const site of current) {
  const key = group(site);
  if (!site.origins.every(item => reached.get(key)?.has(item))) fresh.add(site);
  else if (count(lines, JSON.stringify([key, after(site)]), -1) >= 0) count(remaining, key, -1);
  else unmatched.push(site);
 }
 for (const site of unmatched) if (count(remaining, group(site), -1) < 0) fresh.add(site);
 return current.filter(site => fresh.has(site));
}

export function newPollingSites(file, before, after, dispositions = [], policy = {}) {
 validateDispositions(dispositions, policy);
 const disposed = new Set(dispositions.filter(item => item.file === file).map(item => item.fingerprint));
 const reviewed = source => pollingSites(file, source).filter(site => !disposed.has(site.fingerprint));
 const text = source => { const lines = source.split('\n'); return site => (lines[site.line - 1] ?? '').trim(); };
 return newSites(reviewed(before), reviewed(after), text(before), text(after));
}
