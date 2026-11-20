import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
const require = createRequire(import.meta.url);
let ts;

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
   else if (property === undefined && ts.isIdentifier(node.expression) && hostObjects.has(node.expression.text)) out.push({value: node.getText(parsed), kind: 'indirection', line, start});
  }
  if (ts.isPropertyAccessExpression(node) && (node.name.text === 'wait' || node.name.text === 'yield')) out.push({value: node.name.text, kind: 'indirection', line, start});
  if (ts.isCallExpression(node) || ts.isNewExpression(node)) {
   const callee = node.expression;
   const specifier = callee.kind === ts.SyntaxKind.ImportKeyword && node.arguments?.length === 1 ? node.arguments[0] : undefined;
   if (specifier && ts.isStringLiteralLike(specifier) && /^\.{1,2}\//.test(specifier.text))
    out.push({value: specifier.text, kind: 'module-load', line, start});
   else if (ts.isElementAccessExpression(callee) || callee.kind === ts.SyntaxKind.ImportKeyword
    || (ts.isIdentifier(callee) && (callee.text === 'eval' || callee.text === 'Function' || callee.text === 'require'))
    || (ts.isPropertyAccessExpression(callee) && ts.isIdentifier(callee.expression) && callee.expression.text === 'Reflect')) out.push({value: callee.getText(parsed), kind: 'indirection', line, start});
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
function rustImportLayout(file, source) {
 const tokens = nativeTokens(source, file).map(token => [token.kind, token.value]);
 const punctuation = (index, value) => tokens[index]?.[0] === 'punctuation' && tokens[index][1] === value;
 let start = 0;
 while (punctuation(start, '#') && punctuation(start + 1, '!') && punctuation(start + 2, '[')) {
  let depth = 1;
  start += 3;
  while (depth && start < tokens.length) {
   if (punctuation(start, '[')) depth++;
   if (punctuation(start, ']')) depth--;
   start++;
  }
 }
 const imports = [];
 let end = start;
 while (tokens[end]?.[0] === 'word' && tokens[end][1] === 'use') {
  let stop = end + 1;
  while (tokens[stop]?.[0] === 'word' || ['{', '}', ',', ':', '*'].some(value => punctuation(stop, value))) stop++;
  if (!punctuation(stop, ';')) break;
  const declaration = tokens.slice(end, stop).filter((_, offset) =>
   !punctuation(end + offset, ',') || !punctuation(end + offset + 1, '}'));
  imports.push(JSON.stringify(declaration));
  end = stop + 1;
 }
 return JSON.stringify([tokens.slice(0, start), imports.sort(), tokens.slice(end)]);
}

export function sameRustImportLayout(file, before, after) {
 if (!file.endsWith('.rs') || before === after) return false;
 try { return rustImportLayout(file, before) === rustImportLayout(file, after); }
 catch (error) { if (error instanceof SourceSyntaxError) return false; throw error; }
}

// Recognize finite iteration, casts, and typed bindings in Rust syntax. Macro
// and attribute bodies may use other grammars, so their candidates stay intact.
function rustSyntax(stream) {
 const finite = new Set(), typed = new Set(), casts = new Set(), typeFor = new Set();
 const syntax = {finite, typed, casts, typeFor};
 const ends = new Map(), stack = [];
 const pairs = new Map([['(', ')'], ['[', ']'], ['{', '}']]);
 const decimalEnd = start => {
  let end = start, offset = stream[start]?.start;
  while (stream[end]?.kind === 'punctuation' && stream[end].start === offset && /^[0-9]$/.test(stream[end].value)) {
   end++;
   offset++;
  }
  return end;
 };
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
  let at = i + 1;
  if (stream[at]?.value === 'ref') at++;
  if (stream[at]?.value === 'mut') at++;
  if (stream[at]?.value === '(') at = (ends.get(at) ?? at) + 1;
  else if (stream[at]?.kind === 'word' && /^[A-Za-z_]\w*$/.test(stream[at].value)) at++;
  else continue;
  if (stream[at]?.value !== 'in') continue;
  at++;
  const left = decimalEnd(at);
  if (left > at && stream[left]?.value === '.' && stream[left + 1]?.value === '.') {
   const start = left + (stream[left + 2]?.value === '=' ? 3 : 2);
   const right = decimalEnd(start);
   if (right > start && stream[right]?.value === '{') finite.add(i);
  }
  if (stream[at]?.value === '&') {
   at++;
   if (stream[at]?.value === 'mut') at++;
  }
  // Iterator adapters can change termination. Require the array itself.
  const arrayEnd = ends.get(at);
  if (stream[at]?.value === '[' && arrayEnd !== undefined && stream[arrayEnd + 1]?.value === '{') finite.add(i);
 }
 return syntax;
}

// Recognize finite JavaScript iteration. `for...of` and `for...in` walk a
// finite iterable/object; a classic `for` is finite when its comparison and
// step move the same way. `while`, `do...while` and `for (;;)` stay candidates.
function scriptSyntax(parsed, stream) {
 const finite = new Set();
 const starts = new Map();
 for (let i = 0; i < stream.length; i++) if (stream[i].value === 'for') starts.set(stream[i].start, i);
 const step = node => {
  if (!node) return 0;
  if (ts.isPostfixUnaryExpression(node) || ts.isPrefixUnaryExpression(node))
   return node.operator === ts.SyntaxKind.PlusPlusToken ? 1 : node.operator === ts.SyntaxKind.MinusMinusToken ? -1 : 0;
  if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.PlusEqualsToken) return 1;
  if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.MinusEqualsToken) return -1;
  return 0;
 };
 const direction = node => {
  if (!node || !ts.isBinaryExpression(node)) return 0;
  const kind = node.operatorToken.kind;
  return kind === ts.SyntaxKind.LessThanToken || kind === ts.SyntaxKind.LessThanEqualsToken ? 1
   : kind === ts.SyntaxKind.GreaterThanToken || kind === ts.SyntaxKind.GreaterThanEqualsToken ? -1 : 0;
 };
 const visit = node => {
  if (ts.isForOfStatement(node) || ts.isForInStatement(node)) {
   const at = starts.get(node.getStart(parsed));
   if (at !== undefined) finite.add(at);
  } else if (ts.isForStatement(node)) {
   const at = starts.get(node.getStart(parsed));
   if (at !== undefined && direction(node.condition) !== 0 && direction(node.condition) === step(node.incrementor)) finite.add(at);
  }
  ts.forEachChild(node, visit);
 };
 visit(parsed);
 return {finite};
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
 const futurePoll = i => rust && stream[i]?.value === 'poll' && (path(i, futureTrait)
  || (stream[i + 1]?.value === '!' && (path(i, futureCrates) || stream[i - 1]?.value !== ':')));
 // A changed producer/condition elsewhere in the source unit must reopen the
 // disposition; a short context hash could preserve approval of a changed loop.
 const unit = createHash('sha256').update(JSON.stringify(stream.map(t => [t.kind, t.value]))).digest('hex');
 const aliases = new Map();
 const name = lexicalNames(stream, rust);
 const forbidden = new Set(stream.flatMap((t, i) => t.kind === 'word' && !futurePoll(i) && (temporal.test(t.value) || policyName.test(t.value)) ? [name(i)] : []));
 const edge = (from, to) => {
  if (!from || !to) return;
  if (!aliases.has(from)) aliases.set(from, new Set());
  aliases.get(from).add(to);
 };
 const rustForms = rust ? rustSyntax(stream) : undefined;
 const scriptForms = parsed?.parsed ? scriptSyntax(parsed.parsed, stream) : undefined;
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
   if (!rustForms?.typed.has(i) && stream[i + 1]?.value === ':' && stream[i + 2]?.kind === 'word')
    edge(name(i), name(i + 2));
   if (stream[i + 1]?.value === '=' && stream[i + 2]?.value !== '=' && stream[i + 2]?.value !== '>') bindings.push({name: name(i), depth, braceDepth, line: t.line});
  }
  if (t.kind === 'punctuation' && opening.has(t.value)) depth++;
  if (t.kind === 'punctuation' && closing.has(t.value)) depth--;
  if (t.kind === 'punctuation' && t.value === '{') braceDepth++;
  if (t.kind === 'punctuation' && t.value === '}') braceDepth--;
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
  const kind = t.kind === 'indirection' || t.kind === 'module-load' ? 'indirect-timer-or-call'
   : t.kind === 'word' && loop.has(t.value) && !(rustForms ?? scriptForms)?.typeFor?.has(i) && !(rustForms ?? scriptForms)?.finite.has(i) ? 'loop'
   : t.kind === 'word' && !futurePoll(i) && forbidden.has(name(i)) ? 'timer-or-readiness'
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
