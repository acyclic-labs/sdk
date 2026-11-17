export const cFamily = /\.(?:[ch](?:pp|xx)?|cc|hh|cs)$/;

export class SourceSyntaxError extends Error {
 constructor(message) {
  super(message);
  this.name = 'SourceSyntaxError';
 }
}

// Host identifiers and quoted child programs can spell the same operation with
// Unicode, shell or PowerShell escapes. Decode across wrapper quoting: a
// wrapper must not turn a readiness timer into an approved ordinary identifier.
export function escapedSpelling(source) {
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

// Nested comments, raw strings and lifetimes have Rust lexical boundaries.
// Quoted child programs and computed property names remain visible as literals.
export function nativeTokens(source, file) {
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
 return out;
}

