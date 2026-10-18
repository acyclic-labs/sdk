// Token boundaries only: no callable provenance, taint, or assertion inference.
export function rustTokens(source) {
  const tokens = [];
  let offset = 0, line = 1;
  const take = end => { const text = source.slice(offset, end); line += (text.match(/\n/g) ?? []).length; offset = end; };
  while (offset < source.length) {
    const rest = source.slice(offset);
    if (/^\s/.test(rest)) { take(offset + /^\s+/.exec(rest)[0].length); continue; }
    if (rest.startsWith('//')) { const end = source.indexOf('\n', offset); take(end < 0 ? source.length : end); continue; }
    if (rest.startsWith('/*')) {
      let depth = 1, end = offset + 2;
      while (depth && end < source.length) {
        if (source.startsWith('/*', end)) { depth++; end += 2; }
        else if (source.startsWith('*/', end)) { depth--; end += 2; }
        else end++;
      }
      if (depth) throw new Error('unterminated Rust comment');
      take(end); continue;
    }
    const raw = /^(?:br|cr|r)(#{0,255})"/.exec(rest);
    let literal;
    if (raw) {
      const end = source.indexOf('"' + raw[1], offset + raw[0].length);
      if (end < 0) throw new Error('unterminated Rust raw string');
      literal = end + 1 + raw[1].length;
    } else {
      const quoted = /^(?:b|c)?"(?:\\[\s\S]|[^"\\])*"|^(?:b)?'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|[\s\S])|[^'\\\r\n])'/.exec(rest);
      if (quoted) literal = offset + quoted[0].length;
    }
    const at = line, start = offset;
    if (literal) { const raw = source.slice(offset, literal); take(literal); tokens.push({text: '<literal>', raw, line: at, start, finish: offset}); continue; }
    if (/^(?:b|c)?"/.test(rest)) throw new Error('unterminated Rust string');
    const text = /^(?:r#)?[\p{ID_Start}_][\p{ID_Continue}]*|^\d[\w.]*|^(?:::|=>|->|&&|\|\||==|!=|<=|>=)|^[\s\S]/u.exec(rest)[0];
    take(offset + text.length);
    tokens.push({text: text.replace(/^r#/, ''), line: at, start, finish: offset});
  }
  const stack = [];
  for (let i = 0; i < tokens.length; i++) {
    if ('([{'.includes(tokens[i].text)) stack.push(i);
    if (')]}'.includes(tokens[i].text)) {
      const begin = stack.pop();
      if (begin === undefined || '([{'.indexOf(tokens[begin].text) !== ')]}'.indexOf(tokens[i].text)) throw new Error('unbalanced Rust token tree');
      tokens[begin].end = i;
      tokens[i].begin = begin;
    }
  }
  if (stack.length) throw new Error('unbalanced Rust token tree');
  return tokens;
}

export function expandFuzzIncludes(file, source, resolve, load, stack = new Set()) {
  if (stack.has(file)) throw new Error('cyclic Rust include: ' + file);
  const ancestors = new Set([...stack, file]), tokens = rustTokens(source), edits = [];
  for (let i = 0; i < tokens.length; i++) if (tokens[i].text === 'include' && tokens[i + 1]?.text === '!') {
    const group = tokens[i + 2];
    if (!group?.end) throw new Error('unsupported include in ' + file);
    const args = tokens.slice(i + 3, group.end);
    let reference, manifest = false;
    if (args.length === 1 && args[0].raw?.startsWith('"')) reference = JSON.parse(args[0].raw);
    else if (/^concat ! \( env ! \( <literal> \) , <literal>(?: ,)? \)$/.test(args.map(token => token.text).join(' '))) {
      const values = args.filter(token => token.raw);
      if (JSON.parse(values[0].raw) !== 'CARGO_MANIFEST_DIR') throw new Error('unsupported include environment in ' + file);
      reference = JSON.parse(values[1].raw); manifest = true;
    } else throw new Error('opaque Rust include in ' + file);
    const path = resolve(file, reference, manifest);
    edits.push({start: tokens[i].start, end: tokens[group.end].finish,
      source: expandFuzzIncludes(path, load(path), resolve, load, ancestors)});
    i = group.end;
  }
  for (const edit of edits.reverse()) source = source.slice(0, edit.start) + edit.source + source.slice(edit.end);
  return source;
}

// These operations compute/encode state or perform I/O; none consumes encoded
// wire data. Only the actual fully qualified API is accepted, never an alias.
const nonCodecCalls = new Set([
  'acyclic_harness::workflow::ResumableMachine::initialize',
  'acyclic_harness::workflow::ResumableMachine::transition',
  'acyclic_harness::client::CommandResolution::file_changes',
  'acyclic_harness::client::CommandCatalog::resolve',
  'acyclic_harness::client::CommandSession::open',
  'acyclic_harness::model::PreparedModelRequest::prepare',
  'acyclic_harness::hooks::Hooks::encode',
  'acyclic_harness_tui::term::encode_key',
  'acyclic_stream::session::serve',
  'crate::file::FileDiff::between',
  'crate::local_case::bind_seed_files',
  'crate::executable::resolve',
]);

function nonCodecGuard(tokens, start, limit = tokens.length) {
  let open = start;
  while (open < limit && tokens[open].text !== '(') {
    if (!/^(?:::|[A-Za-z_]\w*)$/.test(tokens[open].text)) return false;
    open++;
  }
  const end = tokens[open]?.end;
  if (end === undefined || end >= limit) return false;
  const path = tokens.slice(start, open).map(t => t.text).join('').replace(/^::/, '');
  if (path === 'futures::executor::block_on') return nonCodecGuard(tokens, open + 1, end);
  if (!nonCodecCalls.has(path)) return false;
  let next = end + 1;
  if (tokens[next]?.text === '.' && tokens[next + 1]?.text === 'await') next += 2;
  if (tokens[next]?.text === ',' && next + 1 === limit) next++;
  return next === limit || ['{', 'else', '&&', '||', ';'].includes(tokens[next]?.text);
}

// Result discrimination belongs exclusively to the shared oracle. This rule does
// not depend on the decoder name, imported signature, or apparent assertions.
export function fuzzOracleViolations(file, source) {
  const tokens = rustTokens(source), errors = [];
  const report = (token, reason) => errors.push(
    file + ':' + token.line + ': fuzz round-trip oracle: ' + reason);
  const hasResult = (start, end) => tokens.slice(start, end)
    .some(t => t.text === 'Ok' || t.text === 'Err' || t.text === '$');
  const helpers = [];
  const localMacros = new Set();
  const macroRules = new Set();
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i].text === 'macro_rules' && tokens[i + 1]?.text === '!') {
      localMacros.add(tokens[i + 2]?.text);
      const end = tokens[i + 3]?.end;
      for (let rule = i + 4; rule < end;) {
        if ([';', ','].includes(tokens[rule].text)) { rule++; continue; }
        const arrow = tokens[rule]?.end + 1;
        if (tokens[arrow]?.text !== '=>' || tokens[arrow + 1]?.end === undefined)
          throw new Error('unsupported macro rule');
        macroRules.add(arrow);
        rule = tokens[arrow + 1].end + 1;
      }
    }
  }
  const macros = new Set(['assert', 'assert_eq', 'assert_ne', 'debug_assert', 'debug_assert_eq', 'debug_assert_ne',
    'panic', 'unreachable', 'unimplemented', 'todo', 'vec', 'format', 'format_args', 'write', 'writeln',
    'eprintln', 'println', 'concat', 'env', 'option_env', 'include_bytes', 'include_str', 'matches', 'json', 'fuzz_target', 'protobuf_input', 'protobuf_inputs', 'json_input', 'stream_round_trip']);
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i].text === 'fuzz_support' && tokens[i + 1]?.text === '::'
      && ['round_trip', 'must_decode', 'must_reject', 'accepts', 'same_result', 'input_result', 'round_trip_option', 'round_trip_by'].includes(tokens[i + 2]?.text) && tokens[i + 3]?.text === '(') {
      helpers.push([i + 3, tokens[i + 3].end]);
    }
  }
  const rawMethods = new Set(['parse', 'parse_bounded', 'parse_text', 'parse_text_bounded',
    'parse_lossy', 'optional', 'optional_view', 'optional_text']);
  for (let i = 0; i < tokens.length; i++) {
    if (tokens.slice(i, i + 4).map(t => t.text).join(' ') === 'fuzz_support :: FuzzInput ::'
      && rawMethods.has(tokens[i + 4]?.text) && tokens[i + 5]?.text === '(')
      helpers.push([i + 3, tokens[i + 5].end]);
  }
  const inHelper = index => helpers.some(([start, end]) => start < index && index < end);
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (rawMethods.has(token.text) && !inHelper(i)
      && tokens[i - 1]?.text === '.' && tokens[i + 1]?.text === '(')
      report(token, 'raw admission must name fuzz_support::FuzzInput explicitly');
    if (['parse_with', 'parse_text_with', 'parse_lossy_with', 'optional_with'].includes(token.text)
      && tokens[i - 1]?.text === '.') report(token, 'raw parser configuration cannot carry generated buffers');
    const protectedCrates = ['std', 'core', 'acyclic_fuzz_support', 'acyclic_harness', 'acyclic_stream', 'acyclic_harness_tui', 'serde', 'serde_json', 'libfuzzer_sys', 'tokio_util', 'bytes'];
    if (token.text === 'mod' && protectedCrates.includes(tokens[i + 1]?.text))
      report(token, 'do not shadow a canonical crate');
    if (token.text === 'as' && protectedCrates.includes(tokens[i + 1]?.text)
      && tokens.slice(i - 2, i).map(t => t.text).join(' ') !== 'use crate')
      report(token, 'do not alias a replacement canonical crate');
    if (token.text === 'mod' && tokens[i + 1]?.text === 'fuzz_support')
      report(token, 'the oracle must be the canonical shared crate');
    if (token.text === 'as' && tokens[i + 1]?.text === 'fuzz_support'
      && tokens.slice(i - 2, i).map(t => t.text).join(' ') !== 'use acyclic_fuzz_support')
      report(token, 'do not alias a replacement oracle crate');
    if (token.text.startsWith('__acyclic_') || token.text === 'test_input') report(token, 'input constructors belong exclusively to the shared entrypoint');
    if (token.text === 'FuzzInput' && tokens[i + 1]?.text === '{') report(token, 'do not manufacture raw-input views');
    if (token.text === 'include' && tokens[i + 1]?.text === '!') report(token, 'includes must be expanded by the runner');
    if (/^[\p{ID_Start}_][\p{ID_Continue}]*$/u.test(token.text) && tokens[i + 1]?.text === '!' && tokens[i + 2]?.end !== undefined) {
      let start = i;
      while (start >= 2 && tokens[start - 1].text === '::') start -= 2;
      const path = tokens.slice(start, i + 1).map(t => t.text).join('');
      const knownPath = !path.includes('::') || /^(?:std|core)::/.test(path)
        || path === 'serde_json::json' || path === 'libfuzzer_sys::fuzz_target'
        || /^(?:crate::)?fuzz_support::(?:fuzz_target|protobuf_input|protobuf_inputs|json_input|stream_round_trip)$/.test(path);
      if (!knownPath || !macros.has(token.text) && !localMacros.has(token.text))
        report(token, 'opaque macros cannot contain an oracle');
    }
    if (token.text === '#' && (tokens[i + 1]?.text === '[' || tokens[i + 1]?.text === '!' && tokens[i + 2]?.text === '[')) {
      const open = tokens[i + 1].text === '!' ? i + 2 : i + 1;
      const name = tokens[open + 1]?.text;
      if (!['cfg', 'cfg_attr', 'derive', 'no_main', 'path', 'serde', 'doc', 'inline', 'must_use', 'track_caller'].includes(name))
        report(token, 'opaque attributes cannot generate an oracle');
      if (name === 'cfg_attr') {
        const args = open + 3, end = tokens[args - 1]?.end;
        let comma = args;
        while (comma < end && tokens[comma].text !== ',') { if (tokens[comma].end !== undefined) comma = tokens[comma].end; comma++; }
        if (tokens.slice(comma + 1, end).map(t => t.text).join(' ') !== 'no_main')
          report(token, 'conditional oracle-generating attributes are opaque');
      }
      if (name === 'derive') {
        const end = tokens[open + 2]?.end;
        const supported = new Set(['Clone', 'Copy', 'Debug', 'Default', 'Eq', 'PartialEq', 'Serialize', 'Deserialize', 'serde', '::', ',']);
        if (end === undefined || tokens.slice(open + 3, end).some(t => !supported.has(t.text)))
          report(token, 'opaque derives cannot generate an oracle');
      }
    }
    if (token.text === 'macro_rules' && ['assert', 'assert_eq', 'assert_ne', 'fuzz_target', 'protobuf_input', 'protobuf_inputs', 'json_input', 'stream_round_trip'].includes(tokens[i + 2]?.text))
      report(token, 'do not shadow the shared oracle or assertions');
    if (/^(?:decode(?:_\w+)?|parse(?:_\w+)?|from_(?:slice|str|value|utf8)|deserialize(?:_\w+)?)$/.test(token.text)
      && tokens[i - 1]?.text !== 'fn' && !inHelper(i)
      && (tokens[i + 1]?.text === '(' || tokens[i + 1]?.text === '::' && tokens[i + 2]?.text === '<')) {
      report(token, 'decode results must go through fuzz_support');
    }
    if (token.text === 'use') {
      let end = i; while (end < tokens.length && tokens[end].text !== ';') end++;
      const imported = tokens.slice(i, end);
      const canonicalHelper = imported.map(t => t.text).join(' ') === 'use acyclic_fuzz_support as fuzz_support';
      if (imported.some((t, j) => ['file', 'local_case', 'executable'].includes(t.text) && imported[j + 1]?.text !== '::'))
        report(token, 'do not import a replacement non-codec module');
      if (!canonicalHelper && imported.some(t => t.text === 'fuzz_support')
        && !['use goal_parser_input :: fuzz_support', 'use recorded_invocation_input :: fuzz_support'].includes(imported.map(t => t.text).join(' ')))
        report(token, 'do not import a replacement oracle module');
      const crateTestAlias = imported[1]?.text === 'crate'
        && tokens.slice(i - 7, i).map(t => t.text).join(' ') === '# [ cfg ( test ) ]';
      if (!canonicalHelper && !crateTestAlias && imported.some((t, j) => protectedCrates.includes(t.text) && imported[j + 1]?.text !== '::'))
        report(token, 'do not import a replacement canonical crate');
      if (!canonicalHelper && !crateTestAlias && imported.some((t, j) => t.text === 'as' && imported[j + 1]?.text !== '_')) report(token, 'callable import aliases are opaque; use the original path');
      if (imported.some(t => ['Serialize', 'Deserialize'].includes(t.text)) && imported[1]?.text !== 'serde')
        report(token, 'derive identity must come directly from serde');
      if (imported.some(t => ['assert', 'assert_eq', 'assert_ne', 'round_trip'].includes(t.text))
        || imported.some(t => t.text === 'fuzz_target') && !imported.some(t => t.text === 'libfuzzer_sys')) report(token, 'do not import replacement oracle macros or helpers');
      if (imported.some(t => t.text === '*')) report(token, 'glob imports can shadow the oracle');
    }
    if (token.text === 'let') {
      let end = i + 1;
      while (end < tokens.length && !['=', ';'].includes(tokens[end].text)) {
        if (tokens[end].end !== undefined) end = tokens[end].end;
        end++;
      }
      if (hasResult(i + 1, end) && !nonCodecGuard(tokens, end + 1))
        report(token, 'Result patterns must stay in fuzz_support');
    }
    if (token.text === '=>' && !macroRules.has(i)) {
      let start = i - 1;
      while (start >= 0 && ![',', '{', '}', '=>'].includes(tokens[start].text)) {
        if (tokens[start].begin !== undefined) start = tokens[start].begin;
        start--;
      }
      if (hasResult(start + 1, i)) report(token, 'Result arms must stay in fuzz_support');
    }
    if (token.text === 'matches' && tokens[i + 1]?.text === '!') {
      const end = tokens[i + 2]?.end;
      if (end !== undefined && hasResult(i + 3, end)) report(token, 'Result patterns must stay in fuzz_support');
    }
    if (token.text === 'ok' && tokens[i - 1]?.text === '.' && tokens[i + 1]?.text === '(') {
      report(token, 'do not discard decoder errors with Result::ok');
    }
  }
  return errors;
}
