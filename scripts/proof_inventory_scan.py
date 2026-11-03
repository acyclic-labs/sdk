"""Canonical lexical Rust proof discovery; effective exports need rustdoc join.

Shared by the historical read-only inventory and the change gate. This scanner
is conservative source discovery, not compiler API reachability or a proof.
"""
import bisect
import hashlib
import re
from pathlib import Path

def mask(source):
    # Retain offsets/newlines while removing comments, strings and character
    # literals. Rust lifetimes are retained. Raw and nested block comments count.
    chars = list(source)
    i = 0
    while i < len(source):
        end = None
        if source.startswith('//', i):
            end = source.find('\n', i)
            end = len(source) if end < 0 else end
        elif source.startswith('/*', i):
            j, depth = i + 2, 1
            while j < len(source) and depth:
                if source.startswith('/*', j):
                    depth += 1
                    j += 2
                elif source.startswith('*/', j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            end = j
        else:
            raw = re.match(r'(?:br|cr|r)(#{0,255})"', source[i:]) if source[i] in 'brc' else None
            if raw and (i == 0 or not (source[i - 1].isalnum() or source[i - 1] == '_')):
                terminator = '"' + raw[1]
                j = source.find(terminator, i + raw.end())
                end = len(source) if j < 0 else j + len(terminator)
            elif source[i] == '"':
                j = i + 1
                while j < len(source):
                    if source[j] == '\\':
                        j += 2
                    elif source[j] == '"':
                        j += 1
                        break
                    else:
                        j += 1
                end = j
            elif source[i] == "'":
                char = re.match(r"'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^'\\\n])'", source[i:])
                if char:
                    end = i + char.end()
        if end is not None:
            for j in range(i, min(end, len(chars))):
                if chars[j] != '\n':
                    chars[j] = ' '
            i = end
        else:
            i += 1
    return ''.join(chars)


def brace_pairs(code):
    stack, pairs = [], {}
    for i, ch in enumerate(code):
        if ch == '{':
            stack.append(i)
        elif ch == '}' and stack:
            pairs[stack.pop()] = i
    return pairs


def attrs_before(code, source, offset):
    cursor = offset
    while cursor > 0 and code[cursor - 1].isspace():
        cursor -= 1
    start = cursor
    while cursor > 0 and code[cursor - 1] == ']':
        depth, j = 1, cursor - 2
        while j >= 0 and depth:
            depth += (code[j] == ']') - (code[j] == '[')
            j -= 1
        if j < 0 or code[j] != '#':
            break
        start = j
        cursor = j
        while cursor > 0 and code[cursor - 1].isspace():
            cursor -= 1
    # Comments masked by code can include the entire preceding item, so bound
    # documentation inspection to the current attribute/declaration paragraph.
    return source[start:offset], start


def scan_file(path, root):
    data = (root / path).read_bytes()
    source = data.decode('utf-8-sig')
    source_lines = source.splitlines()
    code = mask(source)
    pairs = brace_pairs(code)
    starts = [0] + [m.end() for m in re.finditer('\n', source)]
    def line(pos):
        return bisect.bisect_right(starts, pos)
    def ref(pos):
        return f'{path}:{line(pos)}'
    test_file = any(x in {'tests', 'benches', 'examples'} for x in Path(path).parts) or bool(re.search(r'/(?:tests(?:_[^/]*)?|[^/]*_tests|formal_proofs|kani_proofs|kani_[^/]*)\.rs$', path))
    test_ranges = []
    for m in re.finditer(r'#\[\s*cfg\s*\(\s*(?:test\b|kani\b)|#\[\s*(?:(?:tokio|wasm_bindgen_test)::)?(?:test|wasm_bindgen_test)\s*(?:\]|\()', code):
        b = code.find('{', m.end())
        if b in pairs:
            test_ranges.append((m.start(), pairs[b]))
    def test(pos):
        return test_file or any(a <= pos <= b for a, b in test_ranges)
    functions = []
    pattern = r'(?<![\w:])(?:(pub(?:\([^)]*\))?)\s+)?(?:(?:async|unsafe|const|extern)\s+)*(?:fn)\s+(\w+)'
    for m in re.finditer(pattern, code):
        b = code.find('{', m.end())
        semi = code.find(';', m.end())
        end = pairs.get(b, b) if b >= 0 and (semi < 0 or b < semi) else semi
        if end < 0:
            end = m.end()
        attr, attr_start = attrs_before(code, source, m.start())
        functions.append(dict(name=m[2], visibility=m[1] or 'implicit', start=m.start(), end=end, location=ref(m.start()), attributes=attr, test=test(m.start())))
    def context(pos):
        parents = [f for f in functions if f['start'] <= pos <= f['end']]
        return min(parents, key=lambda f:f['end']-f['start']) if parents else None
    def record(m, kind):
        fn = context(m.start())
        return dict(kind=kind, location=ref(m.start()), line=line(m.start()), text=source_lines[line(m.start())-1].strip()[:350], function=fn['name'] if fn else None, test_or_example=test(m.start()))
    entries, unsafe, signals, proofs = [], [], [], []
    item_pattern = r'\bpub\s+(?:(?:async|unsafe|const|extern)\s+)*(fn|struct|enum|trait|type|mod|use|static|const)\s+(\w+)'
    for m in re.finditer(item_pattern, code):
        if test(m.start()):
            continue
        attr, a = attrs_before(code, source, m.start())
        prefix = source[max(0, a-700):m.start()]
        doc = bool(re.search(r'///|#\[\s*doc\s*(?:=|\()', attr)) or bool(re.search(r'(?:^|\n)\s*///[^\n]*(?:\n\s*(?:///[^\n]*|#\[[^\n]*\]))*\s*$', prefix))
        entries.append(dict(**record(m,'public_'+m[1]), name=m[2], documentation='lexically_present' if doc else 'needs_semantic_review', invariants=[], proofs=[], mode_coverage={'local':'unreviewed','cloud':'unreviewed'}, review_status='discovered_not_semantically_validated'))
    # Include implicit public trait methods and all methods of relevant callback
    # impls. The owning brace is located, so later methods are not lost to a
    # fixed-size look-behind window.
    for owner in re.finditer(r'\b(?:pub\s+trait\s+\w+|impl[^{};]*\b(?:Filesystem|FileSystem|NFS|Nfs|\w*Service)\s+for[^{};]*)[^{};]*\{',code):
        b=owner.end()-1
        if b not in pairs or test(owner.start()):
            continue
        for f in functions:
            if owner.end()<=f['start']<pairs[b] and not f['test']:
                entries.append(dict(kind='trait_method_or_callback',name=f['name'],location=f['location'],owner=source[owner.start():b].strip()[:350],invariants=[],proofs=[],mode_coverage={'local':'unreviewed','cloud':'unreviewed'},review_status='needs_visibility_and_cfg_review'))
    # Trait impl methods, native callbacks and binary main are not necessarily pub.
    for f in functions:
        if f['test']:
            continue
        prefix = code[max(0, f['start']-500):f['start']]
        if f['name'] == 'main' or 'extern' in code[f['start']:min(f['start']+120,f['end'])] or re.search(r'impl[^{}]+(?:Filesystem|FileSystem|NFS|Nfs|Service)\s+for[^{}]*\{[^{}]*$', prefix):
            entries.append(dict(kind='callback_or_binary_candidate', name=f['name'], location=f['location'], attributes=f['attributes'], invariants=[], proofs=[], review_status='needs_trait_and_registration_review', mode_coverage={'local':'unreviewed','cloud':'unreviewed'}))
    for m in re.finditer(r'\bunsafe\s*(?:\{|fn\b|impl\b|extern\b|trait\b)', code):
        item = record(m,'unsafe')
        ln = item['line']
        # Preserve the actual argument; don't invent one from the operation name.
        prior = source_lines[max(0,ln-14):ln]
        item.update(safety_context='\n'.join(prior), safety_argument_status='comment_candidate' if any(re.search(r'SAFETY|# Safety', x) for x in prior) else 'not_found_in_previous_14_lines', miri_coverage='unreviewed', kani_coverage='unreviewed')
        unsafe.append(item)
    for kind, regex in [
        ('panic_unwrap_expect', r'\b(?:panic|todo|unimplemented|unreachable)\s*!|\.\s*(?:unwrap|expect)\s*\('),
        ('untyped_error', r'\b(?:anyhow|eyre)::|Box\s*<\s*dyn\s+(?:std::error::)?Error\b|Result\s*<[^;{}\n]*,\s*String\s*>'),
        ('catch_all_arm', r'\b_\s*=>'),
        ('parser_decoder', r'\bfn\s+\w*(?:decode|parse|deserialize|from_bytes|from_str)\w*'),
        ('tool_or_cli_registration', r'#\[\s*(?:command|arg|tool|tool_router|tool_handler)\b'),
        ('lint_allowance', r'#\s*!?\[\s*(?:allow|expect)\b'),
    ]:
        for m in re.finditer(regex, code):
            if not test(m.start()):
                item = record(m,kind)
                if kind == 'catch_all_arm':
                    fn = context(m.start())
                    signature = source[fn['start']:min(fn['start']+600,fn['end'])] if fn else ''
                    item['typed_mapper_candidate'] = bool(re.search(r'error|status|errno|failure|reject|mapping', (fn['name'] if fn else '')+' '+signature,re.I))
                    item['classification'] = 'needs_typed_error_semantic_review'
                signals.append(item)
    for kind, regex in [
        ('kani', r'#\[\s*kani::proof(?:\s*\]|_for_contract\s*\()'),
        ('loom', r'\bloom::(?:model|sync|thread)\b'),
        ('proptest', r'\b(?:proptest::)?proptest\s*!'),
        ('fuzz', r'\bfuzz_target\s*!|\bbolero::check\s*!'),
        ('example_test', r'#\[\s*(?:(?:tokio|wasm_bindgen_test)::)?(?:test|wasm_bindgen_test)\s*(?:\]|\()'),
    ]:
        for m in re.finditer(regex,code):
            fn = next((f for f in functions if m.end() <= f['start'] and f['start']-m.end() < 700),None)
            item = record(m,kind)
            item.update(name=fn['name'] if fn else None, negative_control='unreviewed', mode_coverage={'local':'unreviewed','cloud':'unreviewed'}, runtime_receipt=None, review_status='candidate')
            proofs.append(item)
    return dict(path=path,sha256=hashlib.sha256(data).hexdigest(),lines=len(starts),functions=functions,entries=entries,unsafe=unsafe,signals=signals,proofs=proofs)
