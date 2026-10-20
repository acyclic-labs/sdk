// Grammar-generated adversarial search for append work that grows with
// history. Each case streams a prefix and then a cycle of grammar fragments,
// each followed by a separator, and records every work category at n, 2n and
// 4n fragments. Linear work doubles its increment; history-proportional work
// quadruples it.
use super::*;
use proptest::test_runner::{Config, RngAlgorithm, TestCaseError, TestError, TestRng, TestRunner};

const CATEGORIES: usize = 10;
const NAMES: [&str; CATEGORIES] = [
    "scan",
    "copy",
    "visibility",
    "feed",
    "parser",
    "visible",
    "cache",
    "moved",
    "boundary",
    "replay",
];

// Grammar terminals: inline delimiters, entities, links, images, code, HTML,
// comments, fences, definitions, containers, tables and separators.
const FRAGMENTS: &[&str] = &[
    "x",
    "word",
    " ",
    "*",
    "**",
    "_",
    "__",
    "~",
    "~~",
    "`",
    "``",
    "\\",
    "\\*",
    "[",
    "]",
    "![",
    "](u)",
    "[x](u)",
    "![x](u)",
    "[a *b*](<u> \"t\")",
    "[x][r]",
    "[r]",
    "![][r]",
    "[s]",
    "<https://x>",
    "<a@b.example>",
    "&amp;",
    "&#10;",
    "&#0;",
    "&copy",
    ";",
    "<b>",
    "</b>",
    "<!--",
    "-->",
    "<!--\n x\n-->",
    "`code`",
    "**bold**",
    "🦀",
    "¡",
    "\n",
    "\n\n",
    "- item\n",
    "- item\n\n",
    "1. one\n",
    "> quote\n",
    "# head\n",
    "---\n",
    "***\n",
    "```\n",
    "```rust\nlet x = 1;\n",
    "~~~\n",
    "    code\n",
    "<div>\n",
    "</div>\n",
    "<div>\n x\n</div>\n",
    "[r]: /url\n\n",
    "[s]: /s\n",
    "[r]:",
    " /url",
    "| a | b |\n",
    "| - | - |\n",
    "| x | y |\n",
    "\t",
    "-   parent\n\n      child\n",
    "<",
    ">",
    "<a ",
    "(",
    ")",
    "](",
    "<?",
    "?>",
    "<!X",
    "<![CDATA[",
    "]]>",
    "\"",
    "'",
    "=",
];
const SEPARATORS: &[&str] = &["", " ", "\n", "\n\n", "x", "!"];
const PREFIXES: &[&str] = &["", "x ", "- ", "> ", "| a |\n| - |\n", "```\n", "[r]\n\n"];

#[derive(Clone, Debug)]
pub(super) struct Cycle {
    pub(super) prefix: &'static str,
    pub(super) parts: Vec<&'static str>,
    pub(super) separator: &'static str,
}

fn cycle() -> impl Strategy<Value = Cycle> {
    (
        prop::sample::select(PREFIXES),
        prop::collection::vec(prop::sample::select(FRAGMENTS), 1..=4),
        prop::sample::select(SEPARATORS),
    )
        .prop_map(|(prefix, parts, separator)| Cycle {
            prefix,
            parts,
            separator,
        })
}

fn totals(markdown: &Markdown, replay: usize) -> [usize; CATEGORIES] {
    let [scan, copy, visibility, feed, parser] = work::parts();
    [
        scan,
        copy,
        visibility,
        feed,
        parser,
        markdown.visible_work.get(),
        markdown.reference_cache_visits,
        markdown.reference_moved_lines,
        markdown.boundary_work,
        replay,
    ]
}

pub(super) struct Run {
    pub(super) snapshots: Vec<[usize; CATEGORIES]>,
    /// Bytes of open containers, and of the open block when no incremental
    /// inline state covers it, at each snapshot.
    pub(super) unretained: Vec<usize>,
    /// Output lines whose content changed, from the first difference on.
    pub(super) output: Vec<usize>,
}

/// Bytes of the open block: the reconstructed prefix of its open containers
/// and the source after the block checkpoint.
pub(super) fn open(markdown: &Markdown) -> usize {
    crate::context_prefix(&markdown.context).len() + markdown.source.len() - markdown.offset
}

/// Per-push allowance. A definition that resolves or changes earlier
/// references re-renders each affected cached block, which the reference
/// update properties bound per visit and moved line.
pub(super) fn allowance(open: usize, chunk: usize, references: usize) -> usize {
    128 * (open + chunk + 1) + 256 * (references + 1)
}

/// Work per category after each requested fragment count, measured from the
/// end of the prefix. Every push stays within the specified O(chunk + open
/// block + affected references) bound, and the final document equals a
/// one-shot render.
pub(super) fn samples(case: &Cycle, counts: &[usize]) -> Run {
    let mut markdown = Markdown::default();
    markdown.push(case.prefix);
    let (mut replay, mut output) = (0, 0);
    // Work the harness itself does to compare output, excluded from samples.
    let mut harness = [0; CATEGORIES];
    let start = totals(&markdown, replay);
    let mut run = Run {
        snapshots: Vec::new(),
        unretained: Vec::new(),
        output: Vec::new(),
    };
    for (index, part) in case
        .parts
        .iter()
        .cycle()
        .take(*counts.last().expect("sample count"))
        .enumerate()
    {
        for delta in [*part, case.separator] {
            let unmeasured = totals(&markdown, replay);
            let previous: Vec<Line> = markdown.lines().iter().cloned().collect();
            let measured = totals(&markdown, replay);
            for ((total, measured), unmeasured) in harness.iter_mut().zip(measured).zip(unmeasured)
            {
                *total += measured - unmeasured;
            }
            let open = open(&markdown);
            let before = totals(&markdown, replay);
            let growth = work::source_growth();
            let changed = markdown.push(delta);
            replay += markdown.lines().len() - changed;
            // Source relocation is prepaid amortized work.
            let after = totals(&markdown, replay);
            let used = sum(&after) - sum(&before) - (work::source_growth() - growth);
            let references = after[6] + after[7] - before[6] - before[7];
            assert!(
                used <= allowance(open, delta.len(), references),
                "{case:?}: {used} units for {open} open + {} chunk bytes, {references} references",
                delta.len()
            );
            let lines = markdown.lines();
            let same = previous
                .iter()
                .zip(lines.iter())
                .take_while(|(old, new)| old == new)
                .count();
            output += lines.len() - same;
            let compared = totals(&markdown, replay);
            for ((total, compared), after) in harness.iter_mut().zip(compared).zip(after) {
                *total += compared - after;
            }
        }
        if counts.contains(&(index + 1)) {
            let now = totals(&markdown, replay);
            run.snapshots
                .push(difference(difference(now, start), harness));
            run.output.push(output);
            let block = if markdown.inline.is_some() {
                0
            } else {
                open(&markdown)
            };
            run.unretained
                .push(crate::context_prefix(&markdown.context).len() + block);
        }
    }
    assert_eq!(
        markdown.lines(),
        full_parse(&markdown.source),
        "{case:?} final render"
    );
    run
}

fn sum(work: &[usize; CATEGORIES]) -> usize {
    work.iter().sum()
}

fn difference(a: [usize; CATEGORIES], b: [usize; CATEGORIES]) -> [usize; CATEGORIES] {
    let mut result = a;
    for (value, before) in result.iter_mut().zip(b) {
        *value -= before;
    }
    result
}

/// Linear work doubles its increment from [n, 2n] to [2n, 4n]. Allow a
/// quarter of the first increment for amortized buffer growth, which is
/// geometric and therefore lands unevenly across the two windows.
pub(super) fn superlinear(case: &Cycle, n: usize) -> Option<String> {
    let run = samples(case, &[n, 2 * n, 4 * n]);
    // An open block that the engine reparses whole, or a nesting whose every
    // line repeats its container prefix, is bounded per push above; it cannot
    // also be linear in total while it keeps growing.
    let [_, previous_open, next_open]: [usize; 3] =
        run.unretained.try_into().expect("three snapshots");
    if next_open > previous_open {
        return None;
    }
    let [a, b, c]: [[usize; CATEGORIES]; 3] = run.snapshots.try_into().expect("three snapshots");
    let over = |first: usize, second: usize| second > 2 * first + first / 4 + 512;
    // Changed output lines are counted exactly, so they need no slack.
    let quadratic = |first: usize, second: usize| first > 0 && second > 2 * first + first / 4;
    let first = difference(b, a);
    let second = difference(c, b);
    // Output that itself changes superlinearly, as when each setext underline
    // restyles a growing paragraph, must be redrawn by any renderer that
    // matches every prefix; the per-push bound above still holds. This counts
    // lines that really differ, not lines the engine reports as changed.
    let [x, y, z]: [usize; 3] = run.output.try_into().expect("three snapshots");
    if quadratic(y - x, z - y) {
        return None;
    }
    let total = over(sum(&first), sum(&second));
    let categories: Vec<_> = NAMES
        .into_iter()
        .zip(first)
        .zip(second)
        .filter(|&((_, first), second)| over(first, second))
        .map(|((name, first), second)| format!("{name} {first} -> {second}"))
        .collect();
    (total || !categories.is_empty()).then(|| {
        format!(
            "{case:?}: increments {} -> {} over {n} fragments; {categories:?}",
            sum(&first),
            sum(&second)
        )
    })
}

fn search(cases: u32, threads: u32) {
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|thread| {
                scope.spawn(move || {
                    let mut seed = [0u8; 32];
                    seed.get_mut(..8)
                        .expect("seed header")
                        .copy_from_slice(&(u64::from(thread) + 1).to_le_bytes());
                    let mut runner = TestRunner::new_with_rng(
                        Config {
                            cases: cases.div_ceil(threads),
                            failure_persistence: None,
                            max_shrink_iters: 256,
                            ..Config::default()
                        },
                        TestRng::from_seed(RngAlgorithm::ChaCha, &seed),
                    );
                    runner.run(&cycle(), |case| match superlinear(&case, 24) {
                        Some(message) => Err(TestCaseError::fail(message)),
                        None => Ok(()),
                    })
                })
            })
            .collect();
        let failures: Vec<_> = workers
            .into_iter()
            .filter_map(|worker| match worker.join().expect("search worker") {
                Ok(()) => None,
                Err(TestError::Fail(reason, case)) => Some(format!("{reason} (minimal {case:?})")),
                Err(error) => Some(error.to_string()),
            })
            .collect();
        assert!(failures.is_empty(), "{failures:#?}");
    });
}

#[test]
fn grammar_cycles_have_linear_append_work() {
    let cases = std::env::var("GC_SEARCH_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(2048);
    search(cases, 8);
}

// Every cycle the search has found, kept as a regression.
const FOUND: &[(&str, &[&str], &str)] = &[
    ("x ", &["&#10;", "*", "<!--\n x\n-->"], ""),
    ("", &["\\", "**", "**", "&#0;"], "\n"),
    ("[r]\n\n", &["_", "<!--\n x\n-->", "    code\n"], " "),
    ("| a |\n| - |\n", &["<!--\n x\n-->", "- item\n", "="], " "),
    (
        "| a |\n| - |\n",
        &["<!--\n x\n-->", "<div>\n", "=", ";"],
        " ",
    ),
    ("| a |\n| - |\n", &[")", "-->", "&#10;"], ""),
    ("| a |\n| - |\n", &["`"], " "),
    ("| a |\n| - |\n", &["``", "&#10;"], "x"),
    ("| a |\n| - |\n", &["x"], ""),
    ("| a |\n| - |\n", &["~"], " "),
    ("- ", &["*", "](", "---\n", "\t"], " "),
    ("- ", &["]]>", "<https://x>", "\n\n", "[r]:"], " "),
    ("> ", &["🦀", "**bold**", "[r]: /url\n\n", "[r]:"], ""),
    ("[r]\n\n", &["***\n", " ", "[r]:"], " "),
    ("x ", &["&#10;", "- item\n", "="], ""),
    ("```\n", &["*", "    code\n", "```\n", "\n\n"], "\n"),
    ("```\n", &[">"], ""),
    ("```\n", &["_"], "x"),
];

fn found(index: usize) -> Cycle {
    let &(prefix, parts, separator) = FOUND.get(index).expect("known regression");
    Cycle {
        prefix,
        parts: parts.to_vec(),
        separator,
    }
}

#[test]
fn found_cycles_have_linear_append_work() {
    for index in 0..FOUND.len() {
        let case = found(index);
        assert_eq!(superlinear(&case, 24), None, "{case:?}");
        if std::env::var_os("GC_WORK_REPORT").is_some() {
            let run = samples(&case, &[128, 256]);
            println!("{case:?}: {:?}", run.snapshots);
        }
    }
}

// Negative control: a frontier that stops at the first definition of each
// label rereads every later duplicate definition on each push.
#[test]
fn duplicate_definitions_seal_the_frontier() {
    let case = found(13);
    assert_eq!(superlinear(&case, 24), None);
    crate::references::STALL_DUPLICATES.set(true);
    let stalled = superlinear(&case, 24);
    crate::references::STALL_DUPLICATES.set(false);
    assert!(stalled.is_some());
}
