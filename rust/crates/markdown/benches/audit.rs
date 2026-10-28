//! Checks append work independently of completed history and machine load.

use acyclic_markdown::Markdown;
use acyclic_markdown_work::{self as work, Counts};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

fn bounded(small: Counts, large: Counts) -> bool {
    large.rescanned_bytes <= small.rescanned_bytes
        && large.reparsed_nodes <= small.reparsed_nodes
        && large.span_copy_bytes <= small.span_copy_bytes
}

fn append(markdown: &mut Markdown, delta: &str) -> (Counts, Duration) {
    let before = work::snapshot();
    let started = Instant::now();
    black_box(markdown.push(black_box(delta)));
    (work::snapshot() - before, started.elapsed())
}

// Execute an actual document-start parser variant, rather than inventing a
// larger counter value. The same production parsing and copying hooks see it.
fn reparse(markdown: &mut Markdown, delta: &str) -> Counts {
    let mut source = std::mem::take(markdown).into_source();
    source.push_str(delta);
    let before = work::snapshot();
    markdown.push(&source);
    work::snapshot() - before
}

fn p99(times: &mut [Duration]) -> Duration {
    times.sort_unstable();
    times
        .get(times.len() * 99 / 100)
        .copied()
        .unwrap_or_default()
}

fn check(name: &str, small: &str, large: &str, delta: impl Fn(usize) -> String) {
    let mut small_markdown = Markdown::default();
    let mut large_markdown = Markdown::default();
    small_markdown.push(small);
    large_markdown.push(large);
    let mut small_times = Vec::new();
    let mut large_times = Vec::new();
    for index in 0..544 {
        let delta = delta(index);
        let (small, large) = if index % 2 == 0 {
            let small = append(&mut small_markdown, &delta);
            (small, append(&mut large_markdown, &delta))
        } else {
            let large = append(&mut large_markdown, &delta);
            (append(&mut small_markdown, &delta), large)
        };
        assert!(
            bounded(small.0, large.0),
            "{name} push {index}: append work scales with history: {:?} -> {:?}",
            small.0,
            large.0
        );
        if index == 543 {
            println!("{name}: final append work {:?} / {:?}", small.0, large.0);
        }
        if index >= 32 {
            small_times.push(small.1);
            large_times.push(large.1);
        }
    }
    let small = p99(&mut small_times);
    let large = p99(&mut large_times);
    println!(
        "{name}: p99 {:.3} / {:.3} us, ratio {:.2} (information only)",
        small.as_secs_f64() * 1e6,
        large.as_secs_f64() * 1e6,
        large.as_secs_f64() / small.as_secs_f64()
    );
    let delta = delta(544);
    let small = reparse(&mut small_markdown, &delta);
    let large = reparse(&mut large_markdown, &delta);
    assert!(
        !bounded(small, large),
        "{name}: document-start negative control escaped the work assertion"
    );
    println!("{name}: document-start negative control rejected: {small:?} -> {large:?}");
}

fn main() {
    let fence = |lines| format!("```rust\n{}```\n\n", "let x = 1;\n".repeat(lines));
    // Initialize shared syntax tables before collecting informational timings.
    let mut warm = Markdown::default();
    warm.push(&fence(1));
    check(
        "unused definitions (100 / 10000 code lines)",
        &fence(100),
        &fence(10_000),
        |index| format!("[unused{index}]: /url\n\n"),
    );
    let nested = |lines| {
        format!(
            "- parent\n\n  ```rust\n{}  ```\n",
            "  let x = 1;\n".repeat(lines)
        )
    };
    check(
        "nested fence (100 / 10000 code lines)",
        &nested(100),
        &nested(10_000),
        |_| "- next\n".into(),
    );
    check(
        "paragraph (1 / 100 KiB)",
        &"x".repeat(1024),
        &"x".repeat(100 * 1024),
        |_| "x".into(),
    );
    check(
        "punctuation (1 / 100 KiB)",
        &"x".repeat(1024),
        &"x".repeat(100 * 1024),
        |_| "word, (word); #yes! ".into(),
    );
    let lazy = |count| {
        format!(
            "{}> quote\n- item\n- item\ntext 🦀\n| a | b |\n| - | - |\n[r]: url\n\n[r]: /",
            "- earlier\n".repeat(count)
        )
    };
    check(
        "lazy list references (100 / 10000 items)",
        &lazy(100),
        &lazy(10_000),
        |_| "u".into(),
    );
    check(
        "indented code (100 / 10000 lines)",
        &"    indented\n".repeat(100),
        &"    indented\n".repeat(10_000),
        |_| "\n    next\n".into(),
    );
    let definitions = |count| {
        let mut source = String::new();
        for index in 0..count {
            source.push_str(&format!("[r{index}]: /url\n\n"));
        }
        source.push_str("a *unfinished ");
        source
    };
    check(
        "references (100 / 10000 definitions)",
        &definitions(100),
        &definitions(10_000),
        |_| "x".into(),
    );
    let emphasis = |count| format!("{}a *unfinished ", "*completed*\n\n".repeat(count));
    check(
        "emphasis (100 / 10000 completed blocks)",
        &emphasis(100),
        &emphasis(10_000),
        |_| "x".into(),
    );
}
