//! Checks that ordinary appends do not scale with finalized content or references.

use acyclic_markdown::Markdown;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

fn measure(small: &str, large: &str, delta: impl Fn(usize) -> String) -> (Duration, Duration) {
    let mut small_markdown = Markdown::default();
    let mut large_markdown = Markdown::default();
    small_markdown.push(small);
    large_markdown.push(large);
    for index in 0..32 {
        let delta = delta(index);
        small_markdown.push(&delta);
        large_markdown.push(&delta);
    }
    let mut small_times = Vec::new();
    let mut large_times = Vec::new();
    for index in 32..544 {
        let delta = delta(index);
        let sample = |markdown: &mut Markdown, times: &mut Vec<Duration>| {
            let before = Instant::now();
            black_box(markdown.push(black_box(&delta)));
            times.push(before.elapsed());
        };
        if index.is_multiple_of(2) {
            sample(&mut small_markdown, &mut small_times);
            sample(&mut large_markdown, &mut large_times);
        } else {
            sample(&mut large_markdown, &mut large_times);
            sample(&mut small_markdown, &mut small_times);
        }
    }
    let p99 = |times: &mut Vec<Duration>| {
        times.sort_unstable();
        times
            .get(times.len() * 99 / 100)
            .copied()
            .unwrap_or_default()
    };
    (p99(&mut small_times), p99(&mut large_times))
}

fn check(name: &str, small: &str, large: &str, delta: impl Fn(usize) -> String + Copy) -> bool {
    // Warm both instances, then interleave appends to compare under the same load.
    let mut small_times = Vec::new();
    let mut large_times = Vec::new();
    for _ in 0..3 {
        let (small, large) = measure(small, large, delta);
        small_times.push(small);
        large_times.push(large);
    }
    small_times.sort_unstable();
    large_times.sort_unstable();
    let small = small_times.get(1).copied().unwrap_or_default();
    let large = large_times.get(1).copied().unwrap_or_default();
    println!(
        "{name}: p99 {:.3} / {:.3} us, ratio {:.2}",
        small.as_secs_f64() * 1e6,
        large.as_secs_f64() * 1e6,
        large.as_secs_f64() / small.as_secs_f64()
    );
    large <= small.mul_f64(1.5)
}

fn main() {
    let fence = |lines| format!("```rust\n{}```\n\n", "let x = 1;\n".repeat(lines));
    // Initialize shared syntax tables before timing any append.
    let mut warm = Markdown::default();
    warm.push(&fence(1));
    let mut passed = check(
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
    passed &= check(
        "nested fence (100 / 10000 code lines)",
        &nested(100),
        &nested(10_000),
        |_| "- next\n".into(),
    );
    passed &= check(
        "paragraph (1 / 100 KiB)",
        &"x".repeat(1024),
        &"x".repeat(100 * 1024),
        |_| "x".into(),
    );
    passed &= check(
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
    passed &= check(
        "lazy list references (100 / 10000 items)",
        &lazy(100),
        &lazy(10_000),
        |_| "u".into(),
    );
    passed &= check(
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
    passed &= check(
        "references (100 / 10000 definitions)",
        &definitions(100),
        &definitions(10_000),
        |_| "x".into(),
    );
    assert!(passed, "append p99 scales with completed history");
}
