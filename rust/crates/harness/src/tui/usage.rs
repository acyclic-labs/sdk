//! Presentation of the shared committed usage projection. The caller retains
//! its query frontier and caches this line until a contribution changes.
use crate::executor::{ModelUsageTotals, UsageTotal};
use acyclic_harness_tui::ratatui::text::Line;
use std::fmt::Write as _;

/// Format one already-folded usage total for [`super::Screen::usage`].
///
/// Unknown observations stay unknown, including partial known subtotals. The
/// charge uses exact currency nanounits; no floating-point conversion, price
/// selection, token counting, source traversal or I/O occurs here.
pub fn usage_summary(totals: &ModelUsageTotals) -> Line<'static> {
    // Six fixed-width u64 counters and their labels fit without reallocating.
    let mut text = String::with_capacity(256);
    text.push_str("estimated token charge: ");
    match (totals.currency.as_deref(), totals.token_nano_units.value()) {
        (Some(currency), Some(nanos)) => {
            let _ = write!(
                text,
                "{} {}.{:09}",
                currency,
                nanos / 1_000_000_000,
                nanos % 1_000_000_000
            );
        }
        _ => text.push('?'),
    }
    for (label, count) in [
        ("in", totals.input),
        ("out", totals.output),
        ("cache read", totals.cached),
        ("cache write", totals.cache_write),
        ("reasoning", totals.reasoning),
    ] {
        text.push_str(" · ");
        text.push_str(label);
        text.push(' ');
        count_text(&mut text, count);
    }
    Line::raw(text)
}

fn count_text(text: &mut String, count: UsageTotal) {
    match count.value() {
        Some(value) => {
            let _ = write!(text, "{value}");
        }
        None => text.push('?'),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::{
        PromptInput, QueryPage, Screen, Viewport,
        ratatui::{Terminal, backend::TestBackend},
    };

    fn projected_footer<const PARTIAL: bool>(priced: bool) {
        let mut totals = if priced {
            ModelUsageTotals::new("USD".into()).unwrap()
        } else {
            ModelUsageTotals::unpriced()
        };
        totals.token_nano_units = UsageTotal {
            known: 98_765_432_101,
            unknown: u64::from(!PARTIAL),
        };
        totals.input = UsageTotal {
            known: 987_654_321,
            unknown: 1,
        };
        for counter in [
            &mut totals.output,
            &mut totals.cached,
            &mut totals.cache_write,
            &mut totals.reasoning,
        ] {
            *counter = UsageTotal {
                known: u64::MAX,
                unknown: 0,
            };
        }
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        let mut usage = QueryPage::<()>::new(vec![usage_summary(&totals)], None, false);
        usage.wrap(80);
        let screen = Screen {
            usage: &usage.lines,
            status: Line::raw("approval requested"),
            ..Screen::default()
        };
        let frame = terminal
            .draw(|frame| {
                screen.render(
                    frame,
                    &PromptInput::default(),
                    &mut Viewport::default(),
                    0,
                    0,
                );
            })
            .unwrap();
        let visible = frame
            .buffer
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(visible.contains("approval requested"));
        assert!(
            visible.contains("estimated token charge: ? · in ?"),
            "a partial subtotal must remain unknown on the screen"
        );
        let compact = visible.replace(' ', "");
        for label in ["out", "cacheread", "cachewrite", "reasoning"] {
            assert!(
                compact.contains(&format!("{label}{}", u64::MAX)),
                "counter clipped: {label}"
            );
        }
        // A one-row consumer must fail the same complete-counter oracle.
        let clipped = usage.lines[0].to_string().replace(' ', "");
        assert!(!clipped.contains(&format!("reasoning{}", u64::MAX)));
        // Complete arithmetic alone cannot name a monetary value without a
        // selected currency. A selected currency retains exact nanounits.
        totals.token_nano_units = UsageTotal {
            known: 1,
            unknown: 0,
        };
        let line = usage_summary(&totals).to_string();
        assert!(line.starts_with(if priced {
            "estimated token charge: USD 0.000000001"
        } else {
            "estimated token charge: ?"
        }));
    }

    #[test]
    fn projected_usage_keeps_partial_cost_unknown_and_attention_visible() {
        projected_footer::<false>(true);
        projected_footer::<false>(false);
    }

    #[test]
    #[should_panic(expected = "a partial subtotal must remain unknown on the screen")]
    fn partial_subtotal_negative_control() {
        projected_footer::<true>(true);
    }
}
