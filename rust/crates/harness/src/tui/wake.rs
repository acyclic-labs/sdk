//! Presentation of admitted monitor and schedule documents. The query owner
//! retains the original file identity, generation and continuation for actions.
use super::{display_text, ratatui::text::Line};
use crate::agent::wake::{Lifetime, Missed, Monitor, Recurrence, Schedule};

/// Render one validated monitor from the visible file page.
///
/// A declaration describes delivery policy and its committed source cursor;
/// neither the declaration nor its presence proves that a subscription is live
/// or that a model has seen an event. This function never opens its source.
pub fn monitor_rows(monitor: &Monitor) -> [Line<'static>; 4] {
    [
        Line::raw(format!(
            "{} · {} delivery",
            display_text(monitor.id.as_str()),
            if monitor.wake { "waking" } else { "quiet" },
        )),
        Line::raw(format!("source: {}", display_text(&monitor.source))),
        Line::raw(format!("committed source cursor: {}", monitor.cursor.next)),
        Line::raw(match monitor.deadline_unix_ms {
            Some(deadline) => format!("deadline: {deadline} Unix ms"),
            None => "deadline: none".into(),
        }),
    ]
}

/// Render one validated schedule from the visible file page.
///
/// Times are the document's absolute Unix milliseconds and configured timezone.
/// There is no wall-clock read, countdown, recurrence evaluation or alarm here.
/// An absent expiry or occurrence count remains explicitly unbounded.
pub fn schedule_rows(schedule: &Schedule) -> [Line<'static>; 5] {
    [
        Line::raw(format!(
            "{} · owner {}",
            display_text(schedule.id.as_str()),
            display_text(schedule.owner.as_str()),
        )),
        Line::raw(format!(
            "next due: {} Unix ms · timezone {}",
            schedule.next_due_unix_ms,
            display_text(&schedule.timezone),
        )),
        Line::raw(match &schedule.recurrence {
            None => "one-shot".into(),
            Some(Recurrence::Every { milliseconds }) => format!("every {milliseconds} ms"),
            Some(Recurrence::Cron { expression }) => {
                format!("cron: {}", display_text(expression))
            }
        }),
        Line::raw(format!(
            "{} lifetime · missed occurrences: {}",
            match schedule.lifetime {
                Lifetime::Session => "session",
                Lifetime::Daemon => "daemon",
            },
            match schedule.missed {
                Missed::CatchUp => "catch up",
                Missed::Coalesce => "coalesce",
            },
        )),
        Line::raw({
            use std::fmt::Write as _;
            // Both u64 values and labels fit without growing this row.
            let mut text = String::with_capacity(112);
            text.push_str("expiry: ");
            match schedule.expires_unix_ms {
                Some(expiry) => {
                    let _ = write!(text, "{expiry} Unix ms");
                }
                None => text.push_str("none"),
            }
            text.push_str(" · occurrences remaining: ");
            match schedule.remaining {
                Some(remaining) => {
                    let _ = write!(text, "{remaining}");
                }
                None => text.push_str("unlimited"),
            }
            text
        }),
    ]
}
