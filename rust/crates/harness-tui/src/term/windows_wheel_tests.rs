use super::*;
use crate::{InputEffect, PromptInput, Viewport, WHEEL_ROWS, ratatui::layout::Rect};
use std::path::Path;
use std::time::Duration;

fn route<const COPY: bool>(events: &[InputEvent]) -> (Vec<InputEffect>, (u64, u64)) {
    let mut prompt = PromptInput::default();
    let mut viewport = Viewport::default();
    let mut effects = Vec::with_capacity(events.len());
    let measured = acyclic_test_support::measure(|| {
        for event in events.iter().cloned() {
            let routed = prompt.event(event, Rect::new(0, 0, 80, 1), &mut viewport, 0);
            if COPY {
                effects.extend(routed.collect::<Vec<_>>());
            } else {
                effects.extend(routed);
            }
        }
    });
    (effects, (measured.count_total, measured.bytes_total))
}

pub(super) fn read_bursts(tty: &mut Tty, report: &Path) -> io::Result<()> {
    for count in [32, 64] {
        std::fs::write(
            report.with_extension(format!("wheel{count}-ready")),
            b"ready",
        )?;
        let deadline = Instant::now() + Duration::from_millis(500);
        let mut events = Vec::with_capacity(count);
        while events.len() < count && Instant::now() < deadline {
            events.extend(tty.read_events(deadline)?);
        }
        assert_eq!(
            events.len(),
            count,
            "every physical wheel packet is decoded"
        );
        let expected: Vec<_> = (0..count)
            .map(|at| InputEffect::Scroll(if at % 3 == 0 { -WHEEL_ROWS } else { WHEEL_ROWS }))
            .collect();
        let (effects, allocation) = route::<false>(&events);
        assert_eq!(effects, expected, "physical wheel order and direction");
        assert_eq!(
            allocation,
            (0, 0),
            "physical wheel routing allocates nothing"
        );
        let (copied, control) = route::<true>(&events);
        assert_eq!(copied, expected, "control preserves every physical effect");
        assert_eq!(control.0, count as u64);
        assert!(
            std::panic::catch_unwind(|| {
                assert_eq!(control, (0, 0), "physical wheel routing allocates nothing");
            })
            .is_err()
        );
        std::fs::write(
            report.with_extension(format!("wheel{count}-done")),
            format!(
                "notches={count} effects={} allocations={allocation:?} control={control:?}",
                effects.len()
            ),
        )?;
    }
    Ok(())
}
