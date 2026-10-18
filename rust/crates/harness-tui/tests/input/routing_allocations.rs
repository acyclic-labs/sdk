use acyclic_harness_tui::{
    InputEffect, PromptInput, Viewport, WHEEL_ROWS,
    key_event::{InputEvent, KeyModifiers, MouseEvent, MouseEventKind},
    ratatui::layout::Rect,
};

fn route<const ALLOCATE: bool>(input: &mut PromptInput, n: usize, output: &mut Vec<InputEffect>) {
    output.clear();
    for at in 0..n {
        let event = InputEvent::Mouse(MouseEvent {
            kind: if at % 3 == 0 {
                MouseEventKind::ScrollDown
            } else {
                MouseEventKind::ScrollUp
            },
            column: 1,
            row: 0,
            modifiers: KeyModifiers::NONE,
        });
        if ALLOCATE {
            // Restore the old per-notch Vec while retaining the real router.
            output.extend(
                input
                    .event(event, Rect::new(0, 0, 80, 1), &mut Viewport::default(), 0)
                    .collect::<Vec<_>>(),
            );
        } else {
            output.extend(input.event(event, Rect::new(0, 0, 80, 1), &mut Viewport::default(), 0));
        }
    }
}

fn assert_no_notch_allocations<const ALLOCATE: bool>() {
    for n in [32, 64] {
        let mut input = PromptInput::default();
        let mut output = Vec::with_capacity(n);
        let measured =
            acyclic_test_support::measure(|| route::<ALLOCATE>(&mut input, n, &mut output));
        assert_eq!(
            (measured.count_total, measured.bytes_total),
            (0, 0),
            "wheel routing allocates nothing per notch"
        );
        assert_eq!(output.len(), n);
    }
}

#[test]
fn wheel_routing_is_allocation_free_at_n_and_2n() {
    assert_no_notch_allocations::<false>();
    for n in [32, 64] {
        let [old, new] = acyclic_test_support::differential::pair::<_, _, InputEffect>(
            || (PromptInput::default(), Vec::with_capacity(n)),
            |(mut input, mut output), old| {
                if old {
                    for at in 0..n {
                        output.extend(vec![InputEffect::Scroll(if at % 3 == 0 {
                            -WHEEL_ROWS
                        } else {
                            WHEEL_ROWS
                        })]);
                    }
                } else {
                    route::<false>(&mut input, n, &mut output);
                }
                for effect in &output {
                    acyclic_test_support::differential::record(effect.clone());
                }
                (output, format!("{:?}", input.editor))
            },
        );
        assert_eq!(
            old, new,
            "all effects, editor state and ordered observer trace"
        );
    }
}

#[test]
#[should_panic(expected = "wheel routing allocates nothing per notch")]
fn negative_control_per_notch_vec_fails_the_same_allocation_assertion() {
    assert_no_notch_allocations::<true>();
}
