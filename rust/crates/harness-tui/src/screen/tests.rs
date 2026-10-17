use super::*;
use ratatui::{Terminal, backend::TestBackend};

fn model<const SCAN: bool, const UNGATED: bool>(length: usize, hidden: u8) {
    let transcript = vec![Line::raw("retained history"); length];
    let help = [Line::raw("Help")];
    let input = PromptInput {
        busy: true,
        ..PromptInput::default()
    };
    let screen = Screen {
        transcript: &transcript,
        feedback: hidden == 1,
        overlay: if hidden == 2 { &help } else { &[] },
        ..Screen::default()
    };
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    ROW_WORK.with(|work| work.set(0));
    crate::thinking::WORK.with(|work| work.set(0));
    let mut height = 0;
    terminal
        .draw(|frame| {
            height = usize::from(screen.areas(frame.area(), &input).transcript.height);
            let scroll = screen
                .tail_scroll(frame.area(), &input)
                .saturating_sub(usize::from(hidden == 3));
            if SCAN {
                for _ in &transcript {
                    ROW_WORK.with(|work| work.set(work.get() + 1));
                }
            }
            if UNGATED && hidden != 0 {
                std::hint::black_box(crate::Thinking::new(&input.theme, 0).line());
            }
            screen.render(frame, &input, &mut Viewport::default(), scroll, 0);
        })
        .unwrap();
    assert_eq!(
        crate::thinking::WORK.with(|work| work.get()),
        usize::from(hidden == 0),
        "hidden activity must construct no spans"
    );
    assert_eq!(
        ROW_WORK.with(|work| work.get()),
        if hidden == 1 || hidden == 2 {
            0
        } else {
            height
        },
        "frame must touch only visible rows"
    );
}

#[test]
fn tail_animation_and_hidden_views_have_exact_viewport_work() {
    for length in [100, 10000] {
        for hidden in 0..4 {
            model::<false, false>(length, hidden);
        }
    }
}

#[test]
#[should_panic(expected = "hidden activity must construct no spans")]
fn hidden_span_construction_negative_control() {
    model::<false, true>(100, 1);
}

#[test]
#[should_panic(expected = "frame must touch only visible rows")]
fn archive_scan_negative_control() {
    model::<true, false>(10000, 0);
}
