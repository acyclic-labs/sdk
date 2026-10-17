use super::*;
use ratatui::{Terminal, backend::TestBackend};

fn model<const SCAN: bool, const UNGATED: bool>(length: usize, hidden: u8) {
    let transcript = vec![Line::raw("retained history"); length / 2];
    let tools = vec![Line::raw("retained tool"); length - transcript.len()];
    let help = [Line::raw("Help")];
    let mut input = PromptInput::default();
    input.busy = true;
    if hidden >= 4 {
        input.editor.set_text("@sec", 0);
        input
            .completion
            .set_file("workspace/second", Some(b"source"))
            .unwrap();
        if hidden == 5 {
            let mut settings = *input.editor.settings();
            settings.completion_max_width = 3;
            input.set_settings(settings);
        }
        input.refresh_completions();
    }
    let screen = Screen {
        transcript: &transcript,
        tools: &tools,
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
                for _ in transcript.iter().chain(&tools) {
                    ROW_WORK.with(|work| work.set(work.get() + 1));
                }
            }
            if UNGATED && hidden != 0 {
                std::hint::black_box(crate::Thinking::new(&input.theme, 0).line());
            }
            let areas = screen.render(frame, &input, &mut Viewport::default(), scroll, 0);
            assert_eq!(
                areas.animation,
                hidden == 0 || hidden == 5,
                "occluded activity must schedule no animation"
            );
        })
        .unwrap();
    assert_eq!(
        crate::thinking::WORK.with(|work| work.get()),
        usize::from(hidden == 0 || hidden == 5),
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
        for hidden in 0..6 {
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

#[test]
#[should_panic(expected = "hidden activity must construct no spans")]
fn popup_occlusion_construction_negative_control() {
    model::<false, true>(100, 4);
}
