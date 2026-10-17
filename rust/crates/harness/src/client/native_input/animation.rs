use super::*;
use futures::FutureExt as _;

async fn deadline_model<const ENABLED: bool>() {
    for busy in [false, true] {
        let mut frame = Box::pin(animation_frame::<ENABLED>(busy));
        assert!(frame.as_mut().now_or_never().is_none());
        tokio::time::advance(std::time::Duration::from_millis(
            crate::tui::Thinking::FRAME_MS - 1,
        ))
        .await;
        assert!(
            frame.as_mut().now_or_never().is_none(),
            "frame woke before its deadline"
        );
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        assert_eq!(
            frame.as_mut().now_or_never().is_some(),
            busy,
            "busy frame must wake at its deadline"
        );
        if !busy {
            tokio::time::advance(std::time::Duration::from_secs(3600)).await;
            assert!(
                frame.now_or_never().is_none(),
                "idle input must not animate"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn only_busy_tasks_schedule_animation_at_the_frame_deadline() {
    deadline_model::<true>().await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "busy frame must wake at its deadline")]
async fn omitted_animation_wakeup_is_detected_negative_control() {
    deadline_model::<false>().await;
}

async fn visibility_model<const IGNORE_VISIBILITY: bool, const IGNORE_MOTION: bool>() {
    use crate::tui::{
        PromptInput, Screen, Viewport,
        ratatui::{Terminal, backend::TestBackend, text::Line},
    };
    let history = vec![Line::from("retained"); 40];
    let help = vec![Line::from("Help")];
    for (feedback, overlay, at_tail, reduced) in [
        (false, false, true, false),
        (true, false, true, false),
        (false, true, true, false),
        (false, false, false, false),
        (false, false, true, true),
    ] {
        let mut input = PromptInput::default();
        input.busy = true;
        let mut settings = *input.editor.settings();
        settings.reduced_motion = reduced;
        input.set_settings(settings);
        let screen = Screen {
            transcript: &history,
            feedback,
            overlay: if overlay { &help } else { &[] },
            ..Screen::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
        let mut animate = false;
        terminal
            .draw(|frame| {
                let scroll = if at_tail {
                    screen.tail_scroll(frame.area(), &input)
                } else {
                    0
                };
                let visible = screen
                    .thinking_line(
                        &input,
                        screen.areas(frame.area(), &input).transcript,
                        scroll,
                        0,
                    )
                    .is_some();
                assert_eq!(visible, !feedback && !overlay && at_tail);
                animate = screen
                    .render(frame, &input, &mut Viewport::default(), scroll, 0)
                    .animation;
            })
            .unwrap();
        let animate = animate || (IGNORE_VISIBILITY && !reduced) || (IGNORE_MOTION && reduced);
        let mut timer = Box::pin(animation_frame::<true>(animate));
        assert!(timer.as_mut().now_or_never().is_none());
        tokio::time::advance(std::time::Duration::from_secs(3600)).await;
        assert_eq!(
            timer.now_or_never().is_some(),
            !feedback && !overlay && at_tail && !reduced,
            "hidden or reduced-motion state must not schedule frames"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn composed_visibility_and_motion_gate_animation() {
    visibility_model::<false, false>().await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "hidden or reduced-motion state must not schedule frames")]
async fn hidden_timer_negative_control() {
    visibility_model::<true, false>().await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "hidden or reduced-motion state must not schedule frames")]
async fn reduced_motion_timer_negative_control() {
    visibility_model::<false, true>().await;
}
