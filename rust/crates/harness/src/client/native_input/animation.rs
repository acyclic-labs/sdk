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
