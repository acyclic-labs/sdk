//! A bounded asynchronous view of the terminal's one native blocking reader.
use super::Tty;
use crate::key_event::InputEvent;
use futures::{SinkExt as _, StreamExt as _, stream::LocalBoxStream};
use std::io;

/// Read decoded batches on a dedicated thread, keeping the runtime's blocking
/// worker available for file work. One outstanding batch applies backpressure.
/// Dropping the output half restores the terminal and wakes this native read;
/// dropping the stream releases a sender waiting for consumer credit.
pub fn input_events(
    mut tty: Tty,
) -> io::Result<LocalBoxStream<'static, io::Result<Vec<InputEvent>>>> {
    // futures mpsc reserves one slot for this sole sender.
    let (mut sender, receiver) = futures::channel::mpsc::channel(0);
    std::thread::Builder::new()
        .name("terminal-input".into())
        .spawn(move || {
            loop {
                let events = tty.read_events(None);
                let failed = events.is_err();
                if matches!(&events, Ok(events) if events.is_empty()) {
                    continue;
                }
                if futures::executor::block_on(sender.send(events)).is_err() || failed {
                    return;
                }
            }
        })?;
    Ok(receiver.boxed_local())
}
