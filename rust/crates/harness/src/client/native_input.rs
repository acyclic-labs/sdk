//! Native input adapters; JSONL decoding and settlement stay in the shared runner.
use crate::{Error, Result};
use futures::{StreamExt, stream::BoxStream};

/// Bounded JSONL stdin, independently scheduled from replay/follow.
pub fn stdin_jsonl() -> BoxStream<'static, Result<String>> {
    Box::pin(
        tokio_util::codec::FramedRead::new(
            tokio::io::stdin(),
            tokio_util::codec::LinesCodec::new_with_max_length(65_536),
        )
        .map(|line| line.map_err(|error| Error::Invalid(error.to_string()))),
    )
}

/// Multiplex terminal input with authorized file publications.
#[cfg(all(feature = "tui", feature = "filesystem-local"))]
pub async fn next_file_tui_event(
    client: &mut super::TaskFileClient,
    input: &mut (impl futures::Stream<Item = Result<crate::tui::key_event::InputEvent>> + Unpin),
    completions: &mut crate::tui_files::CompletionFiles,
    prompt: &mut crate::tui::PromptInput,
    commands: &mut super::CommandSession<acyclic_stream::LocalStream>,
    documents: &mut crate::task_volume::ChangeStream,
) -> Result<Option<crate::tui::key_event::InputEvent>> {
    tokio::select! {
        event = input.next() => event.transpose()?.map(Some).ok_or_else(|| Error::Storage("terminal input closed".into())),
        change = client.next_change() => { change?; Ok(None) },
        update = documents.next() => {
            client.apply_change(update.ok_or_else(|| Error::Conflict("document follow ended; reopen snapshots".into()))??)?;
            Ok(None)
        },
        change = commands.observe_change() => { change?; Ok(None) },
        change = completions.read_change() => {
            completions.apply_change(change?, prompt)?;
            Ok(None)
        },
    }
}

/// Follow controlling-terminal input through the decoder retained by negotiation.
/// Terminal waits use Tokio's blocking executor, never the bounded filesystem
/// I/O service needed by the active task. Closing output wakes the retained read.
#[cfg(feature = "tui")]
pub fn terminal_events(
    tty: crate::tui::term::Tty,
) -> acyclic_stream::BoxProviderStream<'static, Result<crate::tui::key_event::InputEvent>> {
    Box::pin(futures::stream::try_unfold(
        (tty, std::collections::VecDeque::new()),
        |(mut tty, mut pending)| async move {
            loop {
                if let Some(event) = pending.pop_front() {
                    return Ok(Some((event, (tty, pending))));
                }
                let completion = tokio::task::spawn_blocking(move || {
                    let events = tty.read_events(
                        std::time::Instant::now() + std::time::Duration::from_secs(86_400),
                    );
                    (tty, events)
                });
                let (reader, events) = completion
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                tty = reader;
                pending.extend(events.map_err(|error| Error::Storage(error.to_string()))?);
            }
        },
    ))
}
