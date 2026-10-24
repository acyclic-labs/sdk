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
pub async fn next_file_tui_event<P: acyclic_stream::StreamProvider + ?Sized>(
    client: &mut super::TaskFileClient,
    input: &mut (impl futures::Stream<Item = Result<InputBatch>> + Unpin),
    completions: &mut crate::tui_files::CompletionFiles,
    prompt: &mut crate::tui::PromptInput,
    commands: &mut super::CommandSession<P>,
    animation: bool,
    renderer: &mut crate::tui::ClientRenderer,
    documents: &mut super::DocumentFiles,
) -> Result<Option<InputBatch>> {
    next_event::<P, false, true>(
        client,
        input,
        completions,
        prompt,
        commands,
        animation,
        renderer,
        documents,
    )
    .await
}

#[cfg(all(feature = "tui", feature = "filesystem-local"))]
async fn next_event<
    P: acyclic_stream::StreamProvider + ?Sized,
    const REDRAW_CHECKPOINTS: bool,
    const RETAIN: bool,
>(
    client: &mut super::TaskFileClient,
    input: &mut (impl futures::Stream<Item = Result<InputBatch>> + Unpin),
    completions: &mut crate::tui_files::CompletionFiles,
    prompt: &mut crate::tui::PromptInput,
    commands: &mut super::CommandSession<P>,
    animation: bool,
    renderer: &mut crate::tui::ClientRenderer,
    documents: &mut super::DocumentFiles,
) -> Result<Option<InputBatch>> {
    let animation = animation_frame::<true>(animation);
    tokio::pin!(animation);
    let mut animated = false;
    loop {
        let revision = client.presentation_revision();
        let deadline = renderer.frame_deadline();
        tokio::select! {
            biased;
            event = input.next() => return event.transpose()?.map(Some).ok_or_else(|| Error::Storage("terminal input closed".into())),
            _ = redraw(deadline) => {
                renderer.frame_ready();
                return Ok(None);
            },
            _ = &mut animation, if !animated => {
                animated = true;
                renderer.request_frame();
            },
            _ = renderer.search_progress() => renderer.request_frame(),
            change = client.next_change() => {
                change?;
                if REDRAW_CHECKPOINTS || client.presentation_revision() != revision { renderer.request_frame(); }
            },
            update = documents.read_change() => {
                client.apply_document_change(documents, update?)?;
                if REDRAW_CHECKPOINTS || client.presentation_revision() != revision { renderer.request_frame(); }
            },
            change = commands.observe_change_visible::<RETAIN>() => {
                if change? || REDRAW_CHECKPOINTS { renderer.request_frame(); }
            },
            change = completions.read_change() => {
                let change = change?;
                let visible = !change.is_checkpoint();
                completions.apply_change(change, prompt)?;
                if REDRAW_CHECKPOINTS || visible { renderer.request_frame(); }
            },
        }
    }
}

#[cfg(all(feature = "tui", feature = "filesystem-local"))]
async fn redraw(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) if deadline > tokio::time::Instant::now() => {
            tokio::time::sleep_until(deadline).await;
        }
        Some(_) => (),
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[cfg(all(
    feature = "tui",
    feature = "filesystem-local",
    not(acyclic_harness_loom)
))]
#[path = "native_input/pacing.rs"]
mod pacing;

#[cfg(feature = "tui")]
async fn animation_frame<const ENABLED: bool>(busy: bool) {
    if !ENABLED || !busy {
        return std::future::pending().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(
        crate::tui::Thinking::FRAME_MS,
    ))
    .await;
}

#[cfg(test)]
#[cfg(all(feature = "tui", not(acyclic_harness_loom)))]
#[path = "native_input/animation.rs"]
mod animation;

/// The events decoded from one terminal read, in order.
#[cfg(feature = "tui")]
pub type InputBatch = std::collections::VecDeque<crate::tui::key_event::InputEvent>;

/// Follow controlling-terminal input through the decoder retained by negotiation,
/// one decoded read per item, so read boundaries survive to the caller.
/// Terminal waits use Tokio's blocking executor, never the bounded filesystem
/// I/O service needed by the active task. Closing output wakes the retained read.
#[cfg(feature = "tui")]
pub fn terminal_events(
    tty: crate::tui::term::Tty,
) -> acyclic_stream::BoxProviderStream<'static, Result<InputBatch>> {
    Box::pin(futures::stream::try_unfold(tty, |mut tty| async move {
        loop {
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
            let batch: InputBatch = events
                .map_err(|error| Error::Storage(error.to_string()))?
                .into_iter()
                .collect();
            if !batch.is_empty() {
                return Ok(Some((batch, tty)));
            }
        }
    }))
}

#[cfg(all(
    test,
    feature = "tui",
    feature = "filesystem-local",
    not(acyclic_harness_loom)
))]
#[path = "native_input/multiplexer.rs"]
mod multiplexer;

#[cfg(all(
    test,
    feature = "tui",
    feature = "filesystem-local",
    not(acyclic_harness_loom)
))]
#[path = "native_input/cancellation.rs"]
mod cancellation;
