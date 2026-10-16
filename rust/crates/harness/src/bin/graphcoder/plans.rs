//! Native presentation binding of the authenticated private-file query.
use crate::{
    native::{NativeSession, storage},
    terminal,
};
use acyclic_harness::{
    Error, Result,
    client::{
        ClientSession, CommandCatalog, CommandResolution, FileClientState, HarnessClient,
        PlanPanel, ViewUpdate,
    },
    filesystem::FilesystemContentVerifier,
    task_volume::Query,
    tui::ratatui::{buffer::Buffer, layout::Rect, widgets::Widget},
};
use futures::StreamExt as _;
use std::{io::Write, sync::Arc};

pub(super) struct Plans {
    client: HarnessClient<FileClientState>,
}

impl Plans {
    pub(super) async fn open(
        native: Arc<NativeSession>,
    ) -> Result<(
        Self,
        acyclic_stream::BoxProviderStream<'static, Result<ViewUpdate>>,
    )> {
        let config = native.config();
        let reader = FilesystemContentVerifier::new(
            native.files.clone(),
            &config.owner.verifier()?,
            &config.owner.scope,
            terminal::MAX_FRAME_BYTES as u64,
            None,
        )?;
        let source = reader.file_query(&config.private_volume, config.tools.content_budget)?;
        let CommandResolution::Select { query } = CommandCatalog::builtins()?
            .resolve("/plan", reader.as_ref(), terminal::MAX_FRAME_BYTES as u64)
            .await?
        else {
            return Err(Error::Invalid("plan command did not select files".into()));
        };
        let state = FileClientState::open(&config.stream_root.join("client.jsonl"))?;
        let mut client = HarnessClient::connect(native, ClientSession::open(state)?).await?;
        let changes = client
            .select_file_view(
                query,
                &source,
                terminal::MAX_PAGE_EVENTS,
                terminal::MAX_FRAME_BYTES,
            )
            .await?;
        let updates = Box::pin(
            changes
                .map(|change| {
                    change
                        .map(ViewUpdate::File)
                        .map_err(|error| Error::Storage(error.to_string()))
                })
                .chain(futures::stream::iter([Err(Error::Conflict(
                    "plan query ended; reopen the view".into(),
                ))])),
        );
        Ok((Self { client }, updates))
    }

    pub(super) fn update(&mut self, update: ViewUpdate) -> Result<bool> {
        let redraw = matches!(&update, ViewUpdate::File(change) if change.file.is_some() || !change.files.is_empty() || !change.removed.is_empty());
        self.client.apply_update(update)?;
        Ok(redraw)
    }

    pub(super) fn render(&self, output: &mut impl Write) -> Result<()> {
        let view = self
            .client
            .session()
            .views()
            .queries
            .values()
            .find(|view| view.query() == &Query::Plans)
            .ok_or_else(|| Error::Conflict("plan snapshot is missing".into()))?;
        let area = Rect::new(0, 0, 80, 20);
        let mut buffer = Buffer::empty(area);
        PlanPanel { files: view }.render(area, &mut buffer);
        for y in 0..20 {
            let mut x = 0;
            while x < 80 {
                let symbol = buffer[(x, y)].symbol();
                output.write_all(symbol.as_bytes()).map_err(storage)?;
                x += u16::try_from(
                    acyclic_harness::tui::ratatui::text::Span::raw(symbol)
                        .width()
                        .max(1),
                )
                .map_err(storage)?;
            }
            writeln!(output).map_err(storage)?;
        }
        output.flush().map_err(storage)
    }
}
