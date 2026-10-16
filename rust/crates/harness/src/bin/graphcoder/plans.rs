//! Native presentation binding of the authenticated private-file query.
use crate::{
    native::{NativeSession, storage},
    terminal,
};
use acyclic_harness::{
    Error, Result,
    client::{
        ClientSession, CommandCatalog, CommandResolution, FileClientState, HarnessClient, PlanPanel,
    },
    filesystem::FilesystemContentVerifier,
    task_volume::{ChangeStream, Query},
    tui::ratatui::{buffer::Buffer, layout::Rect, widgets::Widget},
};
use futures::StreamExt as _;
use std::{io::Write, sync::Arc};

pub(super) struct Plans {
    client: HarnessClient<FileClientState>,
    changes: ChangeStream,
}

impl Plans {
    pub(super) async fn open(native: Arc<NativeSession>) -> Result<Self> {
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
        Ok(Self { client, changes })
    }

    pub(super) async fn next(&mut self) -> Result<bool> {
        let change = self
            .changes
            .next()
            .await
            .ok_or_else(|| Error::Conflict("plan query ended; reopen the view".into()))?
            .map_err(|error| Error::Storage(error.to_string()))?;
        self.client.query_change(change)
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
