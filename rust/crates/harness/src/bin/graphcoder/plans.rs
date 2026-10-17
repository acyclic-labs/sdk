//! Native presentation binding of the authenticated private-file query.
use crate::{
    native::{NativeSession, storage},
    terminal,
};
use acyclic_harness::{
    Error, Result,
    client::{
        ClientViewLimits, ClientViews, CommandCatalog, CommandResolution, PlanPanel,
        QueryViewLimits,
    },
    filesystem::FilesystemContentVerifier,
    task_volume::{Change, Query},
    tui::ratatui::{buffer::Buffer, layout::Rect, widgets::Widget},
};
use futures::StreamExt as _;
use std::{io::Write, sync::Arc};

pub(super) struct Plans {
    views: ClientViews,
}

impl Plans {
    pub(super) async fn open(
        native: Arc<NativeSession>,
    ) -> Result<(
        Self,
        acyclic_stream::BoxProviderStream<'static, Result<Change>>,
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
        let root = acyclic_stream::StreamPath::new(config.owner.authority.stream_path(&mut None)?)
            .map_err(storage)?;
        let mut views = ClientViews::new(
            root,
            ClientViewLimits {
                agents: 0,
                selected: 0,
                overview_values: 0,
                overview_bytes: 0,
                query: QueryViewLimits {
                    files: 0,
                    segments: 0,
                    bytes: 0,
                },
            },
        )?;
        let changes = views
            .select_file_view(
                query,
                &source,
                terminal::MAX_PAGE_EVENTS,
                terminal::MAX_FRAME_BYTES,
            )
            .await?;
        let updates = Box::pin(
            changes
                .map(|change| change.map_err(|error| Error::Storage(error.to_string())))
                .chain(futures::stream::iter([Err(Error::Conflict(
                    "plan query ended; reopen the view".into(),
                ))])),
        );
        Ok((Self { views }, updates))
    }

    pub(super) fn update(&mut self, update: Change) -> Result<bool> {
        let redraw =
            update.file.is_some() || !update.files.is_empty() || !update.removed.is_empty();
        self.views.query_change(update)?;
        Ok(redraw)
    }

    pub(super) fn render(&self, output: &mut impl Write) -> Result<()> {
        let view = self
            .views
            .selections()
            .map(|(_, files)| files)
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
