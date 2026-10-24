//! File-backed project diff; the unfold retains in-flight reads across UI events.
use crate::{Result, local::LocalHarness};
use futures::StreamExt as _;
use std::sync::Arc;

/// Follow immutable project file versions and render only accepted publications.
pub fn local_diff(
    local: Arc<LocalHarness>,
    limits: super::DiffLimits,
) -> acyclic_stream::BoxProviderStream<'static, Result<Vec<crate::tui::ratatui::text::Line<'static>>>>
{
    Box::pin(futures::stream::try_unfold(
        (Some(local), None),
        move |(mut local, mut state)| async move {
            if let Some(local) = local.take() {
                let (reader, volume, mut publications) = local
                    .project_diff(
                        acyclic_fs::performance::BOUNDARY_BUDGET,
                        u32::try_from(limits.changes).map_err(|_| {
                            crate::Error::Invalid("diff change bound exceeds portable count".into())
                        })?,
                    )
                    .await?;
                let snapshot = publications
                    .next()
                    .await
                    .ok_or_else(|| crate::Error::Conflict("diff snapshot missing".into()))??;
                let mut view = super::DiffView::new(volume, snapshot.previous.clone())?;
                view.apply(&snapshot, reader.as_ref(), limits).await?;
                state = Some((reader, publications, view));
            } else if let Some((reader, publications, view)) = &mut state {
                let publication = publications
                    .next()
                    .await
                    .ok_or_else(|| crate::Error::Conflict("diff follow ended".into()))??;
                view.apply(&publication, reader.as_ref(), limits).await?;
            }
            let rows = crate::tui::diff_lines(
                state
                    .as_ref()
                    .ok_or_else(|| crate::Error::Conflict("diff binding missing".into()))?
                    .2
                    .files(),
            );
            Ok(Some((rows, (local, state))))
        },
    ))
}
