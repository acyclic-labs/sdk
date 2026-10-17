use super::*;

pub(in crate::local::children) struct Follow {
    pub(in crate::local::children) cursor: crate::task_volume::Cursor,
    idle: Option<(Query, crate::task_volume::ChangeStream)>,
}
impl Follow {
    pub(in crate::local::children) fn new(cursor: crate::task_volume::Cursor) -> Self {
        Self { cursor, idle: None }
    }
}

// Take the entire stream before polling it. Cancellation drops every pending
// read/permit; only a stream which returned a record can go back in the slot.
// Reusing that idle stream avoids revalidating its accepted anchor per record.
pub(in crate::local::children) async fn next(
    files: &Files,
    query: Query,
    follow: &mut Follow,
) -> Option<crate::task_volume::Result<Change>> {
    let mut changes = match follow.idle.take() {
        Some((previous, changes)) if previous == query => changes,
        _ => match open(files, query.clone(), follow.cursor.clone()).await {
            Ok(changes) => changes,
            Err(error) => return Some(Err(error)),
        },
    };
    let change = changes.next().await;
    if let Some(Ok(change)) = &change {
        follow.cursor = change.cursor.clone();
        follow.idle = Some((query, changes));
    }
    change
}

async fn open(
    files: &Files,
    query: Query,
    cursor: crate::task_volume::Cursor,
) -> crate::task_volume::Result<crate::task_volume::ChangeStream> {
    #[cfg(test)]
    acyclic_test_support::differential::record(("child-follow-open", 1u64));
    files.inner.follow(query, cursor).await
}

// A worker's admission and writes can advance its volume while its local fold
// still names an older cut. Read only lifecycle keys at the new accepted cut;
// never replay retained prompts or retry publication once per historical file.
pub(super) async fn refresh<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    reference: &Component,
    follow: &mut Follow,
    values: &mut BTreeMap<FileKey, FileValue>,
    generation: &mut Generation,
) -> Result<()> {
    let snapshot = files
        .inner
        .child_lifecycle(Some(reference))
        .await
        .map_err(storage)?;
    *follow = Follow::new(snapshot.cursor);
    *generation = snapshot.generation;
    *values = snapshot
        .files
        .into_iter()
        .map(|file| (file.key.clone(), file))
        .collect();
    Ok(())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

#[cfg(all(test, acyclic_harness_loom, acyclic_loom))]
mod models;
