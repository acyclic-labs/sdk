use super::*;
use crate::replicated::{AppendIf, CommandJournal, JournalEntry, JournalFrame, JournalPage};
pub(super) struct PgJournal<'a> {
    pub(super) client: &'a mut Client,
    pub(super) publish: &'a Statement,
    pub(super) read: &'a Statement,
    pub(super) namespace: &'a str,
    pub(super) wakes: &'a mut watch::Receiver<()>,
    pub(super) head: &'a mut u64,
}

impl CommandJournal for PgJournal<'_> {
    async fn append_if(
        &mut self,
        expected: u64,
        frames: &[JournalFrame<'_>],
    ) -> Result<AppendIf, StreamError> {
        append(self.client, self.publish, self.namespace, expected, frames).await
    }

    async fn read(&mut self, from: u64, page: usize) -> Result<JournalPage, StreamError> {
        // Mark before the SQL snapshot. A racing commit remains pending in the
        // owned event returned by changed(), without retaining this connection.
        self.wakes.borrow_and_update();
        let from = i64::try_from(from).map_err(unavailable)?;
        let page = i64::try_from(page).map_err(unavailable)?;
        let mut entries = Vec::new();
        let rows = self
            .client
            .query(self.read, &[&self.namespace, &from, &page])
            .await
            .map_err(unavailable)?;
        let first = rows.first().ok_or(StreamError::InvalidResponse)?;
        let head = u64::try_from(first.get::<_, i64>(3)).map_err(unavailable)?;
        let at_millis = u64::try_from(first.get::<_, i64>(4)).map_err(unavailable)?;
        for row in rows {
            if let Some(sequence) = row.get::<_, Option<i64>>(0) {
                entries.push(JournalEntry {
                    sequence: u64::try_from(sequence).map_err(unavailable)?,
                    at_millis: u64::try_from(row.get::<_, i64>(1)).map_err(unavailable)?,
                    command: row.get::<_, Vec<u8>>(2).into(),
                });
            }
        }
        *self.head = head;
        Ok(JournalPage {
            entries,
            head,
            at_millis,
        })
    }

    fn changed(&mut self, after: u64) -> BoxProviderFuture<'static, Result<(), StreamError>> {
        let head = *self.head;
        let mut wakes = self.wakes.clone();
        Box::pin(async move {
            if head > after {
                return Ok(());
            }
            wakes.changed().await.map_err(unavailable)
        })
    }
}

async fn append(
    client: &Client,
    publish: &Statement,
    namespace: &str,
    expected: u64,
    frames: &[JournalFrame<'_>],
) -> Result<AppendIf, StreamError> {
    if frames.is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    let expected = i64::try_from(expected).map_err(unavailable)?;
    let at_millis = frames.last().ok_or(StreamError::InvalidArgument)?.at_millis;
    // Encode each canonical command once into its storage array element.
    let bytes: Vec<Vec<u8>> = frames
        .iter()
        .map(|frame| frame.command.encode_to_vec())
        .collect();
    let millis: Vec<i64> = frames
        .iter()
        .map(|frame| i64::try_from(frame.at_millis).map_err(unavailable))
        .collect::<Result<_, _>>()?;
    let deadlines: Vec<Option<i64>> = frames
        .iter()
        .map(|frame| {
            frame
                .deadline
                .map(|value| i64::try_from(value).unwrap_or(i64::MAX))
        })
        .collect();
    let row = client
        .query_one(
            publish,
            &[&namespace, &expected, &bytes, &millis, &deadlines],
        )
        .await
        .map_err(|error| {
            if error
                .as_db_error()
                .is_some_and(|error| error.code().code() == "PST01")
            {
                StreamError::DeadlineElapsed
            } else {
                unavailable(error)
            }
        })?;
    let sequence = u64::try_from(row.get::<_, i64>(0)).map_err(unavailable)?;
    if !row.get::<_, bool>(2) {
        return Ok(AppendIf::Stale { head: sequence });
    }
    Ok(AppendIf::Accepted {
        sequence,
        at_millis,
    })
}
