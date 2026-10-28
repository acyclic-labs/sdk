use super::*;
use crate::replicated::{AppendIf, CommandJournal, JournalEntry, JournalFrame, JournalPage};
use tokio_postgres::Transaction;

pub(super) enum Storage<'a> {
    Connection(&'a mut Client),
    Fenced(Option<Transaction<'a>>),
}

pub(super) struct PgJournal<'a> {
    pub(super) storage: Storage<'a>,
    pub(super) publish: &'a Statement,
    pub(super) read_fenced: &'a Statement,
    pub(super) namespace: &'a str,
    pub(super) wakes: &'a mut watch::Receiver<()>,
    pub(super) at_millis: u64,
    pub(super) head: u64,
}

impl CommandJournal for PgJournal<'_> {
    async fn append_if(
        &mut self,
        expected: u64,
        frames: &[JournalFrame<'_>],
    ) -> Result<AppendIf, StreamError> {
        let transaction = match &mut self.storage {
            Storage::Fenced(transaction) => transaction.take().ok_or(StreamError::Unavailable)?,
            Storage::Connection(client) => client
                .build_transaction()
                .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
                .start()
                .await
                .map_err(unavailable)?,
        };
        append(transaction, self.publish, self.namespace, expected, frames).await
    }

    async fn read(&mut self, from: u64, page: usize) -> Result<JournalPage, StreamError> {
        // Mark before the SQL snapshot. A racing commit remains pending in the
        // owned event returned by changed(), without retaining this connection.
        self.wakes.borrow_and_update();
        let from = i64::try_from(from).map_err(unavailable)?;
        let page = i64::try_from(page).map_err(unavailable)?;
        let mut entries = Vec::new();
        let (head, at_millis) = match &self.storage {
            Storage::Fenced(transaction) => {
                // The function acquires the namespace fence, then reads its
                // page/head and samples the clock in this one server call.
                let rows = transaction
                    .as_ref()
                    .ok_or(StreamError::Unavailable)?
                    .query(self.read_fenced, &[&self.namespace, &from, &page])
                    .await
                    .map_err(unavailable)?;
                let mut captured = None;
                for row in rows {
                    let sequence = u64::try_from(row.get::<_, i64>(0)).map_err(unavailable)?;
                    let at = u64::try_from(row.get::<_, i64>(1)).map_err(unavailable)?;
                    match row.get::<_, Option<Vec<u8>>>(2) {
                        Some(command) if captured.is_none() => entries.push(JournalEntry {
                            sequence,
                            at_millis: at,
                            command: command.into(),
                        }),
                        None if captured.is_none() => captured = Some((sequence, at)),
                        _ => return Err(StreamError::InvalidResponse),
                    }
                }
                captured.ok_or(StreamError::InvalidResponse)?
            }
            Storage::Connection(client) => {
                let rows = client.query(
                    "SELECT j.revision,j.at_millis,j.command,h.head,h.at_millis FROM \
                     (SELECT COALESCE(max(revision),0) AS head, \
                     floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint AS at_millis \
                     FROM acyclic_stream_journal WHERE namespace=$1) h \
                     LEFT JOIN LATERAL (SELECT revision,at_millis,command FROM acyclic_stream_journal \
                     WHERE namespace=$1 AND revision>$2 ORDER BY revision LIMIT $3) j ON true \
                     ORDER BY j.revision", &[&self.namespace, &from, &page],
                ).await.map_err(unavailable)?;
                let first = rows.first().ok_or(StreamError::InvalidResponse)?;
                let captured = (
                    u64::try_from(first.get::<_, i64>(3)).map_err(unavailable)?,
                    u64::try_from(first.get::<_, i64>(4)).map_err(unavailable)?,
                );
                for row in rows {
                    if let Some(sequence) = row.get::<_, Option<i64>>(0) {
                        entries.push(JournalEntry {
                            sequence: u64::try_from(sequence).map_err(unavailable)?,
                            at_millis: u64::try_from(row.get::<_, i64>(1)).map_err(unavailable)?,
                            command: row.get::<_, Vec<u8>>(2).into(),
                        });
                    }
                }
                captured
            }
        };
        self.at_millis = at_millis;
        self.head = head;
        Ok(JournalPage {
            entries,
            head,
            at_millis,
        })
    }

    fn changed(&mut self, after: u64) -> BoxProviderFuture<'static, Result<(), StreamError>> {
        let head = self.head;
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
    transaction: Transaction<'_>,
    publish: &Statement,
    namespace: &str,
    expected: u64,
    frames: &[JournalFrame<'_>],
) -> Result<AppendIf, StreamError> {
    if frames.is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    let mut expected = i64::try_from(expected).map_err(unavailable)?;
    let at_millis = frames.last().ok_or(StreamError::InvalidArgument)?.at_millis;
    for frame in frames {
        let millis = i64::try_from(frame.at_millis).map_err(unavailable)?;
        let deadline = frame
            .deadline
            .map(|value| i64::try_from(value).unwrap_or(i64::MAX));
        let bytes = frame.command.encode_to_vec();
        let row = transaction
            .query_one(
                publish,
                &[&namespace, &expected, &bytes, &millis, &deadline],
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
        let sequence: i64 = row.get(0);
        if !row.get::<_, bool>(2) {
            return Ok(AppendIf::Stale {
                head: u64::try_from(sequence).map_err(unavailable)?,
            });
        }
        expected = sequence;
    }
    let sequence = u64::try_from(expected).map_err(unavailable)?;
    transaction.commit().await.map_err(unavailable)?;
    Ok(AppendIf::Accepted {
        sequence,
        at_millis,
    })
}
