use super::*;

/// Immutable inbox admission, including the runtime's cancellation distinction.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub(in super::super) struct Admitted {
    pub event: Event,
    pub cancellation: bool,
    pub sequence: u64,
}

impl<P: StreamProvider> Journal<P> {
    fn operation_path(&self, event: &Event) -> Result<StreamPath> {
        StreamPath::new(format!(
            "{}/operations/{}",
            self.path.as_str(),
            event.operation_id
        ))
        .map_err(|error| stream_error(&error))
    }

    fn key_path(&self, event: &Event) -> Result<StreamPath> {
        let bytes =
            serde_json::to_vec(&event.idempotency_key).map_err(|error| codec_error(&error))?;
        StreamPath::new(format!(
            "{}/keys/{}",
            self.path.as_str(),
            blake3::hash(&bytes).to_hex()
        ))
        .map_err(|error| stream_error(&error))
    }

    async fn indexed(&self, path: StreamPath) -> Result<Option<(Admitted, CommitId)>> {
        let mut records = match self
            .stream
            .read(ReadRequest {
                path,
                from: 0,
                limit: 1,
            })
            .await
        {
            Ok(records) => records,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(stream_error(&error)),
        };
        let record = records
            .try_next()
            .await
            .map_err(|error| stream_error(&error))?
            .ok_or_else(|| Error::Storage("actor admission index is empty".into()))?;
        Ok(Some((
            serde_json::from_slice(&record.value).map_err(|error| codec_error(&error))?,
            record.commit_id,
        )))
    }

    pub async fn admit_event(&self, event: Event, cancellation: bool) -> Result<Admitted> {
        for path in [self.operation_path(&event)?, self.key_path(&event)?] {
            if let Some((existing, _)) = self.indexed(path).await? {
                if existing.event != event || existing.cancellation != cancellation {
                    return Err(Error::Conflict(
                        "actor admission retry identity is already bound".into(),
                    ));
                }
                return Ok(existing);
            }
        }
        let current = self.load().await?;
        self.require_owner(&current)?;
        if current.last.cancelling {
            return Err(Error::Conflict(
                "actor cancellation has fenced admission".into(),
            ));
        }
        let tail = match self.stream.tail(self.inbox.clone()).await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(stream_error(&error)),
        };
        let admitted = Admitted {
            event,
            cancellation,
            sequence: tail,
        };
        let value =
            Bytes::from(serde_json::to_vec(&admitted).map_err(|error| codec_error(&error))?);
        let operation = self.operation_path(&admitted.event)?;
        let key = self.key_path(&admitted.event)?;
        let conditions = vec![
            CommitCondition::Head {
                path: self.path.clone(),
                expected: current.tail,
                commit_id: current.head,
            },
            if tail == 0 {
                CommitCondition::Absent {
                    path: self.inbox.clone(),
                }
            } else {
                CommitCondition::Tail {
                    path: self.inbox.clone(),
                    expected: tail,
                }
            },
            CommitCondition::Absent {
                path: operation.clone(),
            },
            CommitCondition::Absent { path: key.clone() },
        ];
        let mut mutations: Vec<_> = [self.inbox.clone(), operation, key]
            .into_iter()
            .map(|path| CommitMutation::Append {
                path,
                records: vec![value.clone().into()],
            })
            .collect();
        if cancellation {
            let mut requested = current.last;
            requested.bootstrap = None;
            requested.workflow = None;
            requested.event = Some(admitted.event.clone());
            requested.cancelling = true;
            // An explicit stop closes earlier unconsumed inputs as well. The
            // inbox append and this cursor must never survive independently.
            requested.snapshot.consumed = tail
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("actor inbox cursor overflow".into()))?;
            mutations.push(CommitMutation::Append {
                path: self.path.clone(),
                records: vec![
                    Bytes::from(
                        serde_json::to_vec(&requested).map_err(|error| codec_error(&error))?,
                    )
                    .into(),
                ],
            });
        }
        self.commit_transaction(conditions, mutations).await?;
        Ok(admitted)
    }

    pub async fn next_input(&self, sequence: u64) -> Result<Option<Admitted>> {
        let mut records = match self
            .stream
            .read(ReadRequest {
                path: self.inbox.clone(),
                from: sequence,
                limit: 1,
            })
            .await
        {
            Ok(records) => records,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(stream_error(&error)),
        };
        let Some(record) = records
            .try_next()
            .await
            .map_err(|error| stream_error(&error))?
        else {
            return Ok(None);
        };
        let admitted: Admitted =
            serde_json::from_slice(&record.value).map_err(|error| codec_error(&error))?;
        if record.sequence != sequence || admitted.sequence != sequence {
            return Err(Error::Storage(
                "actor inbox admission cursor changed".into(),
            ));
        }
        Ok(Some(admitted))
    }

    pub async fn input_condition(
        &self,
        event: &Event,
        cancellation: bool,
        consumed: u64,
    ) -> Result<CommitCondition> {
        let path = self.operation_path(event)?;
        let (admitted, commit_id) = self
            .indexed(path.clone())
            .await?
            .ok_or_else(|| Error::Conflict("actor event was not admitted".into()))?;
        if admitted.event != *event
            || admitted.cancellation != cancellation
            || admitted.sequence != consumed
        {
            return Err(Error::Conflict(
                "actor input is not the next admitted event".into(),
            ));
        }
        Ok(CommitCondition::Head {
            path,
            expected: 1,
            commit_id,
        })
    }
}
