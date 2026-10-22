fn parse_string(value: &Value) -> Result<&str, StreamError> {
    value.as_str().ok_or(StreamError::Unavailable)
}
fn parse_u64(value: &Value) -> Result<u64, StreamError> {
    parse_string(value)?
        .parse()
        .map_err(|_| StreamError::Unavailable)
}
fn parse_path(value: &Value) -> Result<StreamPath, StreamError> {
    StreamPath::new(parse_string(value)?).map_err(|_| StreamError::Unavailable)
}
fn parse_bytes(value: &Value) -> Result<Bytes, StreamError> {
    base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        parse_string(value)?,
    )
    .map(Bytes::from)
    .map_err(|_| StreamError::Unavailable)
}
fn parse_id(value: &Value) -> Result<CommitId, StreamError> {
    Ok(CommitId::from_bytes(
        parse_bytes(value)?
            .as_ref()
            .try_into()
            .map_err(|_| StreamError::Unavailable)?,
    ))
}
fn parse_records(value: &Value) -> Result<Vec<Record>, StreamError> {
    value
        .as_array()
        .ok_or(StreamError::Unavailable)?
        .iter()
        .map(|value| {
            let bytes = parse_bytes(field(value, "value")?)?;
            let keys = field(value, "keys")?
                .as_array()
                .ok_or(StreamError::Unavailable)?;
            let attributes = field(value, "attributes")?
                .as_array()
                .ok_or(StreamError::Unavailable)?;
            if keys.len() > crate::MAX_RECORD_KEYS
                || attributes.len() > crate::MAX_RECORD_ATTRIBUTES
            {
                return Err(StreamError::Unavailable);
            }
            let headers = crate::RecordHeaders::new(
                keys.iter()
                    .map(parse_bytes)
                    .collect::<Result<Vec<_>, _>>()?,
                attributes
                    .iter()
                    .map(|attribute| {
                        Ok(crate::Attribute {
                            name: parse_string(field(attribute, "name")?)?.to_owned(),
                            value: parse_bytes(field(attribute, "value")?)?,
                        })
                    })
                    .collect::<Result<Vec<_>, StreamError>>()?,
            )
            .map_err(|_| StreamError::Unavailable)?;
            if bytes.len().saturating_add(headers.payload_bytes()) > MAX_RECORD_BYTES {
                return Err(StreamError::Unavailable);
            }
            Ok(Record {
                sequence: parse_u64(field(value, "sequence")?)?,
                value: bytes.into(),
                commit_id: parse_id(field(value, "commitId")?)?,
                committed_at_micros: parse_u64(field(value, "committedAtMicros")?)?,
                headers,
            })
        })
        .collect()
}
fn parse_children(value: &Value) -> Result<Vec<Child>, StreamError> {
    value
        .as_array()
        .ok_or(StreamError::Unavailable)?
        .iter()
        .map(|value| {
            Ok(Child {
                path: parse_path(field(value, "path")?)?,
            })
        })
        .collect()
}
fn parse_append(value: &Value) -> Result<AppendOutcome, StreamError> {
    if value["ok"] == true {
        Ok(AppendOutcome::Committed(AppendReceipt {
            start: parse_u64(field(value, "start")?)?,
            end: parse_u64(field(value, "end")?)?,
            tail: parse_u64(field(value, "tail")?)?,
            commit_id: parse_id(field(value, "commitId")?)?,
        }))
    } else {
        Ok(AppendOutcome::TailConflict {
            actual_tail: parse_u64(field(value, "actualTail")?)?,
        })
    }
}
fn parse_fork(value: &Value) -> Result<ForkReceipt, StreamError> {
    Ok(ForkReceipt {
        source: parse_path(field(value, "source")?)?,
        destination: parse_path(field(value, "destination")?)?,
        forked_at: parse_u64(field(value, "forkedAt")?)?,
        tail: parse_u64(field(value, "tail")?)?,
        commit_id: parse_id(field(value, "commitId")?)?,
    })
}
fn parse_envelope(value: &Value) -> Result<CommittedEnvelope, StreamError> {
    let mutations = value["mutations"]
        .as_array()
        .ok_or(StreamError::Unavailable)?;
    if mutations.len() > MAX_ITEMS {
        return Err(StreamError::Unavailable);
    }
    let mutations = mutations
        .iter()
        .map(|value| {
            let records = value
                .get("records")
                .map(parse_records)
                .transpose()?
                .unwrap_or_default();
            if records.len() > MAX_ITEMS {
                return Err(StreamError::Unavailable);
            }
            match parse_string(field(value, "type")?)? {
                "append" => Ok(CommittedMutation::Append(CommittedAppend {
                    path: parse_path(field(value, "path")?)?,
                    start: parse_u64(field(value, "start")?)?,
                    end: parse_u64(field(value, "end")?)?,
                    tail: parse_u64(field(value, "tail")?)?,
                    records,
                })),
                "fork" => Ok(CommittedMutation::Fork(CommittedFork {
                    source: parse_path(field(value, "source")?)?,
                    destination: parse_path(field(value, "destination")?)?,
                    forked_at: parse_u64(field(value, "forkedAt")?)?,
                    tail: parse_u64(field(value, "tail")?)?,
                    records,
                })),
                _ => Err(StreamError::Unavailable),
            }
        })
        .collect::<Result<Vec<_>, StreamError>>()?;
    let envelope = CommittedEnvelope {
        commit_id: parse_id(field(value, "commitId")?)?,
        mutations,
    };
    wire_codec::envelope_from_wire(wire_codec::envelope_to_wire(envelope))
}
fn contract_error(code: &str) -> StreamError {
    match code {
        "stream_not_found" | "commit_not_found" => StreamError::NotFound,
        "destination_exists" => StreamError::AlreadyExists,
        "capacity_exhausted" => StreamError::Capacity,
        _ => StreamError::from_code(code).unwrap_or(StreamError::Unavailable),
    }
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, StreamError> {
    value.get(name).ok_or(StreamError::Unavailable)
}
