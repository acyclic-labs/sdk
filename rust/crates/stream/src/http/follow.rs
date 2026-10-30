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
// An envelope's records belong to the mutation's path, which spells none.
fn parse_records(value: &Value, owner: &StreamPath) -> Result<Vec<Record>, StreamError> {
    value
        .as_array()
        .ok_or(StreamError::Unavailable)?
        .iter()
        .map(|record| parse_record(record, owner))
        .collect()
}
fn parse_record(value: &Value, owner: &StreamPath) -> Result<Record, StreamError> {
    if value.get("origin").is_some() {
        return Err(StreamError::Unavailable);
    }
    let bytes = parse_bytes(field(value, "value")?)?;
    let keys = field(value, "keys")?
        .as_array()
        .ok_or(StreamError::Unavailable)?;
    let attributes = field(value, "attributes")?
        .as_array()
        .ok_or(StreamError::Unavailable)?;
    if keys.len() > crate::MAX_RECORD_KEYS || attributes.len() > crate::MAX_RECORD_ATTRIBUTES {
        return Err(StreamError::Unavailable);
    }
    let headers = crate::changes::record_headers(
        &bytes,
        keys.iter()
            .map(|key| parse_bytes(key).map(Into::into))
            .collect::<Result<Vec<_>, _>>()?,
        attributes
            .iter()
            .map(|attribute| {
                Ok(crate::Attribute {
                    name: parse_string(field(attribute, "name")?)?.to_owned(),
                    value: parse_bytes(field(attribute, "value")?)?.into(),
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
        origin: owner.clone(),
    })
}
fn parse_append(value: &Value) -> Result<AppendOutcome, StreamError> {
    if value["ok"] == true {
        Ok(AppendOutcome::Committed(AppendReceipt {
            start: parse_u64(field(value, "start")?)?,
            end: parse_u64(field(value, "end")?)?,
            tail: parse_u64(field(value, "tail")?)?,
            commit_id: parse_id(field(value, "commitId")?)?,
            revision: parse_revision(field(value, "revision")?)?,
        }))
    } else {
        Ok(AppendOutcome::TailConflict {
            actual_tail: parse_u64(field(value, "actualTail")?)?,
        })
    }
}
/// Parses the hosted spelling of a peer-issued revision.
fn parse_revision(value: &Value) -> Result<crate::Revision, StreamError> {
    crate::wire_codec::issued_from_wire(Some(crate::wire::Revision {
        scope: parse_bytes(field(value, "scope")?)?,
        incarnation: parse_bytes(field(value, "incarnation")?)?,
        index: parse_u64(field(value, "index")?)?,
    }))
}
/// A fork nested in a commit receipt carries that commit's revision.
fn parse_nested_fork(
    value: &Value,
    revision: &crate::Revision,
) -> Result<ForkReceipt, StreamError> {
    Ok(ForkReceipt {
        source: parse_path(field(value, "source")?)?,
        destination: parse_path(field(value, "destination")?)?,
        forked_at: parse_u64(field(value, "forkedAt")?)?,
        tail: parse_u64(field(value, "tail")?)?,
        commit_id: parse_id(field(value, "commitId")?)?,
        revision: revision.clone(),
    })
}
fn parse_fork(value: &Value) -> Result<ForkReceipt, StreamError> {
    Ok(ForkReceipt {
        source: parse_path(field(value, "source")?)?,
        destination: parse_path(field(value, "destination")?)?,
        forked_at: parse_u64(field(value, "forkedAt")?)?,
        tail: parse_u64(field(value, "tail")?)?,
        commit_id: parse_id(field(value, "commitId")?)?,
        revision: parse_revision(field(value, "revision")?)?,
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
            let kind = parse_string(field(value, "type")?)?;
            let owner = match kind {
                "append" => parse_path(field(value, "path")?)?,
                "fork" => parse_path(field(value, "destination")?)?,
                _ => return Err(StreamError::Unavailable),
            };
            let records = value
                .get("records")
                .map(|records| parse_records(records, &owner))
                .transpose()?
                .unwrap_or_default();
            if records.len() > MAX_ITEMS {
                return Err(StreamError::Unavailable);
            }
            match kind {
                "append" => Ok(CommittedMutation::Append(CommittedAppend {
                    path: owner,
                    start: parse_u64(field(value, "start")?)?,
                    end: parse_u64(field(value, "end")?)?,
                    tail: parse_u64(field(value, "tail")?)?,
                    records,
                })),
                "fork" => Ok(CommittedMutation::Fork(CommittedFork {
                    exclude: field(value, "exclude")?
                        .as_array()
                        .ok_or(StreamError::Unavailable)?
                        .iter()
                        .map(parse_path)
                        .collect::<Result<_, _>>()?,
                    source: parse_path(field(value, "source")?)?,
                    destination: owner,
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
        committed_at_micros: parse_u64(field(value, "committedAtMicros")?)?,
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
