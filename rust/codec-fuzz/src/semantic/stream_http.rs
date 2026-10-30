//! The hosted response projection decodes binary input at its own boundary.
use acyclic_stream::{http_response, wire};
use prost::Message;

fn value(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("actual hosted JSON writer output")
}

fn response<W: Message + Default>(route: &str, bytes: &[u8]) {
    let Ok(encoded) = http_response::encode(route, bytes, usize::MAX) else {
        return;
    };
    let canonical = W::decode(bytes)
        .expect("hosted wire decoder accepted")
        .encode_to_vec();
    let repeated = http_response::encode(route, &canonical, usize::MAX)
        .expect("normalized hosted response admitted");
    assert_eq!(value(&encoded), value(&repeated));
    let required = bytes.len().max(encoded.len());
    assert_eq!(
        http_response::encode(route, bytes, required).unwrap(),
        encoded
    );
    assert_eq!(
        http_response::encode(route, bytes, required - 1),
        Err("limit_exceeded")
    );
    assert_eq!(
        http_response::encode(route, bytes, 0),
        Err("limit_exceeded")
    );
    assert_eq!(
        http_response::encode("\0", bytes, usize::MAX),
        Err("invalid_argument")
    );
}

pub(super) fn check(name: &str, bytes: &[u8]) {
    match name {
        "acyclic.stream.v1.HeadsResponse" => response::<wire::HeadsResponse>("heads", bytes),
        "acyclic.stream.v1.PathHead" => response::<wire::PathHead>("head", bytes),
        "acyclic.stream.v1.AppendResponse" => response::<wire::AppendResponse>("append", bytes),
        "acyclic.stream.v1.ForkReceipt" => response::<wire::ForkReceipt>("fork", bytes),
        "acyclic.stream.v1.CommitResponse" => response::<wire::CommitResponse>("commit", bytes),
        "acyclic.stream.v1.CommittedEnvelope" => {
            response::<wire::CommittedEnvelope>("commits/read", bytes)
        }
        "acyclic.stream.v1.ChildrenPageResponse" => {
            response::<wire::ChildrenPageResponse>("children/page", bytes)
        }
        "acyclic.stream.v1.InspectIdempotencyResponse" => {
            response::<wire::InspectIdempotencyResponse>("idempotency/inspect", bytes)
        }
        _ => {}
    }
}

type Seeds = Vec<(&'static str, &'static str, Vec<u8>)>;

fn seed(
    outputs: &mut Seeds,
    name: &'static str,
    variant: &'static str,
    route: &str,
    wire: &impl Message,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = wire.encode_to_vec();
    http_response::encode(route, &encoded, usize::MAX)
        .map_err(|error| format!("hosted writer {name}/{variant} refused: {error}"))?;
    outputs.push((name, variant, encoded));
    Ok(())
}

pub(super) fn seeds(streams: &[(String, Vec<u8>)]) -> Result<Seeds, Box<dyn std::error::Error>> {
    use acyclic_stream::{
        Child, ChildrenPage, CommitOutcome, CommitReceipt, CommittedMutation, HeadProjection,
        Heads, IdempotencyOutcome, PathHead, StreamError, StreamPath, persistence, wire_codec as c,
    };
    let mut outputs = Vec::new();
    let mut snapshot = None;
    let mut last = None;
    macro_rules! capture {
        ($ty:ident, $variant:expr, $route:expr, $value:expr) => {
            seed(
                &mut outputs,
                concat!("acyclic.stream.v1.", stringify!($ty)),
                $variant,
                $route,
                &$value,
            )?
        };
    }
    for (name, bytes) in streams {
        let variant = match name.as_str() {
            "observation-append" | "envelope-append" => "retained-append",
            "observation-fork" | "envelope-fork" => "retained-fork",
            "observation-commit" | "envelope-commit" => "retained-commit",
            _ => continue,
        };
        if name.starts_with("envelope-") {
            let envelope = persistence::decode_envelope(bytes, usize::MAX)?;
            if name == "envelope-commit" {
                last = envelope
                    .mutations
                    .iter()
                    .find_map(|mutation| match mutation {
                        CommittedMutation::Append(append) => append.records.last().cloned(),
                        CommittedMutation::Fork(_) => None,
                    });
            }
            capture!(
                CommittedEnvelope,
                variant,
                "commits/read",
                c::envelope_to_wire(envelope)
            );
            continue;
        }
        let observation = persistence::decode_observation(bytes, usize::MAX)?;
        let revision = observation
            .revision
            .clone()
            .ok_or("retained observation lost its authority revision")?;
        if name == "observation-commit" {
            snapshot = Some(revision.clone());
        }
        capture!(
            InspectIdempotencyResponse,
            variant,
            "idempotency/inspect",
            wire::InspectIdempotencyResponse {
                observation: Some(c::observation_wire(observation.clone()))
            }
        );
        match observation.outcome {
            IdempotencyOutcome::Append(value) => capture!(
                AppendResponse,
                variant,
                "append",
                c::append_outcome_to_wire(value)
            ),
            IdempotencyOutcome::Fork(value) => capture!(
                ForkReceipt,
                variant,
                "fork",
                c::fork_receipt_to_wire(&value)
            ),
            IdempotencyOutcome::Commit(envelope) => {
                let receipt = CommitReceipt::project::<StreamError>(
                    &envelope,
                    revision,
                    Vec::with_capacity(envelope.mutations.len()),
                    Vec::with_capacity(envelope.mutations.len()),
                    |_, _| Ok(()),
                )?;
                capture!(
                    CommitResponse,
                    variant,
                    "commit",
                    c::commit_outcome_to_wire(CommitOutcome::Committed(receipt))
                );
            }
        }
    }
    // Project metadata from the actual retained publication and its own cut.
    let record = last.ok_or("missing retained Stream commit record producer")?;
    let tail = record
        .sequence
        .checked_add(1)
        .ok_or("Stream producer tail overflow")?;
    let head = PathHead {
        tail,
        durable: tail,
        retained_through: None,
        last: Some(record.clone()),
        origin: None,
    };
    capture!(
        PathHead,
        "original-record",
        "head",
        c::head_to_wire(head.clone())
    );
    let page = ChildrenPage {
        hierarchy_version: record.commit_id,
        children: vec![Child {
            path: StreamPath::new("codec")?,
            tail_record: Some(record),
            origin: None,
        }],
        next_after: None,
    };
    capture!(
        ChildrenPageResponse,
        "original-record",
        "children/page",
        c::children_page_to_wire(page.clone())
    );
    let heads = Heads {
        retained: Default::default(),
        projection: HeadProjection::History,
        snapshot: snapshot.ok_or("missing original Stream revision producer")?,
        heads: [Some(head)].into(),
        children: Some(page),
    };
    capture!(
        HeadsResponse,
        "original-record",
        "heads",
        c::heads_to_wire(heads)?
    );
    Ok(outputs)
}
