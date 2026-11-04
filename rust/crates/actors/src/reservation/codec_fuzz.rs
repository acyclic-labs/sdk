//! Exercise the canonical reservation wire and checkpoint decoders on demand.
use super::*;

/// Checks canonical requests and decisions, and validates checkpoint ownership.
pub fn check(bytes: &[u8]) -> bool {
    static WRITERS: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
        seeds().expect("reservation decoders must admit their production writers");
    });
    std::sync::LazyLock::force(&WRITERS);
    if let Ok(book) = decode::<Book>(bytes)
        && book.validate().is_ok()
    {
        let Ok(encoded) = encode(&book) else {
            return false;
        };
        let Ok(decoded) = decode::<Book>(&encoded) else {
            return false;
        };
        if decoded != book || decoded.validate().is_err() {
            return false;
        }
    }
    if let Ok(request) = decode::<Request>(bytes) {
        let Ok(actor) = crate::addressing::ActorReference::new(b"account", b"pool", b"codec")
        else {
            return false;
        };
        let Ok(caller) = acyclic_stream::StreamPath::new("reservation-codec/caller") else {
            return false;
        };
        if let Ok(invocation) = request.invocation(caller, actor, "inbox".into())
            && invocation.request().as_ref() != bytes
        {
            return false;
        }
    }
    if let Ok(decision) = decode::<Decision>(bytes)
        && encode(&decision).as_deref() != Ok(bytes)
    {
        return false;
    }
    true
}

/// Capture actual reservation request, result and initial checkpoint encoders.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    let owner = OwnerEvidence::new(
        Bytes::from_static(b"owner"),
        Bytes::from_static(b"evidence"),
    )?;
    let request = Request::Reserve {
        request_id: Bytes::from_static(b"request"),
        units: 1,
        owner,
        deadline_unix_ms: 100,
    };
    let request_bytes = encode(&request)?;
    let admitted = decode::<Request>(&request_bytes)?;
    assert_eq!(admitted.operation()?, request.operation()?);
    assert_eq!(encode(&admitted)?, request_bytes);
    let decision = Decision::Exhausted;
    let decision_bytes = encode(&decision)?;
    assert_eq!(decode::<Decision>(&decision_bytes)?, decision);
    let book_bytes = initial_checkpoint(1)?.bytes;
    assert_eq!(decode::<Book>(&book_bytes)?, Book::new(1)?);
    let mut seeds = vec![
        ("reservation-request", request_bytes.to_vec()),
        ("reservation-decision", decision_bytes.to_vec()),
        ("reservation-book", book_bytes.to_vec()),
    ];
    seeds.extend(futures::executor::block_on(publication_seeds())?);
    Ok(seeds)
}

// The original Host executes committed invocation records. Keep grants and
// checkpoints from those publications; never manufacture populated Books.
async fn publication_seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    use crate::{
        addressing::ActorReference,
        domain::{ActorLimits, CodeSha256},
        hosting::Host,
        local::{ControlledClock, PreparedPublish, RuntimeLimits, Store},
    };
    use acyclic_stream::{IdempotencyKey, MemoryStream, StreamPath};
    use std::sync::Arc;

    let provider = Arc::new(MemoryStream::default());
    let root = StreamPath::new("codec/reservations")?;
    let actor = ActorReference::new(b"account", b"pools", b"codec")?;
    let store = Store::new(Arc::clone(&provider), root.clone());
    let mut creation = PreparedPublish::foreign(IdempotencyKey::unique());
    for effect in create(
        &store,
        &actor,
        StreamPath::new("codec/reservation-input")?,
        1,
    )? {
        creation.join(effect)?;
    }
    store.submit(creation).await?;
    let (clock, _controller) = ControlledClock::new(10);
    let host = Host::new(
        Store::new(provider, root),
        actor.clone(),
        Arc::new(Package::new(
            CodeSha256::new(vec![1; 32])?,
            ActorLimits::new(10_000, 4096, 4096)?,
        )),
        Arc::new(clock),
        RuntimeLimits::new(1, 1)?,
    );
    let owner = OwnerEvidence::new(
        Bytes::from_static(b"owner"),
        Bytes::from_static(b"evidence"),
    )?;
    let mut grant = None;
    let mut seeds = Vec::new();
    for (index, name) in ["granted", "exhausted", "expired", "released", "stale"]
        .into_iter()
        .enumerate()
    {
        let request = match index {
            0..=2 => Request::Reserve {
                request_id: Bytes::copy_from_slice(name.as_bytes()),
                units: 1,
                owner: owner.clone(),
                deadline_unix_ms: if index == 2 { 1 } else { 100 },
            },
            _ => Request::Release {
                grant: grant.clone().ok_or("original grant is absent")?,
            },
        };
        let invocation = request.invocation(
            StreamPath::new(format!("codec/reservation-caller/{name}"))?,
            actor.clone(),
            "inbox".into(),
        )?;
        let ticket = store.enqueue_invoke(&invocation).await?;
        host.wake().await?;
        let committed = store.wait_invocation(ticket).await?;
        let decision: Decision = decode(committed.response())?;
        if let Decision::Granted(original) = &decision {
            assert_eq!(index, 0);
            assert_eq!(original.pool.as_ref(), actor.encode());
            assert_eq!(original.state_path, store.path(&actor)?);
            assert_eq!(original.request_id.as_ref(), b"granted");
            assert_eq!(original.owner, owner);
            assert_eq!(
                (
                    original.generation,
                    original.units,
                    original.deadline_unix_ms
                ),
                (1, 1, 100)
            );
            grant = Some(original.clone());
        } else {
            assert_eq!(
                decision,
                match index {
                    1 => Decision::Exhausted,
                    2 => Decision::Expired,
                    3 => Decision::Released,
                    4 => Decision::Stale,
                    _ => panic!("reservation writer must grant original capacity"),
                }
            );
        }
        let snapshot = store.load(&actor).await?;
        let checkpoint = snapshot
            .state()
            .and_then(|state| state.checkpoint())
            .ok_or("published reservation checkpoint is absent")?;
        let book: Book = decode(&checkpoint.bytes)?;
        assert_eq!(
            book.validate(),
            Ok(()),
            "reservation writer must admit its populated checkpoint"
        );
        assert_eq!(
            (book.capacity, book.used, book.generation, book.deadline()),
            (1, u64::from(index < 3), 1, (index < 3).then_some(100))
        );
        if index < 3 {
            assert_eq!(
                book.active.get(&100).and_then(|grants| grants.get(&1)),
                grant.as_ref()
            );
        } else {
            assert!(book.active.is_empty());
        }
        assert_eq!(encode(&decision)?.as_ref(), committed.response().as_ref());
        assert_eq!(encode(&book)?, checkpoint.bytes);
        match index {
            0 => seeds.extend([
                (
                    "reservation-committed-reserve",
                    invocation.request().to_vec(),
                ),
                ("reservation-granted", committed.response().to_vec()),
                ("reservation-populated", checkpoint.bytes.to_vec()),
            ]),
            // Exhausted already has an original writer capture. Keep one
            // capture per new decision/Book shape, not one per request.
            1 => {}
            2 => seeds.push(("reservation-expired", committed.response().to_vec())),
            3 => seeds.extend([
                ("reservation-release", invocation.request().to_vec()),
                ("reservation-released", committed.response().to_vec()),
                ("reservation-released-book", checkpoint.bytes.to_vec()),
            ]),
            _ => seeds.push(("reservation-stale", committed.response().to_vec())),
        }
    }
    Ok(seeds)
}
