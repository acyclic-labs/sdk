pub use acyclic_stream_contract::trace::*;

#[cfg(all(test, feature = "serde", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::*;
    use crate::{
        AppendOutcome, AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath,
        StreamProvider,
    };
    use futures::StreamExt as _;

    /// Observation holds the observed work once, beside a constant-size
    /// admission and recording. Nesting adds that constant per layer rather
    /// than multiplying the work, so traced futures stay small on any stack.
    #[test]
    fn observation_adds_a_constant_size_however_deeply_it_nests() {
        const LAYER_BYTES: usize = 512;
        async fn work() -> Result<u8, ()> {
            let held = std::hint::black_box([7u8; 4096]);
            std::future::ready(()).await;
            Ok(held[4095])
        }
        let request = |name| Request {
            name,
            mechanism: Mechanism::Wait,
            parent: None,
            guarantee: None,
            key: None,
        };
        let inner = std::mem::size_of_val(&work());
        let once = std::mem::size_of_val(&observe(&request("once"), work(), |_| None));
        let twice = std::mem::size_of_val(&observe(
            &request("outer"),
            observe(&request("inner"), work(), |_| None),
            |_| None,
        ));
        let guard = std::mem::size_of_val(&observe_guard(request("guard"), work()));
        // A caller's admission may borrow and carry its own error type.
        let borrowed = 7u8;
        let admission: crate::BoxProviderFuture<'_, Result<OwnedRecording, u8>> =
            Box::pin(async { Err(borrowed) });
        let accompanied =
            std::mem::size_of_val(&OwnedRecording::accompany(Some(admission), work()));
        assert!(
            accompanied <= inner + LAYER_BYTES,
            "{accompanied} > {inner} + {LAYER_BYTES}"
        );
        assert!(
            once <= inner + LAYER_BYTES,
            "{once} > {inner} + {LAYER_BYTES}"
        );
        assert!(
            twice <= inner + 2 * LAYER_BYTES,
            "{twice} > {inner} + 2 * {LAYER_BYTES}"
        );
        assert!(
            guard <= inner + LAYER_BYTES,
            "{guard} > {inner} + {LAYER_BYTES}"
        );
    }

    #[tokio::test]
    async fn span_deltas_page_and_retain_original_failure_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = std::sync::Arc::new(MemoryStream::default());
        let path = StreamPath::new("agents/worker/trace")?;
        let evidence = StreamPath::new("agents/worker/errors")?;
        let AppendOutcome::Committed(publication) = provider
            .append(AppendRequest {
                path: evidence.clone(),
                records: vec![crate::OwnedBytes::from(b"original failure".to_vec()).into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?
        else {
            panic!("fresh failure publication conflicted")
        };
        let cause = RetainedRecord {
            commit_id: publication.commit_id,
            path: evidence,
            sequence: 0,
        };
        let recorder = std::sync::Arc::new(Recorder::new(
            crate::Stream::new(provider.clone(), path.as_str())?,
            [1; 16],
        ));
        let admitted = recorder
            .start(
                "stream.read",
                Mechanism::Io,
                None,
                Some("read_committed_prefix"),
            )
            .await?;
        let id = admitted.id();
        let mut events = Vec::new();
        // The real producer publishes its admission before any settlement exists.
        let mut page = provider
            .read(ReadRequest {
                key: None,
                path: path.clone(),
                from: 0,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let record = page.next().await.ok_or("missing original start")??;
        assert_eq!(record.commit_id, admitted.start().await?.commit_id);
        events.push(serde_json::from_slice::<TraceRecord>(
            record.value.as_slice(),
        )?);
        assert!(matches!(&events[0], TraceRecord::V1(SpanEvent::Started {
            id: observed, parent: None, mechanism: Mechanism::Io, guarantee: Some(guarantee), ..
        }) if *observed == id && guarantee == "read_committed_prefix"));
        assert!(page.next().await.is_none());
        let terminal = admitted
            .finish(
                Some(64),
                Outcome::Failed {
                    cause: cause.clone(),
                },
            )
            .await?;
        let mut page = provider
            .read(ReadRequest {
                key: None,
                path: path.clone(),
                from: 1,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let record = page.next().await.ok_or("missing original finish")??;
        assert_eq!(record.commit_id, terminal.commit_id);
        events.push(serde_json::from_slice::<TraceRecord>(
            record.value.as_slice(),
        )?);
        assert!(matches!(&events[1], TraceRecord::V1(SpanEvent::Finished {
            id: observed, bytes: Some(64), result: Outcome::Failed { cause: original }, ..
        }) if *observed == id && original == &cause));
        assert!(page.next().await.is_none());

        let mut original = provider
            .read(ReadRequest {
                key: None,
                path: cause.path,
                from: cause.sequence,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let record = original.next().await.ok_or("missing original failure")??;
        assert_eq!(record.commit_id, cause.commit_id);
        assert_eq!(record.value.as_slice(), b"original failure");

        // Malformed persisted input cannot fabricate a causeless failed span.
        let mut malformed = serde_json::to_value(&events[1])?;
        malformed["span"]["result"]
            .as_object_mut()
            .unwrap()
            .remove("cause");
        assert!(serde_json::from_value::<TraceRecord>(malformed).is_err());
        for (event, key) in [
            (&events[0], "parent"),
            (&events[0], "guarantee"),
            (&events[1], "bytes"),
        ] {
            let mut malformed = serde_json::to_value(event)?;
            malformed["span"].as_object_mut().unwrap().remove(key);
            assert!(
                serde_json::from_value::<TraceRecord>(malformed).is_err(),
                "missing {key}"
            );
            let mut unknown = serde_json::to_value(event)?;
            unknown["span"][key] = serde_json::Value::Null;
            assert!(
                serde_json::from_value::<TraceRecord>(unknown).is_ok(),
                "explicit null {key}"
            );
        }
        Ok(())
    }
}
