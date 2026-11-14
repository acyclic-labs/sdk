use super::*;
use crate::reader::{Op, Priority, ReaderPolicy, driver};
use acyclic_stream::{
    MemoryStream,
    trace::{Outcome, SpanEvent, SpanId, TraceRecord},
};
use futures::StreamExt as _;
use std::sync::Arc;

#[test]
fn original_reader_pages_and_follow_drive_one_source_bound_projection() {
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
        let stream = acyclic_stream::Stream::new(provider.clone(), "agents/worker/trace").unwrap();
        let id = SpanId {
            process: [1; 16],
            sequence: 0,
        };
        let start = TraceRecord::V1(SpanEvent::Started {
            id,
            parent: None,
            name: "wait".into(),
            mechanism: Mechanism::Wait,
            at_ns: 10,
            guarantee: None,
        });
        stream
            .append(
                serde_json::to_vec(&start).unwrap(),
                acyclic_stream::IdempotencyKey::unique(),
            )
            .await
            .unwrap();
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        let mut file = File::new(&mut reader, stream.path().clone(), 0, BTreeMap::new()).unwrap();
        reader.plan([(file.source.want(), Priority::Visible)]);
        let [op]: [Op; 1] = reader.poll(0).try_into().unwrap();
        let op_id = op.id();
        reader.complete(op_id, driver::execute(provider.as_ref(), op).await, 0);
        let page = NonZeroU32::new(1).unwrap();
        let pointer = reader.cache().record(stream.path(), 0).unwrap().as_ptr();
        let issued = reader.issued();
        let charged = reader.cache().backing_bytes();
        assert_eq!(file.advance(&reader, page).unwrap(), 1);
        assert_eq!(reader.issued(), issued);
        assert_eq!(reader.cache().backing_bytes(), charged);
        assert_eq!(
            reader.cache().record(stream.path(), 0).unwrap().as_ptr(),
            pointer
        );
        let value = file.value(&reader).unwrap();
        assert!(value.caught_up);
        assert_eq!(
            (
                value.active_spans,
                value.active_waits,
                value.missing_guarantees
            ),
            (1, 1, 1)
        );
        assert!(value.mechanisms.is_empty());
        let unrelated = Reader::new(ReaderPolicy::default()).unwrap();
        assert!(matches!(file.value(&unrelated), Err(Error::ContinuityLost)));

        reader.set_follows([stream.path().clone()]);
        let mut follow = stream.follow(1, Default::default()).await.unwrap();
        let finish = TraceRecord::V1(SpanEvent::Finished {
            id,
            at_ns: 30,
            bytes: None,
            result: Outcome::Unsettled,
        });
        stream
            .append(
                serde_json::to_vec(&finish).unwrap(),
                acyclic_stream::IdempotencyKey::unique(),
            )
            .await
            .unwrap();
        let original = follow.next().await.unwrap().unwrap();
        let commit = original.commit_id;
        reader.deliver(stream.path(), driver::fetched(original), 2);
        assert_eq!(file.advance(&reader, page).unwrap(), 1);
        let value = file.value(&reader).unwrap();
        assert!(value.caught_up);
        assert_eq!(value.active_waits, 0);
        let waits = &value.mechanisms[&Mechanism::Wait];
        assert_eq!(
            (waits.elapsed_ns, waits.unsettled, waits.unknown_bytes),
            (20, 1, 1)
        );
        assert_eq!(
            reader
                .cache()
                .record(stream.path(), 1)
                .unwrap()
                .evidence()
                .unwrap()
                .commit,
            commit
        );

        // A selected suffix must expose its cut and cannot claim paired history.
        let mut suffix = File::new(&mut reader, stream.path().clone(), 1, BTreeMap::new()).unwrap();
        assert_eq!(suffix.advance(&reader, page).unwrap(), 1);
        let suffix_value = suffix.value(&reader).unwrap();
        assert_eq!(
            (suffix_value.covered_from, suffix_value.covered_through),
            (1, 2)
        );
        assert!(suffix_value.caught_up);
        assert_eq!(suffix_value.unpaired_finishes, 1);
        assert!(suffix_value.mechanisms.is_empty());
        let encoded = serde_json::to_value(&suffix_value).unwrap();
        assert_eq!(encoded["coveredFrom"], 1);
        assert_eq!(encoded["coveredThrough"], 2);

        stream
            .append(
                b"not JSON".to_vec(),
                acyclic_stream::IdempotencyKey::unique(),
            )
            .await
            .unwrap();
        reader.deliver(
            stream.path(),
            driver::fetched(follow.next().await.unwrap().unwrap()),
            3,
        );
        assert!(
            matches!(file.advance(&reader, page), Err(ReadError::Projection(QueryError::Decode { evidence, .. })) if evidence.sequence == 2)
        );
        let value = file.value(&reader).unwrap();
        assert_eq!((value.covered_through, value.rejected_records), (3, 1));
        assert_eq!(value.mechanisms[&Mechanism::Wait].completed, 1);
        assert_eq!(reader.issued(), issued);

        let snapshot = reader.cache().snapshot();
        // An unrelated source discontinuity must not retire this projection.
        reader.replaced(&StreamPath::new("agents/other/trace").unwrap());
        assert!(file.value(&reader).is_ok());
        reader.replaced(stream.path());
        reader.acknowledge_issue(stream.path());
        assert!(matches!(file.value(&reader), Err(Error::ContinuityLost)));
        assert!(matches!(
            file.advance(&reader, page),
            Err(ReadError::Source(Error::ContinuityLost))
        ));

        let reopened = File::new(&mut reader, stream.path().clone(), 0, BTreeMap::new()).unwrap();
        let previous = std::mem::replace(
            reader.cache_mut(),
            crate::reader::RecordCache::new(1024 * 1024, 16),
        );
        assert!(matches!(
            reopened.query(&reader),
            Err(Error::ContinuityLost)
        ));
        drop(previous);
        let reopened = File::new(&mut reader, stream.path().clone(), 0, BTreeMap::new()).unwrap();
        reader.configure_source(provider.as_ref(), 0);
        assert!(matches!(
            reopened.query(&reader),
            Err(Error::ContinuityLost)
        ));
        reader.cache_mut().restore(&snapshot).unwrap();
        let mut restored =
            File::new(&mut reader, stream.path().clone(), 0, BTreeMap::new()).unwrap();
        assert!(matches!(
            restored.advance(&reader, page),
            Err(ReadError::MissingEvidence { sequence: 0 })
        ));
    });
}
