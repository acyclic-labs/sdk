use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_harness::{
    Error, Result,
    scheduler::LeaseFence,
    search::{
        SearchProvider, SearchRequest, SearchResponse, decode_search_http_response,
        files::{SearchOutcome, drive_for_test},
    },
    task_volume::{Component, FileKey, FileValue, Query, TaskVolume},
};
use acyclic_stream::{BoxProviderFuture, MemoryStream, StreamPath};
use futures::StreamExt as _;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[path = "oracle.rs"]
mod oracle;

struct Response {
    bytes: Vec<u8>,
    exa: bool,
    calls: AtomicUsize,
}
impl SearchProvider for Response {
    fn search<'a>(&'a self, _: &'a SearchRequest) -> BoxProviderFuture<'a, Result<SearchResponse>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let work = NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &CancellationToken::new(),
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let expected = oracle::expected(&self.bytes, self.exa);
            let ordinary = decode_search_http_response(&self.bytes, 5, self.exa, None);
            assert!(
                oracle::agrees(
                    &expected,
                    ordinary.as_ref().map(|(response, _)| response),
                    false,
                ),
                "ordinary codec disagrees with independent oracle: {ordinary:?}"
            );
            if let Ok((_, custody)) = &ordinary {
                assert!(custody.is_none());
            }
            drop(ordinary);
            let result = decode_search_http_response(&self.bytes, 5, self.exa, Some(&work));
            assert!(
                oracle::agrees(
                    &expected,
                    result.as_ref().map(|(response, _)| response),
                    true,
                ),
                "codec disagrees with independent oracle: {result:?}"
            );
            match result {
                Ok((response, custody)) => {
                    assert!(custody.as_ref().unwrap().work().shares_ledger(&work));
                    let encoded = serde_json::to_vec(&response).unwrap();
                    let ordinary = serde_json::from_slice::<SearchResponse>(&encoded).unwrap();
                    assert_eq!(ordinary, response);
                    drop((response, custody));
                    assert_eq!(work.live_allocation_bytes(), baseline);
                    Ok(ordinary)
                }
                Err(error) => {
                    assert!(matches!(error, Error::ToolRejected(_)), "{error:?}");
                    assert_eq!(work.live_allocation_bytes(), baseline);
                    Err(error)
                }
            }
        })
    }
}

// The real task-volume worker must publish exactly the independently expected
// outcome. Both same-owner replay and a replacement owner reuse the file result.
pub async fn check(bytes: Vec<u8>, exa: bool) {
    let expected = oracle::expected(&bytes, exa);
    let backend = Arc::new(Response {
        bytes,
        exa,
        calls: AtomicUsize::new(0),
    });
    let stream = Arc::new(MemoryStream::default());
    let volume = TaskVolume::open(stream, StreamPath::new("codec-files").unwrap())
        .await
        .unwrap();
    let task = Component::new("codec").unwrap();
    let request = FileKey::parse(".tasks/codec/requests/search-codec.json").unwrap();
    let result = FileKey::parse(".tasks/codec/requests/search-codec/result.json").unwrap();
    let initial = volume
        .snapshot(&Query::Task(task.clone()), 4)
        .await
        .unwrap();
    let mut client = volume
        .observe(Query::File(result.clone()), None, 1)
        .await
        .unwrap();
    assert!(client.snapshot.take().unwrap().files.is_empty());
    volume
        .append(
            initial.generation,
            &FileValue::new(request, br#"{"query":"codec"}"#.as_slice()),
        )
        .await
        .unwrap();
    let mut fence = LeaseFence {
        reservation_id: "first".into(),
        placement: "codec".into(),
    };
    drive_for_test(
        &volume,
        &task,
        &fence,
        || Ok(Some(backend.clone())),
        vec![],
        None,
        3,
    )
    .await
    .unwrap();
    let published = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let change = client.changes.next().await.unwrap().unwrap();
            if let Some(file) = change.file {
                break file;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(published.key, result);
    let outcome: SearchOutcome = serde_json::from_slice(&published.bytes).unwrap();
    let expected = expected.map_or(SearchOutcome::Rejected, |response| {
        let success = SearchOutcome::Succeeded { response };
        // Independently predict admission of the complete packed result file.
        // Codec acceptance and exact normalization were asserted above, even
        // when a normalized result exceeds the smaller task-record limit.
        let metadata =
            serde_json::to_vec(&serde_json::json!({"key":result,"channel":null})).unwrap();
        let size = serde_json::to_vec(&success).unwrap().len() + metadata.len() + 12;
        if size > acyclic_stream::MAX_RECORD_BYTES {
            SearchOutcome::Rejected
        } else {
            success
        }
    });
    assert_eq!(outcome, expected);
    let before = volume
        .snapshot(&Query::Task(task.clone()), 4)
        .await
        .unwrap();
    assert_eq!(before.files.len(), 3);
    for replacement in [false, true] {
        if replacement {
            fence.reservation_id = "replacement".into();
        }
        drive_for_test(
            &volume,
            &task,
            &fence,
            || panic!("replay resolved credentials or contacted provider"),
            vec![],
            None,
            3,
        )
        .await
        .unwrap();
        let after = volume
            .snapshot(&Query::Task(task.clone()), 4)
            .await
            .unwrap();
        assert_eq!(after.files, before.files);
        assert_eq!(after.generation, before.generation);
    }
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
}

pub fn fuzz_response(data: &[u8]) -> Vec<u8> {
    let Some(data) = data.strip_prefix(&[0xff]) else {
        return data.to_vec();
    };
    let depth = u16::from_le_bytes([
        data.first().copied().unwrap_or(0),
        data.get(1).copied().unwrap_or(0),
    ]) as usize;
    let size = u32::from_le_bytes([
        data.get(2).copied().unwrap_or(0),
        data.get(3).copied().unwrap_or(0),
        data.get(4).copied().unwrap_or(0),
        data.get(5).copied().unwrap_or(0),
    ]) as usize;
    let size = if size > 1_048_576 {
        1_048_577
    } else {
        size % 16_385
    };
    response(
        depth,
        size,
        data.get(6).copied().unwrap_or(0),
        &serde_json::Value::String(
            String::from_utf8_lossy(data.get(7..).unwrap_or_default()).into_owned(),
        ),
    )
}

// Iterative construction explores depth independently of allocation/stack
// limits in serde_json::Value. Scalar size includes the HTTP byte boundary.
pub fn response(depth: usize, size: usize, shape: u8, leaf: &serde_json::Value) -> Vec<u8> {
    let leaf = serde_json::to_string(leaf).unwrap();
    let text = serde_json::to_string(&leaf).unwrap();
    let mut body = match shape % 6 {
        0 => r#"{"results":[],"metadata":"#.to_owned(),
        1 | 4 | 5 => {
            let highlights = match shape % 6 {
                1 => format!("[{text}]"),
                4 => format!("[{text},{text}]"),
                _ => "[]".into(),
            };
            format!(
                "{{\"results\":[{{\"url\":\"https://example.com\",\"title\":\"source\",\"snippet\":{text},\"highlights\":{highlights}}}],\"metadata\":"
            )
        }
        2 => r#"{"results":false,"metadata":"#.to_owned(),
        _ => r#"{"results":[{"url":null}],"metadata":"#.to_owned(),
    };
    for level in 0..depth {
        body.push_str(if level % 2 == 0 { "[" } else { "{\"x\":" });
    }
    body.push_str("[\"");
    body.extend(std::iter::repeat_n('x', size));
    body.push_str("\",");
    body.push_str(&leaf);
    body.push(']');
    for level in (0..depth).rev() {
        body.push(if level % 2 == 0 { ']' } else { '}' });
    }
    body.push('}');
    body.into_bytes()
}
