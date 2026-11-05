//! `read` selectors: latest-first keys, key ranges, a follow from beyond the
//! tail, and `children` paging, each with a negative control.
use super::*;

/// Every record of one indexed read, paged `page` at a time through the
/// shared continuation rule, [`crate::ReadKeySeek::advance`].
async fn selected<P: StreamProvider + ?Sized>(
    provider: &P,
    source: &StreamPath,
    mut seek: crate::ReadKeySeek,
    mut from: u64,
    page: u32,
) -> Result<Vec<u64>, StreamError> {
    let mut sequences = Vec::new();
    loop {
        let mut records = provider
            .read(ReadRequest {
                key: Some(seek.clone()),
                path: source.clone(),
                from,
                limit: page,
                at_least: Default::default(),
            })
            .await?;
        let mut count = 0;
        let mut upper = page as usize;
        loop {
            upper = upper.min(records.size_hint().1.unwrap_or(usize::MAX));
            let Some(record) = records.try_next().await? else {
                break;
            };
            if upper == 0 {
                return Err(StreamError::InvalidResponse);
            }
            upper -= 1;
            seek.advance(&mut from, record.sequence, record.headers.keys())?;
            sequences.push(record.sequence);
            count += 1;
        }
        if count < page {
            return Ok(sequences);
        }
    }
}

pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), StreamError> {
    let source = path("selectors/source")?;
    let record = |keys: &[&'static [u8]]| -> Result<crate::AppendRecord, StreamError> {
        Ok(crate::AppendRecord {
            value: Bytes::from_static(b"value"),
            headers: crate::RecordHeaders::new(
                keys.iter().map(|key| Bytes::from_static(key)).collect(),
                Vec::new(),
            )?,
        })
    };
    provider
        .append(AppendRequest {
            path: source.clone(),
            if_tail: Some(0),
            idempotency_key: key(b"selectors-source")?,
            records: vec![
                record(&[b"a"])?,
                record(&[b"b", b"c"])?,
                record(&[b"a", b"c"])?,
                record(&[b"b"])?,
                record(&[b"a"])?,
            ],
        })
        .await?;
    use crate::ReadKeySeek as Seek;

    // Latest first: one lookup finds the latest match; paging walks back.
    // The ascending order of the same key is the negative control.
    let latest = provider
        .latest(source.clone(), Bytes::from_static(b"a"), Default::default())
        .await?
        .ok_or(StreamError::Unavailable)?;
    if latest.sequence != 4
        || provider
            .latest(source.clone(), Bytes::from_static(b"z"), Default::default())
            .await?
            .is_some()
        || selected(provider, &source, Seek::latest(&b"a"[..], u64::MAX), 0, 1).await? != [4, 2, 0]
        || selected(provider, &source, Seek::latest(&b"a"[..], 4), 1, 10).await? != [2]
        || selected(provider, &source, Seek::exact(&b"a"[..], u64::MAX), 0, 1).await? != [0, 2, 4]
    {
        return Err(StreamError::Unavailable);
    }

    // Key ranges: ascending (key, sequence) entries; a record appears once
    // per key it carries in the range. Pages resume through the continuation.
    let ranges: [(Seek, u64, u32, &[u64]); 5] = [
        (
            Seek::range(&b"a"[..], &b"d"[..], u64::MAX),
            0,
            2,
            &[0, 2, 4, 1, 3, 1, 2],
        ),
        (Seek::range(&b"b"[..], &b"c"[..], 3), 0, 10, &[1]),
        (
            Seek::range(&b"a"[..], &b"c"[..], u64::MAX),
            3,
            10,
            &[4, 1, 3],
        ),
        (Seek::range(&b"c"[..], &b"c"[..], u64::MAX), 0, 10, &[]),
        (Seek::range(&b"d"[..], &b"z"[..], u64::MAX), 0, 10, &[]),
    ];
    for (seek, from, page, expected) in ranges {
        let actual = selected(provider, &source, seek, from, page).await?;
        if actual != expected {
            return Err(StreamError::Unavailable);
        }
    }
    // Ranges are ascending only, refused before any observation.
    let refused = provider
        .read(ReadRequest {
            key: Some(Seek {
                order: crate::ReadOrder::LatestFirst,
                ..Seek::range(&b"a"[..], &b"d"[..], u64::MAX)
            }),
            path: source.clone(),
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .err();
    if refused != Some(StreamError::InvalidArgument) {
        return Err(StreamError::Unavailable);
    }

    // `from` beyond the tail waits there: the k-th record is the first delivery.
    let mut follow = crate::records_only(
        provider
            .follow(crate::FollowRequest::new(source.clone(), 7))
            .await?,
    );
    for (index, sequence) in [5_u64, 6, 7].into_iter().enumerate() {
        let outcome = provider
            .append(AppendRequest {
                path: source.clone(),
                if_tail: Some(sequence),
                idempotency_key: key([
                    b"selectors-late-0",
                    b"selectors-late-1",
                    b"selectors-late-2",
                ][index])?,
                records: vec![record(&[b"a"])?],
            })
            .await?;
        if !matches!(outcome, AppendOutcome::Committed(_)) {
            return Err(StreamError::Unavailable);
        }
    }
    let first = follow
        .next()
        .await
        .transpose()?
        .ok_or(StreamError::Unavailable)?;
    if first.sequence != 7 {
        return Err(StreamError::Unavailable);
    }

    // `children` pages a parent's direct children; a leaf has none.
    let tree = path("selectors/tree")?;
    for (child, idempotency) in [
        ("selectors/tree/x", b"selectors-tree-x"),
        ("selectors/tree/y", b"selectors-tree-y"),
        ("selectors/tree/z", b"selectors-tree-z"),
    ] {
        provider
            .append(AppendRequest {
                path: path(child)?,
                if_tail: Some(0),
                idempotency_key: key(idempotency)?,
                records: vec![record(&[])?],
            })
            .await?;
    }
    let page = |parent: StreamPath, after: Option<StreamPath>| ChildrenPageRequest {
        tail_stream: None,
        parent: Some(parent),
        after,
        hierarchy_version: None,
        limit: 2,
    };
    let names = |page: &crate::ChildrenPage| {
        page.children
            .iter()
            .map(|child| child.path.to_string())
            .collect::<Vec<_>>()
    };
    let first = provider.children_page(page(tree.clone(), None)).await?;
    let rest = provider
        .children_page(page(tree, first.next_after.clone()))
        .await?;
    let leaf = provider
        .children_page(page(path("selectors/tree/x")?, None))
        .await?;
    if names(&first) != ["selectors/tree/x", "selectors/tree/y"]
        || first.next_after != Some(path("selectors/tree/y")?)
        || names(&rest) != ["selectors/tree/z"]
        || rest.next_after.is_some()
        || !leaf.children.is_empty()
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}

#[cfg(all(test, target_arch = "wasm32", feature = "wasm"))]
mod injected {
    use wasm_bindgen::prelude::*;

    // Only the host port's method names and result envelope are adapted.
    // The shipping WASM provider produces every frame and owns cancellation.
    #[wasm_bindgen(inline_js = r#"
export function selectorPort(stream) {
  return {
    async dispatch(operation, input) {
      return {value: await stream.dispatch(operation, input)};
    },
    async open(operation, input, preceding) {
      const cursor = await stream.openProviderCursor(operation, input, preceding);
      return {value: {
        async next() { return {value: await cursor.next()}; },
        close() { cursor.close(); }
      }};
    }
  };
}
"#)]
    extern "C" {
        #[wasm_bindgen(js_name = selectorPort)]
        fn port(stream: JsValue) -> JsValue;
    }

    #[wasm_bindgen_test::wasm_bindgen_test]
    async fn selectors_survive_the_paired_injected_provider() {
        super::verify(&crate::MemoryStream::default())
            .await
            .expect("memory selector contract");
        let provider = crate::wasm::provider_rpc::InjectedStream::new(port(
            crate::wasm::WasmStream::new().into(),
        ))
        .unwrap();
        super::verify(&provider)
            .await
            .expect("paired injected selector contract");
    }
}
