use super::*;

fn key(index: u32) -> [u8; 32] {
    *blake3::hash(&index.to_be_bytes()).as_bytes()
}

#[tokio::test]
async fn immutable_roots_reopen_without_loading_the_index() -> Result<()> {
    for native in [false, true] {
        check_roots(native).await?;
    }
    Ok(())
}

async fn check_roots(native: bool) -> Result<()> {
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let path = StreamPath::new("index/pages")?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = if native {
        PreparationIo::from_native_work(&work)?
    } else {
        PreparationIo::from_budget(budget, &cancellation)?
    };
    let baseline = io.owned_bytes()?;
    let mut writer = Writer::new(&provider, &path, 0, Some(&io), None)?;
    let mut root = None;
    let mut versions = Vec::new();
    for index in 0..1_000_u32 {
        root = Some(writer.set(root, key(index), &index.to_be_bytes()).await?);
        if [0, 99, 999].contains(&index) {
            versions.push((index, root));
        }
    }
    // Updating one key leaves every earlier root readable.
    let old = root;
    root = Some(writer.set(root, key(0), b"replacement").await?);
    let tail = writer.tail()?;
    for (index, records) in writer.records().chunks(128).enumerate() {
        let outcome = provider
            .append(acyclic_stream::AppendRequest {
                path: path.clone(),
                records: records.iter().cloned().map(Into::into).collect(),
                if_tail: Some(index as u64 * 128),
                idempotency_key: None,
            })
            .await?;
        assert!(matches!(outcome, AppendOutcome::Committed(_)));
    }
    drop(writer);
    let reader = Reader {
        provider: &provider,
        path: &path,
        base: tail,
        staged: &[],
        io: Some(&io),
        cache: None,
    };
    for (index, version) in versions {
        let before = io.receipt();
        let read = reader
            .get(version, &key(index))
            .await?
            .expect("indexed key");
        assert!(
            matches!(read.node(Some(&io))?, Node::Leaf { value, .. } if value == index.to_be_bytes())
        );
        let records = io.receipt().authority_records_read - before.authority_records_read;
        eprintln!(
            "persisted index native={native} keys={} read_records={records}",
            index + 1
        );
        assert!(
            records <= 257,
            "a fixed-width radix lookup exceeded its key bound"
        );
        drop(read);
        assert!(reader.get(version, &key(index + 1)).await?.is_none());
    }
    let original = reader.get(old, &key(0)).await?.expect("old key");
    assert!(
        matches!(original.node(Some(&io))?, Node::Leaf { value, .. } if value == 0_u32.to_be_bytes())
    );
    drop(original);
    let replacement = reader.get(root, &key(0)).await?.expect("replacement");
    assert!(
        matches!(replacement.node(Some(&io))?, Node::Leaf { value, .. } if value == b"replacement")
    );
    drop(replacement);
    let mut forged = root.expect("root");
    forged.digest[0] ^= 1;
    assert!(
        reader.read(forged).await.is_err(),
        "forged page passed its digest check"
    );
    drop(provider);
    assert_eq!(
        io.owned_bytes()?,
        baseline,
        "page buffers outlived their funded storage"
    );
    Ok(())
}

#[test]
fn malformed_nodes_cannot_create_cycles_or_unbounded_paths() -> Result<()> {
    let child = NodeRef {
        sequence: 1,
        digest: [1; 32],
    };
    let (bytes, _) = Node::Branch {
        bit: 255,
        children: [child; 2],
    }
    .encode(None)?;
    assert!(
        Node::decode(&bytes, 1, None).is_err(),
        "forward/self link accepted"
    );
    assert!(Node::decode(&bytes, 2, None).is_ok());
    assert!(
        Node::Branch {
            bit: 256,
            children: [child; 2],
        }
        .encode(None)
        .is_err(),
        "out-of-range branch was encoded"
    );
    let (bytes, _) = Node::Leaf {
        key: [0; 32],
        value: b"value",
    }
    .encode(None)?;
    for length in 0..node::LEAF_HEADER_BYTES {
        assert!(Node::decode(&bytes[..length], 0, None).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn record_framing_never_substitutes_for_the_authenticated_value_digest() -> Result<()> {
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let path = StreamPath::new("truncated/index")?;
    let (bytes, _) = Node::Leaf {
        key: [1; 32],
        value: b"value",
    }
    .encode(None)?;
    let digest = crate::model::hash_bytes(&bytes, None)?;
    for (sequence, length) in (node::LEAF_HEADER_BYTES..bytes.len()).enumerate() {
        let sequence = sequence as u64;
        // A shorter value is structurally valid inside its SDK frame. The
        // authenticated parent must still reject every such substitution.
        assert!(Node::decode(&bytes[..length], sequence, None).is_ok());
        provider
            .append(acyclic_stream::AppendRequest {
                path: path.clone(),
                records: vec![bytes[..length].to_vec().into()],
                if_tail: Some(sequence),
                idempotency_key: None,
            })
            .await?;
        let reader = Reader {
            provider: &provider,
            path: &path,
            base: sequence + 1,
            staged: &[],
            io: None,
            cache: None,
        };
        assert!(
            reader.read(NodeRef { sequence, digest }).await.is_err(),
            "truncated value reused its parent's proof"
        );
    }
    Ok(())
}
