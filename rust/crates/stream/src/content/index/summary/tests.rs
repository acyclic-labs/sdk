use super::*;
use crate::content::tree::tests::{Delayed, Disk, reachable};
use std::sync::atomic::Ordering;

fn field(value: impl AsRef<[u8]>) -> Field {
    Field::Inline(OwnedBytes::copy_from_slice(value.as_ref()))
}

async fn page(storage: &impl Storage, key: Field) -> Result<Root, StreamError> {
    Index::default()
        .set(storage, key, field(b""))
        .await?
        .root()
        .ok_or(StreamError::InvalidResponse)
}

#[tokio::test]
async fn paged_summaries_reopen_ordered_rows_and_retain_every_child()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let mut writer = Writer::new(&storage, 0).await?;
    let mut model = Vec::new();
    for ordinal in 0..1025_u64 {
        let key = field(format!("summary/{ordinal:06}"));
        let child = page(&storage, key.clone()).await?;
        writer.push(key.clone(), child).await?;
        model.push((key, child));
    }
    let mut content = tree::Writer::new(&storage)?;
    content.write(&vec![b'z'; tree::PAGE_BYTES * 2]).await?;
    let large = content.finish().await?;
    let key = Field::Content(large);
    let child = page(&storage, key.clone()).await?;
    writer.push(key.clone(), child).await?;
    model.push((key, child));
    let summaries = writer.finish().await?;
    let checkpoint = summaries.checkpoint(&storage).await?;
    assert!(summaries.body().part.length > tree::PAGE_BYTES as u64);
    assert!(storage.largest.load(Ordering::Relaxed) <= tree::PAGE_BYTES);
    drop(storage);
    let storage = Disk::open(directory.path());
    let reopened = Summaries::open(&storage, checkpoint).await?;
    assert_eq!(reopened, summaries);
    let graph = reachable(&storage, checkpoint).await?;
    assert!(
        graph.contains(&large.part.digest),
        "summary lost its large-key dependency"
    );
    let mut cursor = reopened.cursor(&storage).await?;
    for (key, page) in &model {
        assert!(
            graph.contains(&page.part.digest),
            "summary omitted a child-page dependency"
        );
        assert_eq!(cursor.next().await?, Some((key.clone(), *page)));
    }
    assert!(cursor.next().await?.is_none());
    let empty = Writer::new(&storage, 0).await?.finish().await?;
    assert!(empty.cursor(&storage).await?.next().await?.is_none());
    for key in [model[0].0.clone(), field(b"a")] {
        let mut writer = Writer::new(&storage, 0).await?;
        writer.push(model[0].0.clone(), model[0].1).await?;
        assert!(matches!(
            writer.push(key, model[0].1).await,
            Err(StreamError::InvalidArgument)
        ));
        assert!(matches!(
            writer.finish().await,
            Err(StreamError::Unavailable)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_summary_key_read_cannot_seal_or_replace_a_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let disk = Disk::open(directory.path());
    let first = page(&disk, field(b"a")).await?;
    let second = page(&disk, field(b"b")).await?;
    let mut content = tree::Writer::new(&disk)?;
    content.write(b"z-large-key").await?;
    let large = content.finish().await?;
    let target = page(&disk, Field::Content(large)).await?;
    let storage = Delayed {
        disk,
        put: Default::default(),
        get: Default::default(),
    };
    let mut writer = Writer::new(&storage, 0).await?;
    writer.push(field(b"a"), first).await?;
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (_resume, resumed) = tokio::sync::oneshot::channel();
    *storage.get.lock().map_err(|_| "gate poisoned")? = Some((entered, resumed));
    {
        let append = writer.push(Field::Content(large), target);
        tokio::pin!(append);
        tokio::select! {
            _ = waiting => {},
            result = &mut append => { result?; return Err("summary key read did not suspend".into()); },
        }
    }
    assert_eq!(
        writer.push(field(b"b"), second).await,
        Err(StreamError::Unavailable),
        "cancelled summary writer accepted replacement row"
    );
    assert!(matches!(
        writer.finish().await,
        Err(StreamError::Unavailable)
    ));
    Ok(())
}

#[tokio::test]
async fn malformed_summary_row_cannot_be_skipped_by_the_same_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let storage = Disk::open(directory.path());
    let mut writer = Writer::new(&storage, 0).await?;
    for key in [b'a', b'b'] {
        let first = field([key]);
        writer
            .push(first.clone(), page(&storage, first).await?)
            .await?;
    }
    let summaries = writer.finish().await?;
    let mut reader = tree::Reader::new(&storage, summaries.body)?;
    let mut bytes = Vec::new();
    while let Some(page) = reader.next().await? {
        bytes.extend_from_slice(page.as_slice());
    }
    let offset = MAGIC.len() + 1 + 32 + 9 + 1 + 32;
    bytes[offset..offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    let mut changed = tree::Writer::new(&storage)?;
    changed.write(&bytes).await?;
    let invalid = Summaries {
        body: changed.finish().await?,
        ..summaries
    };
    let mut cursor = invalid.cursor(&storage).await?;
    assert!(matches!(
        cursor.next().await,
        Err(StreamError::InvalidResponse)
    ));
    assert!(
        matches!(cursor.next().await, Err(StreamError::Unavailable)),
        "summary cursor skipped a failed complete row"
    );
    Ok(())
}
