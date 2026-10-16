use super::{FileKey, FileValue, Result, TaskVolume};
use acyclic_stream::StreamProvider;
use std::ops::Bound::{Excluded, Unbounded};

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Read a bounded root-directory page, resuming after its last typed key.
    /// Existing keys remain ordered while concurrent file replacements arrive.
    /// `None` means a bounded history-replay slice made progress; yield and retry
    /// the same key. `Some([])` ends enumeration. The history anchor is always
    /// verified, including when the cached index was already current.
    pub async fn page(
        &self,
        after: Option<&FileKey>,
        maximum_files: usize,
    ) -> Result<Option<Vec<FileValue>>> {
        if maximum_files == 0 {
            return Err(super::Error::LimitExceeded);
        }
        let (_, complete) = self.refresh_bounded::<true>(maximum_files as u64).await?;
        if !complete {
            return Ok(None);
        }
        let index = self.index.lock().await;
        Ok(Some(
            index
                .files
                .range((after.map_or(Unbounded, Excluded), Unbounded))
                .take(maximum_files)
                .map(|(_, file)| file.file.clone())
                .collect(),
        ))
    }
}

#[cfg(all(test, acyclic_loom))]
mod models {
    use super::*;
    use acyclic_stream::{MemoryStream, StreamPath, verification::block_on};
    use std::sync::Arc;

    fn enumeration(broken: bool) {
        loom::model(move || {
            let provider = Arc::new(MemoryStream::default());
            let path = StreamPath::new("bounded-enumeration-model").unwrap();
            let writer = block_on(TaskVolume::open(provider.clone(), path.clone())).unwrap();
            let reader = block_on(TaskVolume::open(provider, path)).unwrap();
            let key = |name| FileKey::parse(&format!(".ui/{name}/settings.json")).unwrap();
            let mut cut = block_on(writer.snapshot(&super::super::Query::Root, 0))
                .unwrap()
                .generation;
            for name in ["a", "b"] {
                cut = block_on(writer.append(cut, &FileValue::new(key(name), b"{}".to_vec())))
                    .unwrap();
            }
            assert!(block_on(reader.page(None, 1)).unwrap().is_none());
            // Only one reader slice races one atomic publication; bootstrap
            // and the remaining replay are outside the concurrent region.
            let publisher = loom::thread::spawn(move || {
                block_on(writer.append(cut, &FileValue::new(key("c"), b"{}".to_vec()))).unwrap();
            });
            let mut ready = block_on(reader.page(None, 1)).unwrap();
            publisher.join().unwrap();
            let mut after = None;
            let mut keys = Vec::new();
            let mut complete = false;
            // Three replay records, three rows, one terminal page.
            for _ in 0..7 {
                let page = ready.take().map(Some).unwrap_or_else(|| {
                    block_on(reader.page(if broken { None } else { after.as_ref() }, 1)).unwrap()
                });
                let Some(page) = page else {
                    continue;
                };
                if page.is_empty() {
                    complete = true;
                    break;
                }
                let file = &page[0];
                assert!(
                    after.as_ref().is_none_or(|old| old < &file.key),
                    "page cursor stalled"
                );
                after = Some(file.key.clone());
                keys.push(file.key.clone());
            }
            assert!(complete, "page cursor stalled");
            assert_eq!(keys, [key("a"), key("b"), key("c")]);
        });
    }
    #[test]
    fn paged_replay_and_enumeration_make_progress_through_publications() {
        enumeration(false);
    }
    #[test]
    #[should_panic(expected = "page cursor stalled")]
    fn negative_control_detects_restarting_a_page_cursor() {
        enumeration(true);
    }
}
