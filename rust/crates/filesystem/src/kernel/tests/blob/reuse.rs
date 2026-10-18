struct ReuseSource<'a> {
    bytes: &'a [u8],
    offset: usize,
}

thread_local! {
    static FORCE_PRIOR_MATCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn force_prior_match() -> bool {
    FORCE_PRIOR_MATCH.get()
}

struct PriorMatchControl;
impl Drop for PriorMatchControl {
    fn drop(&mut self) {
        FORCE_PRIOR_MATCH.set(false);
    }
}

impl AsyncBlobSource for ReuseSource<'_> {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        _budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        cancellation
            .check()
            .map_err(|_| std::io::ErrorKind::Interrupted)?;
        acyclic_test_support::differential::record("source-read");
        let count = destination.len().min(self.bytes.len() - self.offset);
        destination[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}

#[test]
fn reuse_matches_independent_staging_for_leaf_and_multilevel_changes() {
    use acyclic_test_support::differential;
    use proptest::prelude::*;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(64));
    runner
        .run(
            &(
                prop::collection::vec(any::<u8>(), 1..65),
                1u32..9,
                2u32..5,
                any::<usize>(),
            ),
            |(bytes, chunk_bytes, width, edit)| {
                let options = BlobBuildOptions {
                    chunk_bytes,
                    page_items: width,
                    page_bytes: 256,
                    maximum_blob_bytes: bytes.len() as u64,
                };
                let limits = DecodeLimits {
                    maximum_page_items: width,
                    maximum_page_bytes: 256,
                    ..DecodeLimits::default()
                };
                let mut changed = bytes.clone();
                if edit % 2 == 0 {
                    let index = edit % changed.len();
                    changed[index] ^= 1;
                }
                let paired = differential::pair::<_, _, &'static str>(
                    || {
                        let store = MemoryObjectStore::default();
                        let cancellation = CancellationToken::new();
                        let prior = runtime
                            .block_on(build_blob(
                                &store,
                                &mut std::io::Cursor::new(&bytes),
                                options,
                                WorkBudget::UNBOUNDED,
                                &cancellation,
                            ))
                            .unwrap();
                        (store, cancellation, prior)
                    },
                    |(store, cancellation, prior), old| {
                        let mut accounting = crate::kernel::KernelAccounting::standalone(
                            WorkBudget {
                                source_bytes_read: bytes.len() as u64,
                                ..WorkBudget::UNBOUNDED
                            },
                            &cancellation,
                        );
                        let mut source = ReuseSource {
                            bytes: &changed,
                            offset: 0,
                        };
                        let built = if old {
                            runtime.block_on(build_blob_with_accounting(
                                &store,
                                &mut source,
                                options,
                                &mut accounting,
                                &cancellation,
                            ))
                        } else {
                            runtime.block_on(reuse_blob_with_accounting(
                                &store,
                                &mut source,
                                options,
                                (prior.root, limits),
                                &mut accounting,
                                &cancellation,
                            ))
                        }
                        .unwrap();
                        let work = accounting.spent();
                        let output = runtime
                            .block_on(read_blob_range(
                                &store,
                                built.root,
                                ByteRange {
                                    offset: 0,
                                    length: bytes.len() as u64,
                                },
                                limits,
                                WorkBudget::UNBOUNDED,
                                &cancellation,
                            ))
                            .unwrap();
                        (
                            built.root,
                            output.bytes.as_ref().to_vec(),
                            source.offset,
                            work.source_bytes_read,
                            work.object_bytes_written,
                        )
                    },
                );
                let [(old, old_trace), (new, trace)] = paired;
                prop_assert_eq!(
                    (&old.0, &old.1, old.2, old.3),
                    (&new.0, &new.1, new.2, new.3)
                );
                prop_assert_eq!(&new.1, &changed);
                prop_assert_eq!(new.2, bytes.len());
                prop_assert_eq!(new.3, bytes.len() as u64);
                prop_assert_eq!(trace, old_trace);
                if bytes == changed {
                    prop_assert_eq!(new.4, 0);
                } else {
                    prop_assert!(new.4 > 0);
                }
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn pretending_a_changed_chunk_matches_fails_the_complete_output_oracle() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let cancellation = CancellationToken::new();
    let options = BlobBuildOptions {
        chunk_bytes: 2,
        page_items: 2,
        page_bytes: 256,
        maximum_blob_bytes: 9,
    };
    let initialize = || {
        let store = MemoryObjectStore::default();
        let prior = runtime
            .block_on(build_blob(
                &store,
                &mut std::io::Cursor::new(b"abcdefghi"),
                options,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .unwrap();
        (store, prior.root)
    };
    let (baseline, _) = initialize();
    let expected = runtime
        .block_on(build_blob(
            &baseline,
            &mut std::io::Cursor::new(b"abcdXfghi"),
            options,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .unwrap();
    let (candidate, prior) = initialize();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let broken = {
        FORCE_PRIOR_MATCH.set(true);
        let _control = PriorMatchControl;
        runtime
            .block_on(reuse_blob_with_accounting(
                &candidate,
                &mut std::io::Cursor::new(b"abcdXfghi"),
                options,
                (prior, DecodeLimits::default()),
                &mut accounting,
                &cancellation,
            ))
            .unwrap()
    };
    assert_eq!(broken.root, expected.root);
    let read = |store: &MemoryObjectStore, root| {
        runtime
            .block_on(read_blob_range(
                store,
                root,
                ByteRange {
                    offset: 0,
                    length: 9,
                },
                DecodeLimits::default(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .map(|read| read.bytes.as_ref().to_vec())
    };
    assert_eq!(read(&baseline, expected.root).unwrap(), b"abcdXfghi");
    assert!(read(&candidate, broken.root).is_err());
}
