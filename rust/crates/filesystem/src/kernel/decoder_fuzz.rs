//! Shared byte-input oracle for the real canonical decoder accounting boundary.
use super::allocation::KernelAccounting;
use super::codec::{CanonicalInvariant, DecodedPageKind, DecodedPageShape, ReadAllocationStep};
use super::file_read::OriginalReadAccounting;
use super::*;
use crate::storage::{ObjectId, ObjectKind};
use crate::{CancellationToken, FsError, WorkBudget, WorkCounters};
use {
    crate::foundation::{Digest, FileId, VolumeId},
    acyclic_fs_format::object_reference::GenerationId,
};

pub mod records;

/// Re-encodes an admitted value and requires the encoding to decode to it.
/// `PLANTED` flips one canonical byte: the negative control for this oracle.
pub(crate) fn canonical<
    T: PartialEq + std::fmt::Debug,
    E: std::fmt::Debug + std::error::Error + 'static,
    const PLANTED: bool,
>(
    value: &T,
    encoded: Result<Vec<u8>, E>,
    decode: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if !(encoded.is_ok()) {
        return Err("an admitted value has a canonical encoding".into());
    };
    let mut canonical =
        encoded.map_err(|error| format!("canonical encoding refused: {error:?}"))?;
    if PLANTED {
        if !(!canonical.is_empty()) {
            return Err("encodings carry a header".into());
        };
        if let Some(last) = canonical.last_mut() {
            *last ^= 1;
        }
    }
    let decoded = decode(&canonical);
    if !(decoded.is_ok()) {
        return Err("a canonical encoding decodes".into());
    };
    let decoded = decoded.map_err(|error| format!("canonical decoding refused: {error:?}"))?;
    if !(&(&decoded) == &(value)) {
        return Err("canonical encoding changes the decoded value".into());
    };
    Ok(canonical)
}

/// One persisted page or object kind per selector, in decoder-oracle order.
const KINDS: u8 = 6;

fn decode<const PLANTED: bool>(
    kind: u8,
    bytes: &[u8],
    limits: DecodeLimits,
    prefix: usize,
    observe: &mut impl FnMut(ReadAllocationStep) -> Result<(), FsError>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let allocation_error = |_| CanonicalDecodeError::AllocationFailed.into();
    let items = limits.maximum_page_items;
    Ok(match kind {
        0 => {
            let value = decode_file_metadata(bytes, limits, observe)?;
            canonical::<_, _, PLANTED>(
                &value,
                encode_file_metadata(value, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_file_metadata(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )?
        }
        1 => {
            let value = decode_generation_root(bytes, limits, observe)?;
            canonical::<_, _, PLANTED>(
                &value,
                encode_generation_root(&value, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_generation_root(bytes, limits, |_| Ok::<_, FsError>(())),
            )?
        }
        2 => {
            let value = decode_file_table_page(bytes, limits, observe)?;
            canonical::<_, _, PLANTED>(
                &value,
                encode_file_table_page(&value, items, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_file_table_page(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )?
        }
        3 => {
            let value = decode_tree_page(bytes, limits, observe)?;
            canonical::<_, _, PLANTED>(
                &value,
                encode_tree_page(&value, items, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_tree_page(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )?
        }
        4 => {
            // Shape admission is a separate production entry point. Exercise
            // arbitrary bytes even when full decoding or its admission fails.
            let shape = super::extent::decode_extent_page_shape(bytes, limits);
            let decoded = decode_extent_page(bytes, limits, observe);
            if let Err(FsError::Decode(error)) = &decoded
                && matches!(
                    error,
                    CanonicalDecodeError::WrongDomain
                        | CanonicalDecodeError::UnsupportedVersion { .. }
                        | CanonicalDecodeError::ObjectTooLarge { .. }
                        | CanonicalDecodeError::FieldTooLarge { .. }
                        | CanonicalDecodeError::UnknownTag { .. }
                        | CanonicalDecodeError::TrailingBytes
                        | CanonicalDecodeError::Invariant(CanonicalInvariant::Static(_))
                )
            {
                // Full-only extent semantics and injected Truncated refusals
                // do not establish a structural shape-decoder error.
                if !(&(shape) == &(Err(error.clone()))) {
                    return Err("extent shape refusal differs from the full decoder".into());
                };
            }
            let value = decoded?;
            let (kind, count) = match &value {
                ExtentPage::Leaf(items) => (DecodedPageKind::Leaf, items.len()),
                ExtentPage::Internal(items) => (DecodedPageKind::Internal, items.len()),
            };
            if !(&(shape)
                == &(Ok(DecodedPageShape {
                    kind,
                    items: count,
                    nested_bytes: 0,
                })))
            {
                return Err("extent shape differs from the fully validated page".into());
            };
            if !(super::extent::decode_extent_page_shape(&bytes[..prefix % bytes.len()], limits)
                .is_err())
            {
                return Err("extent shape admitted a truncated frame".into());
            };
            let mut trailing = bytes.to_vec();
            trailing.push(0);
            if !(super::extent::decode_extent_page_shape(&trailing, limits).is_err()) {
                return Err("extent shape admitted trailing bytes".into());
            };
            let version = [0_u16, 2, u16::MAX][prefix % 3];
            let mut header = bytes[..10].to_vec();
            header[8..10].copy_from_slice(&version.to_le_bytes());
            if !(&(super::extent::decode_extent_page_shape(&header, limits))
                == &(Err(CanonicalDecodeError::UnsupportedVersion {
                    found: version,
                    expected: 1,
                })))
            {
                return Err("extent shape did not retain version refusal".into());
            };
            canonical::<_, _, PLANTED>(
                &value,
                encode_extent_page(&value, items, |_| Ok::<_, FsError>(()), allocation_error),
                |bytes| decode_extent_page(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )?
        }
        _ => {
            let value = bindings::decode_binding_page(bytes, limits, observe)?;
            let page = match &value {
                persistent_btree::Page::Leaf(entries) => persistent_btree::PageRef::Leaf(entries),
                persistent_btree::Page::Internal(children) => {
                    persistent_btree::PageRef::Internal(children)
                }
            };
            canonical::<_, _, PLANTED>(
                &value,
                bindings::encode_binding_page(
                    &page,
                    items,
                    |_| Ok::<_, FsError>(()),
                    allocation_error,
                ),
                |bytes| bindings::decode_binding_page(bytes, limits, &mut |_| Ok::<_, FsError>(())),
            )?
        }
    })
}

/// Fuzz bytes, limits, cancellation and injected before-effect decoder failures.
pub fn check(input: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    records::check(input)?;
    let mut number = [0; 8];
    let count = input.len().min(number.len());
    number[..count].copy_from_slice(&input[..count]);
    let number = u64::from_le_bytes(number);
    let payload = &input[..input.len().min(MAXIMUM_INLINE_FILE_BYTES)];
    for kind in 0..KINDS {
        let mut canonical = vec![kind, 255, 255, 0, 0, 0];
        canonical.extend_from_slice(&fixture(kind, payload, number)?);
        check_inner::<false>(&canonical)?;
    }
    match check_inner::<false>(input) {
        Ok(()) => Ok(()),
        Err(error) if error.is::<FsError>() || error.is::<CanonicalDecodeError>() => Ok(()),
        Err(error) => Err(error),
    }
}

fn fixture(kind: u8, payload: &[u8], number: u64) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    Ok(match kind {
        0 => encode_file_metadata(
            FileMetadata {
                posix_uid: MetadataField::Value(number as u32),
                created_ns: MetadataField::Value(number as i64),
                ..FileMetadata::default()
            },
            |_| Ok::<_, FsError>(()),
            |_| CanonicalDecodeError::AllocationFailed.into(),
        )?,
        1 => encode_generation_root(
            &GenerationRoot {
                volume_id: VolumeId::from_bytes([1; 16]),
                root_file_id: FileId::from_bytes([2; 16]),
                file_table: ObjectId {
                    storage: None,
                    kind: ObjectKind::FileTablePage,
                    digest: Digest::from_bytes([3; 32]),
                },
                parents: (0..number % 3)
                    .map(|index| GenerationId::new(Digest::from_bytes([index as u8; 32])))
                    .collect(),
                required_features: 0,
            },
            |_| Ok::<_, FsError>(()),
            |_| CanonicalDecodeError::AllocationFailed.into(),
        )?,
        2 => encode_file_table_page(
            &FileTablePage::Leaf(vec![FileRecord {
                bindings: None,
                file_id: FileId::from_bytes([2; 16]),
                kind: FileKind::Regular,
                link_count: 1,
                metadata: ObjectId {
                    storage: None,
                    kind: ObjectKind::Metadata,
                    digest: Digest::from_bytes([3; 32]),
                },
                payload: FilePayload::InlineRegular(InlineFileData::new(payload)?),
            }]),
            32,
            |_| Ok::<_, FsError>(()),
            |_| CanonicalDecodeError::AllocationFailed.into(),
        )?,
        3 => encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, format!("n{number:016x}").into_bytes())?,
                file_id: FileId::from_bytes([2; 16]),
                kind: FileKind::Regular,
            }]),
            32,
            |_| Ok::<_, FsError>(()),
            |_| CanonicalDecodeError::AllocationFailed.into(),
        )?,
        4 => encode_extent_page(
            &ExtentPage::Leaf(vec![Extent {
                offset: 0,
                length: number % 65 + 1,
                kind: ExtentKind::Hole,
            }]),
            32,
            |_| Ok::<_, FsError>(()),
            |_| CanonicalDecodeError::AllocationFailed.into(),
        )?,
        _ => {
            let binding = |parent: u8,
                           name: String|
             -> Result<bindings::Binding, Box<dyn std::error::Error>> {
                Ok(bindings::Binding {
                    parent: FileId::from_bytes([parent; 16]),
                    name: LogicalName::new(NameEncoding::Utf8, name.into_bytes())?,
                })
            };
            let first = binding(2, format!("n{number:016x}"))?;
            let leaf = [first.clone(), binding(3, format!("p{}", payload.len()))?];
            let children = [persistent_btree::Child {
                first,
                page: ObjectId {
                    storage: None,
                    kind: ObjectKind::BindingPage,
                    digest: Digest::from_bytes([4; 32]),
                },
            }];
            let page = if number % 2 == 0 {
                persistent_btree::PageRef::Leaf(&leaf)
            } else {
                persistent_btree::PageRef::Internal(&children)
            };
            bindings::encode_binding_page(
                &page,
                32,
                |_| Ok::<_, FsError>(()),
                |_| CanonicalDecodeError::AllocationFailed.into(),
            )?
        }
    })
}

fn check_inner<const PLANTED: bool>(input: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let Some((&kind, input)) = input.split_first() else {
        return Err(CanonicalDecodeError::Truncated.into());
    };
    let Some(controls) = input.get(..5) else {
        return Err(CanonicalDecodeError::Truncated.into());
    };
    let bytes = &input[5..];
    let maximum = u64::from(u16::from_le_bytes([controls[0], controls[1]]));
    let mode = controls[2] % 5;
    let budget = WorkBudget {
        bytes_copied: maximum,
        ..WorkBudget::UNBOUNDED
    };
    let cancellation = CancellationToken::new();
    #[cfg(not(target_arch = "wasm32"))]
    let original = (mode != 0)
        .then(|| {
            crate::NativeMaterializationWork::new(
                if mode == 2 || mode == 4 {
                    WorkBudget::UNBOUNDED
                } else {
                    budget
                },
                &cancellation,
            )
            .map_err(|(error, _)| error)
        })
        .transpose();
    #[cfg(not(target_arch = "wasm32"))]
    if !(original.is_ok()) {
        return Err("original work setup failed".into());
    };
    #[cfg(not(target_arch = "wasm32"))]
    let original = original?;
    #[cfg(not(target_arch = "wasm32"))]
    let context = original
        .as_ref()
        .map(
            |original| -> Result<_, crate::NativeTreeMaterializationError> {
                let work = if mode == 2 || mode == 4 {
                    original.native_volume_work(budget)?
                } else {
                    original.clone()
                };
                if mode >= 3 {
                    Ok(work.recheck_admitted())
                } else {
                    Ok(work)
                }
            },
        )
        .transpose();
    #[cfg(not(target_arch = "wasm32"))]
    if !(context.is_ok()) {
        return Err("native work setup failed".into());
    };
    #[cfg(not(target_arch = "wasm32"))]
    let context = context?;
    #[cfg(not(target_arch = "wasm32"))]
    if !(mode == 0 || context.is_some()) {
        return Err("native context missing".into());
    };
    let mut accounting = match mode {
        #[cfg(not(target_arch = "wasm32"))]
        1..=4 => KernelAccounting::native(
            context
                .as_ref()
                .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?,
            &cancellation,
        ),
        _ => KernelAccounting::standalone(budget, &cancellation),
    };
    let baseline = accounting.spent()?;
    let observed = OriginalReadAccounting::new(&mut accounting);
    if !(observed.is_ok()) {
        return Err("read accounting setup failed".into());
    };
    let mut observed = observed?;
    let mut actual = WorkCounters::UNCHARGED;
    let mut pending = (0, 0, 0);
    let mut admissions = 0_u8;
    let limits = DecodeLimits {
        maximum_object_bytes: 4096,
        maximum_page_bytes: 4096,
        maximum_page_items: 32,
        ..DecodeLimits::default()
    };
    let prefix = usize::from(u16::from_le_bytes([controls[3], controls[4]]));
    if kind % KINDS == 2 {
        if !(super::file_table::check_fuzz::<true>(bytes, limits, prefix).is_ok()) {
            return Err("filesystem oracle invariant failed".into());
        };
    }
    let mut oracle_failure = None;
    let decoded = decode::<PLANTED>(kind % KINDS, bytes, limits, prefix, &mut |step| {
        let before = matches!(
            step,
            ReadAllocationStep::BeforeCopy(_)
                | ReadAllocationStep::BeforeHash(_)
                | ReadAllocationStep::BeforeItem
                | ReadAllocationStep::Allocate { .. }
                | ReadAllocationStep::CheckCopy(_)
        );
        if before {
            if controls[3] & 1 != 0 && admissions == controls[4] {
                return Err(CanonicalDecodeError::Truncated.into());
            }
            admissions = admissions.wrapping_add(1);
        }
        // A completion is an actual effect even if its observer returns an
        // error. Track the effect independently of the production receipt.
        match &step {
            ReadAllocationStep::Copied(n) => {
                if !(*n <= pending.0) {
                    oracle_failure = Some("copy without a successful admission");
                    return Err(CanonicalDecodeError::Truncated.into());
                };
                pending.0 -= *n;
                actual.bytes_copied += *n;
            }
            ReadAllocationStep::Hashed(n) => {
                if !(*n <= pending.1) {
                    oracle_failure = Some("hash without a successful admission");
                    return Err(CanonicalDecodeError::Truncated.into());
                };
                pending.1 -= *n;
                actual.bytes_hashed += *n;
            }
            ReadAllocationStep::Item => {
                if !(pending.2 > 0) {
                    oracle_failure = Some("item without a successful admission");
                    return Err(CanonicalDecodeError::Truncated.into());
                };
                pending.2 -= 1;
                actual.items_examined += 1;
            }
            _ => {}
        }
        let reserved = match &step {
            ReadAllocationStep::BeforeCopy(n) => (*n, 0, 0),
            ReadAllocationStep::BeforeHash(n) => (0, *n, 0),
            ReadAllocationStep::BeforeItem => (0, 0, 1),
            _ => (0, 0, 0),
        };
        observed.observe(step)?;
        pending.0 += reserved.0;
        pending.1 += reserved.1;
        pending.2 += reserved.2;
        if before && controls[3] & 2 != 0 && admissions == controls[4] {
            cancellation.cancel();
        }
        Ok(())
    });
    if let Some(error) = oracle_failure {
        return Err(error.into());
    }
    if let Ok(canonical) = &decoded {
        if !(&(canonical) == &(bytes)) {
            return Err("canonical round trip changes wire bytes".into());
        };
        if !(&(actual.bytes_copied) == &((canonical.len() - 8) as u64)) {
            return Err("all canonical body bytes must have completion callbacks".into());
        };
    }
    let result = decoded.map(|_| ());
    let retired = observed.retire();
    if !(retired.is_ok()) {
        return Err("read accounting retirement failed".into());
    };
    retired?;
    let receipt = accounting.spent()?;
    if !(&(receipt.bytes_copied - baseline.bytes_copied) == &(actual.bytes_copied)) {
        return Err("missing copy completion callback".into());
    };
    if !(&(receipt.bytes_hashed - baseline.bytes_hashed) == &(actual.bytes_hashed)) {
        return Err("filesystem oracle invariant failed".into());
    };
    let recheck = cfg!(not(target_arch = "wasm32")) && mode >= 3;
    if !(&(receipt.items_examined - baseline.items_examined)
        == &(if recheck { 0 } else { actual.items_examined }))
    {
        return Err("filesystem oracle invariant failed".into());
    };
    if !(actual.bytes_copied <= maximum) {
        return Err("denied copy effect executed".into());
    };
    result
}

#[cfg(test)]
#[cfg(not(loom))]
#[cfg(any(not(unit_test_split), unit_test_kernel))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn arbitrary_lengths_truncation_and_before_effect_errors_are_bounded(
            bytes in prop::collection::vec(any::<u8>(), 0..512),
            controls in any::<[u8; 6]>(),
        ) {
            let mut input = controls.to_vec(); input.extend_from_slice(&bytes);
            check(&input).expect("decoder oracle");
        }

        #[test]
        fn canonical_round_trips_keep_exact_completed_work_and_denied_effects(
            kind in 0..KINDS,
            payload in prop::collection::vec(any::<u8>(), 0..65),
            number in any::<u64>(),
            maximum in 0_u16..192,
            fault in any::<u8>(), index in 0_u8..32,
        ) {
            let bytes = fixture(kind, &payload, number).expect("writer fixture");
            for mode in [0,
                #[cfg(not(target_arch = "wasm32"))] 1,
                #[cfg(not(target_arch = "wasm32"))] 2,
                #[cfg(not(target_arch = "wasm32"))] 3,
                #[cfg(not(target_arch = "wasm32"))] 4,
            ] {
                let mut input = vec![kind, 255, 255, mode, 0, 0];
                input.extend_from_slice(&bytes);
                let result = check_inner::<false>(&input);
                prop_assert!(result.is_ok(), "{result:?}");
                input[1..3].copy_from_slice(&maximum.to_le_bytes());
                input[4] = fault; input[5] = index;
                check(&input).expect("decoder oracle");
                // Every truncation of a valid object reaches the real decoder,
                // with an independently witnessed exact completion receipt.
                input[1..3].fill(255); input[4] = 0;
                for length in 0..bytes.len() { check(&input[..6 + length]).expect("decoder oracle"); }
            }
        }
    }

    #[test]
    fn canonical_writer_corpus_reaches_the_shared_differential_oracles() {
        for (index, bytes) in [
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-empty-leaf").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-plain-internal").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-sealed-internal").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-plain-leaf").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-sealed-leaf").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-empty").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-populated").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-populated-sealed").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-internal-lower-bounds")
                .as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-internal-lower-bounds-sealed")
                .as_slice(),
        ]
        .into_iter()
        .enumerate()
        {
            let result = check_inner::<false>(bytes);
            assert!(
                result.is_ok(),
                "writer-derived seed {index} refused: {result:?}"
            );
        }
        // Preserve old producer bytes as rejected inputs after format changes.
        // Positive seeds above reuse the registered canonical writer captures.
        for bytes in [
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-internal").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-single-fifo").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/inline-file-table").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-trailing").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/file-table-unordered").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/truncated-inline-payload").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-unknown-page-kind").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-unknown-extent-kind").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-wrong-blob-kind").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-internal").as_slice(),
            include_bytes!("../../fuzz/corpus/decoder_work/extents-internal-sealed").as_slice(),
        ] {
            assert!(
                check_inner::<false>(bytes).is_err(),
                "malformed seed admitted"
            );
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn the_committed_corpus_replays_and_admits_its_canonical_binding_pages() {
        let corpus = acyclic_test_support::crate_path!("fuzz/corpus/decoder_work");
        for entry in std::fs::read_dir(corpus).unwrap() {
            let path = entry.unwrap().path();
            let input = std::fs::read(&path).unwrap();
            let admitted = check_inner::<false>(&input);
            let name = path.file_name().unwrap().to_string_lossy();
            if matches!(
                name.as_ref(),
                "binding-leaf" | "binding-internal" | "binding-sealed-internal"
            ) {
                assert!(admitted.is_ok(), "{name}: {admitted:?}");
            }
        }
    }

    #[test]
    fn a_planted_encoder_byte_is_detected_for_every_kind() {
        for kind in 0..KINDS {
            for number in [0, 1] {
                let mut input = vec![kind, 255, 255, 0, 0, 0];
                input.extend_from_slice(&fixture(kind, b"a", number).expect("writer fixture"));
                assert!(check_inner::<false>(&input).is_ok());
                let panic = std::panic::catch_unwind(|| {
                    check_inner::<true>(&input).expect("decoder oracle invariant")
                })
                .unwrap_err();
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("");
                assert!(
                    message.contains("a canonical encoding decodes")
                        || message.contains("canonical encoding changes the decoded value"),
                    "kind {kind}: {message}"
                );
            }
        }
    }
}
