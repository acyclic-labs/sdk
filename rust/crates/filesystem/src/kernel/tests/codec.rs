use super::{CanonicalDecodeError, Decoder, EncodeAllocationStep, Encoder};

#[test]
fn canonical_object_prefix_preserves_original_copy_admission_and_stops_at_denial() {
    use super::ReadAllocationStep;
    use crate::storage::{ObjectId, ObjectKind, ObjectStorage};

    for storage in [
        None,
        Some(ObjectStorage {
            locator: crate::Digest::from_bytes([2; 32]),
            root_file_id: crate::FileId::from_bytes([3; 16]),
            origin_generation: crate::Digest::from_bytes([4; 32]),
        }),
    ] {
        let object = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: crate::Digest::from_bytes([1; 32]),
            storage,
        };
        let fields: &[u64] = if storage.is_some() {
            &[1, 32, 32, 16, 32]
        } else {
            &[1, 32]
        };
        let bytes = [object.to_bytes().as_slice(), b"following field"].concat();
        for denied in 0..=fields.len() {
            let mut decoder = Decoder {
                bytes: &bytes,
                cursor: 0,
            };
            let mut admitted = Vec::new();
            let mut completed = Vec::new();
            let result = decoder.any_object(&mut |step| {
                match step {
                    ReadAllocationStep::BeforeCopy(length) => {
                        if admitted.len() == denied {
                            return Err(CanonicalDecodeError::AllocationFailed);
                        }
                        admitted.push(length);
                    }
                    ReadAllocationStep::Copied(length) => completed.push(length),
                    _ => return Err(CanonicalDecodeError::WrongDomain),
                }
                Ok(())
            });
            assert_eq!(admitted, fields[..denied]);
            assert_eq!(completed, fields[..denied]);
            if denied == fields.len() {
                assert_eq!(result, Ok(object));
                assert_eq!(&bytes[decoder.cursor..], b"following field");
            } else {
                assert_eq!(result, Err(CanonicalDecodeError::AllocationFailed));
            }
        }
    }
}

#[test]
fn exact_capacity_accepts_only_the_complete_header_boundary() {
    let domain = b"domain";
    let minimum = domain.len() + 2;

    assert_eq!(
        Encoder::with_exact_capacity(
            domain,
            7,
            minimum,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        )
        .map(Encoder::finish),
        Ok([domain.as_slice(), &7_u16.to_le_bytes()].concat())
    );

    assert!(matches!(
        Encoder::with_exact_capacity(
            domain,
            7,
            minimum - 1,
            |_| Ok::<_, CanonicalDecodeError>(()),
            |_| CanonicalDecodeError::AllocationFailed
        ),
        Err(CanonicalDecodeError::LengthOverflow)
    ));
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_reservation_failure_keeps_actual_typed_reserve_cause() {
    let mut reserved = false;
    let result = Encoder::with_exact_capacity(
        b"domain",
        7,
        usize::MAX,
        |step| {
            match step {
                EncodeAllocationStep::Reserve { .. } => reserved = true,
                _ => panic!("failed reservation must not publish capacity or header work"),
            }
            Ok::<_, crate::FsError>(())
        },
        |error| {
            crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(
                error,
            ))
        },
    );
    assert!(reserved);
    assert!(matches!(
        result,
        Err(crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::AllocationFailed(_)
        ))
    ));
}

#[test]
fn decoder_rejects_version_and_bounded_field_overclaims_before_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let domain = b"domain";
    let mut wrong_version = Encoder::new(domain, 8);
    wrong_version.u8(0);
    assert_eq!(
        Decoder::new(&wrong_version.finish(), domain, 7, u64::MAX, &mut |_| Ok::<
            _,
            crate::kernel::CanonicalDecodeError,
        >(
            ()
        ))
        .err(),
        Some(CanonicalDecodeError::UnsupportedVersion {
            found: 8,
            expected: 7
        })
    );

    let mut oversized = Encoder::new(domain, 7);
    oversized.u32(5);
    oversized.fixed(b"value");
    let encoded = oversized.finish();
    let mut copying = Decoder::new(&encoded, domain, 7, u64::MAX, &mut |_| {
        Ok::<_, crate::kernel::CanonicalDecodeError>(())
    })?;
    assert_eq!(
        copying.bounded_bytes(4, &mut |_| Ok::<_, CanonicalDecodeError>(())),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 5,
            maximum: 4,
        })
    );
    let mut skipping = Decoder::new(&encoded, domain, 7, u64::MAX, &mut |_| {
        Ok::<_, crate::kernel::CanonicalDecodeError>(())
    })?;
    assert_eq!(
        skipping.skip_bounded_bytes(4),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 5,
            maximum: 4,
        })
    );
    Ok(())
}

#[path = "../../../tests/support/codec_properties.rs"]
mod codec_properties;

mod properties {
    use super::super::{CanonicalDecodeError, DecodeLimits, Decoder, Encoder};
    use super::codec_properties::{change, check_canonical};
    use crate::kernel::{decode_extent_page, decode_file_metadata, decode_tree_page};

    use proptest::prelude::*;

    const DOMAIN: &[u8] = b"PROPTEST";

    #[derive(Clone, Debug, PartialEq)]
    enum Field {
        U8(u8),
        U32(u32),
        U64(u64),
        I64(i64),
        OwnedBytes(Vec<u8>),
    }

    fn field() -> impl Strategy<Value = Field> {
        prop_oneof![
            any::<u8>().prop_map(Field::U8),
            any::<u32>().prop_map(Field::U32),
            any::<u64>().prop_map(Field::U64),
            any::<i64>().prop_map(Field::I64),
            prop::collection::vec(any::<u8>(), 0..12).prop_map(Field::OwnedBytes),
        ]
    }

    fn encode(fields: &[Field]) -> Result<Vec<u8>, CanonicalDecodeError> {
        let mut encoder = Encoder::new(DOMAIN, 3);
        for field in fields {
            match field {
                Field::U8(value) => encoder.u8(*value),
                Field::U32(value) => encoder.u32(*value),
                Field::U64(value) => encoder.u64(*value),
                Field::I64(value) => encoder.i64(*value),
                Field::OwnedBytes(value) => encoder.bounded_bytes(value)?,
            }
        }
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8], schema: &[Field]) -> Result<Vec<Field>, CanonicalDecodeError> {
        let mut decoder = Decoder::new(bytes, DOMAIN, 3, 256, &mut |_| {
            Ok::<_, crate::kernel::CanonicalDecodeError>(())
        })?;
        let mut fields = Vec::new();
        for field in schema {
            fields.push(match field {
                Field::U8(_) => Field::U8(
                    decoder.u8(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
                ),
                Field::U32(_) => Field::U32(
                    decoder.u32(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
                ),
                Field::U64(_) => Field::U64(
                    decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
                ),
                Field::I64(_) => Field::I64(
                    decoder.i64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?,
                ),
                Field::OwnedBytes(_) => {
                    Field::OwnedBytes(decoder.bounded_bytes(12, &mut |_| {
                        Ok::<_, crate::kernel::CanonicalDecodeError>(())
                    })?)
                }
            });
        }
        decoder.finish()?;
        Ok(fields)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn primitive_fields_round_trip_and_reject_noncanonical_bytes(
            fields in prop::collection::vec(field(), 0..8),
            change in change(),
        ) {
            check_canonical(&fields, |fields| encode(fields), |bytes| decode(bytes, &fields), &change)?;
        }

        #[test]
        fn decoders_never_panic_on_arbitrary_bytes(
            bytes in prop::collection::vec(any::<u8>(), 0..96),
            schema in prop::collection::vec(field(), 0..8),
        ) {
            let limits = DecodeLimits::default();
            let _ = decode(&bytes, &schema);
            let _ = decode_tree_page(&bytes, limits, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()));
            let _ = decode_extent_page(&bytes, limits, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()));
            let _ = decode_file_metadata(&bytes, limits, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()));
        }


    }
}
