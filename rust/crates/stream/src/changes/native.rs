//! The native producer writes directly into paid storage, without a temporary
//! protobuf tree. Its headers' keys live in one paid buffer of their own.
use super::*;
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use std::{io, sync::Arc};

impl AppendRecord {
    /// Encodes provider-authored changes under the caller's original native
    /// meter. All physical capacity is admitted before allocation.
    pub fn changes_native(
        context: &[u8],
        entries: &[ChangeEntryRef<'_>],
        meter: &Arc<dyn NativeReadMeter>,
    ) -> io::Result<Result<Self, StreamError>> {
        let length = match changes_encoded_len(context, entries) {
            Ok(length) => length,
            Err(error) => return Ok(Err(error)),
        };
        let key_bytes = entries.iter().map(|entry| entry.key.len()).sum::<usize>();
        meter.check()?;
        meter.admit_codec(length as u64, (length + key_bytes) as u64, 0)?;
        let mut allocation = ReadAllocation::new(meter, 0)?;
        let mut buffer = crate::native::vector(length, meter, &mut allocation)?;
        encode_into(&mut buffer, context, entries);
        let mut key_allocation = ReadAllocation::new(meter, 0)?;
        let mut key_buffer = crate::native::vector(key_bytes, meter, &mut key_allocation)?;
        let mut ranges = [(0, 0); crate::MAX_RECORD_KEYS];
        for (entry, range) in entries.iter().zip(&mut ranges) {
            let start = key_buffer.len();
            key_buffer.extend_from_slice(entry.key);
            *range = (start, key_buffer.len());
        }
        meter.record_codec(length as u64, (length + key_bytes) as u64, 0)?;
        let mut allocation = Some(allocation);
        let value =
            crate::OwnedBytes::from_vec_with_guard_in_place(&mut buffer, &mut allocation, meter)?;
        let mut key_allocation = Some(key_allocation);
        let key_owner = crate::OwnedBytes::from_vec_with_guard_in_place(
            &mut key_buffer,
            &mut key_allocation,
            meter,
        )?;
        let mut allocation = ReadAllocation::new(meter, 0)?;
        let mut keys = crate::native::vector(entries.len(), meter, &mut allocation)?;
        for (start, end) in ranges.iter().take(entries.len()) {
            keys.push(key_owner.slice(*start..*end));
        }
        let mut attributes = crate::native::vector(1, meter, &mut allocation)?;
        let mut name = crate::native::vector(ATTRIBUTE.len(), meter, &mut allocation)?;
        meter.admit_codec(0, ATTRIBUTE.len() as u64, 0)?;
        name.extend_from_slice(ATTRIBUTE.as_bytes());
        meter.record_codec(0, ATTRIBUTE.len() as u64, 0)?;
        attributes.push(Attribute {
            name: String::from_utf8(name).map_err(|_| io::ErrorKind::InvalidData)?,
            value: crate::OwnedBytes::from_static(FORMAT),
        });
        let headers = match RecordHeaders::from_native_parts(keys, attributes, allocation, meter)? {
            Ok(headers) => headers,
            Err(error) => return Ok(Err(error)),
        };
        meter.check()?;
        Ok(validate(&value, &headers).map(|()| Self { value, headers }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_meter::Meter;
    use std::sync::atomic::Ordering;

    fn entries() -> [ChangeEntryRef<'static>; 2] {
        [
            ChangeEntryRef {
                key: b"",
                operation: "write",
                metadata: b"root",
            },
            ChangeEntryRef {
                key: b"nested/file",
                operation: "delete",
                metadata: b"old",
            },
        ]
    }

    #[test]
    fn native_encoding_matches_the_contract_and_keeps_key_views_paid()
    -> Result<(), Box<dyn std::error::Error>> {
        let observed = Arc::new(Meter::default());
        let meter = observed.original();
        let record = AppendRecord::changes_native(b"generation", &entries(), &meter)??;
        let expected = AppendRecord::changes(
            b"generation",
            &entries()
                .iter()
                .map(|entry| wire::ChangeEntry {
                    key: entry.key.into(),
                    operation: entry.operation.into(),
                    metadata: entry.metadata.into(),
                })
                .collect::<Vec<_>>(),
            vec![],
        )?;
        assert_eq!(record, expected);
        assert!(record.value.matches_meter(&meter));
        assert!(
            record
                .headers
                .keys()
                .iter()
                .all(|key| key.matches_meter(&meter))
        );
        assert!(observed.held.load(Ordering::SeqCst) >= record.value.len() as u64);
        let headers = record.headers.clone();
        drop(record);
        assert!(observed.held.load(Ordering::SeqCst) > 0);
        let aliases = headers.keys().to_vec();
        drop(headers);
        assert_eq!(
            aliases.last().ok_or("missing key")?.as_ref(),
            b"nested/file"
        );
        assert!(observed.held.load(Ordering::SeqCst) > 0);
        drop(aliases);
        assert_eq!(observed.held.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn every_native_allocation_denial_retires_completed_storage()
    -> Result<(), Box<dyn std::error::Error>> {
        let observed = Arc::new(Meter::default());
        drop(AppendRecord::changes_native(
            b"generation",
            &entries(),
            &observed.original(),
        )??);
        let allocations = observed.allocations.load(Ordering::SeqCst);
        assert!(
            allocations > 0,
            "unfunded encoding would bypass every denial"
        );
        for limit in 0..allocations {
            let observed = Arc::new(Meter::default());
            observed.limit.store(limit, Ordering::SeqCst);
            assert!(
                AppendRecord::changes_native(b"generation", &entries(), &observed.original())
                    .is_err()
            );
            assert_eq!(observed.held.load(Ordering::SeqCst), 0);
            observed.assert_refunded();
        }
        for after_copy in [false, true] {
            let observed = Arc::new(Meter::default());
            if after_copy {
                observed.cancel_on_copy.store(true, Ordering::SeqCst);
            } else {
                observed.cancelled.store(true, Ordering::SeqCst);
            }
            assert!(
                AppendRecord::changes_native(b"generation", &entries(), &observed.original())
                    .is_err()
            );
            assert_eq!(observed.held.load(Ordering::SeqCst), 0);
        }
        Ok(())
    }

    #[test]
    fn invalid_native_changes_are_rejected_before_allocating() {
        for entries in [
            vec![],
            vec![ChangeEntryRef {
                key: b"a",
                operation: "",
                metadata: b"",
            }],
            vec![ChangeEntryRef {
                key: b"a",
                operation: "write\n",
                metadata: b"",
            }],
            vec![
                ChangeEntryRef {
                    key: b"b",
                    operation: "write",
                    metadata: b"",
                },
                ChangeEntryRef {
                    key: b"a",
                    operation: "write",
                    metadata: b"",
                },
            ],
        ] {
            let observed = Arc::new(Meter::default());
            assert!(matches!(
                AppendRecord::changes_native(b"", &entries, &observed.original()),
                Ok(Err(_))
            ));
            assert_eq!(observed.allocations.load(Ordering::SeqCst), 0);
        }
    }
}
