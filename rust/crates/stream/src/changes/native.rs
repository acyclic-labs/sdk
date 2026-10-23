//! The native producer writes directly into paid storage, without a temporary
//! protobuf tree or a second copy of each indexed key.
use super::*;
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use std::{io, sync::Arc};

fn field_len(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        0
    } else {
        1 + prost::encoding::encoded_len_varint(bytes.len() as u64) + bytes.len()
    }
}

fn entry_len(entry: &ChangeEntryRef<'_>) -> usize {
    field_len(entry.key) + field_len(entry.operation.as_bytes()) + field_len(entry.metadata)
}

fn field(buffer: &mut Vec<u8>, tag: u8, bytes: &[u8]) -> (usize, usize) {
    if bytes.is_empty() {
        return (buffer.len(), buffer.len());
    }
    buffer.push(tag);
    prost::encoding::encode_varint(bytes.len() as u64, buffer);
    let start = buffer.len();
    buffer.extend_from_slice(bytes);
    (start, buffer.len())
}

fn encoded_len(context: &[u8], entries: &[ChangeEntryRef<'_>]) -> Result<usize, StreamError> {
    if entries.is_empty()
        || entries.len() > crate::MAX_RECORD_KEYS
        || entries.iter().any(|entry| {
            entry.key.len() > crate::MAX_RECORD_KEY_BYTES
                || entry.operation.is_empty()
                || entry.operation.chars().any(char::is_control)
                || entry.metadata.len() > crate::MAX_RECORD_BYTES
                || entry.operation.len() > crate::MAX_RECORD_BYTES
        })
        || entries
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left.key >= right.key))
        || context.len() > crate::MAX_RECORD_BYTES
    {
        return Err(StreamError::InvalidArgument);
    }
    let mut total = field_len(context);
    let mut headers = ATTRIBUTE.len() + FORMAT.len();
    for entry in entries {
        let length = entry_len(entry);
        total += 1 + prost::encoding::encoded_len_varint(length as u64) + length;
        headers += entry.key.len();
    }
    if total + headers > crate::MAX_RECORD_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    Ok(total)
}

impl AppendRecord {
    /// Encodes provider-authored changes under the caller's original native
    /// meter. The value owns the key bytes; headers retain checked views into
    /// that same owner. All physical capacity is admitted before allocation.
    pub fn changes_native(
        context: &[u8],
        entries: &[ChangeEntryRef<'_>],
        meter: &Arc<dyn NativeReadMeter>,
    ) -> io::Result<Result<Self, StreamError>> {
        let length = match encoded_len(context, entries) {
            Ok(length) => length,
            Err(error) => return Ok(Err(error)),
        };
        meter.check()?;
        meter.admit_codec(length as u64, length as u64, 0)?;
        let mut allocation = ReadAllocation::new(meter, 0)?;
        let mut buffer = crate::native::vector(length, meter, &mut allocation)?;
        let mut ranges = [(0, 0); crate::MAX_RECORD_KEYS];
        field(&mut buffer, 10, context);
        for (entry, range) in entries.iter().zip(&mut ranges) {
            buffer.push(18);
            prost::encoding::encode_varint(entry_len(entry) as u64, &mut buffer);
            *range = field(&mut buffer, 10, entry.key);
            field(&mut buffer, 18, entry.operation.as_bytes());
            field(&mut buffer, 26, entry.metadata);
        }
        meter.record_codec(length as u64, length as u64, 0)?;
        let mut allocation = Some(allocation);
        let value =
            crate::OwnedBytes::from_vec_with_guard_in_place(&mut buffer, &mut allocation, meter)?;
        let mut allocation = ReadAllocation::new(meter, 0)?;
        let mut keys = crate::native::vector(entries.len(), meter, &mut allocation)?;
        for (start, end) in ranges.iter().take(entries.len()) {
            keys.push(value.slice(*start..*end));
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
        let headers = RecordHeaders::from_native_parts(keys, attributes, allocation, meter)?;
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
            bytes::Bytes::from_static(b"generation"),
            entries()
                .iter()
                .map(|entry| wire::ChangeEntry {
                    key: entry.key.into(),
                    operation: entry.operation.into(),
                    metadata: entry.metadata.into(),
                })
                .collect(),
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
