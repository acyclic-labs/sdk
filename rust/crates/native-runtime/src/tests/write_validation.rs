use super::*;

#[test]
fn write_validation_matches_pairwise_overlap_for_all_small_batches() {
    let writes: Vec<_> = [0, 1, 2, 5, u64::MAX - 1, u64::MAX]
        .into_iter()
        .flat_map(|offset| {
            [b"".as_slice(), b"a", b"ab", b"abcd"]
                .into_iter()
                .map(move |bytes| OwnedWrite {
                    offset,
                    bytes: Bytes::from_static(bytes),
                })
        })
        .collect();
    assert!(validate_write_batch(&[]).is_ok());
    for first in &writes {
        check(std::slice::from_ref(first));
        for second in &writes {
            check(&[first.clone(), second.clone()]);
            for third in &writes {
                check(&[first.clone(), second.clone(), third.clone()]);
            }
        }
    }
}

fn check(writes: &[OwnedWrite]) {
    let invalid_range = writes.iter().any(|write| {
        write.offset == u64::MAX || write.offset.checked_add(write.bytes.len() as u64).is_none()
    });
    let overlap = writes.iter().enumerate().any(|(index, left)| {
        writes.iter().skip(index + 1).any(|right| {
            !left.bytes.is_empty()
                && !right.bytes.is_empty()
                && u128::from(left.offset) < u128::from(right.offset) + right.bytes.len() as u128
                && u128::from(right.offset) < u128::from(left.offset) + left.bytes.len() as u128
        })
    });
    match validate_write_batch(writes) {
        Ok(()) => assert!(!invalid_range && !overlap, "accepted {writes:?}"),
        Err(error) => {
            assert!(invalid_range || overlap, "rejected {writes:?}: {error}");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }
}
