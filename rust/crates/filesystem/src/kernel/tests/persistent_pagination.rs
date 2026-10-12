use super::*;
use crate::foundation::Digest;
use crate::kernel::attribute_mutation::AttributeFormat;
use crate::kernel::{AttributeClass, AttributeEntry, AttributeName};
use crate::storage::ObjectKind;

fn name(byte: u8) -> Result<AttributeName, crate::kernel::AttributeError> {
    AttributeName::new(AttributeClass::PosixXattr, vec![byte], 1)
}

fn entry(byte: u8) -> Result<AttributeEntry, crate::kernel::AttributeError> {
    Ok(AttributeEntry {
        name: name(byte)?,
        value_bytes: u64::from(byte),
        value: ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::from_bytes([byte; 32]),
        },
    })
}

fn child(byte: u8) -> Result<Child<AttributeName>, crate::kernel::AttributeError> {
    Ok(Child {
        first: name(byte)?,
        page: ObjectId {
            kind: ObjectKind::AttributePage,
            digest: Digest::from_bytes([byte; 32]),
        },
    })
}

#[test]
fn page_bounds_cover_every_edge() -> Result<(), Box<dyn std::error::Error>> {
    let values = [entry(2)?, entry(4)?, entry(6)?];
    assert!(leaf_within::<AttributeFormat>(&values, None, None));
    assert!(leaf_within::<AttributeFormat>(
        &values,
        Some(&name(2)?),
        Some(&name(7)?)
    ));
    assert!(!leaf_within::<AttributeFormat>(
        &values,
        Some(&name(3)?),
        None
    ));
    assert!(!leaf_within::<AttributeFormat>(
        &values,
        None,
        Some(&name(6)?)
    ));

    let children = [child(2)?, child(4)?, child(6)?];
    assert!(children_within(&children, None, None));
    assert!(children_within(&children, Some(&name(2)?), Some(&name(7)?)));
    assert!(!children_within(&children, Some(&name(3)?), None));
    assert!(!children_within(&children, None, Some(&name(6)?)));
    Ok(())
}
