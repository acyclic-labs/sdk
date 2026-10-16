use crate::{public_api::PublicItemSignature, Error};
use std::{cell::RefCell, collections::BTreeMap};

type Rendered = BTreeMap<Vec<u8>, Vec<PublicItemSignature>>;
thread_local! {
    // Owned by one calling thread; no synchronization or lock order is involved.
    static RENDERED: RefCell<Option<Rendered>> = const { RefCell::new(None) };
}

/// Reuse rendering of byte-identical Rustdoc inputs within one operation.
/// Reads, source attestations and profile qualification still run for every receipt.
pub fn with_rustdoc_cache<T>(operation: impl FnOnce() -> T) -> T {
    struct Restore(Option<Rendered>);
    impl Drop for Restore {
        fn drop(&mut self) {
            RENDERED.with_borrow_mut(|cache| *cache = self.0.take());
        }
    }
    let _restore = Restore(RENDERED.replace(Some(BTreeMap::new())));
    operation()
}

pub(crate) fn extract(
    bytes: &[u8],
    render: impl FnOnce() -> Result<Vec<PublicItemSignature>, Error>,
) -> Result<Vec<PublicItemSignature>, Error> {
    if let Some(items) =
        RENDERED.with_borrow(|cache| cache.as_ref().and_then(|cache| cache.get(bytes)).cloned())
    {
        return Ok(items);
    }
    let items = render()?;
    RENDERED.with_borrow_mut(|cache| {
        if let Some(cache) = cache {
            cache.insert(bytes.to_vec(), items.clone());
        }
    });
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rustdoc_types::Id;

    fn item(bytes: &[u8]) -> Vec<PublicItemSignature> {
        vec![PublicItemSignature {
            id: Id(0),
            parent_id: None,
            display: format!("{bytes:?}"),
            path: Vec::new(),
        }]
    }

    proptest! {
        #[test]
        fn reuse_requires_every_input_byte_and_ends_with_the_operation(
            inputs in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..32), 0..32),
            tag in any::<u8>(),
        ) {
            let mut calls = 0;
            with_rustdoc_cache(|| {
                // A prefix-only cache key aliases these two distinct inputs.
                let mut inputs = inputs;
                inputs.extend([vec![tag, 0], vec![tag, 1]]);
                let unique = inputs.iter().collect::<std::collections::BTreeSet<_>>().len();
                for bytes in inputs.iter().chain(&inputs) {
                    let actual = extract(bytes, || { calls += 1; Ok(item(bytes)) }).unwrap();
                    prop_assert_eq!(actual, item(bytes));
                }
                prop_assert_eq!(calls, unique);
                Ok(())
            })?;
            prop_assert!(RENDERED.with_borrow(|cache| cache.is_none()));
        }
    }

    #[test]
    fn nested_and_unwinding_operations_restore_the_outer_cache() {
        with_rustdoc_cache(|| {
            extract(b"outer", || Ok(item(b"outer"))).unwrap();
            assert!(std::panic::catch_unwind(|| with_rustdoc_cache(|| {
                extract(b"inner", || Ok(item(b"inner"))).unwrap();
                panic!("negative control");
            }))
            .is_err());
            assert_eq!(
                extract(b"outer", || panic!("outer result was lost")).unwrap(),
                item(b"outer")
            );
            assert_eq!(
                RENDERED.with_borrow(|cache| cache.as_ref().unwrap().len()),
                1
            );
        });
        assert!(RENDERED.with_borrow(|cache| cache.is_none()));
    }
}
