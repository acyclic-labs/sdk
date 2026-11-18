#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::test_meter::Meter;

    #[test]
    fn prepared_ingress_rejects_noncanonical_key_order() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = OwnedBytes::from_static(b"ab");
        for keys in [[0..1, 0..1], [1..2, 0..1]] {
            let meter = Arc::new(Meter::default());
            let prepared = PreparedHeaderControls::new_shape(&meter.original(), 2, keys.len(), 0)?;
            assert!(
                prepared
                    .finish(bytes.clone(), keys.iter().cloned(), std::iter::empty())
                    .is_err(),
                "prepared ingress bypassed canonical key order"
            );
            meter.assert_refunded();
        }
        Ok(())
    }
}
