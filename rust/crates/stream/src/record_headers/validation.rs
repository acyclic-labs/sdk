//! The canonical header contract is shared by owned and page-backed ingress.
use super::*;

#[derive(Default)]
pub(super) struct Validation<'a> {
    keys: usize,
    attributes: usize,
    payload: usize,
    invalid: bool,
    previous_key: Option<&'a [u8]>,
    previous_name: Option<&'a str>,
}

impl<'a> Validation<'a> {
    pub(super) fn key(&mut self, key: &'a [u8]) -> Result<(), StreamError> {
        if self.keys == MAX_RECORD_KEYS || key.len() > MAX_RECORD_KEY_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        self.invalid |= self.previous_key.is_some_and(|previous| previous >= key);
        self.keys += 1;
        self.previous_key = Some(key);
        self.payload = self.payload.saturating_add(key.len());
        Ok(())
    }

    pub(super) fn attribute(&mut self, name: &'a str, value: &[u8]) -> Result<(), StreamError> {
        if self.attributes == MAX_RECORD_ATTRIBUTES {
            return Err(StreamError::LimitExceeded);
        }
        self.invalid |= name.is_empty()
            || name.chars().any(char::is_control)
            || self.previous_name.is_some_and(|previous| previous >= name);
        self.attributes += 1;
        self.previous_name = Some(name);
        self.payload = self
            .payload
            .saturating_add(name.len())
            .saturating_add(value.len());
        Ok(())
    }

    pub(super) fn finish(self, maximum: usize) -> Result<(), StreamError> {
        if self.invalid {
            Err(StreamError::InvalidArgument)
        } else if self.payload > maximum {
            Err(StreamError::LimitExceeded)
        } else {
            Ok(())
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::test_meter::Meter;

    #[test]
    fn prepared_ingress_rejects_noncanonical_key_order() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = OwnedBytes::from_static(b"ab");
        for keys in [[0..1, 0..1], [1..2, 0..1]] {
            let meter = Arc::new(Meter::default());
            let prepared = PreparedHeaderControls::new(&meter.original(), 2)?;
            assert!(
                prepared.finish(bytes.clone(), &keys, &[]).is_err(),
                "prepared ingress bypassed canonical key order"
            );
            meter.assert_refunded();
        }
        Ok(())
    }
}
