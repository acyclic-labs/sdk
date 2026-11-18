//! The canonical header contract is shared by owned and page-backed ingress.
/// Maximum encoded width of one indexed record key.
pub const MAX_RECORD_KEY_BYTES: usize = 1024;

/// Header grammar or size rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderError {
    /// Keys or names are out of order, duplicated or invalid.
    Invalid,
    /// A key or the total header payload exceeds its budget.
    Limit,
}

/// Validates an ordered sequence of record keys and named attributes.
#[derive(Default)]
pub struct HeaderValidation<'a> {
    payload: usize,
    invalid: bool,
    previous_key: Option<&'a [u8]>,
    previous_name: Option<&'a str>,
}

impl<'a> HeaderValidation<'a> {
    /// Records one key against its byte budget.
    ///
    /// # Errors
    /// Returns a limit error for an oversized key.
    pub fn key(&mut self, key: &'a [u8]) -> Result<(), HeaderError> {
        if key.len() > MAX_RECORD_KEY_BYTES {
            return Err(HeaderError::Limit);
        }
        self.invalid |= self.previous_key.is_some_and(|previous| previous >= key);
        self.previous_key = Some(key);
        self.payload = self.payload.saturating_add(key.len());
        Ok(())
    }

    /// Records one name and value for validation at completion.
    pub fn attribute(&mut self, name: &'a str, value: &[u8]) {
        self.invalid |= name.is_empty()
            || name.chars().any(char::is_control)
            || self.previous_name.is_some_and(|previous| previous >= name);
        self.previous_name = Some(name);
        self.payload = self
            .payload
            .saturating_add(name.len())
            .saturating_add(value.len());
    }

    /// Header bytes charged against the record budget.
    #[must_use]
    pub const fn payload_bytes(&self) -> usize {
        self.payload
    }

    /// Validates the accumulated ordering, names and payload size.
    ///
    /// # Errors
    /// Returns the first applicable grammar or limit rejection.
    pub fn finish(self, maximum: usize) -> Result<(), HeaderError> {
        if self.invalid {
            Err(HeaderError::Invalid)
        } else if self.payload > maximum {
            Err(HeaderError::Limit)
        } else {
            Ok(())
        }
    }
}
