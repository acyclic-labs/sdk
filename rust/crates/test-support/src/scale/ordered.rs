/// Positive, strictly increasing input sizes for a scale comparison.
pub(super) struct OrderedInputs {
    small: u64,
    large: u64,
}

impl OrderedInputs {
    pub(super) const fn new(small: u64, large: u64) -> Option<Self> {
        if small == 0 || large <= small {
            None
        } else {
            Some(Self { small, large })
        }
    }

    pub(super) const fn small(&self) -> u64 {
        self.small
    }

    pub(super) const fn large(&self) -> u64 {
        self.large
    }
}
