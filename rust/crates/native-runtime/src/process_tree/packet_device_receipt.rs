//! One atomic control receipt for physical packet publication and retirement.
//! Shared with the static host, without a standard-library dependency.

pub(super) enum Receipt {
    Retired,
    Published,
    Rejected(i32),
}

impl Receipt {
    pub(super) const SIZE: usize = 4;
}

impl TryFrom<Receipt> for [u8; Receipt::SIZE] {
    type Error = ();

    fn try_from(receipt: Receipt) -> Result<Self, Self::Error> {
        let word = match receipt {
            Receipt::Retired => 0,
            Receipt::Published => 1,
            Receipt::Rejected(errno) if errno > 0 => u32::try_from(errno)
                .ok()
                .and_then(|errno| errno.checked_add(1))
                .ok_or(())?,
            Receipt::Rejected(_) => return Err(()),
        };
        Ok(word.to_ne_bytes())
    }
}

impl TryFrom<[u8; Receipt::SIZE]> for Receipt {
    type Error = ();

    fn try_from(bytes: [u8; Receipt::SIZE]) -> Result<Self, Self::Error> {
        match u32::from_ne_bytes(bytes) {
            0 => Ok(Self::Retired),
            1 => Ok(Self::Published),
            word => Ok(Self::Rejected(i32::try_from(word - 1).map_err(|_| ())?)),
        }
    }
}
