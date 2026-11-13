//! One atomic control receipt for physical packet publication and retirement.
//! Shared with the static host, without a standard-library dependency.

pub(super) const VERSION: u32 = 2;

pub(super) enum Receipt {
    Retired,
    Published(bool),
    Rejected(bool, i32),
}

impl Receipt {
    pub(super) const SIZE: usize = 4;
}

impl TryFrom<Receipt> for [u8; Receipt::SIZE] {
    type Error = ();

    fn try_from(receipt: Receipt) -> Result<Self, Self::Error> {
        let (notification, word) = match receipt {
            Receipt::Retired => (false, 0),
            Receipt::Published(notification) => (notification, 1),
            Receipt::Rejected(notification, errno) if errno > 0 => (
                notification,
                u32::try_from(errno)
                    .ok()
                    .and_then(|errno| errno.checked_add(1))
                    .filter(|word| word & (1 << 31) == 0)
                    .ok_or(())?,
            ),
            Receipt::Rejected(_, _) => return Err(()),
        };
        Ok((word | (u32::from(notification) << 31)).to_ne_bytes())
    }
}

impl TryFrom<[u8; Receipt::SIZE]> for Receipt {
    type Error = ();

    fn try_from(bytes: [u8; Receipt::SIZE]) -> Result<Self, Self::Error> {
        let word = u32::from_ne_bytes(bytes);
        let notification = word & (1 << 31) != 0;
        match word & !(1 << 31) {
            0 if !notification => Ok(Self::Retired),
            0 => Err(()),
            1 => Ok(Self::Published(notification)),
            status => Ok(Self::Rejected(
                notification,
                i32::try_from(status - 1).map_err(|_| ())?,
            )),
        }
    }
}
