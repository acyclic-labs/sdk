use crate::*;

pub(crate) enum ReadDestination<'a> {
    Slice(&'a mut [u8]),
    Spare {
        data: &'a mut Vec<u8>,
        length: usize,
    },
}

impl ReadDestination<'_> {
    pub(crate) fn parts(&mut self) -> io::Result<(*mut u8, usize)> {
        match self {
            Self::Slice(data) => Ok((data.as_mut_ptr(), data.len())),
            Self::Spare { data, length } => {
                let spare = data
                    .spare_capacity_mut()
                    .get_mut(..*length)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                Ok((spare.as_mut_ptr().cast(), spare.len()))
            }
        }
    }
}
