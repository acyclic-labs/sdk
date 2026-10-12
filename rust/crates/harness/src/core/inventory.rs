//! Bounded inspection of closed admission metadata.
use super::*;

pub(crate) fn inspect_metadata<T: Serialize + ?Sized>(
    source: &T,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    if work.is_none() {
        return Ok(());
    }
    crate::contract::count_trusted_json_bytes_with_work(source, u64::MAX, work).map(|_| ())
}
