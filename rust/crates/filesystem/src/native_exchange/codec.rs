//! The native adapter and on-demand checks share one retained journal codec.
use super::*;

const DOMAIN: &[u8] = b"{\"version\":";

pub(super) fn decode(bytes: &[u8]) -> Result<NativeExchangeJournal, NativeExchangeError> {
    // The retained writer puts this fixed header first. Refuse another version
    // before serde can allocate paths or recovery bytes, even with a cut body.
    acyclic_auth::format::header(
        bytes,
        DOMAIN,
        b"1,",
        || NativeExchangeError::IncompatibleJournal,
        || {
            let version = bytes.strip_prefix(DOMAIN).and_then(|value| {
                u32::deserialize(&mut serde_json::Deserializer::from_slice(value)).ok()
            });
            match version {
                Some(version) if version != NATIVE_EXCHANGE_JOURNAL_VERSION => {
                    NativeExchangeError::UnsupportedJournalVersion(version)
                }
                _ => NativeExchangeError::IncompatibleJournal,
            }
        },
    )?;
    let journal: NativeExchangeJournal =
        serde_json::from_slice(bytes).map_err(|_| NativeExchangeError::IncompatibleJournal)?;
    tree::validate_carried_paths(&journal.carried)?;
    Ok(journal)
}

pub(super) fn encode(journal: &NativeExchangeJournal) -> Result<Vec<u8>, NativeExchangeError> {
    serde_json::to_vec(journal).map_err(|_| NativeExchangeError::IncompatibleJournal)
}
