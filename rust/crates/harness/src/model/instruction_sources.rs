//! One funded selection shared by discovery validation and instruction projection.
use super::instruction_index::{Index, history_step_traced, step, trace_operation};
use super::*;

pub(crate) struct InstructionSources<'a> {
    files: &'a [ProjectInstruction],
    positions: Vec<usize>,
}
impl<'a> InstructionSources<'a> {
    pub(crate) fn new(
        files: &'a [ProjectInstruction],
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        if files.is_empty() {
            return Ok(Self {
                files,
                positions: Vec::new(),
            });
        }
        let mut positions = crate::core::reserve_copy_vector(files.len(), quota)?;
        // Lookup storage dies with its own quota before returning. Only the
        // funded first-source positions survive through reservation/projection.
        let mut lookup_quota = crate::wire_codec::CodecQuota::new(quota.work(), 0)?;
        let mut lookup: Index<usize> = Index::new(files.len(), &mut lookup_quota)?;
        for (position, file) in files.iter().enumerate() {
            history_step_traced(lookup_quota.work(), 1)?;
            trace_operation("instruction-source-visit", 1);
            file.validate_source(lookup_quota.work())?;
            #[cfg(all(test, feature = "test-support"))]
            if PREFIX_SCAN.with(std::cell::Cell::get) {
                for prior in &files[..position] {
                    std::hint::black_box(
                        prior
                            .source()
                            .same_source(file.source(), lookup_quota.work())?,
                    );
                }
            }
            let key = file.source().index_key(lookup_quota.work())?;
            if let Some(prior) = lookup.get_history(0, &key, lookup_quota.work())? {
                if !files[prior]
                    .source()
                    .same_source(file.source(), lookup_quota.work())?
                {
                    return Err(crate::store::guarded_boundary(
                        lookup_quota.work(),
                        crate::wire::ErrorCode::Conflict,
                        "instruction source has conflicting contents",
                    ));
                }
            } else {
                lookup.set_history(0, key, Some(position), lookup_quota.work())?;
                positions.push(position);
            }
        }
        Ok(Self { files, positions })
    }

    pub(crate) fn iter<'b>(
        &'b self,
        io: Option<&'b PreparationIo>,
    ) -> impl Iterator<Item = Result<&'a ProjectInstruction>> + 'b {
        self.positions.iter().map(move |&position| {
            step(io, 1)?;
            trace_operation("instruction-source-projection", 1);
            Ok(&self.files[position])
        })
    }
}

#[cfg(all(test, feature = "test-support"))]
thread_local! { static PREFIX_SCAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(all(test, feature = "test-support"))]
fn with_prefix_scan<T>(enabled: bool, run: impl FnOnce() -> T) -> T {
    PREFIX_SCAN.with(|state| assert!(!state.replace(enabled)));
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            PREFIX_SCAN.with(|state| state.set(false));
        }
    }
    let _reset = Reset;
    run()
}

#[cfg(all(
    test,
    feature = "test-support",
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
mod tests;
