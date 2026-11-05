use super::*;

pub(crate) fn stock_boundary(
    io: Option<&PreparationIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return crate::Error::NativeBoundary { code, reason };
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    match code {
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

pub(crate) fn stock_string_copy(value: &str, io: Option<&PreparationIo>) -> Result<String> {
    let mut copied = String::new();
    stock_string_push(&mut copied, value, None, "stock text size overflow", io)?;
    Ok(copied)
}

pub(crate) fn stock_retire_text(text: String, io: Option<&PreparationIo>) -> Result<()> {
    let bytes = text.capacity() as u64;
    stock_retire(text, bytes, io)
}

fn stock_retire<T>(value: T, bytes: u64, io: Option<&PreparationIo>) -> Result<()> {
    #[cfg(feature = "filesystem")]
    let custody = io
        .filter(|_| bytes != 0)
        .map(|io| io.guard_owned(bytes))
        .transpose()?;
    #[cfg(not(feature = "filesystem"))]
    let _ = (io, bytes);
    drop(value);
    #[cfg(feature = "filesystem")]
    drop(custody);
    Ok(())
}

#[cfg(feature = "filesystem")]
pub(in crate::executor) fn stock_events_payload_bytes(events: &Vec<ModelEvent>) -> Result<u64> {
    events.iter().try_fold(
        (events.capacity() as u64)
            .checked_mul(std::mem::size_of::<ModelEvent>() as u64)
            .ok_or_else(|| {
                stock_boundary(
                    None,
                    crate::wire::ErrorCode::Invalid,
                    "event storage overflows",
                )
            })?,
        |bytes, event| crate::core::owned_sum([bytes, stock_event_payload_bytes(event)?]),
    )
}

#[cfg(feature = "filesystem")]
fn stock_event_payload_bytes(event: &ModelEvent) -> Result<u64> {
    Ok(event.retained_owned_bytes()? - std::mem::size_of::<ModelEvent>() as u64)
}

pub(in crate::executor) fn stock_retire_event(
    event: ModelEvent,
    io: Option<&PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    let bytes = if io.is_some() {
        stock_event_payload_bytes(&event)?
    } else {
        0
    };
    #[cfg(not(feature = "filesystem"))]
    let bytes = 0;
    stock_retire(event, bytes, io)
}

pub(crate) fn stock_vec_push<T>(
    values: &mut Vec<T>,
    value: T,
    io: Option<&PreparationIo>,
) -> Result<()> {
    stock_vec_prepare_push(values, io)?;
    values.push(value);
    Ok(())
}

/// Complete the fallible storage admission before moving a funded value.
pub(in crate::executor) fn stock_vec_prepare_push<T>(
    values: &mut Vec<T>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if io.is_none() {
        values.reserve(1);
        return Ok(());
    }
    if values.len() < values.capacity() {
        return Ok(());
    }
    let capacity = values
        .capacity()
        .checked_mul(2)
        .map(|capacity| capacity.max(1))
        .ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "vector capacity overflows",
            )
        })?;
    stock_vec_reserve(values, capacity, io)
}

/// Reserves retained index storage, including a relocating allocation's peak.
pub(crate) fn stock_vec_reserve<T>(
    values: &mut Vec<T>,
    capacity: usize,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if capacity <= values.capacity() {
        return Ok(());
    }
    let overflow = || {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "index storage overflows",
        )
    };
    let copied = values
        .len()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(overflow)? as u64;
    #[cfg(feature = "filesystem")]
    let _peak = io
        .map(|io| {
            let bytes = values
                .capacity()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(overflow)? as u64;
            io.reserve_decode(bytes)
        })
        .transpose()?;
    with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            work.admit_copy(copied)?;
            work.codec_charge(crate::executor::json::JsonCodecCharge::Work {
                bytes: 0,
                items: values.len() as u64,
            })?;
            crate::store::history::reserve(values, capacity, work)?;
            work.copy_bytes(copied)?;
        } else {
            values
                .try_reserve_exact(capacity - values.len())
                .map_err(Error::Allocation)?;
        }
        Ok(())
    })
}

pub(crate) fn stock_string_push(
    text: &mut String,
    suffix: &str,
    maximum: Option<u64>,
    reason: &'static str,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let target = text
        .len()
        .checked_add(suffix.len())
        .filter(|target| maximum.is_none_or(|maximum| *target as u64 <= maximum))
        .ok_or_else(|| stock_boundary(io, crate::wire::ErrorCode::Invalid, reason))?;
    with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            work.admit_copy(suffix.len() as u64)?;
            work.reserve_text(text, target)?;
            text.push_str(suffix);
            work.copy_bytes(suffix.len() as u64)
        } else {
            text.push_str(suffix);
            Ok(())
        }
    })
}

/// Keeps the consumed backing allocation funded until its remaining values
/// and storage have died. Moved values retain their own payload funding.
pub(crate) struct StockVecIter<'a, T> {
    values: std::vec::IntoIter<T>,
    _hold: Option<crate::conversation::ContentHold<'a>>,
}

impl<T> Iterator for StockVecIter<'_, T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.values.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.values.size_hint()
    }
}
impl<T> ExactSizeIterator for StockVecIter<'_, T> {}

/// Yields admitted payload custody back to its original ledger. The shared
/// vector owner destroys an unconsumed suffix before retiring its funding.
pub(in crate::executor) struct StockEventIter<'a> {
    storage: StockVecIter<'a, ModelEvent>,
}

impl Iterator for StockEventIter<'_> {
    type Item = Result<ModelEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        let event = self.storage.next()?;
        Some((|| {
            #[cfg(feature = "filesystem")]
            if let Some(hold) = &mut self.storage._hold {
                hold.retain_part(stock_event_payload_bytes(&event)?)?;
            }
            Ok(event)
        })())
    }
}

pub(in crate::executor) fn stock_events_into_iter(
    events: Vec<ModelEvent>,
    io: Option<&PreparationIo>,
) -> Result<StockEventIter<'_>> {
    #[cfg(feature = "filesystem")]
    let hold = io
        .map(|io| io.guard_owned(stock_events_payload_bytes(&events)?))
        .transpose()?;
    #[cfg(not(feature = "filesystem"))]
    let hold = {
        let _ = io;
        None
    };
    Ok(StockEventIter {
        storage: StockVecIter {
            values: events.into_iter(),
            _hold: hold,
        },
    })
}

pub(crate) fn stock_into_iter<'a, T>(
    values: Vec<T>,
    io: Option<&'a PreparationIo>,
) -> Result<StockVecIter<'a, T>> {
    #[cfg(feature = "filesystem")]
    let hold = io
        .map(|io| {
            let bytes = values
                .capacity()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| {
                    stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "consumed vector storage overflows",
                    )
                })?;
            io.guard_owned(bytes as u64)
        })
        .transpose()?;
    #[cfg(not(feature = "filesystem"))]
    let hold = {
        let _ = io;
        None
    };
    Ok(StockVecIter {
        values: values.into_iter(),
        _hold: hold,
    })
}

#[cfg(all(
    test,
    feature = "filesystem",
    feature = "test-support",
    not(target_arch = "wasm32")
))]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn event_selection_retires_only_its_unconsumed_suffix() -> Result<()> {
        for native in [false, true] {
            for consumed in 0..=3 {
                let observed =
                    crate::test_support::accounted::accounted(native, 65_536, false, |io| {
                        let sibling = stock_string_copy("retained sibling", Some(io))?;
                        let sibling_bytes = sibling.capacity() as u64;
                        let mut events = Vec::new();
                        for text in ["first", "second", "unselected tail"] {
                            stock_vec_push(
                                &mut events,
                                ModelEvent::Content {
                                    delta: stock_string_copy(text, Some(io))?,
                                },
                                Some(io),
                            )?;
                        }
                        let original = match &events[0] {
                            ModelEvent::Content { delta } => delta.as_ptr(),
                            _ => unreachable!(),
                        };
                        let mut iterator = stock_events_into_iter(events, Some(io))?;
                        let mut selected = None;
                        for at in 0..consumed {
                            let event = iterator.next().unwrap()?;
                            if at == 0 {
                                let ModelEvent::Content { delta } = &event else {
                                    unreachable!()
                                };
                                assert_eq!(delta.as_ptr(), original);
                                selected = Some(event);
                            } else {
                                stock_retire_event(event, Some(io))?;
                            }
                        }
                        // This is the same early drop as a selected call or a
                        // consumer error; the original vector owns the suffix.
                        drop(iterator);
                        let retained = selected
                            .as_ref()
                            .map(stock_event_payload_bytes)
                            .transpose()?
                            .unwrap_or(0);
                        assert_eq!(io.owned_bytes()?, sibling_bytes + retained);
                        if let Some(event) = selected {
                            stock_retire_event(event, Some(io))?;
                        }
                        assert_eq!(io.owned_bytes()?, sibling_bytes);
                        stock_retire_text(sibling, Some(io))?;
                        Ok::<_, Error>(())
                    })
                    .map_err(|(cause, _)| Error::Storage(cause))?;
                observed.0?;
                assert_eq!(observed.2, 0);
                if native {
                    assert_eq!(
                        observed.5,
                        acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                    );
                }
            }
        }
        Ok(())
    }

    struct Witness<'a> {
        io: &'a PreparationIo,
        lowest: &'a Cell<Option<u64>>,
        drops: &'a Cell<usize>,
    }
    impl Drop for Witness<'_> {
        fn drop(&mut self) {
            let live = self.io.original_native_work().map_or_else(
                || self.io.owned_bytes().ok(),
                |work| Some(work.live_allocation_bytes()),
            );
            self.lowest
                .set(self.lowest.get().zip(live).map(|(old, live)| old.min(live)));
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn consumed_vector_values_die_before_their_original_storage_funding() -> Result<()> {
        for native in [false, true] {
            let result = crate::test_support::accounted::accounted(native, 65_536, false, |io| {
                let lowest = Cell::new(Some(u64::MAX));
                let drops = Cell::new(0);
                let mut values = Vec::new();
                for _ in 0..2 {
                    stock_vec_push(
                        &mut values,
                        Witness {
                            io,
                            lowest: &lowest,
                            drops: &drops,
                        },
                        Some(io),
                    )?;
                }
                let owned = io.owned_bytes()?;
                let minimum = io.original_native_work().map_or(
                    owned,
                    acyclic_fs::NativeMaterializationWork::live_allocation_bytes,
                );
                let slots = values.capacity() * std::mem::size_of::<Witness<'_>>();
                // Construct directly to make destruction order independent of
                // the factory's admission/retirement negative control.
                let mut iterator = StockVecIter {
                    values: values.into_iter(),
                    _hold: Some(io.guard_owned(slots as u64)?),
                };
                drop(iterator.next());
                drop(iterator);
                assert_eq!(drops.get(), 2);
                assert!(
                    lowest.get().is_some_and(|live| live >= minimum),
                    "consumed vector retired funding before destroying its values"
                );
                assert_eq!(io.owned_bytes()?, owned - slots as u64);
                Ok::<_, Error>(())
            })
            .map_err(|(cause, _)| Error::Storage(cause))?;
            result.0?;
            if native {
                assert_eq!(
                    result.5,
                    acyclic_fs::NativeMaterializationWork::accounting_state_bytes()
                );
            }
        }
        Ok(())
    }
}
