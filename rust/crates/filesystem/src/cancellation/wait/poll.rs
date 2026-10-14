use super::*;

impl Cancelled<'_> {
    pub(in crate::cancellation) fn poll_result(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), CancellationWaitError>> {
        if self.token.is_cancelled() {
            return Poll::Ready(Ok(()));
        }
        let mut waiters = self
            .token
            .state
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(meter) = &mut self.meter {
            meter.observe_registry(self.token.state.registry_generation.load(Ordering::Acquire));
        }
        if self.token.is_cancelled() {
            return Poll::Ready(Ok(()));
        }
        if self.registered.is_none() {
            let mut free = None;
            for (index, entry) in waiters.iter().enumerate() {
                if let Some(meter) = &mut self.meter
                    && let Err(error) = meter.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::default()
                    })
                {
                    return Poll::Ready(Err(error));
                }
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = self.native_original
                    && let Err(error) = original.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })
                {
                    return Poll::Ready(Err(error.into()));
                }
                if entry.is_none() {
                    free = Some(index);
                    break;
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            let previous_lease = match registry_lease(&waiters, || {
                self.native_original.map_or(Ok(()), |original| {
                    original.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })
                })
            }) {
                Ok(lease) => lease,
                Err(error) => return Poll::Ready(Err(error.into())),
            };
            #[cfg(not(target_arch = "wasm32"))]
            let mut cancellation_visits = None;
            #[cfg(not(target_arch = "wasm32"))]
            let inherited_original = previous_lease.as_ref().map(|lease| lease.original.clone());
            #[cfg(not(target_arch = "wasm32"))]
            let native_original = inherited_original.as_ref().or(self.native_original);
            #[cfg(not(target_arch = "wasm32"))]
            let funded_slots = previous_lease
                .as_ref()
                .map_or(waiters.capacity(), |lease| lease.funded_slots);
            #[cfg(target_arch = "wasm32")]
            let funded_slots = waiters.capacity();
            let grows = free.is_none() && waiters.len() == funded_slots;
            let old_bytes = (waiters.capacity()
                * std::mem::size_of::<Option<Weak<CancellationWaiter>>>())
                as u64;
            let count = if grows {
                waiters
                    .capacity()
                    .max(1)
                    .checked_mul(2)
                    .ok_or(crate::WorkError::Overflow)?
            } else {
                0
            };
            let new_bytes = count
                .checked_mul(std::mem::size_of::<Option<Weak<CancellationWaiter>>>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            let waiter_bytes = std::mem::size_of::<CancellationWaiter>() as u64;
            #[cfg(not(target_arch = "wasm32"))]
            let mut growth_visits = None;
            #[cfg(not(target_arch = "wasm32"))]
            let native_waiter_allocation = if let Some(original) = native_original {
                let registry_metadata = if grows {
                    std::mem::size_of::<NativeRegistryLease>() as u64
                } else {
                    0
                };
                let retained = waiter_bytes
                    .checked_add(new_bytes)
                    .and_then(|bytes| bytes.checked_add(registry_metadata));
                let retained = retained.ok_or(crate::WorkError::Overflow)?;
                let cancellation_items = if grows {
                    (count as u64)
                        .checked_mul(2)
                        .ok_or(crate::WorkError::Overflow)?
                } else {
                    0
                };
                if let Err(error) = original
                    .admit(crate::WorkCounters {
                        allocation_operations: 1 + 2 * u64::from(grows),
                        bytes_copied: if grows {
                            (waiters.len()
                                * std::mem::size_of::<Option<Weak<CancellationWaiter>>>())
                                as u64
                        } else {
                            0
                        },
                        items_examined: 2 + if grows {
                            waiters.len() as u64 + cancellation_items
                        } else {
                            0
                        },
                        ..crate::WorkCounters::UNCHARGED
                    })
                    .and_then(|()| original.admit_retained(retained))
                {
                    return Poll::Ready(Err(error.into()));
                }
                self.native_cleanup = Some(original.reserve_work(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?);
                if grows {
                    growth_visits = Some(original.reserve_work(crate::WorkCounters {
                        items_examined: waiters.len() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    })?);
                    cancellation_visits = Some(original.reserve_work(crate::WorkCounters {
                        items_examined: cancellation_items,
                        ..crate::WorkCounters::UNCHARGED
                    })?);
                }
                Some(original.claim(waiter_bytes, 1, crate::WorkCounters::UNCHARGED)?)
            } else {
                None
            };
            if let Some(meter) = &mut self.meter {
                // Admit the complete first-poll allocation/registration,
                // including its eventual O(1) unregister, before any allocation.
                let adopted = if grows && meter.admission.registry.is_none() {
                    old_bytes
                } else {
                    0
                };
                let peak = meter
                    .allocations
                    .live_bytes()
                    .checked_add(waiter_bytes)
                    .and_then(|bytes| bytes.checked_add(adopted))
                    .and_then(|bytes| bytes.checked_add(new_bytes));
                let peak = peak.ok_or(crate::WorkError::Overflow)?;
                let delta = crate::WorkCounters {
                    allocation_operations: 1 + u64::from(grows),
                    peak_allocation_bytes: peak,
                    bytes_copied: if grows {
                        (waiters.len() * std::mem::size_of::<Option<Weak<CancellationWaiter>>>())
                            as u64
                    } else {
                        0
                    },
                    items_examined: 1,
                    ..crate::WorkCounters::default()
                };
                if let Err(error) = meter.work.admit(&delta, &meter.budget) {
                    return Poll::Ready(Err(error.into()));
                }
                meter.cleanup_reserved = true;
                if let Err(error) = meter.claim(waiter_bytes, 1) {
                    return Poll::Ready(Err(error));
                }
                self.waiter_bytes = waiter_bytes;
            }
            let waiter_value = CancellationWaiter {
                #[cfg(not(target_arch = "wasm32"))]
                _native_allocation: native_waiter_allocation,
                #[cfg(not(target_arch = "wasm32"))]
                registry_lease: std::sync::Mutex::new(previous_lease.clone()),
                ..CancellationWaiter::default()
            };
            let waiter = Arc::new(waiter_value);
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = native_original
                && let Err(error) = original.record_admitted(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })
            {
                return Poll::Ready(Err(error.into()));
            }
            if grows {
                if let Some(meter) = &mut self.meter {
                    if meter.admission.registry.is_none() {
                        if let Err(error) = meter.claim(old_bytes, 0) {
                            return Poll::Ready(Err(error));
                        }
                        meter.temporary_registry_bytes = old_bytes;
                    }
                    if let Err(error) = meter.claim(new_bytes, 1) {
                        return Poll::Ready(Err(error));
                    }
                    meter.pending_registry_bytes = new_bytes;
                }
                #[cfg(not(target_arch = "wasm32"))]
                let native_registry_allocation = if let Some(original) = native_original {
                    match original.claim(new_bytes, 1, crate::WorkCounters::UNCHARGED) {
                        Ok(allocation) => Some((original, allocation)),
                        Err(error) => return Poll::Ready(Err(error.into())),
                    }
                } else {
                    None
                };
                let mut replacement = Vec::new();
                if let Err(_error) = replacement.try_reserve_exact(count) {
                    if let Some(meter) = &mut self.meter {
                        meter.release(new_bytes);
                        meter.pending_registry_bytes = 0;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    if native_original.is_some() {
                        return Poll::Ready(Err(CancellationWaitError::Native(
                            crate::NativeTreeMaterializationError::AllocationFailed(_error),
                        )));
                    }
                    return Poll::Ready(Err(CancellationWaitError::AllocationFailed));
                }
                if let Some(meter) = &mut self.meter
                    && let Err(error) = meter.charge(crate::WorkCounters {
                        bytes_copied: (waiters.len()
                            * std::mem::size_of::<Option<Weak<CancellationWaiter>>>())
                            as u64,
                        ..crate::WorkCounters::default()
                    })
                {
                    return Poll::Ready(Err(error));
                }
                #[cfg(not(target_arch = "wasm32"))]
                let new_lease = if let Some((original, mut allocation)) = native_registry_allocation
                {
                    let actual = (replacement.capacity()
                        * std::mem::size_of::<Option<Weak<CancellationWaiter>>>())
                        as u64;
                    if let Err(error) = allocation.reconcile_capacity(actual) {
                        drop(waiters);
                        drop(replacement);
                        return Poll::Ready(Err(error.into()));
                    }
                    let Some(generation) = self
                        .token
                        .state
                        .registry_generation
                        .load(Ordering::Acquire)
                        .checked_add(1)
                    else {
                        drop(waiters);
                        drop(replacement);
                        return Poll::Ready(Err(crate::WorkError::Overflow.into()));
                    };
                    let mut metadata = match original.claim(
                        std::mem::size_of::<NativeRegistryLease>() as u64,
                        1,
                        crate::WorkCounters::UNCHARGED,
                    ) {
                        Ok(metadata) => metadata,
                        Err(error) => {
                            drop(waiters);
                            drop(replacement);
                            return Poll::Ready(Err(error.into()));
                        }
                    };
                    if let Err(error) = metadata.absorb(&mut allocation) {
                        drop(waiters);
                        drop(replacement);
                        return Poll::Ready(Err(error.into()));
                    }
                    Some(Arc::new(NativeRegistryLease {
                        state: Arc::downgrade(&self.token.state),
                        generation,
                        registry_bytes: AtomicU64::new(actual),
                        registered: AtomicU64::new(0),
                        funded_slots: count,
                        original: original.clone(),
                        cancellation_visits: parking_lot::Mutex::new(cancellation_visits),
                        allocation: parking_lot::Mutex::new(metadata),
                    }))
                } else {
                    None
                };
                let _copied_bytes = (waiters.len()
                    * std::mem::size_of::<Option<Weak<CancellationWaiter>>>())
                    as u64;
                replacement.append(&mut waiters);
                let old_backing = std::mem::replace(&mut *waiters, replacement);
                let generation = self
                    .token
                    .state
                    .registry_generation
                    .fetch_add(1, Ordering::AcqRel)
                    + 1;
                if let Some(meter) = &mut self.meter {
                    // The old backing remains physically alive until unlock.
                    meter.temporary_registry_bytes = 0;
                    meter.pending_registry_bytes = 0;
                    meter.admission.registry = Some((generation, new_bytes));
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    // Only Arc aliases of this one backing are changed. Keep
                    // the old lease alive until its physical Vec is dropped.
                    for slot in waiters.iter() {
                        if let Some(existing) = slot.as_ref().and_then(Weak::upgrade) {
                            *existing
                                .registry_lease
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                new_lease.clone();
                            if let Some(lease) = &new_lease {
                                lease.registered.fetch_add(1, Ordering::AcqRel);
                            }
                        }
                        if let Some(credit) = &mut growth_visits {
                            credit.record_deferred(crate::WorkCounters {
                                items_examined: 1,
                                ..crate::WorkCounters::UNCHARGED
                            });
                        }
                    }
                    *waiter
                        .registry_lease
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = new_lease;
                }
                let index = if let Some(index) = free {
                    waiters[index] = Some(Arc::downgrade(&waiter));
                    index
                } else {
                    waiters.push(Some(Arc::downgrade(&waiter)));
                    waiters.len() - 1
                };
                self.registered = Some(index);
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(lease) = waiter
                    .registry_lease
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                {
                    lease.registered.fetch_add(1, Ordering::AcqRel);
                }
                self.waiter = Some(waiter.clone());
                drop(waiters);
                drop(old_backing);
                if let Some(meter) = &mut self.meter {
                    meter.release(old_bytes);
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let copied = native_original.map_or(Ok(()), |original| {
                        original.record_admitted(crate::WorkCounters {
                            bytes_copied: _copied_bytes,
                            ..crate::WorkCounters::UNCHARGED
                        })
                    });
                    let retired = previous_lease
                        .as_ref()
                        .map_or(Ok(()), |lease| lease.retire_backing());
                    if let Err(error) = copied.and(retired) {
                        return Poll::Ready(Err(error.into()));
                    }
                }
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = native_original
                    && let Err(error) = original.admit(crate::WorkCounters::UNCHARGED)
                {
                    return Poll::Ready(Err(error.into()));
                }
                waiters = self
                    .token
                    .state
                    .waiters
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // Cancellation may have drained the newly installed registry
                // while its old backing was being physically retired.
                if self.token.is_cancelled() {
                    return Poll::Ready(Ok(()));
                }
            }
            if self.registered.is_none() {
                let weak = Some(Arc::downgrade(&waiter));
                let index = if let Some(index) = free {
                    *waiters.get_mut(index).ok_or(crate::WorkError::Overflow)? = weak;
                    index
                } else {
                    waiters.push(weak);
                    waiters.len() - 1
                };
                self.registered = Some(index);
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(lease) = waiter
                    .registry_lease
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                {
                    lease.registered.fetch_add(1, Ordering::AcqRel);
                }
                self.waiter = Some(waiter);
            }
        }
        let waiter = self.waiter.as_ref().ok_or(crate::WorkError::Overflow)?;
        drop(waiters);
        let replace = {
            let waker = waiter
                .waker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            !waker
                .as_ref()
                .is_some_and(|waker| waker.will_wake(context.waker()))
        };
        if replace {
            // RawWaker clone/drop may run arbitrary executor code, including
            // cancellation of this same token. Neither registry nor waiter
            // locks may span those callbacks.
            let replacement = context.waker().clone();
            let previous = waiter
                .waker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .replace(replacement);
            drop(previous);
        }
        // A cancellation that won before replacement may already have drained
        // this slot. Observing it here closes that lost-wakeup window; one that
        // wins afterward takes the newly installed waker from this same node.
        if self.token.is_cancelled() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
