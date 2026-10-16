//! Synchronous admission for handles retained while a client owner is busy.
use super::{FileClientError, FileClientLimits};
use std::{cell::Cell, rc::Rc};

pub(crate) struct WaitingCalls {
    limits: FileClientLimits,
    count: Cell<usize>,
    bytes: Cell<u64>,
}

pub(crate) struct Reservation {
    calls: Rc<WaitingCalls>,
    bytes: u64,
}

impl WaitingCalls {
    pub(crate) fn new(limits: FileClientLimits) -> Rc<Self> {
        Rc::new(Self {
            limits,
            count: Cell::new(0),
            bytes: Cell::new(0),
        })
    }

    pub(crate) fn reserve(self: &Rc<Self>, bytes: u64) -> Result<Reservation, FileClientError> {
        if bytes > self.limits.bytes.saturating_sub(self.bytes.get()) {
            return Err(FileClientError::ByteQuota {
                limit: self.limits.bytes,
            });
        }
        if self.count.get() >= self.limits.commands {
            return Err(FileClientError::CommandQuota {
                limit: self.limits.commands,
            });
        }
        self.count.set(self.count.get() + 1);
        self.bytes.set(self.bytes.get() + bytes);
        Ok(Reservation {
            calls: self.clone(),
            bytes,
        })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.calls.count.set(self.calls.count.get() - 1);
        self.calls.bytes.set(self.calls.bytes.get() - self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn check(commands: usize, bytes: u64, actions: &[(bool, u64)], omit_reservation: bool) -> bool {
        let calls = WaitingCalls::new(FileClientLimits {
            commands,
            bytes,
            durability: super::super::FileClientDurability::Accepted,
        });
        let mut held = Vec::new();
        let mut expected = Vec::new();
        for &(release, size) in actions {
            if release {
                held.pop();
                expected.pop();
            } else {
                let used: u64 = expected.iter().sum();
                let byte_full = size > bytes - used;
                let command_full = expected.len() == commands;
                let result = if omit_reservation {
                    Ok(None)
                } else {
                    calls.reserve(size).map(Some)
                };
                match result {
                    Ok(guard) if !byte_full && !command_full => {
                        held.push(guard);
                        expected.push(size);
                    }
                    Err(FileClientError::ByteQuota { limit }) if byte_full && limit == bytes => {}
                    Err(FileClientError::CommandQuota { limit })
                        if !byte_full && command_full && limit == commands => {}
                    _ => return false,
                }
            }
            if calls.count.get() != expected.len()
                || calls.bytes.get() != expected.iter().sum::<u64>()
            {
                return false;
            }
        }
        drop(held);
        calls.count.get() == 0 && calls.bytes.get() == 0
    }

    fn idle_leaves_flush_capacity(commands: usize, retain_registered_waiter: bool) -> bool {
        let calls = WaitingCalls::new(FileClientLimits {
            commands,
            bytes: 1,
            durability: super::super::FileClientDurability::Accepted,
        });
        let _busy: Vec<_> = (1..commands).map(|_| calls.reserve(0).unwrap()).collect();
        let mut idle = Some(calls.reserve(0).unwrap());
        if !retain_registered_waiter {
            drop(idle.take());
        }
        calls.reserve(0).is_ok()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn waiting_handles_obey_both_quotas_and_release_on_drop(
            commands in 1_usize..8, bytes in 1_u64..128,
            actions in prop::collection::vec((any::<bool>(), 0_u64..256), 0..64),
        ) {
            prop_assert!(check(commands, bytes, &actions, false));
        }

        #[test]
        fn registered_idle_waiter_leaves_capacity_for_progress(commands in 1_usize..8) {
            prop_assert!(idle_leaves_flush_capacity(commands, false));
        }
    }

    #[test]
    fn negative_control_retained_idle_reservation_prevents_progress() {
        use proptest::test_runner::{Config, TestError, TestRunner};
        let mut runner = TestRunner::new(Config {
            failure_persistence: None,
            ..Config::default()
        });
        let result = runner.run(&(1_usize..8), |commands| {
            prop_assert!(idle_leaves_flush_capacity(commands, true));
            Ok(())
        });
        let Err(TestError::Fail(_, commands)) = result else {
            panic!("retained idle reservation escaped")
        };
        assert_eq!(commands, 1);
        assert!(idle_leaves_flush_capacity(commands, false));
    }

    #[test]
    fn negative_control_unreserved_waiter_is_rejected_by_same_property() {
        use proptest::test_runner::{Config, TestError, TestRunner};
        let mut runner = TestRunner::new(Config {
            failure_persistence: None,
            ..Config::default()
        });
        let result = runner.run(&(0_u64..256), |size| {
            prop_assert!(check(1, 3, &[(false, size)], true));
            Ok(())
        });
        let Err(TestError::Fail(_, size)) = result else {
            panic!("unreserved waiter escaped")
        };
        assert_eq!(size, 0);
        assert!(check(1, 3, &[(false, size)], false));
    }
}
