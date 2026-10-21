//! Mount providers capture time before callbacks leave their creating scope.
#[cfg(feature = "test-support")]
use std::time::Duration;
use std::time::SystemTime;

#[derive(Clone, Default)]
pub(super) struct Clock {
    #[cfg(feature = "test-support")]
    seeded: Option<acyclic_native_runtime::testing::CapturedClock>,
}

impl Clock {
    pub(super) fn capture() -> Self {
        Self {
            #[cfg(feature = "test-support")]
            seeded: acyclic_native_runtime::testing::CapturedClock::capture(),
        }
    }

    pub(super) fn now(&self) -> SystemTime {
        #[cfg(feature = "test-support")]
        {
            if let Some(clock) = &self.seeded {
                return SystemTime::UNIX_EPOCH + Duration::from_millis(clock.unix_millis());
            }
            if let Some(millis) = HOST_MILLIS.get() {
                return SystemTime::UNIX_EPOCH + Duration::from_millis(millis);
            }
        }
        SystemTime::now()
    }

    #[cfg(windows)]
    pub(super) fn instant(&self) -> std::time::Instant {
        #[cfg(feature = "test-support")]
        if let Some(clock) = &self.seeded {
            return clock.instant();
        }
        std::time::Instant::now()
    }

    #[cfg(all(test, target_os = "macos", feature = "test-support"))]
    pub(super) fn elapsed(&self) -> Option<Duration> {
        self.seeded.as_ref().map(|clock| clock.elapsed())
    }
}

#[cfg(feature = "test-support")]
thread_local! {
    static HOST_MILLIS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Exercises the production timestamp callbacks on a thread without a Tokio or
/// simulation context. The host-clock override is thread-local and restored on
/// unwind; it never changes the machine clock or another simulation's inputs.
#[cfg(all(
    feature = "test-support",
    any(target_os = "linux", target_os = "macos")
))]
pub fn capture_mount_clock_callbacks() -> impl FnOnce(u64, bool) -> Vec<u8> + Send {
    let clock = Clock::capture();
    move |host_millis, restore_host_callback| {
        struct Restore(Option<u64>);
        impl Drop for Restore {
            fn drop(&mut self) {
                HOST_MILLIS.set(self.0);
            }
        }
        let _restore = Restore(HOST_MILLIS.replace(Some(host_millis)));
        let metadata = super::create_metadata(&clock, 0o644, libc::S_IFREG as u32, 1, 2);
        let mut bytes = crate::kernel::encode_file_metadata(
            metadata,
            |_| Ok::<_, crate::FsError>(()),
            |_| crate::FsError::from(crate::kernel::CanonicalDecodeError::AllocationFailed),
        )
        .expect("canonical metadata trace");
        bytes.extend_from_slice(
            &super::adapter::content_change_time(&clock)
                .unwrap()
                .to_le_bytes(),
        );
        // The mutant restores only one callback's host source. It is stable
        // within a run, so ordinary in-process repetition cannot hide it.
        let callback_clock = if restore_host_callback {
            Clock::default()
        } else {
            clock
        };
        #[cfg(target_os = "linux")]
        let timestamp =
            super::fuse::time_or_now_ns(&callback_clock, fuser::TimeOrNow::Now).unwrap();
        #[cfg(target_os = "macos")]
        let timestamp = {
            let mut field = crate::kernel::MetadataField::Unavailable;
            super::darwin_mount::update_time(&callback_clock, &mut field, 0, libc::UTIME_NOW)
                .unwrap();
            match field {
                crate::kernel::MetadataField::Value(value) => value,
                _ => unreachable!(),
            }
        };
        bytes.extend_from_slice(&timestamp.to_le_bytes());
        bytes
    }
}
