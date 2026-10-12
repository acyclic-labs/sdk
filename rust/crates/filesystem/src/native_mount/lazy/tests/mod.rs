use super::*;

/// Every view change a source reports.
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[derive(Default)]
struct Recorded(Mutex<Vec<ViewStamp>>);

/// Whether a test's source reports its changes or, like a root the host
#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
enum Watched {
    Yes,
    No,
}

mod fixtures;
mod live_mount;
mod publication;
mod source_changes;
use fixtures::*;
