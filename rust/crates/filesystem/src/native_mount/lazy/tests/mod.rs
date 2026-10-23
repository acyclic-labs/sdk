use super::*;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[derive(Default)]
struct Recorded(Mutex<Vec<ViewStamp>>);

mod fixtures;
#[cfg(unix)]
mod hard_links;
mod live_mount;
mod publication;
mod source_changes;
use fixtures::*;

mod access;
