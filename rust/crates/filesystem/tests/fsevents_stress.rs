//! FSEvents capacity and handoff guarantees on the native shared hub.

#![cfg(target_os = "macos")]

mod fsevents {
    mod hub {
        mod tests {
            include!("fsevents_stress/cases.rs");
        }
    }
}
