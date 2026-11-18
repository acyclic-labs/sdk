//! Public contracts for workspace generations and conditional mutations.

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;

#[path = "support/workspace.rs"]
mod fixtures;

mod workspace {
    mod tests {
        mod conditions;
        mod generation;
    }
}
