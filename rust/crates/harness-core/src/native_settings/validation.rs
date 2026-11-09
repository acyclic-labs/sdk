//! The same production-bound oracles run in unit tests and fuzz targets.
use super::{model_selection as selection, workspace};
#[cfg(feature = "hooks")]
use crate as harness;

mod model;
#[cfg(feature = "hooks")]
mod settings;
#[path = "validation/workspace.rs"]
mod workspace_contract;

proptest::proptest! {
    #[test]
    fn shared_workspace_contract(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..128)) {
        workspace_contract::exercise(&input);
    }

    #[test]
    fn shared_model_selection_contract(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..128)) {
        model::exercise(&input);
    }
}

#[cfg(feature = "hooks")]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn shared_settings_selection_contract(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64)) {
        settings::exercise(&input);
    }
}
