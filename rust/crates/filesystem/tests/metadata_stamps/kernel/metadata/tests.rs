use acyclic_fs_format::metadata::{FileMetadata, MetadataField};
use proptest::prelude::*;

use crate::portable_metadata as fixture;
use fixture::metadata;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn derived_stamp_updates_allow_equal_ticks_and_preserve_recorded_facts(
        accessed in any::<i64>(),
        changed in any::<i64>(),
        advance in 1_i64..i64::MAX,
    ) {
        let prior = FileMetadata {
            accessed_ns: MetadataField::Value(accessed),
            changed_ns: MetadataField::Value(changed),
            ..metadata()
        };
        prop_assert!(prior.only_advanced_derived_stamps(&prior));
        // In particular, ctime need not advance when atime does.
        let advanced = FileMetadata {
            accessed_ns: MetadataField::Value(accessed.saturating_add(advance)),
            ..prior
        };
        prop_assert!(advanced.only_advanced_derived_stamps(&prior));
        let advanced = FileMetadata {
            changed_ns: MetadataField::Value(changed.saturating_add(advance)),
            ..advanced
        };
        prop_assert!(advanced.only_advanced_derived_stamps(&prior));
        let recorded_change = FileMetadata {
            modified_ns: MetadataField::Unavailable,
            ..advanced
        };
        prop_assert!(!recorded_change.only_advanced_derived_stamps(&prior));
        for access_time in [false, true] {
            let mut unavailable = prior;
            let before = if access_time {
                unavailable.accessed_ns = MetadataField::Unavailable;
                accessed
            } else {
                unavailable.changed_ns = MetadataField::Unavailable;
                changed
            };
            prop_assert!(unavailable.only_advanced_derived_stamps(&unavailable));
            prop_assert!(!unavailable.only_advanced_derived_stamps(&prior));
            prop_assert!(!prior.only_advanced_derived_stamps(&unavailable));
            if let Some(rewound) = before.checked_sub(1) {
                let mut rewound_record = prior;
                if access_time {
                    rewound_record.accessed_ns = MetadataField::Value(rewound);
                } else {
                    rewound_record.changed_ns = MetadataField::Value(rewound);
                }
                prop_assert!(!rewound_record.only_advanced_derived_stamps(&prior));
            }
        }
    }
}
