use acyclic_fs::GenerationId;
use acyclic_fs::merge_driver::*;
use acyclic_fs_format::{
    identity::{Digest, FileId},
    object_reference::ObjectStorage,
};
use std::sync::Arc;

struct MarkerDriver(&'static [u8]);

impl MergeDriver for MarkerDriver {
    fn fingerprint(&self) -> &[u8] {
        self.0
    }

    fn mode(&self) -> MergeDriverMode {
        MergeDriverMode::Deterministic
    }

    fn resolve(&self, _: &ConflictView) -> Result<MergeResolution, DriverError> {
        Err(DriverError::Unsupported)
    }
}

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

#[test]
fn attribute_selection_is_ordered_and_text_projection_is_deterministic() {
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("text", Arc::new(DefaultTextMergeDriver))
        .expect("register driver");
    registry
        .set_rules(vec![AttributeRule {
            pattern: "*.rs".to_owned(),
            driver: "text".to_owned(),
        }])
        .expect("set rule");
    let driver = registry.select("src/lib.rs").expect("matching driver");
    let conflict = ConflictView {
        key: ConflictKey::Metadata(FileId::new()),
        path: Some("src/lib.rs".to_owned()),
        kind: ConflictKind::Text,
        base: ConflictValue::Text("base".to_owned()),
        ours: ConflictValue::Text("ours".to_owned()),
        theirs: ConflictValue::Text("theirs".to_owned()),
    };
    assert!(matches!(
        driver.resolve(&conflict).expect("resolve"),
        MergeResolution::Text(text)
            if acyclic_fs_base::text_merge::has_conflict_markers(&text)
                && std::str::from_utf8(&text).unwrap().contains("<<<<<<< ours")
                && std::str::from_utf8(&text).unwrap().contains(">>>>>>> theirs")
    ));
}

#[test]
fn default_text_driver_refuses_binary_text_values_on_every_side() {
    for side in 0..3 {
        for offset in [0, 8191, 8192] {
            let mut values = ["base".to_owned(), "ours".to_owned(), "theirs".to_owned()];
            let mut text = "a".repeat(8193);
            text.replace_range(offset..=offset, "\0");
            values[side] = text;
            let [base, ours, theirs] = values;
            let conflict = ConflictView {
                key: ConflictKey::File(FileId::new()),
                path: None,
                kind: ConflictKind::Text,
                base: ConflictValue::Text(base),
                ours: ConflictValue::Text(ours),
                theirs: ConflictValue::Text(theirs),
            };
            let result = DefaultTextMergeDriver.resolve(&conflict);
            if offset < 8192 {
                assert_eq!(result, Err(DriverError::Unsupported));
            } else {
                assert!(matches!(result, Ok(MergeResolution::Text(_))));
            }
        }
    }
}

#[test]
fn duplicate_registration_keeps_the_original_driver() {
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("driver", Arc::new(MarkerDriver(b"original")))
        .expect("register original");
    assert_eq!(
        registry.register("driver", Arc::new(MarkerDriver(b"replacement"))),
        Err(DriverRegistrationError::Duplicate("driver".to_owned()))
    );
    registry.set_default("driver").expect("select original");
    assert_eq!(
        registry
            .select("anything")
            .expect("registered driver")
            .fingerprint(),
        b"original"
    );
}

#[test]
fn git_attributes_ignore_unrelated_attributes_and_accept_merge_among_them() {
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("text", Arc::new(DefaultTextMergeDriver))
        .expect("register driver");
    registry
        .set_git_attributes("*.json text eol=lf\n*.bin binary\n*.rs text merge=text eol=lf\n")
        .expect("ordinary Git attributes");
    assert!(registry.select("data.json").is_none());
    assert!(registry.select("asset.bin").is_none());
    assert!(registry.select("src/lib.rs").is_some());
    assert_eq!(
        registry.set_git_attributes("*.rs merge="),
        Err(DriverRegistrationError::InvalidAttributeRule(1))
    );
}

#[test]
fn resolution_reuse_is_bound_to_exact_sides_and_driver_version() {
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("text", Arc::new(DefaultTextMergeDriver))
        .expect("register driver");
    registry.set_default("text").expect("default driver");
    let conflict = ConflictView {
        key: ConflictKey::ContentRange {
            file_id: FileId::new(),
            offset: 0,
            length: 1,
        },
        path: Some("file.txt".to_owned()),
        kind: ConflictKind::Text,
        base: ConflictValue::Text("base".to_owned()),
        ours: ConflictValue::Text("ours".to_owned()),
        theirs: ConflictValue::Text("theirs".to_owned()),
    };
    let plan = MergePlan {
        base: generation(1),
        ours: generation(2),
        theirs: generation(3),
        conflicts: vec![conflict],
        truncated: false,
    };
    let mut cache = MemoryMergeResolutionCache::default();
    let first =
        resolve_merge_plan(plan.clone(), &registry, &mut cache, false).expect("initial resolution");
    let key = |plan: &MergePlan| {
        ResolutionKey::new(
            plan.base,
            plan.ours,
            plan.theirs,
            &plan.conflicts[0].key,
            registry.select("file.txt").unwrap().fingerprint(),
        )
    };
    assert!(cache.get(key(&plan)).is_some());
    for side in 0..3 {
        let mut alias = plan.clone();
        let target = match side {
            0 => &mut alias.base,
            1 => &mut alias.ours,
            _ => &mut alias.theirs,
        };
        let mut root = target.object_id();
        root.storage = Some(ObjectStorage {
            locator: Digest::from_bytes([189; 32]),
            root_file_id: FileId::from_bytes([190; 16]),
            origin_generation: root.digest,
        });
        *target = GenerationId::from_root(root);
        assert!(cache.get(key(&alias)).is_none());
    }
    assert_eq!(
        resolve_merge_plan(plan, &registry, &mut cache, true).expect("exact cache reuse"),
        first
    );
}

#[test]
fn duplicate_conflict_keys_are_rejected_before_driver_execution() {
    let mut registry = MergeDriverRegistry::new();
    registry
        .register("text", Arc::new(DefaultTextMergeDriver))
        .expect("register driver");
    registry.set_default("text").expect("default driver");
    let conflict = ConflictView {
        key: ConflictKey::Metadata(FileId::new()),
        path: Some("file.txt".to_owned()),
        kind: ConflictKind::Text,
        base: ConflictValue::Text("base".to_owned()),
        ours: ConflictValue::Text("ours".to_owned()),
        theirs: ConflictValue::Text("theirs".to_owned()),
    };
    let plan = MergePlan {
        base: generation(1),
        ours: generation(2),
        theirs: generation(3),
        conflicts: vec![conflict.clone(), conflict.clone()],
        truncated: false,
    };

    assert_eq!(
        resolve_merge_plan(
            plan,
            &registry,
            &mut MemoryMergeResolutionCache::default(),
            false,
        ),
        Err(MergePlanResolutionError::DuplicateConflict(conflict.key))
    );
}
