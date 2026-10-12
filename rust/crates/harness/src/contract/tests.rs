use super::*;

    #[test]
    fn policy_grants_intersect_and_any_deny_wins() {
        let resolved = resolve_policies(&[
            AuthorityPolicy {
                grants: Capabilities::new(["read", "write", "shell"]),
                denies: Capabilities::new(["shell"]),
            },
            AuthorityPolicy {
                grants: Capabilities::new(["read", "write"]),
                denies: Capabilities::new(["write"]),
            },
        ]);
        assert!(resolved.contains("read"));
        assert!(!resolved.contains("write"));
        assert!(!resolved.contains("shell"));
    }

    #[test]
    fn named_policy_layers_require_canonical_hierarchy_order() {
        let layer = |level| PolicyLayer { level, policy: AuthorityPolicy {
            grants: Capabilities::new(["read"]), denies: Capabilities::default(),
        } };
        assert!(resolve_policy_layers(&[layer(AuthorityLevel::Runtime), layer(AuthorityLevel::Invocation)]).is_ok());
        assert!(resolve_policy_layers(&[layer(AuthorityLevel::Agent)]).is_err());
    }

    #[test]
    fn component_label_validation_is_byte_bounded_and_unicode_precise() {
        assert!(validate_component_label("alpha", "label").is_ok());
        assert!(validate_component_label(&"é".repeat(127), "label").is_ok());
        assert!(validate_component_label(&"😀".repeat(63), "label").is_ok());
        for value in ["", ".", "..", "/", "\\", "a\n", "a\u{00a0}", "a\u{2003}", "a\u{007f}"] {
            assert!(
                validate_component_label(value, "label").is_err(),
                "{value:?}"
            );
        }
        assert!(validate_component_label(&"a".repeat(255), "label").is_ok());
        assert!(validate_component_label(&"a".repeat(256), "label").is_ok());
        assert!(validate_component_label(&"é".repeat(128), "label").is_ok());
    }
