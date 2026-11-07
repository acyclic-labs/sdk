//! Appended blocks only narrow: typed grant values and allowance products.
#[path = "common/mod.rs"]
mod common;
use acyclic_auth::{authority::*, contract::*};
use common::*;

fn resource(name: &str, action: &str) -> ResourceGrant {
    ResourceGrant {
        resource: ResourcePattern {
            name: name.into(),
            prefix: true,
        },
        actions: vec![action.into()],
    }
}

fn execution(cpu: Option<u64>, concurrency: Option<u64>, reserved: Option<u64>) -> Grant {
    Grant {
        kind: GrantKind::Execution(ExecutionGrant {
            grant: resource("execution:/", "execution/invoke"),
            cpu_millis: cpu,
            memory_bytes: None,
            policy_generation: None,
            usage_owner: None,
            max_concurrency: concurrency,
            reserved_capacity_units: reserved,
        }),
    }
}

fn mount(volume: &str, uid: Option<u32>, gid: Option<u32>) -> Grant {
    Grant {
        kind: GrantKind::Filesystem(FilesystemGrant {
            grant: resource(volume, "fs/read"),
            posix_uid: uid,
            posix_gid: gid,
        }),
    }
}

fn allowance(products: &[&str]) -> Caveat {
    Caveat {
        kind: CaveatKind::Allowance(SignedAllowance {
            allowance: Allowance {
                account_id: "account".into(),
                spend_micros: 10,
                expires_at_unix_seconds: 1000,
                products: products.iter().map(|p| (*p).into()).collect(),
            },
            signature: vec![9; 64],
        }),
    }
}

fn granting(grants: Vec<Grant>) -> Caveat {
    Caveat {
        kind: CaveatKind::Grants(GrantSet { grants }),
    }
}

fn chain(leaf: Vec<Grant>) -> Vec<Block> {
    let mut root = root();
    root.certificates.as_mut().unwrap().keys[0].key.grants = GrantSet { grants: leaf };
    vec![root]
}

fn append(blocks: &[Block], caveat: Caveat) -> Option<Vec<Block>> {
    attenuate(blocks, vec![caveat], vec![6; 32]).ok()
}

#[test]
fn execution_limits_keep_or_lower_and_unset_inherits() {
    let parent = append(
        &chain(vec![execution(None, None, None)]),
        granting(vec![execution(Some(100), Some(4), Some(8))]),
    )
    .expect("a bounded child of an unbounded leaf");
    for (cpu, concurrency, reserved) in [
        (Some(100), Some(4), Some(8)),
        (Some(50), Some(1), Some(2)),
        (None, None, None),
    ] {
        assert!(
            append(
                &parent,
                granting(vec![execution(cpu, concurrency, reserved)])
            )
            .is_some()
        );
    }
    // Negative controls: raising any one bound refuses the whole chain.
    for (cpu, concurrency, reserved) in [
        (Some(101), Some(4), Some(8)),
        (Some(100), Some(5), Some(8)),
        (Some(100), Some(4), Some(9)),
    ] {
        let mut widened = parent.clone();
        widened.push(Block {
            certificates: None,
            caveats: vec![granting(vec![execution(cpu, concurrency, reserved)])],
            next_public_key: vec![6; 32],
        });
        assert!(
            !valid_chain(&widened),
            "{cpu:?} {concurrency:?} {reserved:?}"
        );
        assert!(
            append(
                &parent,
                granting(vec![execution(cpu, concurrency, reserved)])
            )
            .is_none()
        );
    }
}

#[test]
fn filesystem_generation_and_owner_pin_once() {
    let open = chain(vec![mount("fs:/v/", None, None)]);
    let pinned = append(&open, granting(vec![mount("fs:/v~3/", Some(10), None)]))
        .expect("a child pins an open generation and an unset owner");
    assert!(
        append(
            &pinned,
            granting(vec![mount("fs:/v~3/dir/", None, Some(20))])
        )
        .is_some()
    );
    assert!(
        append(
            &pinned,
            granting(vec![mount("fs:/v~3/", Some(10), Some(20))])
        )
        .is_some()
    );
    // Negative controls: re-pinning, reopening, or changing a set owner.
    assert!(append(&pinned, granting(vec![mount("fs:/v~4/", None, None)])).is_none());
    assert!(append(&pinned, granting(vec![mount("fs:/v/", None, None)])).is_none());
    assert!(append(&pinned, granting(vec![mount("fs:/v~3/", Some(11), None)])).is_none());
    assert!(append(&open, granting(vec![mount("fs:/w~1/", None, None)])).is_none());

    // An open pattern covers every generation at request time; a pinned one only its own.
    let open_pattern = ResourcePattern {
        name: "fs:/v/".into(),
        prefix: true,
    };
    let pinned_pattern = ResourcePattern {
        name: "fs:/v~3/".into(),
        prefix: true,
    };
    assert!(resource_matches(&open_pattern, "fs:/v~3/a"));
    assert!(resource_matches(&open_pattern, "fs:/v~4/a"));
    assert!(resource_matches(&pinned_pattern, "fs:/v~3/a"));
    assert!(!resource_matches(&pinned_pattern, "fs:/v~4/a"));
    assert!(!resource_matches(&pinned_pattern, "fs:/v/a"));
    assert!(!resource_matches(&open_pattern, "fs:/vw~3/a"));
}

#[test]
fn allowance_products_only_shrink_and_bound_requests() {
    let every = append(&chain(vec![mount("fs:/", None, None)]), allowance(&[]))
        .expect("an every-product allowance");
    let fs_only = append(&every, allowance(&["fs", "stream"])).expect("subset of every");
    assert!(append(&fs_only, allowance(&["fs"])).is_some());
    // Negative controls: a superset, a disjoint set, or reverting to every product.
    assert!(append(&fs_only, allowance(&["fs", "billing"])).is_none());
    assert!(append(&fs_only, allowance(&["billing"])).is_none());
    assert!(append(&fs_only, allowance(&[])).is_none());

    let attributes = RecordAttributes::default();
    let usage = limits(None);
    let request = |action, resource| Request {
        target: Target::Resource { action, resource },
        audience: "workers:/app",
        holder: None,
        now_unix_seconds: 2,
        keys: &[],
        attributes: &attributes,
        usage: &usage,
        cpu_millis: 0,
        memory_bytes: 0,
    };
    let narrowed = append(&every, allowance(&["stream"])).unwrap();
    let read = request("fs/read", "fs:/v~3/a");
    assert!(permits(&every, &read, PolicyDecision::Allow, Scope::Exact).is_ok());
    assert!(permits(&fs_only, &read, PolicyDecision::Allow, Scope::Exact).is_ok());
    assert!(permits(&narrowed, &read, PolicyDecision::Allow, Scope::Exact).is_err());
}

fn plain(name: &str, prefix: bool, action: &str) -> Grant {
    Grant {
        kind: GrantKind::Resource(ResourceGrant {
            resource: ResourcePattern {
                name: name.into(),
                prefix,
            },
            actions: vec![action.into()],
        }),
    }
}

fn socket(host: &str, ports: &[(u32, u32)]) -> Grant {
    Grant {
        kind: GrantKind::WebsocketConnect(WebsocketConnectGrant {
            rules: vec![SocketHostRule {
                host: host.into(),
                ports: ports
                    .iter()
                    .map(|&(first, last)| SocketPortRange { first, last })
                    .collect(),
            }],
        }),
    }
}

#[test]
fn plain_resource_and_socket_grants_narrow_too() {
    let leaf = chain(vec![
        plain("fs:/v/", true, "fs/read"),
        socket("*.example.com", &[(1000, 2000)]),
        socket("10.0.0.0/8", &[]),
    ]);
    for narrowed in [
        plain("fs:/v/dir/", true, "fs/read"),
        plain("fs:/v/file", false, "fs/read"),
        mount("fs:/v~2/", Some(1), None),
        socket("a.example.com", &[(1500, 1600)]),
        socket("*.b.example.com", &[(1000, 2000)]),
        socket("10.1.0.0/16", &[]),
        socket("10.2.3.4", &[]),
    ] {
        assert!(
            append(&leaf, granting(vec![narrowed.clone()])).is_some(),
            "{narrowed:?}"
        );
    }
    // Negative controls: a wider resource, a new action, a prefix over an
    // exact name, a sibling host, a wider port range, a wider network.
    for widened in [
        plain("fs:/", true, "fs/read"),
        plain("fs:/v/", true, "fs/write"),
        plain("fs:/w/", true, "fs/read"),
        socket("example.com", &[(1000, 2000)]),
        socket("a.example.org", &[(1000, 2000)]),
        socket("a.example.com", &[(999, 2000)]),
        socket("a.example.com", &[]),
        socket("10.0.0.0/7", &[]),
        socket("11.0.0.1", &[]),
    ] {
        assert!(
            append(&leaf, granting(vec![widened.clone()])).is_none(),
            "{widened:?}"
        );
    }
    let exact = append(&leaf, granting(vec![plain("fs:/v/file", false, "fs/read")])).unwrap();
    assert!(append(&exact, granting(vec![plain("fs:/v/file", true, "fs/read")])).is_none());
}
