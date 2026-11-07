//! Appended blocks only narrow: typed grant values and allowance products;
//! an allowance restricts each action by what the action catalog says it bills.
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

fn owned_execution(
    action: &str,
    owner: &str,
    cpu_millis: Option<u64>,
    memory_bytes: Option<u64>,
    max_concurrency: Option<u64>,
) -> Grant {
    Grant {
        kind: GrantKind::Execution(ExecutionGrant {
            grant: ResourceGrant {
                resource: ResourcePattern {
                    name: "execution:/work".into(),
                    prefix: false,
                },
                actions: vec![action.into()],
            },
            cpu_millis,
            memory_bytes,
            policy_generation: None,
            usage_owner: Some(owner.into()),
            max_concurrency,
            reserved_capacity_units: None,
        }),
    }
}

#[test]
fn renewed_allowance_restricts_only_what_each_action_bills() {
    // What a renewal signs: the billing products the account's usage keys cover.
    let covered = [
        "objects",
        "streams",
        "actors",
        "workers",
        "vmm",
        "inference",
    ];
    let cases = [
        ("billing/usage", "billing:/account", None, true),
        ("secrets/put", "secrets:/account/key", None, true),
        ("actors/deploy-namespace", "actors:/account/ns", None, true),
        ("machines/create", "machines:/account/m", None, true),
        ("infer/generate", "infer:/account/model", None, true),
        (
            "execution/actor-turn",
            "execution:/work",
            Some("actors/receipt"),
            true,
        ),
        (
            "execution/worker-execute",
            "execution:/work",
            Some("managed_agents/a"),
            false,
        ),
        ("fs/write", "fs:/v/file", None, false),
    ];
    let mut leaf = Vec::new();
    for (action, resource, owner, _) in cases {
        leaf.push(match owner {
            Some(owner) => owned_execution(action, owner, None, None, None),
            None => plain(resource, false, action),
        });
    }
    let renewed = append(&chain(leaf), allowance(&covered)).expect("a renewed allowance");
    let attributes = RecordAttributes::default();
    let usage = limits(None);
    for (action, resource, _, admitted) in cases {
        let request = Request {
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
        assert_eq!(
            permits(&renewed, &request, PolicyDecision::Allow, Scope::Exact).is_ok(),
            admitted,
            "{action}"
        );
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn execution_allowances_require_one_eligible_grant(
        ceilings in proptest::array::uniform3((
            proptest::option::of(0_u64..4),
            proptest::option::of(0_u64..4),
            proptest::option::of(0_u64..4),
        )),
        requested in (0_u64..4, 0_u64..4, 0_u64..4),
        masks in proptest::array::uniform2(1_u8..4),
    ) {
        let owners = ["actors/a", "managed_agents/a", "actors/b"];
        let grants = ceilings.iter().zip(owners).map(|(&(cpu, memory, concurrency), owner)| {
            owned_execution("execution/job-execute", owner, cpu, memory, concurrency)
        }).collect();
        let mut blocks = chain(grants);
        for mask in masks {
            let products = [(1, "actors"), (2, "managed_agents")].into_iter()
                .filter_map(|(bit, product)| (mask & bit != 0).then_some(product))
                .collect::<Vec<_>>();
            blocks = append(&blocks, allowance(&products)).expect("canonical allowance attenuation");
        }
        let expected = ceilings.iter().enumerate().any(|(index, &(cpu, memory, concurrency))| {
            cpu.is_none_or(|limit| requested.0 <= limit)
                && memory.is_none_or(|limit| requested.1 <= limit)
                && concurrency.is_none_or(|limit| requested.2 <= limit)
                && masks.iter().all(|mask| mask & if index == 1 { 2 } else { 1 } != 0)
        });
        let attributes = RecordAttributes::default();
        let mut usage = limits(None);
        usage.concurrency = Some(requested.2);
        let request = Request {
            target: Target::Resource { action: "execution/job-execute", resource: "execution:/work" },
            audience: "workers:/app",
            holder: None,
            now_unix_seconds: 2,
            keys: &[],
            attributes: &attributes,
            usage: &usage,
            cpu_millis: requested.0,
            memory_bytes: requested.1,
        };
        proptest::prop_assert_eq!(
            permits(&blocks, &request, PolicyDecision::Allow, Scope::Exact).is_ok(),
            expected,
            "billing allowance combined incompatible grants",
        );
    }
}
