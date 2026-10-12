use super::*;
use crate::{
    pi_caller,
    pi_fork::tests::{policy, work},
    pi_owner::PiOwnerConfig,
};

#[test]
fn operator_cli_requires_original_operation_decision_and_bounded_file_selection() {
    let parse =
        |args: &[&str]| parse_cli(&args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>());
    let digest = "a".repeat(64);
    let approvals = [
        "approvals",
        "--session",
        "root",
        "--operation",
        "sdk",
        "--json",
    ];
    let approve = [
        "approve",
        "--session",
        "root",
        "--operation",
        "sdk",
        "--digest",
        &digest,
        "--decision",
        "approve",
    ];
    assert!(!parse(&approvals).unwrap().project_files);
    parse(&approve).unwrap();
    let mut bad_digest = approve;
    bad_digest[6] = "bad";
    for args in [
        approvals[..5].to_vec(),
        approve[..7].to_vec(),
        bad_digest.to_vec(),
    ]
    .into_iter()
    .chain(
        [
            vec!["--session", "foreign"],
            vec!["--file", "selected.txt"],
            vec![
                "--project-files",
                "--file",
                "selected.txt",
                "--maximum-bytes",
                "262145",
            ],
        ]
        .map(|extra| [approvals.as_slice(), &extra].concat()),
    ) {
        assert!(parse(&args).is_err());
    }
    let selected = parse(
        &[
            approvals.as_slice(),
            &[
                "--project-files",
                "--file",
                "selected.txt",
                "--offset",
                "262144",
                "--maximum-bytes",
                "4096",
            ],
        ]
        .concat(),
    )
    .unwrap();
    assert!(selected.project_files);
    assert_eq!((selected.offset, selected.maximum_bytes), (262_144, 4096));
}

#[test]
fn prepared_descriptor_denies_exhausted_allocation() {
    let native = work();
    let mut held = native
        .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .unwrap();
    held.resize(native.remaining().unwrap().peak_allocation_bytes)
        .unwrap();
    assert!(matches!(
        decode_prepared_approval(b"{}", &native),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded { .. }
            ),
            custody: Some(_),
        })
    ));
}

#[test]
fn operator_proof_binds_body_owner_and_agent_and_cannot_be_replaced_by_a_native_caller() {
    let native = work();
    let owner = PiOwnerConfig::new(&policy(), None, &native).unwrap();
    let foreign = PiOwnerConfig::new(owner.policy(), None, &native).unwrap();
    let session = "original-root";
    let body = PiOperatorBody::Inspect {
        session: session.into(),
        operation: "original-sdk-writeback".into(),
        project_files: false,
        file_evidence: None,
    };
    let request =
        build_control_request(&owner, std::env::current_dir().unwrap(), body, &native).unwrap();
    let mut parsed =
        PiOperatorRequest::from_value_with_native_work(&request.0.arguments, &native).unwrap();
    verify_request(&owner, session, &parsed.0, &native).unwrap();
    assert!(verify_request(&foreign, session, &parsed.0, &native).is_err());
    let original_proof = parsed.0.operator.clone();
    let original_body = parsed.0.body.clone();
    let binding = crate::pi_binding::tests::binding(session, 81);
    let caller = pi_caller::issue(&owner, session, &binding, &native).unwrap();
    parsed.0.operator = caller.0;
    assert!(verify_request(&owner, session, &parsed.0, &native).is_err());
    parsed.0.operator = original_proof;
    let PiOperatorBody::Inspect { project_files, .. } = &mut parsed.0.body else {
        unreachable!()
    };
    *project_files = true;
    assert!(verify_request(&owner, session, &parsed.0, &native).is_err());
    parsed.0.body = PiOperatorBody::Inspect {
        session: session.into(),
        operation: "foreign-sdk-operation".into(),
        project_files: false,
        file_evidence: None,
    };
    assert!(verify_request(&owner, session, &parsed.0, &native).is_err());
    parsed.0.body = original_body;
    verify_request(&owner, session, &parsed.0, &native).unwrap();
    parsed.0.operator = owner
        .authority()
        .unwrap()
        .issuer
        .issue(
            &parsed.0.operator.id(),
            parsed.0.operator.capabilities(),
            Some(acyclic_harness::AgentId::from_bytes([99; 16])),
            None,
            &mut None,
        )
        .unwrap();
    assert!(verify_request(&owner, session, &parsed.0, &native).is_err());
}
