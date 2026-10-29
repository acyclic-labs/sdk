//! Cross-account caller representation and adversarial dispatch metadata.
use acyclic_auth::{Error, contract::*};
use prost::Message as _;

fn identity(account: &[u8], ns: &[u8], id: &[u8]) -> CallerIdentity {
    CallerIdentity {
        account: account.into(),
        ns: ns.into(),
        id: id.into(),
    }
}
fn authority(account: &[u8], expiry: u64) -> CallerAuthority {
    CallerAuthority {
        principal: identity(account, b"principals", b"customer"),
        expires_at_unix_seconds: expiry,
        revocation: CallerRevocation {
            keys: vec![CallerKeyRevocation {
                key_id: "online".into(),
                key_epoch: 7,
            }],
            blocks: vec![vec![1; 32], vec![2; 32]],
        },
    }
}

#[test]
fn direct_and_delegated_calls_preserve_customer_and_exact_recipient()
-> Result<(), Box<dyn std::error::Error>> {
    for delegated in [false, true] {
        let context = CallerContext {
            version: 1,
            caller: authority(b"customer-account", 200),
            audience: identity(
                if delegated {
                    b"platform-account"
                } else {
                    b"customer-account"
                },
                b"inference",
                b"worker",
            ),
            delegation: if delegated {
                vec![authority(b"platform-account", 150)]
            } else {
                vec![]
            },
            expires_at_unix_seconds: if delegated { 150 } else { 200 },
            // A synthetic scope reference, never an actual credential.
            capability_handle: vec![3],
        };
        let encoded = CallerContextProto::from(context.clone()).encode_to_vec();
        let wire = CallerContextProto::decode(encoded.as_slice())?;
        assert_eq!(CallerContext::try_from(wire.clone())?, context);
        let mut changed = wire.clone();
        changed.expires_at_unix_seconds += 1;
        assert!(
            CallerContext::try_from(changed).is_err(),
            "a longer effective lease was admitted"
        );
        let mut changed = wire.clone();
        changed.capability_handle.clear();
        assert!(CallerContext::try_from(changed).is_err());
        let mut changed = wire.clone();
        changed.version = 2;
        assert_eq!(
            CallerContext::try_from(changed),
            Err(Error::UnsupportedVersion(2))
        );
        let mut changed = wire.clone();
        changed
            .caller
            .as_mut()
            .unwrap()
            .revocation
            .as_mut()
            .unwrap()
            .blocks[0]
            .pop();
        assert!(CallerContext::try_from(changed).is_err());
        let mut changed = wire;
        changed.audience.as_mut().unwrap().account.clear();
        assert!(CallerContext::try_from(changed).is_err());
    }
    Ok(())
}
