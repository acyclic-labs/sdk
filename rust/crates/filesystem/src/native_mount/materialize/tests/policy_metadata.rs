use super::*;
use proptest::prelude::*;

fn materialize_policy(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(async {
        let cancellation = CancellationToken::new();
        let volume = super::super::tests::memory_volume(
            crate::VolumeId::new(),
            crate::VolumeConfig::native(crate::Lifecycle::Ephemeral),
            &cancellation,
        )
        .await?;
        let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
        checkout
            .write_access_policy(
                bytes,
                crate::NamedAttributeWriteMode::Create,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        #[cfg(target_os = "linux")]
        {
            let root = NamespacePath::new(Vec::new(), checkout.volume_config().limits)?;
            let name = crate::kernel::AttributeName::new(
                crate::kernel::AttributeClass::PosixXattr,
                b"user.retained".to_vec(),
                255,
            )?;
            checkout
                .change_named_attribute(
                    &root,
                    &name,
                    Some((bytes, crate::NamedAttributeWriteMode::Create)),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?;
        }
        let destination = tempfile::tempdir()?;
        materialize_checkout(
            &mut checkout,
            &MaterializeOptions::native(destination.path()),
            crate::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .await?;
        let (retained, _) = checkout
            .read_access_policy(WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        assert_eq!(retained.ok_or("policy disappeared")?.bytes.as_ref(), bytes);
        assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
        #[cfg(target_os = "linux")]
        assert!(matches!(
            rustix::fs::getxattr(
                destination.path(),
                "security.acyclic.access.v1",
                &mut [0_u8; 1][..],
            ),
            Err(rustix::io::Errno::NODATA)
        ));
        #[cfg(target_os = "linux")]
        {
            let mut exported = [0_u8; 128];
            let count =
                rustix::fs::getxattr(destination.path(), "user.retained", &mut exported[..])?;
            assert_eq!(&exported[..count], bytes);
        }
        Ok(())
    })
}

#[test]
fn volume_policy_is_not_a_native_extended_attribute() -> Result<(), Box<dyn std::error::Error>> {
    materialize_policy(b"volume authority metadata")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Materialization preserves every policy payload in the volume while never
    // exporting it through an operating-system extended attribute or stream.
    #[test]
    fn native_projection_preserves_reserved_policy(bytes in prop::collection::vec(any::<u8>(), 0..128)) {
        materialize_policy(&bytes).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
