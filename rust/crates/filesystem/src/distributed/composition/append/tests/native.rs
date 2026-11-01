use super::*;

#[tokio::test]
async fn contributed_native_backing_survives_preparation_and_refuses_foreign_growth() -> TestResult
{
    crate::initialize_test_native_io();
    for count in [1, 4] {
        let provider = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(provider.clone());
        let authority = AuthorityId::from_bytes([33; 16]);
        let other_authority = AuthorityId::from_bytes([34; 16]);
        let cancellation = CancellationToken::new();
        let mut setup = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        for authority in [authority, other_authority] {
            store
                .create_authority(authority, Epoch::GENESIS, &mut setup, None::<&mut ()>)
                .await?;
        }
        drop(setup);
        let root = crate::NativeMaterializationWork::new(
            crate::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let baseline = root.live_allocation_bytes();
        let (original, stop) = root.invocation_view()?;
        let members = crate::distributed::append::group_tests::requests(authority, count)?;
        let mut publication = caller(b"original-native-append")?;
        let mut work = AuthorityWork::native(&original, &cancellation);
        let FilesystemAppendPreparation::Ready(prepared) = store
            .contribute_append_group(&mut publication, &members, &[], &mut work)
            .await?
        else {
            return Err("original native contribution was refused".into());
        };
        assert_eq!(work.spent().backend_write_operations, 0);
        let allocation = publication
            .allocation
            .as_ref()
            .ok_or("contribution lost its construction allocation")?;
        let retained = allocation.bytes();
        assert!(retained > 0);
        assert!(
            allocation
                .work()
                .authority_read_original()
                .unwrap_or(allocation.work())
                .shares_invocation_context(&original)
        );
        assert_eq!(work.request_allocation.as_ref().unwrap().bytes(), 0);
        drop(work);

        // A second preparation must not install foreign-funded capacity in
        // the original owner before its final context transfer can refuse.
        let foreign = crate::NativeMaterializationWork::new(
            crate::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(error, _)| error)?;
        let foreign_members =
            crate::distributed::append::group_tests::requests(other_authority, count)?;
        let mut foreign_work = AuthorityWork::native(&foreign, &cancellation);
        let request = publication.request().clone();
        let authorities = publication.authorities.as_ptr();
        let contributions = publication.contributions.as_ptr();
        assert!(matches!(
            store
                .contribute_append_group(
                    &mut publication,
                    &foreign_members,
                    &[],
                    &mut foreign_work,
                )
                .await,
            Err(AuthorityStoreError::NativeOriginal(
                crate::NativeTreeMaterializationError::InvalidLayout
            ))
        ));
        assert_eq!(publication.request(), &request);
        assert_eq!(publication.authorities.as_ptr(), authorities);
        assert_eq!(publication.contributions.as_ptr(), contributions);
        assert_eq!(publication.authorities(), &[authority]);
        assert_eq!(publication.contributions.len(), 1);
        assert_eq!(publication.allocation.as_ref().unwrap().bytes(), retained);
        assert!(foreign_work.request_allocation.as_ref().unwrap().bytes() > 0);
        assert_eq!(foreign_work.spent().backend_write_operations, 0);
        drop(foreign_work);
        drop(foreign);

        // Withdrawal does not retire construction backing still owned by the
        // exact unsent request. No publication or replacement producer exists.
        stop.cancel();
        assert_eq!(publication.allocation.as_ref().unwrap().bytes(), retained);
        assert_eq!(publication.request(), &request);
        drop(request);
        let mut observation = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        for authority in [authority, other_authority] {
            assert_eq!(
                store.head(authority, &mut observation).await?,
                Head::genesis(Epoch::GENESIS)
            );
        }
        drop(observation);
        drop(prepared);
        drop(publication);
        drop(store);
        drop(provider);
        drop(original);
        drop(stop);
        assert_eq!(root.live_allocation_bytes(), baseline);
    }
    Ok(())
}
