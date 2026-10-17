use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    // Real provider property: only a denied/absent parcel is an inner error.
    // Stale owners remain outer errors; rejected parcels publish nothing;
    // the public API preserves exact errors and successful retry identity.
    #[test]
    fn preflight_rejections_cannot_mask_fencing_or_publish_a_parcel(
        bytes in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            for mode in 0..4 {
                let f = fixture::fixture().await?;
                let mut sender = f.admission(1, json!(0));
                if mode == 0 {
                    let read = f.storage.capability(VolumeOperation::Read, &mut None)?;
                    sender.grants = Capabilities::new(sender.grants.iter()
                        .filter(|grant| *grant != read).map(str::to_owned));
                }
                f.host.admit(sender).await?;
                f.host.admit(f.admission(2, json!(0))).await?;
                let lease = f.claim(1, "mail-preflight").await?;
                let fence = (&lease.reservation).into();
                if mode == 3 { f.replace(1).await?; }
                let sender = TaskId::from_bytes([1;16]);
                let recipient = TaskId::from_bytes([if mode == 1 {3} else {2};16]);
                let message = OperationId::from_bytes([8;16]);
                let payload = crate::conversation::ContentPublisher::stage(
                    f.publisher.as_ref(), message, "parcel", &bytes,
                    "application/octet-stream", "parcel", None,
                )?.await?.0;
                let result = f.host.send_mail_with_preflight(
                    sender, &fence, recipient, message, payload.clone(), None,
                ).await;
                let public = f.host.send_mail(
                    sender, &fence, recipient, message, payload.clone(), None,
                ).await;
                match result {
                    Ok(Ok(sent)) if mode == 2 => {
                        assert_eq!(sent, payload);
                        assert_eq!(public?, sent);
                    }
                    Ok(Err(error)) if mode <= 1 => {
                        assert!(matches!((&error, mode),
                            (Error::Unauthorized(_),0) | (Error::NotFound(_),1)));
                        let repeated = public.unwrap_err();
                        assert_eq!(std::mem::discriminant(&error), std::mem::discriminant(&repeated));
                        assert_eq!(error.to_string(), repeated.to_string());
                    }
                    Err(error) if mode == 3 => {
                        let repeated = public.unwrap_err();
                        assert_eq!(std::mem::discriminant(&error), std::mem::discriminant(&repeated));
                        assert_eq!(error.to_string(), repeated.to_string());
                    }
                    other => panic!("wrong mail error channel for mode {mode}: {other:?}"),
                }
                assert_eq!(f.host.inbox(TaskId::from_bytes([2;16]),0,2).await?.len(),
                    usize::from(mode == 2), "only an admitted parcel may reach the inbox");
            }
            Ok::<_, Error>(())
        }).unwrap();
    }
}
