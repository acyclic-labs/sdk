use super::*;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

fn publication_model<const OMIT_LOOKUP: bool>() {
    loom::model(|| {
        let client = Arc::new(MemoryStream::default());
        let coordinator = Box::new(block_on(open(&client)).unwrap());
        // The host's production gate orders refresh, retry lookup and mutation.
        let shared = loom::sync::Arc::new(crate::sync::Mutex::new(coordinator));
        let task = OperationId::from_bytes([41; 16]);
        let declaration = spec(task, 0).unwrap();
        // Sign scopes in the serial prefix. Cryptography is covered by the
        // native properties; the modeled race starts at canonical publication.
        let (owner, issuer) = owner_issuer();
        let scope = grant(&issuer, "observe", &["operation:observe"]).unwrap();
        let verifier = issuer.verifier(&mut None).unwrap();
        let reader_authority = Box::new((owner.clone(), scope.clone(), verifier.clone()));
        let writer = {
            let shared = loom::sync::Arc::clone(&shared);
            let declaration = declaration.clone();
            loom::thread::spawn(move || {
                let mut coordinator = block_on(shared.lock());
                super::model_provider::declare_fixture(&mut coordinator, declaration, "task")
                    .unwrap();
            })
        };
        let reader = {
            let shared = loom::sync::Arc::clone(&shared);
            let declaration = declaration.clone();
            loom::thread::spawn(move || {
                let (owner, scope, verifier) = *reader_authority;
                let mut coordinator = block_on(shared.lock());
                let observed = block_on(coordinator.observe_declaration(
                    &owner,
                    &scope,
                    &verifier,
                    task,
                    &IdempotencyKey::new("task").unwrap(),
                    &mut None,
                ))
                .unwrap();
                if let Some(observed) = observed {
                    assert_eq!(observed, declaration);
                }
            })
        };
        writer.join().unwrap();
        reader.join().unwrap();
        struct Restore;
        impl Drop for Restore {
            fn drop(&mut self) {
                intent_index::set_omit_index_lookup(false);
            }
        }
        let _restore = Restore;
        intent_index::set_omit_index_lookup(OMIT_LOOKUP);
        let mut coordinator = block_on(shared.lock());
        let observed = block_on(coordinator.observe_declaration(
            &owner,
            &scope,
            &verifier,
            task,
            &IdempotencyKey::new("task").unwrap(),
            &mut None,
        ))
        .unwrap();
        assert_eq!(
            observed,
            Some(declaration),
            "committed declaration disappeared"
        );
    });
}

#[test]
fn retry_lookup_and_publication_share_the_host_gate() {
    publication_model::<false>();
}

#[test]
#[should_panic(expected = "committed declaration disappeared")]
fn omitted_production_lookup_fails_the_same_publication_model() {
    publication_model::<true>();
}
