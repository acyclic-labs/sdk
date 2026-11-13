use super::*;

struct ControlGated {
    package: Package,
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    release: tokio::sync::Semaphore,
}
impl Handler for ControlGated {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }
    fn admission_class(&self, actor: &ActorReference) -> crate::guest::AdmissionClass {
        if actor.id() == "controller" {
            crate::guest::AdmissionClass::Control
        } else {
            crate::guest::AdmissionClass::Work
        }
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            if self.admission_class(&delivery.actor) == crate::guest::AdmissionClass::Work {
                self.entered.send(()).map_err(|_| {
                    crate::guest::GuestError::Handler("entry observer closed".into())
                })?;
                self.release
                    .acquire()
                    .await
                    .map_err(|_| crate::guest::GuestError::Handler("work credit closed".into()))?
                    .forget();
            }
            self.package
                .on_record(delivery, checkpoint, cancellation)
                .await
        })
    }
}

/// Saturated application work cannot borrow control credit. The default also
/// admits past the former hidden64 ceiling, without tasks or timeout probes.
#[tokio::test]
async fn control_admission_progresses_with_saturated_work_and_default_is_unbounded() -> TestResult {
    for reserved in [false, true] {
        let workers = if reserved { 1 } else { 65 };
        let provider = Arc::new(MemoryStream::default());
        let store = Store::new(provider.clone(), root()?);
        let (entered, mut entries) = tokio::sync::mpsc::unbounded_channel();
        let handler = Arc::new(ControlGated {
            package: Package::new()?,
            entered,
            release: tokio::sync::Semaphore::new(0),
        });
        let limits = if reserved {
            RuntimeLimits::new(1, 1)?
        } else {
            RuntimeLimits::default()
        }
        .with_control_capacity(Some(1))?;
        let runtime = Runtime::new(
            crate::guest::Requests::default(),
            Store::new(provider.clone(), root()?),
            handler.clone(),
            Arc::new(SystemClock),
        )
        .with_limits(limits);
        for index in 0..workers {
            let worker = spawn_with(&store, &format!("worker-{index}"), &[b"emit"]).await?;
            runtime.track(worker)?;
        }
        let mut serving =
            Box::pin(runtime.run(|actor, error| panic!("admission {actor:?}: {error}")));
        observe_while_running(serving.as_mut(), async {
            for _ in 0..workers {
                entries.recv().await.ok_or("work producer closed")?;
            }
            Ok(())
        })
        .await?;
        observe_while_running(serving.as_mut(), async {
            let controller = spawn_with(&store, "controller", &[b"emit"]).await?;
            runtime.track(controller.clone())?;
            state_after(&store, &controller, |state| {
                state.cursor("inbox") == Some(1)
            })
            .await?;
            Ok(())
        })
        .await?;
        assert_eq!(
            records(&*provider, "out/controller").await?,
            vec![Bytes::from_static(b"r0")]
        );
        handler.release.add_permits(workers);
        observe_while_running(serving.as_mut(), async {
            runtime.complete().await?;
            Ok(())
        })
        .await?;
    }
    Ok(())
}

struct ExternalPackage {
    package: Package,
    external: Vec<String>,
    keyed: bool,
}

impl Handler for ExternalPackage {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }
    fn external_subscriptions(&self) -> &[String] {
        &self.external
    }
    fn subscription_key(
        &self,
        _: &str,
        _: Option<&Checkpoint>,
    ) -> Result<Option<Bytes>, crate::guest::GuestError> {
        Ok(self.keyed.then(|| Bytes::from_static(b"control")))
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        self.package.on_record(delivery, checkpoint, cancellation)
    }
}

struct OrderedPackage {
    package: Package,
    manifest: Manifest,
    ordered: Vec<String>,
}
impl Handler for OrderedPackage {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn ordered_subscriptions(&self) -> &[String] {
        &self.ordered
    }
    fn subscription_key(
        &self,
        id: &str,
        _: Option<&Checkpoint>,
    ) -> Result<Option<Bytes>, crate::guest::GuestError> {
        Ok(Some(Bytes::copy_from_slice(id.as_bytes())))
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        self.package.on_record(delivery, checkpoint, cancellation)
    }
}

#[tokio::test]
async fn indexed_inputs_preserve_source_order_without_changing_default_fairness() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let source = StreamPath::new("indexed/order")?;
    provider
        .append(AppendRequest {
            path: source.clone(),
            records: ["z", "inbox", "z"]
                .into_iter()
                .map(|key| {
                    Ok(AppendRecord {
                        value: Bytes::from_static(b"emit").into(),
                        headers: acyclic_stream::RecordHeaders::new(
                            vec![Bytes::copy_from_slice(key.as_bytes()).into()],
                            vec![],
                        )?,
                    })
                })
                .collect::<Result<_, StreamError>>()?,
            if_tail: Some(0),
            idempotency_key: key("ordered/source")?,
        })
        .await?;
    for ordered in [false, true] {
        let store = Store::new(
            provider.clone(),
            StreamPath::new(if ordered {
                "ordered/actors"
            } else {
                "fair/actors"
            })?,
        );
        let address = actor("ordered")?;
        let ids = ["inbox", "z"];
        let mut creation = PreparedPublish::foreign(key(if ordered {
            "ordered/create"
        } else {
            "fair/create"
        })?);
        creation.join(
            store.create(
                None,
                &address,
                crate::local::Creation {
                    subscriptions: ids
                        .into_iter()
                        .map(|id| {
                            (
                                id.into(),
                                Subscription {
                                    path: source.clone(),
                                    cursor: 0,
                                    paused: false,
                                },
                            )
                        })
                        .collect(),
                    checkpoint: Some(ledger(if ordered { "ordered/out" } else { "fair/out" })?),
                    escrow: None,
                },
            )?,
        )?;
        store.submit(creation).await?;
        let package = Package::new()?;
        let mut manifest = crate::guest::wire::decode_manifest(
            &crate::guest::wire::encode_manifest(package.manifest()),
        )?;
        manifest.subscriptions.push("z".into());
        let runtime = Runtime::new(
            crate::guest::Requests::default(),
            Store::new(provider.clone(), store.root().clone()),
            Arc::new(OrderedPackage {
                package,
                manifest,
                ordered: if ordered {
                    ids.into_iter().map(str::to_owned).collect()
                } else {
                    vec![]
                },
            }),
            Arc::new(SystemClock),
        );
        let snapshot = store.load(&address).await?;
        for after in [None, Some("inbox"), Some("z")] {
            let (trigger, record, _) = runtime
                .next_record(snapshot.state().ok_or("missing ordered state")?, after)
                .await?
                .ok_or("missing indexed input")?;
            let earliest = ordered || after == Some("inbox");
            assert_eq!(record.sequence, if earliest { 0 } else { 1 });
            assert_eq!(
                trigger.record_position(),
                Some((if earliest { "z" } else { "inbox" }, record.sequence))
            );
        }
        for paused in ["inbox", "z"] {
            let mut state = snapshot.state().ok_or("missing ordered state")?.clone();
            state.subscriptions.get_mut(paused).unwrap().paused = true;
            for after in [None, Some("inbox"), Some("z")] {
                let (trigger, record, _) = runtime
                    .next_record(&state, after)
                    .await?
                    .ok_or("paused member starved an active source member")?;
                let (active, sequence) = if paused == "inbox" {
                    ("z", 0)
                } else {
                    ("inbox", 1)
                };
                assert_eq!(record.sequence, sequence);
                assert_eq!(trigger.record_position(), Some((active, sequence)));
            }
            for bound in state.subscriptions.values_mut() {
                bound.paused = true;
            }
            assert!(runtime.next_record(&state, None).await?.is_none());
        }
    }
    Ok(())
}

#[tokio::test]
async fn ordered_source_backlog_yields_to_an_unrelated_ready_input() -> TestResult {
    for ordered in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let source = StreamPath::new("group/source")?;
        let unrelated = StreamPath::new("group/unrelated")?;
        for (path, keys) in [
            (source.clone(), vec!["z", "z", "a"]),
            (unrelated.clone(), vec!["other", "other", "other", "b"]),
        ] {
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: keys
                        .into_iter()
                        .map(|id| {
                            Ok(AppendRecord {
                                value: Bytes::from_static(b"emit").into(),
                                headers: acyclic_stream::RecordHeaders::new(
                                    vec![Bytes::copy_from_slice(id.as_bytes()).into()],
                                    vec![],
                                )?,
                            })
                        })
                        .collect::<Result<_, StreamError>>()?,
                    if_tail: Some(0),
                    idempotency_key: key(if path == source {
                        "group/source"
                    } else {
                        "group/unrelated"
                    })?,
                })
                .await?;
        }
        let store = Store::new(provider.clone(), StreamPath::new("group/actors")?);
        let address = actor("group")?;
        let mut creation = PreparedPublish::foreign(key("group/create")?);
        creation.join(
            store.create(
                None,
                &address,
                crate::local::Creation {
                    subscriptions: ["a", "b", "z"]
                        .into_iter()
                        .map(|id| {
                            (
                                id.into(),
                                Subscription {
                                    path: if id == "b" {
                                        unrelated.clone()
                                    } else {
                                        source.clone()
                                    },
                                    cursor: 0,
                                    paused: false,
                                },
                            )
                        })
                        .collect(),
                    checkpoint: Some(ledger("group/out")?),
                    escrow: None,
                },
            )?,
        )?;
        store.submit(creation).await?;
        let package = Package::new()?;
        let mut manifest = crate::guest::wire::decode_manifest(
            &crate::guest::wire::encode_manifest(package.manifest()),
        )?;
        manifest.subscriptions = ["a", "b", "z"].into_iter().map(str::to_owned).collect();
        manifest.interrupts[0].subscription = "a".into();
        let runtime = Runtime::new(
            crate::guest::Requests::default(),
            Store::new(provider.clone(), store.root().clone()),
            Arc::new(OrderedPackage {
                package,
                manifest,
                ordered: if ordered {
                    vec!["a".into(), "z".into()]
                } else {
                    vec![]
                },
            }),
            Arc::new(SystemClock),
        );
        runtime.track(address.clone())?;
        runtime.drain_actor(&address).await?;
        let expected = if ordered {
            vec!["r0", "r3", "r1", "r2"]
        } else {
            vec!["r2", "r3", "r0", "r1"]
        };
        assert_eq!(
            records(provider.as_ref(), "group/out").await?,
            expected.into_iter().map(Bytes::from).collect::<Vec<_>>(),
            "ordered source backlog monopolized delivery or lost source order",
        );
        let settled = store.load(&address).await?;
        let state = settled.state().ok_or("missing group state")?;
        assert_eq!(state.cursor("a"), Some(3));
        assert_eq!(state.cursor("b"), Some(4));
        assert_eq!(state.cursor("z"), Some(2));
    }
    Ok(())
}
async fn append_source<P: StreamProvider + ?Sized>(
    provider: &P,
    path: &StreamPath,
    sequence: u64,
    keyed: bool,
) -> TestResult {
    provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(
                vec![if sequence == 0 {
                    CommitCondition::Absent { path: path.clone() }
                } else {
                    CommitCondition::Tail {
                        path: path.clone(),
                        expected: sequence,
                    }
                }],
                vec![CommitMutation::Append {
                    path: path.clone(),
                    records: if keyed {
                        [b"output".as_slice(), b"control", b"output"]
                            .into_iter()
                            .map(|key| {
                                Ok(AppendRecord {
                                    value: Bytes::from_static(b"emit").into(),
                                    headers: acyclic_stream::RecordHeaders::new(
                                        vec![Bytes::copy_from_slice(key).into()],
                                        vec![],
                                    )?,
                                })
                            })
                            .collect::<Result<_, StreamError>>()?
                    } else {
                        vec![AppendRecord::from("emit")]
                    },
                }],
            )],
            idempotency_key: key(&format!("external-source/{sequence}"))?,
        })
        .await?;
    Ok(())
}

/// A committed source cursor, rather than a notification cursor, owns every
/// effect. Exercise real dispatch, pause/resume, restart and an activated-only
/// negative control without timeouts, readiness polling or producer wake writes.
#[tokio::test]
async fn external_sources_wake_shared_dispatch_and_preserve_committed_cursors() -> TestResult {
    for (external, keyed) in [(false, false), (true, false), (true, true)] {
        eprintln!("external source case external={external} keyed={keyed}: start");
        let width = if keyed { 3 } else { 1 };
        let selected = if keyed { 1 } else { 0 };
        let through = |batch| batch * width + selected + 1;
        let provider = Arc::new(Counted::new(MemoryStream::default()));
        let store = Store::new(provider.clone(), root()?);
        let address = actor("external")?;
        let source = StreamPath::new("agents/external")?;
        let mut creation = PreparedPublish::foreign(key("external/create")?);
        creation.join(store.create(
            None,
            &address,
            crate::local::Creation {
                subscriptions: BTreeMap::from([(
                    "inbox".into(),
                    Subscription {
                        path: source.clone(),
                        cursor: 0,
                        paused: false,
                    },
                )]),
                checkpoint: Some(ledger("out/external")?),
                escrow: None,
            },
        )?)?;
        creation.join(store.activation(&address)?)?;
        store.submit(creation).await?;
        let handler = Arc::new(ExternalPackage {
            package: Package::new()?,
            external: if external {
                vec!["inbox".into()]
            } else {
                Vec::new()
            },
            keyed,
        });
        let runtime = Runtime::new(
            crate::guest::Requests::default(),
            Store::new(provider.clone(), root()?),
            handler.clone(),
            Arc::new(SystemClock),
        );
        let mut serving =
            Box::pin(runtime.run(|_, error| panic!("source delivery failed: {error}")));
        observe_while_running(serving.as_mut(), async {
            runtime.complete().await?;
            Ok(())
        })
        .await?;
        eprintln!("external source: initial completion");
        let activation = provider.tail(root()?).await?;
        append_source(&*provider, &source, 0, keyed).await?;
        assert_eq!(
            provider.tail(root()?).await?,
            activation,
            "source producer wrote an actor wake"
        );
        if !external {
            observe_while_running(serving.as_mut(), async {
                runtime.complete().await?;
                Ok(())
            })
            .await?;
            assert_eq!(
                store
                    .load(&address)
                    .await?
                    .state()
                    .and_then(|state| state.cursor("inbox")),
                Some(0)
            );
            let mut wake = PreparedPublish::foreign(key("external/explicit-activation")?);
            wake.join(store.activation(&address)?)?;
            store.submit(wake).await?;
        }
        eprintln!("external source: awaiting first delivery");
        observe_while_running(
            serving.as_mut(),
            state_after(&store, &address, |state| {
                state.cursor("inbox") == Some(through(0))
            }),
        )
        .await?;
        observe_while_running(serving.as_mut(), async {
            runtime.complete().await?;
            Ok(())
        })
        .await?;
        assert_eq!(records(&*provider, "out/external").await?.len(), 1);
        if !external {
            continue;
        }

        eprintln!("external source: first delivery complete");
        let before = provider.requests();
        assert!(futures::poll!(serving.as_mut()).is_pending());
        let idle = provider.requests().since(before);
        assert_eq!(idle.control(), 0, "idle source follow re-read readiness");
        assert_eq!(idle.data(), 0, "idle source follow submitted work");

        observe_while_running(serving.as_mut(), async {
            runtime.pause(&address, true).await?;
            runtime.complete().await?;
            Ok(())
        })
        .await?;
        eprintln!("external source: pause complete");
        append_source(&*provider, &source, width, keyed).await?;
        observe_while_running(serving.as_mut(), async {
            runtime.complete().await?;
            Ok(())
        })
        .await?;
        assert_eq!(
            store
                .load(&address)
                .await?
                .state()
                .and_then(|state| state.cursor("inbox")),
            Some(through(0))
        );
        eprintln!("external source: paused publication complete");
        observe_while_running(serving.as_mut(), async {
            runtime.pause(&address, false).await?;
            Ok(())
        })
        .await?;
        observe_while_running(
            serving.as_mut(),
            state_after(&store, &address, |state| {
                state.cursor("inbox") == Some(through(1))
            }),
        )
        .await?;
        observe_while_running(serving.as_mut(), async {
            runtime.complete().await?;
            Ok(())
        })
        .await?;
        eprintln!("external source: resumed delivery complete");
        drop(serving);
        assert!(runtime.release(&address));
        append_source(&*provider, &source, 2 * width, keyed).await?;
        let reopened = Runtime::new(
            crate::guest::Requests::default(),
            Store::new(provider.clone(), root()?),
            handler,
            Arc::new(SystemClock),
        );
        let mut serving =
            Box::pin(reopened.run(|_, error| panic!("reopened source failed: {error}")));
        eprintln!("external source: awaiting reopened delivery");
        observe_while_running(
            serving.as_mut(),
            state_after(&store, &address, |state| {
                state.cursor("inbox") == Some(through(2))
            }),
        )
        .await?;
        observe_while_running(serving.as_mut(), async {
            reopened.complete().await?;
            Ok(())
        })
        .await?;
        assert_eq!(
            records(&*provider, "out/external").await?,
            (0..3)
                .map(|batch| Bytes::from(format!("r{}", batch * width + selected)))
                .collect::<Vec<_>>()
        );
        if keyed {
            provider
                .append(AppendRequest {
                    path: source.clone(),
                    records: vec![AppendRecord::from("emit")],
                    if_tail: Some(3 * width),
                    idempotency_key: key("external/unrelated-output")?,
                })
                .await?;
            let before = provider.requests();
            assert!(futures::poll!(serving.as_mut()).is_pending());
            let unrelated = provider.requests().since(before);
            assert_eq!(unrelated.control(), 0, "unrelated output woke the actor");
            assert_eq!(
                unrelated.data(),
                0,
                "unrelated output published a checkpoint"
            );
        }
    }
    Ok(())
}
