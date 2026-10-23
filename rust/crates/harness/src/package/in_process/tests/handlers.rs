use super::*;

struct TextMachine {
    identity: MachineIdentity,
    schema: Value,
}

impl ResumableMachine for TextMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(json!(format!("text:{input}")))
    }
    fn transition(&self, _: &Value, input: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: input.clone(),
            commands: Vec::new(),
            status: MachineStatus::Suspended,
        })
    }
}

struct Package {
    manifest: Manifest,
    machines: Vec<Arc<dyn ResumableMachine>>,
}

impl ActorPackage for Package {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn machines(&self) -> Vec<Arc<dyn ResumableMachine>> {
        self.machines.clone()
    }
}

fn two_handlers() -> Package {
    let counter = package();
    let text = Arc::new(TextMachine {
        identity: MachineIdentity {
            name: "test.actor.text".into(),
            version: "1".into(),
            digest: [2; 32],
        },
        schema: json!({"type":"string"}),
    });
    let mut manifest = counter.manifest.clone();
    manifest.actors.push(text.identity.clone());
    Package {
        manifest,
        machines: vec![counter, text],
    }
}

#[test]
fn installed_handlers_are_exactly_the_declared_set() {
    let mut package = two_handlers();
    assert_eq!(validate_package(&package).unwrap().len(), 2);
    let text = package.machines.pop().unwrap();
    assert!(validate_package(&package).is_err(), "missing handler");
    package.machines.push(text.clone());
    package.machines.push(text);
    assert!(validate_package(&package).is_err(), "duplicate handler");
    package.machines.pop();
    package.manifest.actors.pop();
    assert!(validate_package(&package).is_err(), "undeclared handler");
    package.manifest.actors.clear();
    assert!(validate_package(&package).is_err(), "empty package");
}

#[test]
fn package_cannot_declare_two_digests_for_one_actor_version() {
    let mut package = two_handlers();
    let mut changed = package.manifest.actors[0].clone();
    changed.digest = [3; 32];
    package.manifest.actors.push(changed);
    assert!(validate_package(&package).is_err());
}

#[tokio::test]
async fn placement_selects_the_pinned_handler_and_cannot_rebind_it() {
    let package = Arc::new(two_handlers());
    let stream = Arc::new(MemoryStream::default());
    let host = InProcessRuntime::new(
        stream.clone(),
        package.manifest.bindings.clone(),
        Arc::new(Policy(AtomicBool::new(true))),
        Arc::new(Effects::default()),
    );
    host.install(package.clone()).await.unwrap();
    let text = package.manifest.actors[1].clone();
    let owner = placement();
    let mut foreign = text.clone();
    foreign.digest = [3; 32];
    assert!(host.place(owner.clone(), foreign, json!(7)).await.is_err());
    assert!(matches!(
        stream
            .tail(StreamPath::new(format!("runtime/{}", Manifest::workflow(owner.actor))).unwrap())
            .await,
        Err(acyclic_stream::StreamError::NotFound)
    ));
    let snapshot = host
        .place(owner.clone(), text.clone(), json!(7))
        .await
        .unwrap();
    assert_eq!(snapshot.checkpoint.machine, text);
    assert_eq!(snapshot.checkpoint.state, json!("text:7"));
    assert!(
        host.place(owner.clone(), counter_identity(), json!(7))
            .await
            .is_err()
    );
    assert!(
        host.deliver(owner.clone(), event(2, json!(8)))
            .await
            .is_err()
    );
    let changed = host
        .deliver(owner.clone(), event(2, json!("changed")))
        .await
        .unwrap();
    assert_eq!(changed.checkpoint.state, json!("changed"));
    let reopened = InProcessRuntime::new(
        stream,
        package.manifest.bindings.clone(),
        Arc::new(Policy(AtomicBool::new(true))),
        Arc::new(Effects::default()),
    );
    reopened.install(package).await.unwrap();
    assert_eq!(reopened.recover(owner).await.unwrap(), changed);
}
