//! `hooks/<name>` programs in the agent's home bind their own source
//! subscriptions on this companion. Each selected record becomes one inbox
//! input in the commit that consumes it, so delivery is exactly once.
use super::*;
use crate::config::{HookEvent, ProgramTrigger, Start};
use crate::conversation::ContentGrant;
use acyclic_actors::guest::SubscriptionChange;

/// A registered program's own source, `hook/<name>`.
pub(super) const HOOK: &str = "hook/";

/// The remainder of `key` below the complete component prefix `directory`.
fn below<'k>(key: &'k str, directory: &str) -> Option<&'k str> {
    if directory.is_empty() {
        return Some(key);
    }
    key.strip_prefix(directory)?.strip_prefix('/')
}

/// `root` joined with the relative `path`, either of which may be empty.
fn join(root: &str, path: &str) -> String {
    match (root.is_empty(), path.is_empty()) {
        (true, _) => path.to_owned(),
        (_, true) => root.to_owned(),
        _ => format!("{root}/{path}"),
    }
}

/// One home generation, read through the agent's own grant.
struct Home<'a, P: AuthorityProvider + ?Sized + 'static, O: 'static> {
    host: &'a Host<P, O>,
    entry: &'a WorkspaceEntry,
    grant: &'a ContentGrant,
    generation: &'a crate::resources::GenerationRef,
    ceiling: &'a crate::Capabilities,
    maximum_bytes: Option<u64>,
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> crate::config::Tree
    for Home<'_, P, O>
{
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> futures::future::BoxFuture<'a, crate::Result<Option<crate::conversation::Custody>>> {
        Box::pin(async move {
            let found = self
                .host
                .read_private_path_indexed(
                    self.entry.volume()?,
                    self.grant,
                    &join(&self.entry.root, path),
                    Some(self.generation),
                    self.maximum_bytes,
                    ContentAccess {
                        ceiling: Some(self.ceiling),
                        io: None,
                    },
                )
                .await?;
            Ok(found.map(|(_, bytes, _)| bytes))
        })
    }
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Controls<P, O> {
    /// Re-registers every `hooks/<name>` a home publication changed. An
    /// unchanged followed path keeps its cursor; a removed or refused program
    /// unbinds. A refusal is reported once as a lifecycle event.
    pub(super) async fn register_hooks(
        &self,
        mut state: State,
        delivery: &Delivery,
        record: &acyclic_stream::Record,
    ) -> Result<Transition, Error> {
        let home = state
            .volumes
            .iter()
            .find(|volume| volume.role == crate::agent::WorkspaceRole::Home)
            .cloned()
            .ok_or(Error::Record("hook source has no home"))?;
        // The home's own follow selects the stream. A forked home shares its
        // source's changes, so a record's appender may be an ancestor volume.
        let source = self.source(home.volume()?).map_err(turn)?;
        if delivery
            .subscriptions
            .get(HOOKS)
            .is_none_or(|cut| cut.path != source)
        {
            return Err(Error::Record("hook publication volume differs"));
        }
        let hooks = join(&home.root, "hooks");
        let mut names = std::collections::BTreeSet::new();
        for key in record.headers.keys() {
            if let Some(name) = std::str::from_utf8(key.as_ref())
                .ok()
                .and_then(|key| below(key, &hooks))
                .and_then(|rest| rest.split('/').next())
            {
                names.insert(name.to_owned());
            }
        }
        if names.is_empty() {
            return state.transition(Vec::new(), false);
        }
        let publication =
            acyclic_fs::session::appended_publication(&self.volumes.filesystem, record)
                .map_err(Error::Stream)?;
        let generation = self
            .volumes
            .generation_ref_id(publication.generation, None)
            .map_err(turn)?;
        let bindings = [state.private.clone(), home.clone()];
        let authority =
            agent_authority(&self.issuer, &self.admission, &state.agent, &bindings, &[])
                .await
                .map_err(turn)?;
        let grant = ContentGrant::verify_directory_read(
            &authority.verifier,
            &authority.scope,
            home.volume()?,
            &home.root,
            &mut None,
        )
        .map_err(turn)?;
        let tree = Home {
            host: &self.volumes,
            entry: &home,
            grant: &grant,
            generation: &generation,
            ceiling: authority.scope.capabilities(),
            maximum_bytes: self.maximum_bytes,
        };
        let mut changes = Vec::new();
        let mut rejected = Vec::new();
        for name in names {
            let directory = format!("hooks/{name}");
            let selected = match crate::plugins::load_program(&tree, &directory).await {
                Ok(program) => self.selector(&state, program.on()).await,
                Err(crate::Error::NotFound(path)) if path == format!("{directory}/program.ts") => {
                    Ok(None)
                }
                Err(error) => Err(error),
            };
            let selected = match selected {
                Ok(selected) => selected,
                Err(error) if error.uncertain() => return Err(turn(error)),
                Err(_) => {
                    rejected.push(crate::supervision::Lifecycle::ControlRejected {
                        path: format!("{directory}/program.ts"),
                    });
                    None
                }
            };
            let id = format!("{HOOK}{name}");
            let previous = state.hooks.remove(&name);
            if let Some((registered, from)) = selected {
                if previous
                    .as_ref()
                    .is_none_or(|previous| previous.path != registered.path)
                {
                    if previous.is_some() {
                        changes.push(SubscriptionChange::Remove { id: id.clone() });
                    }
                    changes.push(SubscriptionChange::Add {
                        id,
                        path: StreamPath::new(&registered.path).map_err(Error::Stream)?,
                        from,
                    });
                }
                state.hooks.insert(name, registered);
            } else if previous.is_some() {
                changes.push(SubscriptionChange::Remove { id });
            }
        }
        let mut effects = Vec::new();
        if !rejected.is_empty() {
            let position = crate::supervision::Position {
                stream: source,
                sequence: record.sequence,
            };
            effects.push(
                self.lifecycle(&state.agent, &position, rejected, None)
                    .await?,
            );
        }
        let mut transition = state.transition(effects, false)?;
        transition.subscriptions = changes;
        Ok(transition)
    }

    /// Resolves a program's selector to the stream it follows, under the
    /// agent's own authority. File selectors name a bound volume and need
    /// read access to the selected prefix; stream selectors stay within the
    /// agent's own subtree. Lifecycle programs are not source hooks.
    async fn selector(
        &self,
        state: &State,
        on: Option<&ProgramTrigger>,
    ) -> crate::Result<Option<(Registered, Option<u64>)>> {
        let start = |from: Start| match from {
            Start::Start => Some(0),
            Start::Now => None,
        };
        match on {
            Some(ProgramTrigger::File {
                volume,
                prefix,
                from,
            }) => {
                // The agent's own address of a bound entry, `volume:///<name>`.
                let name = volume
                    .strip_prefix("volume:///")
                    .map(|name| name.strip_suffix('/').unwrap_or(name));
                let entry = state
                    .volumes
                    .iter()
                    .find(|entry| Some(entry.name.as_str()) == name)
                    .ok_or_else(|| crate::Error::Unauthorized("hook volume is not bound".into()))?;
                let keys = join(&entry.root, prefix);
                let bindings = [state.private.clone(), entry.clone()];
                let bindings = if entry.id == state.private.id {
                    &bindings[..1]
                } else {
                    &bindings[..]
                };
                let authority =
                    agent_authority(&self.issuer, &self.admission, &state.agent, bindings, &[])
                        .await?;
                ContentGrant::verify_directory_read(
                    &authority.verifier,
                    &authority.scope,
                    entry.volume()?,
                    &keys,
                    &mut None,
                )?;
                Ok(Some((
                    Registered {
                        path: self.source(entry.volume()?)?.as_str().to_owned(),
                        event: HookEvent::FileChanged,
                        keys: Some((keys, entry.root.clone())),
                    },
                    start(*from),
                )))
            }
            Some(ProgramTrigger::Stream { path, from }) => {
                let agent = state.agent.as_str();
                if path != agent
                    && path
                        .strip_prefix(agent)
                        .is_none_or(|rest| !rest.starts_with('/'))
                {
                    return Err(crate::Error::Unauthorized(
                        "hook streams stay within the agent's subtree".into(),
                    ));
                }
                // Delivery appends to the inbox, and the hook's step moves the
                // status; following either would feed the hook its own output.
                if *path == *state.agent.inbox()?.as_str()
                    || *path == *state.agent.status()?.as_str()
                {
                    return Err(crate::Error::Invalid(
                        "a hook cannot follow the inbox or status its deliveries write".into(),
                    ));
                }
                Ok(Some((
                    Registered {
                        path: StreamPath::new(path)
                            .map_err(|error| crate::Error::Invalid(error.to_string()))?
                            .as_str()
                            .to_owned(),
                        event: HookEvent::StreamRecord,
                        keys: None,
                    },
                    start(*from),
                )))
            }
            Some(ProgramTrigger::Lifecycle { .. }) | None => Err(crate::Error::Unsupported(
                "only file and stream programs register sources".into(),
            )),
        }
    }

    /// Appends one inbox input for a record `hooks/<name>` selected, in the
    /// commit that consumes it. Its operation is the record's delivery
    /// identity, which the hook runner retains its outcome under.
    pub(super) async fn deliver_hook(
        &self,
        state: State,
        delivery: &Delivery,
        name: &str,
        record: &acyclic_stream::Record,
    ) -> Result<Transition, Error> {
        let followed = delivery
            .subscriptions
            .get(&format!("{HOOK}{name}"))
            .map(|cut| cut.path.clone())
            .ok_or(Error::Record("hook source is not bound"))?;
        let Some(registered) = state
            .hooks
            .get(name)
            .filter(|registered| registered.path == followed.as_str())
        else {
            return Err(Error::Record("hook source differs"));
        };
        let path = match &registered.keys {
            None => followed.as_str().to_owned(),
            Some((keys, root)) => {
                // A publication also lists the directories it creates, so a
                // key strictly below the prefix names the changed file; the
                // prefix key itself is selected only when nothing below it is.
                let changed = record
                    .headers
                    .keys()
                    .iter()
                    .filter_map(|key| std::str::from_utf8(key.as_ref()).ok());
                let selected = changed
                    .clone()
                    .find(|key| below(key, keys).is_some())
                    .or_else(|| changed.clone().find(|key| *key == keys.as_str()))
                    .map(|key| below(key, root).unwrap_or(key).to_owned());
                match selected {
                    Some(path) => path,
                    None => return state.transition(Vec::new(), false),
                }
            }
        };
        let closed = state.agent.closed()?;
        match self.store.provider().tail(closed.clone()).await {
            Ok(_) => return state.transition(Vec::new(), true),
            Err(acyclic_stream::StreamError::NotFound) => {}
            Err(error) => return Err(Error::Stream(error)),
        }
        let target = ActorReference::new(
            delivery.actor.account().as_bytes(),
            delivery.actor.namespace().as_str().as_bytes(),
            state.agent.id().as_str().as_bytes(),
        )
        .map_err(|_| Error::Record("control target identity"))?;
        let instance = format!("hooks/{name}");
        let hook = crate::agent::HookTrigger {
            name: name.to_owned(),
            event: registered.event,
            path,
            sequence: record.sequence,
            operation: crate::hooks::effects::record_operation(
                delivery.actor.account(),
                &followed,
                record,
                &instance,
            ),
        };
        let by = Sender::Host {
            host: "harness".into(),
        };
        let mut effects = vec![CommitEffect::hard(
            vec![CommitCondition::Absent { path: closed }],
            vec![],
        )];
        effects.extend(
            crate::agent::send(
                &state.agent,
                &by,
                &crate::agent::InboxRecord::SourceHook(hook),
            )?
            .effects,
        );
        effects.push(
            self.store
                .activation(&target)
                .map_err(|error| Error::Actor(Box::new(error.into_cause())))?,
        );
        state.transition(effects, false)
    }
}
