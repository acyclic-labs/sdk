//! Lowers one [`Step`] into its single Streams commit.
//!
//! Own streams (conversation, status) are conditioned on the exact tails in the
//! prior state, because the agent is their only writer. A message to another
//! agent is a Streams soft append guarded by the recipient's `closed` marker:
//! an absent or archived recipient skips it, which the receipt reports, and
//! never fails the step. Protocol records (a parent archiving its subtree
//! awaits every child's `ChildDone`) go to tree members that exist by
//! construction and stay unconditional appends, so a moved tail or a closed
//! recipient never blocks a step. Spawns fork
//! the agent's own subtree at its consumed cursor; the same-commit conversation
//! append is a source-descendant write, which Streams excludes from the child's
//! inherited cut, so the child sees exactly the conversation before this step.
//!
//! A post to a scope channel appends the original post to the channel and
//! offers its waking Notice, by reference, to each member's inbox like a
//! message. The membership the handler read is a read guard: a membership
//! change racing the step refuses the commit, which redelivers the step over
//! the new membership, so no member that joined before the post misses it.
//! A creation joins its channels in its spawn's commit and a close leaves
//! them in its closing step's.

use super::{
    AgentPath, AgentRecord, AgentState, Closed, Control, Error, ForkAdopted, InboxRecord,
    Membership, Out, Sender, Step, VolumeBinding, commit,
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
struct Builder {
    conditions: BTreeMap<StreamPath, CommitCondition>,
    appends: BTreeMap<StreamPath, Vec<AppendRecord>>,
    /// Soft appends and the closed marker that guards each recipient.
    soft: BTreeMap<StreamPath, (Vec<AppendRecord>, StreamPath)>,
    forks: Vec<CommitMutation>,
    created: BTreeSet<StreamPath>,
}

impl Builder {
    fn condition(&mut self, condition: CommitCondition) {
        let path = match &condition {
            CommitCondition::Head { path, .. }
            | CommitCondition::Tail { path, .. }
            | CommitCondition::Absent { path }
            | CommitCondition::Present { path } => path.clone(),
        };
        self.conditions.entry(path).or_insert(condition);
    }

    /// A hard append to a path that also has soft records keeps their order
    /// and makes them hard: the hard append decides the path in one mutation.
    fn append(&mut self, path: StreamPath, record: impl Into<AppendRecord>) {
        let records = self.appends.entry(path.clone()).or_default();
        if let Some((soft, _)) = self.soft.remove(&path) {
            records.extend(soft);
        }
        records.push(record.into());
    }

    fn offer(&mut self, path: StreamPath, closed: StreamPath, record: impl Into<AppendRecord>) {
        match self.appends.get_mut(&path) {
            Some(records) => records.push(record.into()),
            None => self
                .soft
                .entry(path)
                .or_insert_with(|| (Vec::new(), closed))
                .0
                .push(record.into()),
        }
    }

    /// Rejects a write into a child this same commit forks; Streams forbids
    /// mutations under a fork destination.
    fn recipient(&self, to: &AgentPath) -> Result<(), Error> {
        let inbox = to.inbox()?;
        if self.created.iter().any(|created| {
            inbox == *created
                || inbox
                    .as_str()
                    .strip_prefix(created.as_str())
                    .is_some_and(|rest| rest.starts_with('/'))
        }) {
            return Err(Error::Record("a step cannot write to a child it spawns"));
        }
        Ok(())
    }

    /// Writes `record` to the membership of each of `state`'s `channels`.
    fn membership<'a>(
        &mut self,
        state: &AgentState,
        channels: impl IntoIterator<Item = &'a String>,
        record: &Membership,
    ) -> Result<(), Error> {
        for channel in channels {
            if !may_post(state.parent.as_ref(), state.profile.as_ref(), channel) {
                return Err(Error::Record("membership of a channel the agent lacks"));
            }
            self.append(state.path.channel_members(channel)?, record.encode()?);
        }
        Ok(())
    }

    /// Appends `state`'s `post` to `channel` and offers its delivery to every
    /// other member `members` read, guarded on that membership.
    fn post(
        &mut self,
        state: &AgentState,
        channel: &str,
        post: &super::ChannelPost,
        members: Option<&Members>,
    ) -> Result<(), Error> {
        let path = &state.path;
        if post.from != *path || !may_post(state.parent.as_ref(), state.profile.as_ref(), channel) {
            return Err(Error::Record(
                "post to a channel the agent is not a member of",
            ));
        }
        let members = members.ok_or(Error::Record(
            "post lowered without its channel's membership",
        ))?;
        // A read guard: conditions see the state before the commit, so it
        // holds over this commit's own joins and leaves too.
        self.condition(CommitCondition::Tail {
            path: path.channel_members(channel)?,
            expected: members.tail,
        });
        self.append(path.channel(channel)?, post.encode()?);
        let delivery = post.delivery().encode()?;
        for member in members.agents.iter().filter(|member| *member != path) {
            self.recipient(member)?;
            self.offer(member.inbox()?, member.closed()?, delivery.clone());
        }
        Ok(())
    }

    /// Forks the child `out` spawns, if it is a spawn, at the consumed
    /// `cursor` and the conversation before this step, and joins it to the
    /// channels its creation grants.
    fn spawn(
        &mut self,
        state: &AgentState,
        out: &Out,
        cursor: u64,
        prior_conversation: u64,
    ) -> Result<(), Error> {
        let Out::Spawn { profile, .. } = out else {
            return Ok(());
        };
        let Some(spawn) = spawned(state, out, cursor, prior_conversation)? else {
            return Ok(());
        };
        self.created.insert(spawn.child.inbox()?);
        let granted = profile.iter().flat_map(|profile| &profile.channels);
        self.membership(state, granted, &Membership::Joined(spawn.child.clone()))?;
        for condition in spawn.request.conditions {
            self.condition(condition);
        }
        self.forks.extend(spawn.request.mutations);
        Ok(())
    }

    /// Archives `target` by `id`: its inbox record and its closed marker.
    fn archive(
        &mut self,
        by: &Sender,
        target: &AgentPath,
        id: &super::IntentId,
    ) -> Result<(), Error> {
        self.recipient(target)?;
        let control = Control {
            by: by.clone(),
            id: id.clone(),
        };
        self.append(target.inbox()?, InboxRecord::Archive(control).encode()?);
        // Unconditional: the first marker record closes the child; a
        // cascade racing an independent archive adds a redundant one.
        self.append(
            target.closed()?,
            Closed {
                by: by.clone(),
                id: id.clone(),
            }
            .encode()?,
        );
        Ok(())
    }

    /// The one commit: forks first, then hard and soft appends.
    fn request(self, key: IdempotencyKey) -> CommitRequest {
        let mut mutations = self.forks;
        mutations.extend(
            self.appends
                .into_iter()
                .map(|(path, records)| CommitMutation::Append { path, records }),
        );
        mutations.extend(self.soft.into_iter().map(|(path, (records, closed))| {
            CommitMutation::SoftAppend {
                path,
                records,
                closed: Some(closed),
            }
        }));
        CommitRequest {
            deadline_unix_millis: None,
            conditions: self.conditions.into_values().collect(),
            mutations,
            idempotency_key: key,
        }
    }
}

/// One channel's membership as a step's handler read it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Members {
    /// Membership stream tail the agents were read at.
    pub tail: u64,
    /// Every joined, not yet left, member.
    pub agents: BTreeSet<AgentPath>,
}

/// What a step's channel effects need beyond the step itself.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Channels {
    /// The membership of each channel the step posts to.
    pub members: BTreeMap<String, Members>,
    /// Whether this step closes the agent, which leaves its channels.
    pub closing: bool,
}

/// Whether an agent with `parent` and creation `profile` may post to and
/// grant `channel`: a member, or the scope's root, which owns every channel
/// of its scope.
#[must_use]
pub fn may_post(
    parent: Option<&AgentPath>,
    profile: Option<&super::CreationConfig>,
    channel: &str,
) -> bool {
    parent.is_none() || profile.is_some_and(|profile| profile.channels.contains(channel))
}

/// Builds the step's one commit.
///
/// `cursor` is the number of inbox records consumed after this step, and `key`
/// is the delivery's stable identity, so a retried delivery replays exactly.
/// `channels` holds the membership of every channel the step posts to.
pub fn lower(
    step: &Step,
    cursor: u64,
    key: IdempotencyKey,
    channels: &Channels,
) -> Result<CommitRequest, Error> {
    let state = &step.state;
    let path = &state.path;
    let sender = Sender::Agent { path: path.clone() };
    let mut builder = Builder::default();
    if channels.closing {
        let own = state.profile.iter().flat_map(|profile| &profile.channels);
        builder.membership(state, own, &Membership::Left(path.clone()))?;
    }
    let conversation = path.conversation()?;
    let conversation_count = step
        .outs
        .iter()
        .filter(|out| matches!(out, Out::Conversation(_)))
        .count() as u64;
    let prior_conversation = state
        .conversation_tail
        .checked_sub(conversation_count)
        .ok_or(Error::Record("conversation tail precedes its appends"))?;
    for out in &step.outs {
        builder.spawn(state, out, cursor, prior_conversation)?;
    }
    let mut sequence = prior_conversation;
    for out in &step.outs {
        match out {
            Out::Conversation(record) => {
                builder.append(conversation.clone(), record.clone());
                sequence = sequence
                    .checked_add(1)
                    .ok_or(Error::Record("agent conversation tail overflow"))?;
            }
            Out::GoalCompleted { run } => {
                let sequence = sequence
                    .checked_sub(1)
                    .filter(|sequence| *sequence >= prior_conversation)
                    .ok_or(Error::Record("goal completion has no conversation entry"))?;
                let path = super::goal::result(path, *run)?;
                builder.condition(CommitCondition::Absent { path: path.clone() });
                builder.append(path, super::goal::Completed { sequence }.encode()?);
            }
            Out::Checkpoint | Out::Roster(_) | Out::Steered(_) | Out::Notices(_) => {}
            Out::Send { to, record } => {
                if matches!(
                    record,
                    InboxRecord::ForkAdopted(_) | InboxRecord::Archive(_)
                ) {
                    return Err(Error::Record(
                        "send carries neither ForkAdopted nor Archive",
                    ));
                }
                builder.recipient(to)?;
                if matches!(record, InboxRecord::Message(_) | InboxRecord::FollowUp(_))
                    && to != path
                {
                    builder.offer(to.inbox()?, to.closed()?, record.encode()?);
                } else {
                    builder.append(to.inbox()?, record.encode()?);
                }
            }
            Out::Archive { target, id } => builder.archive(&sender, target, id)?,
            Out::Post { channel, post } => {
                builder.post(state, channel, post, channels.members.get(channel))?;
            }
            Out::Spawn { .. } => {}
        }
    }
    if conversation_count > 0 {
        builder.condition(CommitCondition::Tail {
            path: conversation,
            expected: prior_conversation,
        });
    }
    if let Some(status) = &step.status {
        let path = path.status()?;
        builder.condition(CommitCondition::Tail {
            path: path.clone(),
            expected: state.status_tail - 1,
        });
        builder.append(path, status.append_record()?);
    }
    Ok(builder.request(key))
}

/// The spawn `out` of `state` commits at fork cut `cursor`, over the
/// committed conversation `prior_conversation`; `None` for any other effect.
pub(super) fn spawned(
    state: &AgentState,
    out: &Out,
    cursor: u64,
    prior_conversation: u64,
) -> Result<Option<commit::Spawn>, Error> {
    let Out::Spawn {
        intent,
        brief,
        profile,
        volumes,
        history,
    } = out
    else {
        return Ok(None);
    };
    let mut adopted = adoption(state, volumes, cursor, prior_conversation);
    adopted.brief.clone_from(brief);
    adopted.profile.clone_from(profile);
    if let Some(history) = history {
        if history.conversation > prior_conversation {
            return Err(Error::Record("history fork exceeds committed conversation"));
        }
        adopted.conversation_tail = history.conversation;
        adopted.checkpoint = history.checkpoint;
    }
    commit::spawn_at(&state.path, intent, cursor, adopted, history.as_ref()).map(Some)
}

/// The adoption a spawn of `state` writes as its child's first record; the
/// spawn names the child, and the fork cut is `cursor`.
pub(super) fn adoption(
    state: &AgentState,
    volumes: &[VolumeBinding],
    cursor: u64,
    conversation_tail: u64,
) -> ForkAdopted {
    ForkAdopted {
        profile: None,
        agent: state.path.id(),
        parent: None,
        forked_at: cursor,
        conversation_tail,
        // The checkpoint inherited at the cut, never one this step appends.
        checkpoint: state.checkpoint.filter(|at| *at < conversation_tail),
        volumes: volumes.to_vec(),
        brief: None,
        policy: state.policy.clone(),
    }
}

/// Stable key of one delivery: the agent and the A4 trigger identity.
pub fn delivery_key(agent: &AgentState, trigger: &str) -> Result<IdempotencyKey, Error> {
    commit::intent_key("step", &[agent.path.as_str(), trigger])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{AgentId, Alarm, ChannelPost, CreationConfig, IntentId, SpawnContext};
    use crate::conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef};
    use acyclic_stream::CommitMutation;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn agent(parent: &AgentPath, byte: u8, channels: &[&str]) -> Result<AgentState, Error> {
        let id = AgentId::from_bytes([byte; 16]);
        let adopted = ForkAdopted {
            agent: id.clone(),
            parent: Some(parent.clone()),
            profile: Some(CreationConfig {
                context: SpawnContext::Fresh,
                template: None,
                channels: channels.iter().map(|name| (*name).to_owned()).collect(),
            }),
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: Vec::new(),
            brief: None,
            policy: None,
        };
        AgentState::adopted(parent.child(&id)?, &adopted)
    }

    fn post(from: &AgentPath) -> Result<ChannelPost, Box<dyn std::error::Error>> {
        let volume = VolumeRef::new(
            crate::resources::ProviderRef::new(
                "agents".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(from.id_bytes())),
            &mut None,
        )?;
        Ok(ChannelPost {
            from: from.clone(),
            id: IntentId::try_from("post:1")?,
            notice: Box::new(FileRef::new(
                volume,
                "channels/1.json".into(),
                "revision".into(),
                FileDescriptor::new([3; 32], 64, crate::agent::NOTICE_MEDIA_TYPE.into())?,
                "notice".into(),
                &mut None,
            )?),
        })
    }

    fn lowered(
        state: AgentState,
        outs: Vec<Out>,
        channels: &Channels,
    ) -> Result<CommitRequest, Error> {
        let step = Step {
            state,
            outs,
            status: None,
            alarm: Alarm::Clear,
        };
        lower(&step, 1, delivery_key(&step.state, "record:0")?, channels)
    }

    fn written(request: &CommitRequest, path: &StreamPath) -> bool {
        request
            .mutations
            .iter()
            .any(|mutation| mutation.path() == path)
    }

    /// A post reaches every other member's inbox by reference, never the
    /// poster's own or a non-member's, under a guard on the membership it read.
    /// A non-member's post and a post without its membership refuse; a closing
    /// step leaves and a spawn joins.
    #[test]
    fn posts_reach_exactly_the_members_they_read() -> TestResult {
        let root = AgentPath::root(&AgentId::from_bytes([1; 16]));
        let poster = agent(&root, 2, &["chat"])?;
        let member = agent(&root, 3, &["chat"])?;
        let outsider = agent(&root, 4, &[])?;
        let chat = root.channel_members("chat")?;
        let members = Members {
            tail: 2,
            agents: [poster.path.clone(), member.path.clone()].into(),
        };
        let channels = Channels {
            members: [("chat".to_owned(), members)].into(),
            closing: false,
        };
        let posting = |state: &AgentState| -> Result<Vec<Out>, Box<dyn std::error::Error>> {
            Ok(vec![Out::Post {
                channel: "chat".into(),
                post: post(&state.path)?,
            }])
        };
        let request = lowered(poster.clone(), posting(&poster)?, &channels)?;
        assert!(written(&request, &root.channel("chat")?));
        let inbox = member.path.inbox()?;
        assert!(request.mutations.iter().any(|mutation| matches!(mutation,
            CommitMutation::SoftAppend { path, .. } if *path == inbox)));
        assert!(!written(&request, &poster.path.inbox()?));
        assert!(!written(&request, &outsider.path.inbox()?));
        assert!(request.conditions.contains(&CommitCondition::Tail {
            path: chat.clone(),
            expected: 2,
        }));
        // The outsider is refused, and so is a post its handler read no
        // membership for.
        assert!(lowered(outsider.clone(), posting(&outsider)?, &channels).is_err());
        assert!(lowered(poster.clone(), posting(&poster)?, &Channels::default()).is_err());
        // Closing leaves the channel and still guards the membership it read.
        let closing = Channels {
            closing: true,
            ..channels
        };
        let request = lowered(poster.clone(), posting(&poster)?, &closing)?;
        assert!(written(&request, &chat));
        assert!(request.conditions.contains(&CommitCondition::Tail {
            path: chat.clone(),
            expected: 2,
        }));
        // A member's spawn joins the child; an ungranted channel refuses.
        let spawn = |channel: &str| Out::Spawn {
            intent: IntentId::try_from("spawn:child").expect("intent"),
            brief: None,
            profile: Some(CreationConfig {
                context: SpawnContext::Fresh,
                template: None,
                channels: [channel.to_owned()].into(),
            }),
            volumes: Vec::new(),
            history: None,
        };
        let request = lowered(poster.clone(), vec![spawn("chat")], &Channels::default())?;
        assert!(written(&request, &chat));
        assert!(lowered(poster, vec![spawn("other")], &Channels::default()).is_err());
        Ok(())
    }
}
