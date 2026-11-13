//! Account admission reads directory rows, never a remote agent's state.
use super::{Error, PeerRoute, route};
use crate::agent::{SessionPublication, SessionRow};
use acyclic_stream::{CommitCondition, CommitEffect, HeadProjection, HeadsRequest, StreamProvider};

impl PeerRoute<'_> {
    /// Join host-prepared storage admission to current account membership.
    /// The host's `AdmissionPolicy` must first authorize this sender's peer
    /// grant and prepare funded storage/residency effects. No default funding
    /// or authority policy is supplied here. All effects join the original
    /// relay or canonical refusal publication through the ordinary account path.
    ///
    /// Exhausted credit returns its captured event without reading directory
    /// rows. After it fires, the account service prepares a new guarded cut.
    pub async fn account_admission<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        destination: Option<&route::Placement>,
        account_node: &route::NodeId,
        funded: route::Admission,
    ) -> Result<route::Admission, Error> {
        let (mut effects, refused) = match funded {
            wait @ route::Admission::Credit { .. } => return Ok(wait),
            route::Admission::Allow(effects) => (effects, None),
            route::Admission::Reject {
                reason,
                notice,
                effects,
            } => {
                if notice != self.rejection(account_node.clone(), reason)? {
                    return Err(Error::Record("peer refusal differs from its original send"));
                }
                (effects, Some(reason))
            }
        };
        let (membership, guard) = self.account_membership(provider, destination).await?;
        effects.push(guard);
        match membership.or(refused) {
            Some(reason) => Ok(route::Admission::Reject {
                reason,
                notice: self.rejection(account_node.clone(), reason)?,
                effects,
            }),
            None => Ok(route::Admission::Allow(effects)),
        }
    }

    /// Select both current account memberships in one bounded authority view.
    /// The account's `AdmissionPolicy::prepare` joins this hard guard to its
    /// funded relay admission or canonical rejection. A conditional conflict
    /// requires a fresh preparation; this observation alone grants no send.
    ///
    /// The account host must authenticate the source node, verify its sender's
    /// `agent.send-peer` grant, and resolve the immutable destination placement
    /// before calling. An exact replay recovers the original admission before
    /// preparing current membership, so opt-out cannot rewrite its outcome.
    pub(super) async fn account_membership<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        destination: Option<&route::Placement>,
    ) -> Result<(Option<route::Rejection>, CommitEffect), Error> {
        if destination.is_some_and(|placement| placement.actor != self.envelope.to) {
            return Err(Error::Record("peer placement belongs to another actor"));
        }
        let request = HeadsRequest {
            retained: Default::default(),
            durable_through: None,
            projection: HeadProjection::History,
            paths: [
                SessionPublication::row_path(&self.envelope.sender)?,
                SessionPublication::row_path(&self.envelope.to)?,
            ]
            .into_iter()
            .collect(),
            children: None,
            limit: 2,
            at_least: Default::default(),
        };
        let view = provider
            .heads(request.clone())
            .await
            .map_err(Error::Stream)?;
        view.validate_for(&request).map_err(Error::Stream)?;
        let mut rejection = destination
            .is_none()
            .then_some(route::Rejection::NoSuchActor);
        let mut conditions = Vec::with_capacity(2);
        let nodes = [
            Some(&self.envelope.source),
            destination.map(|placement| &placement.node),
        ];
        for ((path, head), node) in request.paths.into_iter().zip(view.heads).zip(nodes) {
            let Some(head) = head else {
                rejection.get_or_insert(route::Rejection::Closed);
                conditions.push(CommitCondition::Absent { path });
                continue;
            };
            let record = head
                .last
                .ok_or(Error::Record("peer directory row is empty"))?;
            let row = SessionRow::from_record(&path, head.tail, &record)?;
            if node.is_some_and(|node| node != &row.node) {
                return Err(Error::Record(
                    "peer directory row ownership or revision differs",
                ));
            }
            if row.profile.is_none() {
                rejection.get_or_insert(route::Rejection::Closed);
            }
            conditions.push(CommitCondition::Head {
                path,
                expected: head.tail,
                commit_id: record.commit_id,
            });
        }
        Ok((rejection, CommitEffect::hard(conditions, Vec::new())))
    }
}
