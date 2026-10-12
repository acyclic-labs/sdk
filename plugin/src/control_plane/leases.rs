//! Adapter leases over operation windows.

use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LeaseRecord {
    pub(crate) agent_id: String,
    pub(crate) turn_id: String,
    pub(crate) tool_name: String,
    pub(crate) roots: BTreeMap<String, RootLeaseRecord>,
    pub(crate) expires_at_millis: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RootLeaseRecord {
    pub(crate) root_id: [u8; 16],
    pub(crate) workspace_id: [u8; 16],
    pub(crate) lease_id: [u8; 16],
    pub(crate) pinned_parent: [u8; 32],
    pub(crate) lease_expires_at_millis: u64,
}

impl LeaseRecord {
    pub(crate) fn from_leases(
            agent_id: String,
            turn_id: String,
            tool_name: String,
            roots: impl IntoIterator<Item = (WorkspaceRootId, OperationWindowLease)>,
        ) -> Self {
            let roots = roots
                .into_iter()
                .map(|(root_id, lease)| {
                    (
                        root_key(root_id),
                        RootLeaseRecord {
                            root_id: root_id.into_bytes(),
                            workspace_id: lease.workspace_id.into_bytes(),
                            lease_id: lease.lease_id.into_bytes(),
                            pinned_parent: *lease.pinned_parent.digest().as_bytes(),
                            lease_expires_at_millis: lease.expires_at_millis,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let expires_at_millis = roots
                .values()
                .map(|root| root.lease_expires_at_millis)
                .min()
                .unwrap_or_default();
            Self {
                agent_id,
                turn_id,
                tool_name,
                roots,
                expires_at_millis,
            }
        }

}

impl RootLeaseRecord {
    pub(crate) fn lease(&self) -> OperationWindowLease {
            OperationWindowLease {
                workspace_id: acyclic_fs::WorkspaceId::from_bytes(self.workspace_id),
                lease_id: acyclic_fs::OperationLeaseId::from_bytes(self.lease_id),
                pinned_parent: acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
                    self.pinned_parent,
                )),
                expires_at_millis: self.lease_expires_at_millis,
            }
        }

}
