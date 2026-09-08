//! Distinct durable aggregate handles over the shared substrate.

use crate::{
    AgentId, ConversationId, Result, SessionId, TaskId, TurnId,
    core::{
        AggregateKind, ApplyResult, Authority, AuthorityVerifier, Command, Reducer, SchemaRegistry,
    },
    store::StreamAggregate,
};
use acyclic_stream::{StreamClient, StreamProvider};

macro_rules! durable_handle {
    ($name:ident, $id:ty, $kind:ident, $description:literal) => {
        #[doc = $description]
        pub struct $name<P> {
            inner: StreamAggregate<P>,
        }

        impl<P: StreamProvider> $name<P> {
            /// Opens and replays this aggregate from its canonical Stream path.
            pub async fn open(
                client: &StreamClient<P>,
                id: $id,
                authority_verifier: AuthorityVerifier,
                schemas: SchemaRegistry,
            ) -> Result<Self> {
                let identity = id.to_string();
                Ok(Self {
                    inner: StreamAggregate::open(
                        client,
                        Authority {
                            kind: AggregateKind::$kind,
                            id: identity,
                        },
                        authority_verifier,
                        schemas,
                    )
                    .await?,
                })
            }

            /// Returns the current deterministic projection.
            #[must_use]
            pub const fn reducer(&self) -> &Reducer {
                self.inner.reducer()
            }

            /// Plans, durably appends, and applies one command.
            pub async fn execute(&mut self, command: Command) -> Result<ApplyResult> {
                self.inner.execute(command).await
            }
        }
    };
}

durable_handle!(
    Agent,
    AgentId,
    Agent,
    "Typed handle to one durable agent definition."
);
durable_handle!(
    Conversation,
    ConversationId,
    Conversation,
    "Typed handle to one durable conversation."
);
durable_handle!(
    Session,
    SessionId,
    Session,
    "Typed handle to one durable execution session."
);
durable_handle!(
    Turn,
    TurnId,
    Turn,
    "Typed handle to one durable user-driven turn."
);
durable_handle!(
    Task,
    TaskId,
    Task,
    "Typed handle to one durable general-purpose task."
);

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::MemoryStream;
    use std::sync::Arc;

    #[tokio::test]
    async fn handles_use_distinct_canonical_histories() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let agent_authority = Authority {
            kind: AggregateKind::Agent,
            id: AgentId::from_bytes([1; 16]).to_string(),
        };
        let conversation_authority = Authority {
            kind: AggregateKind::Conversation,
            id: ConversationId::from_bytes([1; 16]).to_string(),
        };
        let agent_verifier =
            crate::core::AuthorityIssuer::new("test", [7; 32], agent_authority).verifier();
        let conversation_verifier =
            crate::core::AuthorityIssuer::new("test", [7; 32], conversation_authority).verifier();
        let agent = Agent::open(
            &client,
            AgentId::from_bytes([1; 16]),
            agent_verifier,
            SchemaRegistry::new(),
        )
        .await?;
        let conversation = Conversation::open(
            &client,
            ConversationId::from_bytes([1; 16]),
            conversation_verifier,
            SchemaRegistry::new(),
        )
        .await?;
        assert_eq!(agent.reducer().authority().kind, AggregateKind::Agent);
        assert_eq!(
            conversation.reducer().authority().kind,
            AggregateKind::Conversation
        );
        Ok(())
    }
}
