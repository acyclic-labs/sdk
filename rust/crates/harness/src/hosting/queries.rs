//! The same actor-socket query owner for local and remote serving.
use super::*;
use acyclic_harness_client::DiffProjection;
use acyclic_stream::session::{PreparedSocketOpen, SocketChannel, SocketInbound, SocketProvider};
use futures::StreamExt as _;

struct PreparedDiff<P: AuthorityProvider + ?Sized, O: AsyncObjectStore> {
    server: Arc<Server<P, O>>,
    call: AgentCall,
    projection: DiffProjection,
}

struct ReadOnly;

impl SocketInbound for ReadOnly {
    fn receive(&self, _: Bytes) -> Result<(), StreamError> {
        Err(StreamError::InvalidArgument)
    }
}

impl<P, O> SocketProvider for Server<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    fn prepare(self: Arc<Self>, open: Bytes) -> Result<Box<dyn PreparedSocketOpen>, StreamError> {
        let (projection, invocation) =
            DiffProjection::decode_open(&open).map_err(|_| StreamError::InvalidArgument)?;
        let call = self.admit(invocation)?;
        let AgentCommand::Diff { agent, page } = call.command() else {
            return Err(StreamError::InvalidArgument);
        };
        if page.cursor().is_some() {
            return Err(StreamError::InvalidArgument);
        }
        self.within_tree(agent)
            .map_err(|_| StreamError::AccessDenied)?;
        Ok(Box::new(PreparedDiff {
            server: self,
            call,
            projection,
        }))
    }
}

impl<P, O> PreparedSocketOpen for PreparedDiff<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    fn resource(&self) -> &str {
        &self.server.resource
    }

    fn open(
        self: Box<Self>,
        authorization: CallAuthorization,
    ) -> Result<SocketChannel, StreamError> {
        let attributes = acyclic_auth::contract::RecordAttributes::try_new(Vec::new())
            .map_err(|_| StreamError::InvalidArgument)?;
        // Synchronous original-credential admission installs no work. Current
        // asynchronous platform policy is checked before source IO below.
        self.server.authorize_command(
            &authorization,
            self.call.command(),
            &attributes,
            PolicyDecision::Allow,
        )?;
        let outbound = Box::pin(async_stream::try_stream! {
            let command = self.call.command();
            let selected = match command {
                AgentCommand::Diff { agent, page } => Some((agent, page)),
                _ => None,
            };
            let (agent, page) = selected.ok_or(StreamError::InvalidArgument)?;
            let authorize = async || {
                let principal = authorization.principal()?;
                let policy = self.server.platform.policy(&principal, command).await?;
                self.server.authorize_command(&authorization, command, &attributes, policy)
            };
            authorize().await?;
            self.server.platform.admitted().await?;
            let opened = self.server.platform.diff_source(&authorization, agent).await;
            // Neither a read's data nor its source error bypasses revocation
            // while admission, endpoint selection or source IO was suspended.
            authorize().await?;
            let source = match opened {
                Ok(source) => source,
                Err(error) => {
                    yield refusal(command, page, error)?;
                    return;
                }
            };
            let mut frames = source.frames(page.clone(), self.projection);
            while let Some(frame) = frames.next().await {
                authorize().await?;
                match frame {
                    Ok(frame) => yield frame,
                    Err(error) => {
                        yield refusal(command, page, error)?;
                        return;
                    }
                }
            }
            authorize().await?;
        });
        Ok(SocketChannel {
            outbound,
            inbound: Box::new(ReadOnly),
        })
    }
}

fn refusal(
    command: &AgentCommand,
    page: &AgentPageRequest,
    error: crate::Error,
) -> Result<Bytes, StreamError> {
    let body =
        acyclic_harness_client::encode_agent_reply(&Err(Error::Turn(Box::new(error))), command)
            .map_err(|_| StreamError::InvalidResponse)?;
    diff::frame(5, body, page).map_err(|_| StreamError::Capacity)
}
