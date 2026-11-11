//! The same actor-socket query owner for local and remote serving.
use super::*;
use acyclic_harness_client::QueryProjection;
use acyclic_stream::session::{PreparedSocketOpen, SocketChannel, SocketInbound, SocketProvider};
use futures::StreamExt as _;

struct PreparedQuery<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> {
    server: Arc<Server<P, O>>,
    call: AgentCall,
    projection: QueryProjection,
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
            QueryProjection::decode_open(&open).map_err(|_| StreamError::InvalidArgument)?;
        let call = self.admit(invocation)?;
        let (agent, page) = match (projection, call.command()) {
            (QueryProjection::Diff(_), AgentCommand::Diff { agent, page })
            | (QueryProjection::Input, AgentCommand::History { agent, page }) => (agent, page),
            _ => return Err(StreamError::InvalidArgument),
        };
        if page.cursor().is_some() {
            return Err(StreamError::InvalidArgument);
        }
        self.within_tree(agent)
            .map_err(|_| StreamError::AccessDenied)?;
        if projection == QueryProjection::Input && page.bytes() < 8 {
            return Err(StreamError::Capacity);
        }
        Ok(Box::new(PreparedQuery {
            server: self,
            call,
            projection,
        }))
    }
}

impl<P, O> PreparedSocketOpen for PreparedQuery<P, O>
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
                AgentCommand::Diff { agent, page } | AgentCommand::History { agent, page } => Some((agent, page)),
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
            let opened: crate::Result<acyclic_stream::BoxProviderStream<'static, crate::Result<Bytes>>> = match self.projection {
                QueryProjection::Diff(projection) => self.server.platform.diff_source(&authorization, agent).await
                    .map(|source| source.frames(page.clone(), projection)),
                QueryProjection::Input => self.server.platform.input_preview_source(
                    &authorization, agent, u64::from(page.bytes() - 8),
                ).await.map(|source| input_frames(source, page.clone())),
            };
            // Neither a read's data nor its source error bypasses revocation
            // while admission, endpoint selection or source IO was suspended.
            authorize().await?;
            let mut frames = match opened {
                Ok(source) => source,
                Err(error) => {
                    yield refusal(command, page, error)?;
                    return;
                }
            };
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

fn input_frames(
    mut source: InputPreviewSource,
    page: AgentPageRequest,
) -> acyclic_stream::BoxProviderStream<'static, crate::Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        while let Some(step) = source.next().await {
            match step? {
                crate::agent::InputPreviewStep::Progress => yield diff::frame(4, Bytes::new(), &page)?,
                crate::agent::InputPreviewStep::Selected(None) => yield diff::frame(7, Bytes::new(), &page)?,
                crate::agent::InputPreviewStep::Selected(Some(preview)) => {
                    let mut body = Vec::with_capacity(8 + preview.text.len());
                    body.extend_from_slice(&preview.bytes.to_le_bytes());
                    body.extend_from_slice(preview.text.as_bytes());
                    yield diff::frame(6, body.into(), &page)?;
                }
            }
        }
    })
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
