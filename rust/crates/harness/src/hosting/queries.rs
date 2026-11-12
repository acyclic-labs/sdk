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
            | (QueryProjection::Input(_), AgentCommand::History { agent, page }) => (agent, page),
            (
                QueryProjection::Directory,
                AgentCommand::Directory {
                    agent,
                    target: acyclic_harness_client::DirectoryTarget::Project,
                    generation: None,
                    page,
                    ..
                },
            ) => (agent, page),
            _ => return Err(StreamError::InvalidArgument),
        };
        if page.cursor().is_some() {
            return Err(StreamError::InvalidArgument);
        }
        self.within_tree(agent)
            .map_err(|_| StreamError::AccessDenied)?;
        let minimum = match projection {
            QueryProjection::Input(crate::agent::InputSelection::Latest) => 8,
            QueryProjection::Input(crate::agent::InputSelection::Pending) => 28,
            QueryProjection::Diff(_) => 0,
            QueryProjection::Directory => 0,
        };
        if page.bytes() < minimum {
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
                AgentCommand::Directory { agent, page, .. } => Some((agent, page)),
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
                QueryProjection::Input(selection) => self.server.platform.input_preview_source(
                    &authorization, agent, u64::from(page.bytes() - match selection {
                        crate::agent::InputSelection::Latest => 8,
                        crate::agent::InputSelection::Pending => 24,
                    }), selection,
                ).await.map(|source| input_frames(source, page.clone())),
                QueryProjection::Directory => {
                    if let AgentCommand::Directory { path, .. } = command {
                        self.server.platform.project_directory_source(&authorization, agent, path, page.items()).await
                        .map(|source| directory_frames(source, page.clone()))
                    } else {
                        Err(crate::Error::Invalid("directory projection has no directory command".into()))
                    }
                }
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

fn directory_frames(
    mut source: DirectorySource,
    page: AgentPageRequest,
) -> acyclic_stream::BoxProviderStream<'static, crate::Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        let mut produced = false;
        let mut terminal = false;
        while let Some(selected) = source.next().await {
            let selected = selected?;
            if terminal { Err(crate::Error::Storage("directory source continued past its final page".into()))?; }
            produced = true;
            terminal = !selected.has_more;
            if selected.entries.is_empty() {
                if selected.has_more { yield diff::frame(4, Vec::new(), &page)?; }
                else { yield diff::frame(10, vec![0, b'[', b']'], &page)?; }
                continue;
            }
            let mut body = vec![1, b'['];
            let mut count = 0;
            for entry in selected.entries {
                let encoded = serde_json::to_vec(&entry).map_err(|error| crate::Error::Storage(error.to_string()))?;
                let separator = usize::from(count != 0);
                if body.len() + separator + encoded.len() + 1 > page.bytes() as usize || count == page.items() {
                    if count == 0 { Err(crate::Error::Invalid("directory name exceeds page byte credit".into()))?; }
                    body.push(b']');
                    yield diff::frame(10, body, &page)?;
                    body = vec![1, b'['];
                    count = 0;
                }
                if body.len() + encoded.len() + 1 > page.bytes() as usize { Err(crate::Error::Invalid("directory name exceeds page byte credit".into()))?; }
                if count != 0 { body.push(b','); }
                body.extend_from_slice(&encoded);
                count += 1;
            }
            if count != 0 {
                body[0] = u8::from(selected.has_more);
                body.push(b']'); yield diff::frame(10, body, &page)?;
            }
        }
        if produced && !terminal { Err(crate::Error::Storage("directory source ended with a continuation".into()))?; }
        if !produced { yield diff::frame(10, vec![0, b'[', b']'], &page)?; }
    })
}

fn input_frames(
    mut source: InputPreviewSource,
    page: AgentPageRequest,
) -> acyclic_stream::BoxProviderStream<'static, crate::Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        while let Some(step) = source.next().await {
            match step? {
                crate::agent::InputPreviewStep::Progress => yield diff::frame(4, Vec::new(), &page)?,
                crate::agent::InputPreviewStep::Selected(None) => yield diff::frame(7, Vec::new(), &page)?,
                crate::agent::InputPreviewStep::Selected(Some(preview)) => {
                    let mut body = Vec::with_capacity(8 + preview.text.len());
                    body.extend_from_slice(&preview.bytes.to_le_bytes());
                    body.extend_from_slice(preview.text.as_bytes());
                    yield diff::frame(6, body, &page)?;
                }
                crate::agent::InputPreviewStep::Pending { inbox, offset, preview } => {
                    let mut body = Vec::with_capacity(24 + preview.text.len());
                    body.extend_from_slice(&inbox.to_le_bytes());
                    body.extend_from_slice(&offset.to_le_bytes());
                    body.extend_from_slice(&preview.bytes.to_le_bytes());
                    body.extend_from_slice(preview.text.as_bytes());
                    yield diff::frame(8, body, &page)?;
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
