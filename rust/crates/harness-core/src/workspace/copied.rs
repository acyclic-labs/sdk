//! Selected-volume materialization for every provider without native attachment.
//! Tar is a file transfer format; command execution uses the provider's existing
//! capability-selected exec path. Publication uses the ordinary workspace facade.
use crate::{
    Error, OperationId, Result, filesystem::WorkspaceAccess, machines::MachinesHost,
    resources::SandboxRef,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Generation, TransactionCommit,
    WorkBudget, Workspace,
};
use acyclic_machines::{
    ExecCommand, FileAccess, FileRead, FileWrite, OperatingSystem, read_file_at, write_file_at,
};
use acyclic_stream::BoxProviderFuture;
use std::{collections::BTreeMap, io::Read as _, sync::Arc};

const WORK: WorkBudget = WorkBudget::UNBOUNDED;

pub(crate) trait CopiedWorkspace: acyclic_stream::ProviderPlatform {
    fn cwd(&self, selected: Option<&str>) -> Result<String>;
    fn settle(&self, turn: OperationId) -> BoxProviderFuture<'_, Result<()>>;
}

struct Copy<A: 'static, O: 'static> {
    host: MachinesHost,
    sandbox: SandboxRef,
    workspace: Workspace<A, O>,
    access: WorkspaceAccess,
    deadline: u64,
    root: String,
    state: tokio::sync::Mutex<State<A, O>>,
}
struct State<A: 'static, O: 'static> {
    base: Generation<A, O>,
    settled: Option<OperationId>,
    pending: Option<OperationId>,
}

fn invalid(message: impl std::fmt::Display) -> Error {
    Error::Invalid(format!(
        "copied workspace: {message}; recovery: retain the original selected-volume binding"
    ))
}
pub(crate) async fn bind<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: MachinesHost,
    sandbox: SandboxRef,
    workspace: Workspace<A, O>,
    access: WorkspaceAccess,
    deadline: u64,
    os: OperatingSystem,
) -> Result<Arc<dyn CopiedWorkspace>> {
    access.require(&workspace).await?;
    let base = workspace
        .head(WORK, &CancellationToken::new())
        .await
        .map_err(|e| Error::from(e.error))?
        .value;
    let archive = export(&base).await?;
    let allocate: Vec<String> = match os {
        OperatingSystem::Windows => vec!["powershell.exe".into(), "-NoProfile".into(), "-NonInteractive".into(), "-Command".into(), "$ErrorActionPreference='Stop'; $p=[IO.Path]::Combine([IO.Path]::GetTempPath(),'acyclic-workspace-'+[Guid]::NewGuid().ToString()); [IO.Directory]::CreateDirectory($p) > $null; [Console]::Out.Write($p)".into()],
        _ => vec!["mktemp".into(), "-d".into(), "/tmp/acyclic-workspace-XXXXXXXXXX".into()],
    };
    let root = run(&host, &sandbox, &workspace, &access, deadline, allocate).await?;
    let root = String::from_utf8(root)
        .map_err(invalid)?
        .trim_end_matches(['\r', '\n'])
        .replace('\\', "/");
    if root.is_empty() || root.contains('\0') {
        return Err(invalid("temporary workspace path is invalid"));
    }
    let copy = Copy {
        host,
        sandbox,
        workspace,
        access,
        deadline,
        root,
        state: tokio::sync::Mutex::new(State {
            base,
            settled: None,
            pending: None,
        }),
    };
    copy.upload(&archive).await?;
    copy.access.require(&copy.workspace).await?;
    copy.command(vec![
        "tar".into(),
        "-xf".into(),
        copy.archive(),
        "-C".into(),
        copy.root.clone(),
    ])
    .await?;
    Ok(Arc::new(copy))
}

async fn run<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &MachinesHost,
    sandbox: &SandboxRef,
    workspace: &Workspace<A, O>,
    access: &WorkspaceAccess,
    deadline: u64,
    argv: Vec<String>,
) -> Result<Vec<u8>> {
    access.require(workspace).await?;
    let command = ExecCommand::new(argv, None)
        .map_err(Error::Machines)?
        .with_expiry(deadline);
    let output = acyclic_machines::capture_exec(host.exec(sandbox, command).await?, u64::MAX)
        .await
        .map_err(Error::Machines)?;
    if output.exit_code != Some(0) || output.stop.is_some() {
        return Err(invalid("materialization command failed"));
    }
    Ok(output.stdout)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Copy<A, O> {
    fn archive(&self) -> String {
        format!("{}.tar", self.root)
    }
    async fn command(&self, argv: Vec<String>) -> Result<()> {
        run(
            &self.host,
            &self.sandbox,
            &self.workspace,
            &self.access,
            self.deadline,
            argv,
        )
        .await
        .map(|_| ())
    }
    async fn upload(&self, bytes: &[u8]) -> Result<()> {
        let provider = self.host.provider();
        let machine = self.host.machine_id(&self.sandbox)?;
        if provider.capability(acyclic_machines::ProviderCapability::WholeFile)
            == acyclic_machines::Support::Supported
        {
            self.access.require(&self.workspace).await?;
            return provider
                .upload_file(
                    machine,
                    FileAccess::new(self.archive(), 0, self.deadline).map_err(Error::Machines)?,
                    bytes.to_vec(),
                )
                .await
                .map_err(Error::Machines);
        }
        let mut offset = 0usize;
        // Empty snapshots are still a valid nonempty tar file.
        while offset < bytes.len() {
            let end = offset.saturating_add(65_536).min(bytes.len());
            let access = FileAccess::new(self.archive(), offset as u64, self.deadline)
                .map_err(Error::Machines)?;
            self.access.require(&self.workspace).await?;
            let written = write_file_at(
                provider,
                machine,
                access,
                bytes
                    .get(offset..end)
                    .ok_or_else(|| invalid("missing archive chunk"))?
                    .to_vec(),
                offset == 0,
            )
            .await
            .map_err(Error::Machines)?;
            match written {
                FileWrite::Written(count) if count > 0 && count as usize <= end - offset => {
                    offset += count as usize
                }
                _ => return Err(invalid("archive write made no progress")),
            }
        }
        Ok(())
    }
    async fn download(&self) -> Result<Vec<u8>> {
        if self
            .host
            .provider()
            .capability(acyclic_machines::ProviderCapability::WholeFile)
            == acyclic_machines::Support::Supported
        {
            self.access.require(&self.workspace).await?;
            return match self
                .host
                .provider()
                .download_file(
                    self.host.machine_id(&self.sandbox)?,
                    FileAccess::new(self.archive(), 0, self.deadline).map_err(Error::Machines)?,
                )
                .await
                .map_err(Error::Machines)?
                .collect()
                .await
                .map_err(Error::Machines)?
            {
                FileRead::Data(bytes) => Ok(bytes),
                _ => Err(invalid("archive read failed")),
            };
        }
        let mut bytes = Vec::new();
        loop {
            self.access.require(&self.workspace).await?;
            let access = FileAccess::new(self.archive(), bytes.len() as u64, self.deadline)
                .map_err(Error::Machines)?;
            match read_file_at(
                self.host.provider(),
                self.host.machine_id(&self.sandbox)?,
                access,
                65_536,
            )
            .await
            .map_err(Error::Machines)?
            {
                FileRead::Data(chunk) if chunk.is_empty() => return Ok(bytes),
                FileRead::Data(chunk) => bytes.extend(chunk),
                _ => return Err(invalid("archive read failed")),
            }
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CopiedWorkspace for Copy<A, O> {
    fn cwd(&self, selected: Option<&str>) -> Result<String> {
        if selected.is_some_and(|path| path.split('/').any(|part| part == "..")) {
            return Err(invalid("cwd escapes the selected workspace"));
        }
        match selected.filter(|value| !value.is_empty() && *value != ".") {
            None => Ok(self.root.clone()),
            Some(path) if path == self.root || path.starts_with(&format!("{}/", self.root)) => {
                Ok(path.into())
            }
            Some(path)
                if !path.starts_with('/')
                    && path.split('/').all(|part| !matches!(part, ".." | "")) =>
            {
                Ok(format!("{}/{path}", self.root))
            }
            _ => Err(invalid("cwd escapes the selected workspace")),
        }
    }
    fn settle(&self, turn: OperationId) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if state.settled == Some(turn) {
                return Ok(());
            }
            if let Some(original) = state.pending {
                return Err(Error::Indeterminate(original));
            }
            self.access.require(&self.workspace).await?;
            state.pending = Some(turn); // Never redispatch a capture whose reply was lost.
            self.command(vec![
                "tar".into(),
                "-cf".into(),
                self.archive(),
                "-C".into(),
                self.root.clone(),
                ".".into(),
            ])
            .await?;
            let entries = decode(&self.download().await?)?;
            let key = acyclic_fs::IdempotencyKey::from_bytes(turn.into_bytes());
            let mut transaction = self
                .workspace
                .begin_transaction_at(&state.base, key, WORK, &CancellationToken::new())
                .await
                .map_err(|e| Error::from(e.error))?
                .value;
            // Retain unchanged identities and metadata. Only authored differences
            // enter the ordinary transaction's conflict detection and publication.
            let previous = decode(&export(&state.base).await?)?;
            for (path, entry) in previous.iter().rev() {
                if entries.get(path) != Some(entry) {
                    transaction
                        .remove(&format!("/{path}"), WORK, &CancellationToken::new())
                        .await
                        .map_err(|e| Error::from(e.error))?;
                }
            }
            for (path, entry) in entries {
                if previous.get(&path) == Some(&entry) {
                    continue;
                }
                let path = format!("/{path}");
                if let Some((parent, _)) = path.rsplit_once('/')
                    && !parent.is_empty()
                {
                    transaction
                        .create_dir_all(parent, WORK, &CancellationToken::new())
                        .await
                        .map_err(|e| Error::from(e.error))?;
                }
                match entry {
                    Entry::Directory => {
                        transaction
                            .create_dir_all(&path, WORK, &CancellationToken::new())
                            .await
                            .map_err(|e| Error::from(e.error))?;
                    }
                    Entry::File(bytes, mode) => {
                        let metadata = acyclic_fs::kernel::FileMetadata {
                            posix_mode: acyclic_fs::kernel::MetadataField::Value(mode),
                            ..Default::default()
                        };
                        transaction
                            .create_file(&path, bytes, metadata, WORK, &CancellationToken::new())
                            .await
                            .map_err(|e| Error::from(e.error))?;
                    }
                    Entry::Link(target) => {
                        transaction
                            .create_symbolic_link(&path, target, WORK, &CancellationToken::new())
                            .await
                            .map_err(|e| Error::from(e.error))?;
                    }
                }
            }
            let committed = transaction
                .commit(
                    None,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    WORK,
                    &CancellationToken::new(),
                    |_| async {
                        self.access
                            .require(&self.workspace)
                            .await
                            .map_err(|error| {
                                acyclic_fs::OperationFailure::new(error, Default::default())
                            })?;
                        Ok::<_, acyclic_fs::OperationFailure<Error>>(acyclic_fs::OperationReceipt {
                            value: (),
                            work: Default::default(),
                        })
                    },
                )
                .await
                .map_err(|e| e.error)?
                .value;
            state.base = match committed {
                TransactionCommit::Committed(generation)
                | TransactionCommit::AlreadyCommitted(generation) => generation,
                _ => {
                    return Err(Error::Conflict(
                        "selected volume changed before copied workspace settlement".into(),
                    ));
                }
            };
            state.settled = Some(turn);
            state.pending = None;
            Ok(())
        })
    }
}

#[derive(Eq, PartialEq)]
enum Entry {
    Directory,
    File(Vec<u8>, u32),
    Link(Vec<u8>),
}

fn decode(bytes: &[u8]) -> Result<BTreeMap<String, Entry>> {
    let mut archive = tar::Archive::new(bytes);
    let mut entries = BTreeMap::new();
    for entry in archive.entries().map_err(invalid)? {
        let mut entry = entry.map_err(invalid)?;
        let path = entry.path().map_err(invalid)?;
        let mut parts = Vec::new();
        for part in path.components() {
            match part {
                std::path::Component::CurDir => (),
                std::path::Component::Normal(value) => parts.push(
                    value
                        .to_str()
                        .ok_or_else(|| invalid("non-UTF-8 archive path"))?,
                ),
                _ => return Err(invalid("archive path escapes workspace")),
            }
        }
        let path = parts.join("/");
        if path.is_empty() {
            continue;
        }
        let kind = entry.header().entry_type();
        let value = if kind.is_dir() {
            Entry::Directory
        } else if kind.is_file() {
            let mode = entry.header().mode().map_err(invalid)?;
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).map_err(invalid)?;
            Entry::File(bytes, mode)
        } else if kind.is_symlink() {
            Entry::Link(
                entry
                    .link_name_bytes()
                    .ok_or_else(|| invalid("missing link target"))?
                    .to_vec(),
            )
        } else {
            return Err(invalid("unsupported archive entry kind"));
        };
        if entries.insert(path, value).is_some() {
            return Err(invalid("duplicate archive path"));
        }
    }
    Ok(entries)
}

async fn export<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
) -> Result<Vec<u8>> {
    let mut archive = tar::Builder::new(Vec::new());
    let mut pending = vec![String::new()];
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let page = generation
                .list_directory(
                    &format!("/{directory}"),
                    after.as_ref(),
                    1024,
                    WORK,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|e| Error::from(e.error))?
                .value
                .0;
            for entry in &page.entries {
                let name = entry
                    .name
                    .unicode_text(WORK, &CancellationToken::new())
                    .map_err(|e| Error::from(e.error))?
                    .value;
                let path = if directory.is_empty() {
                    name.to_string()
                } else {
                    format!("{directory}/{name}")
                };
                let absolute = format!("/{path}");
                let stat = generation
                    .stat(&absolute, WORK, &CancellationToken::new())
                    .await
                    .map_err(|e| Error::from(e.error))?
                    .value;
                let mut header = tar::Header::new_gnu();
                header.set_mode(stat.metadata.posix_mode.unwrap_or(
                    if entry.kind == acyclic_fs::kernel::FileKind::Regular {
                        0o644
                    } else {
                        0o755
                    },
                ));
                header.set_size(0);
                match entry.kind {
                    acyclic_fs::kernel::FileKind::Directory => {
                        header.set_entry_type(tar::EntryType::Directory);
                        header.set_cksum();
                        archive
                            .append_data(&mut header, &path, &[][..])
                            .map_err(invalid)?;
                        pending.push(path);
                    }
                    acyclic_fs::kernel::FileKind::Regular => {
                        let bytes = generation
                            .read(&absolute, u64::MAX, WORK, &CancellationToken::new())
                            .await
                            .map_err(|e| Error::from(e.error))?
                            .value;
                        header.set_size(bytes.len() as u64);
                        header.set_cksum();
                        archive
                            .append_data(&mut header, &path, bytes.as_ref())
                            .map_err(invalid)?;
                    }
                    acyclic_fs::kernel::FileKind::SymbolicLink => {
                        let bytes = generation
                            .read_symbolic_link(&absolute, WORK, &CancellationToken::new())
                            .await
                            .map_err(|e| Error::from(e.error))?
                            .value;
                        let target = std::str::from_utf8(bytes.bytes.as_ref()).map_err(invalid)?;
                        header.set_entry_type(tar::EntryType::Symlink);
                        header.set_cksum();
                        archive
                            .append_link(&mut header, &path, target)
                            .map_err(invalid)?;
                    }
                    _ => return Err(invalid("selected volume contains an unsupported file kind")),
                }
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
    }
    archive.into_inner().map_err(invalid)
}
