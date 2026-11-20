//! Grants: a grant admits exactly the operation it declares, on exactly the
//! volume it names, at every point of an agent's lifetime and under every
//! custody mode. The exhaustive matrix is a model check; each world runs the
//! real signing, grant and filesystem paths.
use super::*;
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs, LocalOptions};
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey,
    agent::{AgentId as PathId, AgentPath, WorkspaceEntry, WorkspaceRole},
    conversation::{
        ContentAccess, ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityRoot, AuthorityVerifier},
    filesystem::{DirectoryOptions, FilesystemHost},
};
use acyclic_native_runtime::signing_key::{SigningCustody, SigningKeyReference};
use family::{Generator, Variant};

/// The three custody modes: an in-memory volume, a physical volume, and a
/// physical volume whose signing key is stored in a private file.
const CUSTODY: [&str; 3] = ["logical", "physical", "physical_private_file"];
const SUBJECTS: [&str; 8] = [
    "own_home",
    "shared_volume",
    "project",
    "child_volume",
    "grandchild_volume",
    "findings_dir",
    "skill_path",
    "feedback_sink",
];
const OPS: [&str; 4] = ["read", "write", "list", "signed_payload"];
const TIMINGS: [&str; 3] = ["start", "mid_run", "after_child_settle"];

/// One world per custody mode, each covering every subject, operation and
/// timing for that mode.
pub(super) const GENERATOR: Generator = Generator {
    class: "H2",
    seeds: every_grant_admits_exactly_what_it_declares_at_every_age::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_grant_admits_exactly_what_it_declares_at_every_age,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0, seed_1 => 1, seed_2 => 2],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn custody(seed: u64) -> &'static str {
    CUSTODY
        .get(usize::try_from(seed % CUSTODY.len() as u64).unwrap_or(0))
        .copied()
        .unwrap_or("logical")
}

fn variants(seed: u64) -> Vec<Variant> {
    let custody = custody(seed);
    let mut variants = Vec::new();
    for subject in SUBJECTS {
        for op in OPS {
            for timing in TIMINGS {
                variants.push(Variant::from([
                    ("subject", subject),
                    ("op", op),
                    ("timing", timing),
                    ("custody", custody),
                ]));
            }
        }
    }
    variants
}

type Subject = (&'static str, VolumeRef, &'static str, AgentId);

struct Subjects {
    volumes: Vec<Subject>,
}

fn provider() -> Result<ProviderRef> {
    ProviderRef::new("grants".into(), "filesystem".into(), "2".into(), &mut None)
}

fn volume(
    provider: &ProviderRef,
    id: &str,
    class: VolumeClass,
    owner: VolumeOwner,
) -> Result<VolumeRef> {
    VolumeRef::new(provider.clone(), id.into(), class, owner, &mut None)
}

fn subjects(provider: &ProviderRef) -> Result<Subjects> {
    let root = AgentId::from_bytes([1; 16]);
    let child = AgentId::from_bytes([2; 16]);
    let grandchild = AgentId::from_bytes([3; 16]);
    let root_path = PathId::from_bytes([1; 16]);
    let child_id = PathId::from_bytes([2; 16]);
    let grandchild_id = PathId::from_bytes([3; 16]);
    let session = VolumeOwner::Session("session".into());
    let project = VolumeOwner::Project("project".into());
    let own_home = volume(
        provider,
        "own-home",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(root),
    )?;
    let shared = volume(
        provider,
        "shared",
        VolumeClass::SessionShared,
        session.clone(),
    )?;
    let project_volume = volume(provider, "project", VolumeClass::Project, project.clone())?;
    let findings = volume(provider, "findings", VolumeClass::Project, project)?;
    let skill = volume(
        provider,
        "seed",
        VolumeClass::SessionShared,
        session.clone(),
    )?;
    let feedback = volume(provider, "feedback", VolumeClass::SessionShared, session)?;
    let home_binding =
        WorkspaceEntry::initial_volume(WorkspaceRole::Home, "home".into(), own_home.clone());
    let child_path = AgentPath::root(&root_path).child(&child_id)?;
    let child_home = acyclic_harness::filesystem::spawned_binding(&child_path, &home_binding)?;
    let child_binding =
        WorkspaceEntry::initial_volume(WorkspaceRole::Home, "home".into(), child_home.clone());
    let grandchild_path = child_path.child(&grandchild_id)?;
    let grandchild_home =
        acyclic_harness::filesystem::spawned_binding(&grandchild_path, &child_binding)?;
    Ok(Subjects {
        volumes: vec![
            ("own_home", own_home, "home.txt", root),
            ("shared_volume", shared, "shared.txt", root),
            ("project", project_volume, "project.txt", root),
            ("child_volume", child_home, "child.txt", child),
            (
                "grandchild_volume",
                grandchild_home,
                "grandchild.txt",
                grandchild,
            ),
            ("findings_dir", findings, "findings/001.md", root),
            ("skill_path", skill, "skills/swarm-topology/SKILL.md", root),
            (
                "feedback_sink",
                feedback,
                "live/agents/self/feedback.jsonl",
                root,
            ),
        ],
    })
}

/// Whether the declared policy admits `op` on `subject`.
fn declared(subject: &str, op: &str) -> bool {
    !(subject == "skill_path" && op == "write")
}

fn directory(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

fn display_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

async fn populate<A, O>(
    host: &FilesystemHost<A, O>,
    verifier: &AuthorityVerifier,
    root: &AuthorityRoot,
    volume: &VolumeRef,
    path: &str,
    agent: AgentId,
) -> Result<()>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let capabilities = Capabilities::new([
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
        volume.directory_read_capability("", &mut None)?,
    ]);
    let scope = root.bootstrap_scope("populate", &capabilities, Some(agent), &mut None)?;
    let write = ContentGrant::verify(verifier, &scope, volume, VolumeOperation::Write, &mut None)?;
    host.put_content(
        volume,
        &write,
        path,
        b"declared\n",
        "text/plain",
        display_name(path),
        Some(64),
        &IdempotencyKey::new(format!("populate:{path}"))?,
        None,
        ContentAccess::default(),
    )
    .await?;
    Ok(())
}

/// One subject/op/timing: the grant either admits exactly this operation on
/// this volume, or refuses it before any effect. A volume that does not exist
/// yet is absent, never unauthorized.
async fn check<A, O>(
    host: &FilesystemHost<A, O>,
    verifier: &AuthorityVerifier,
    root: &AuthorityRoot,
    subject: &Subject,
    timing: &'static str,
    op: &'static str,
    exists: bool,
) -> std::result::Result<(), String>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let &(subject, ref volume, path, agent) = subject;
    let label = format!("{subject}/{op}/{timing}");
    let declared = declared(subject, op);
    let capability = match op {
        "list" => volume.directory_read_capability("", &mut None),
        _ => volume.capability(
            if op == "write" {
                VolumeOperation::Write
            } else {
                VolumeOperation::Read
            },
            &mut None,
        ),
    }
    .map_err(|error| format!("{label}: {error}"))?;
    let capabilities = if declared {
        Capabilities::new([capability])
    } else {
        // A declared refusal: the scope carries no capability for this
        // subject, so the signed payload can never authorize it.
        Capabilities::new(["model:generate".to_owned()])
    };
    let scope = root
        .bootstrap_scope("grant-check", &capabilities, Some(agent), &mut None)
        .map_err(|error| format!("{label}: signed scope: {error}"))?;
    verifier
        .verify(&scope, &mut None)
        .map_err(|error| format!("{label}: signed payload did not verify: {error}"))?;
    let grant = match op {
        "write" => {
            ContentGrant::verify(verifier, &scope, volume, VolumeOperation::Write, &mut None)
        }
        _ => ContentGrant::verify_directory_read(verifier, &scope, volume, "", &mut None),
    };
    if !declared {
        return match grant {
            Err(Error::Unauthorized(_)) => Ok(()),
            Err(error) => Err(format!("{label}: refused with the wrong cause: {error}")),
            Ok(_) => Err(format!("{label}: an undeclared grant was admitted")),
        };
    }
    let grant = grant.map_err(|error| format!("{label}: declared grant refused: {error}"))?;
    if op == "signed_payload" {
        return Ok(());
    }
    if !exists {
        // The grant is valid, but the selected volume has not been created.
        let outcome = match op {
            "read" => host
                .read_private_path(
                    volume,
                    &grant,
                    path,
                    None,
                    Some(64),
                    ContentAccess::default(),
                )
                .await
                .map(drop),
            "list" => host
                .list_private_directory(
                    volume,
                    &grant,
                    directory(path),
                    DirectoryOptions {
                        generation: None,
                        after: None,
                        maximum_entries: 16,
                        ceiling: None,
                    },
                )
                .await
                .map(drop),
            _ => host
                .put_content(
                    volume,
                    &grant,
                    path,
                    b"declared\n",
                    "text/plain",
                    display_name(path),
                    Some(64),
                    &IdempotencyKey::new(format!("check:{label}"))
                        .map_err(|error| format!("{label}: key: {error}"))?,
                    None,
                    ContentAccess::default(),
                )
                .await
                .map(drop),
        };
        return match outcome {
            Err(Error::Unauthorized(message)) => Err(format!(
                "{label}: a valid grant became unauthorized before its volume existed: {message}"
            )),
            _ => Ok(()),
        };
    }
    match op {
        "read" => {
            let (_, bytes) = host
                .read_private_path(
                    volume,
                    &grant,
                    path,
                    None,
                    Some(64),
                    ContentAccess::default(),
                )
                .await
                .map_err(|error| format!("{label}: declared read failed: {error}"))?;
            if bytes.as_ref() != b"declared\n" {
                return Err(format!("{label}: read returned the wrong bytes"));
            }
        }
        "list" => {
            let page = host
                .list_private_directory(
                    volume,
                    &grant,
                    directory(path),
                    DirectoryOptions {
                        generation: None,
                        after: None,
                        maximum_entries: 16,
                        ceiling: None,
                    },
                )
                .await
                .map_err(|error| format!("{label}: declared list failed: {error}"))?;
            let page = page.ok_or_else(|| format!("{label}: listed path is absent"))?;
            if page.entries.is_empty() {
                return Err(format!("{label}: declared list returned no entries"));
            }
        }
        "write" => {
            host.put_content(
                volume,
                &grant,
                path,
                b"declared\n",
                "text/plain",
                display_name(path),
                Some(64),
                &IdempotencyKey::new(format!("check:{label}"))
                    .map_err(|error| format!("{label}: key: {error}"))?,
                None,
                ContentAccess::default(),
            )
            .await
            .map_err(|error| format!("{label}: declared write failed: {error}"))?;
        }
        _ => return Err(format!("{label}: unknown operation")),
    }
    Ok(())
}

async fn matrix<A, O>(
    host: &FilesystemHost<A, O>,
    verifier: &AuthorityVerifier,
    root: &AuthorityRoot,
    subjects: &Subjects,
) -> std::result::Result<(), String>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    // Start: only the root's own volumes exist. Child and grandchild grants
    // are already valid signed payloads, but their volumes are absent.
    for subject in &subjects.volumes {
        let exists = !matches!(subject.0, "child_volume" | "grandchild_volume");
        for op in OPS {
            check(host, verifier, root, subject, "start", op, exists).await?;
        }
    }
    // Mid-run: the child and grandchild now own their volumes.
    for subject @ (name, volume, path, agent) in &subjects.volumes {
        if matches!(*name, "child_volume" | "grandchild_volume") {
            host.create_volume(volume)
                .await
                .map_err(|error| format!("{name}: create: {error}"))?;
            populate(host, verifier, root, volume, path, *agent)
                .await
                .map_err(|error| format!("{name}: populate: {error}"))?;
        }
        for op in OPS {
            check(host, verifier, root, subject, "mid_run", op, true).await?;
        }
    }
    // After the child and grandchild settle, their volumes and the parent's
    // grants on them remain. A settling child never revokes a parent read.
    for subject in &subjects.volumes {
        for op in OPS {
            check(
                host,
                verifier,
                root,
                subject,
                "after_child_settle",
                op,
                true,
            )
            .await?;
        }
    }
    let root_agent = AgentId::from_bytes([1; 16]);
    for (subject, volume, path, _) in &subjects.volumes {
        if !matches!(*subject, "child_volume" | "grandchild_volume") {
            continue;
        }
        let capabilities = Capabilities::new([volume
            .capability(VolumeOperation::Read, &mut None)
            .map_err(|error| format!("{subject}: read capability: {error}"))?]);
        let scope = root
            .bootstrap_scope("parent-read", &capabilities, Some(root_agent), &mut None)
            .map_err(|error| format!("{subject}: parent scope: {error}"))?;
        let grant = ContentGrant::verify_directory_read(verifier, &scope, volume, "", &mut None)
            .map_err(|error| format!("{subject}: parent read grant: {error}"))?;
        host.read_private_path(
            volume,
            &grant,
            path,
            None,
            Some(64),
            ContentAccess::default(),
        )
        .await
        .map_err(|error| format!("{subject}: parent read after child settle: {error}"))?;
    }
    Ok(())
}

async fn world_for_custody<A, O>(
    custody: &'static str,
    host: Arc<FilesystemHost<A, O>>,
    root: AuthorityRoot,
) -> std::result::Result<(), String>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let verifier = root
        .verifier(&mut None)
        .map_err(|error| format!("{custody}: verifier: {error}"))?;
    let provider = provider().map_err(|error| format!("{custody}: provider: {error}"))?;
    let subjects = subjects(&provider).map_err(|error| format!("{custody}: subjects: {error}"))?;
    for (_, volume, _, _) in &subjects.volumes {
        if matches!(
            volume.id(),
            "own-home" | "shared" | "project" | "findings" | "seed" | "feedback"
        ) {
            host.create_volume(volume)
                .await
                .map_err(|error| format!("{custody}: create {}: {error}", volume.id()))?;
        }
    }
    for (subject, volume, path, agent) in &subjects.volumes {
        if matches!(*subject, "child_volume" | "grandchild_volume") {
            continue;
        }
        populate(&host, &verifier, &root, volume, path, *agent)
            .await
            .map_err(|error| format!("{custody}: populate {subject}: {error}"))?;
    }
    matrix(&host, &verifier, &root, &subjects).await
}

fn key(seed: u64, custody: &str) -> [u8; 32] {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.dst.grants.v1");
    hash.update(&seed.to_le_bytes());
    hash.update(custody.as_bytes());
    *hash.finalize().as_bytes()
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let custody = custody(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let audience = Authority {
                kind: AggregateKind::Task,
                id: format!("grants-{custody}"),
            };
            let directory = temporary_directory().map_err(oracle::failed)?;
            match custody {
                "logical" => {
                    let host = FilesystemHost::new(
                        Fs::memory(),
                        &provider().map_err(oracle::failed)?,
                        None,
                    )
                    .map_err(oracle::failed)?;
                    let root = AuthorityRoot::new("grants", key(seed, custody), audience);
                    world_for_custody(custody, host, root).await
                }
                "physical" => {
                    let fs = Fs::local(LocalOptions::new(directory.path().join("filesystem")))
                        .await
                        .map_err(oracle::failed)?;
                    let host = FilesystemHost::new(fs, &provider().map_err(oracle::failed)?, None)
                        .map_err(oracle::failed)?;
                    let root = AuthorityRoot::new("grants", key(seed, custody), audience);
                    world_for_custody(custody, host, root).await
                }
                _ => {
                    let custody_dir = directory.path().join("signing");
                    let signing = SigningCustody::PrivateFile(&custody_dir);
                    let provisional =
                        SigningKeyReference::create(signing).map_err(oracle::failed)?;
                    let reference = provisional.reference();
                    let seed_key = reference
                        .with_seed(signing, |seed| *seed)
                        .map_err(oracle::failed)?;
                    let fs = Fs::local(LocalOptions::new(directory.path().join("filesystem")))
                        .await
                        .map_err(oracle::failed)?;
                    let host = FilesystemHost::new(fs, &provider().map_err(oracle::failed)?, None)
                        .map_err(oracle::failed)?;
                    let root = AuthorityRoot::new("grants", seed_key, audience);
                    world_for_custody("physical_private_file", host, root).await
                }
            }
            .map(|()| true)
        })
    })
}
