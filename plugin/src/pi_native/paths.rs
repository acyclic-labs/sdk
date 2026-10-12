use super::*;

/// References only.
pub(crate) struct PiNativePaths {
    pub(crate) cwd: PathBuf,
    pub(crate) session_file: PathBuf,
    pub(crate) view_root: PathBuf,
}

pub(crate) fn planned_native_paths(
    resources: &crate::ServiceResources,
    root_session: &str,
    task: acyclic_harness::TaskId,
    root_key: &str,
    work: &NativeMaterializationWork,
) -> Result<(PiNativePaths, acyclic_fs::NativeAllocation)> {
    work.remaining()?;
    let route = native_root_route(root_key, work)?;
    let task = native_task_directory(resources, root_session, task, work)?;
    let mut view = work.join_path(&task.0, Path::new("view"))?;
    let session = work.join_path(&view.0, Path::new("scratch"))?;
    let mut cwd = work.join_path(&view.0, Path::new(&route.0))?;
    let mut session_file = work.join_path(&session.0, Path::new("session.jsonl"))?;
    let custody = work.claim(
        std::mem::size_of::<PiNativePaths>() as u64,
        0,
        acyclic_fs::WorkCounters::default(),
    )?;
    let mut planned = (
        PiNativePaths {
            cwd: cwd.0,
            session_file: session_file.0,
            view_root: view.0,
        },
        custody,
    );
    for incoming in [&mut cwd.1, &mut session_file.1, &mut view.1] {
        planned.1.absorb(incoming)?;
    }
    Ok(planned)
}

pub(super) fn native_root_route(
    key: &str,
    work: &NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    use base64::Engine as _;
    work.remaining()?;
    work.charge(WorkCounters {
        items_examined: key.len() as u64,
        ..WorkCounters::default()
    })?;
    let reject = || Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason: "Pi native active root has no canonical identity",
    };
    let mut identity = [0; 16];
    hex::decode_to_slice(key, &mut identity).map_err(|_| reject())?;
    let mut canonical = [0; 32];
    hex::encode_to_slice(identity, &mut canonical).expect("exact root identity hexadecimal extent");
    if key.as_bytes() != canonical {
        return Err(reject());
    }
    let mut route = [0_u8; 23];
    route[0] = b'r';
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode_slice(identity, &mut route[1..])
        .expect("exact root identity route extent");
    pi_native_string(
        std::str::from_utf8(&route).expect("root route uses ASCII encoding"),
        work,
    )
}

fn native_task_directory(
    resources: &crate::ServiceResources,
    root_session: &str,
    task: acyclic_harness::TaskId,
    work: &NativeMaterializationWork,
) -> Result<(PathBuf, acyclic_fs::NativeAllocation)> {
    if root_session.is_empty() || !resources.data.is_absolute() {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi native path references require their original absolute service root",
        });
    }
    work.charge(WorkCounters {
        bytes_hashed: root_session.len() as u64 + 16,
        ..WorkCounters::default()
    })?;
    let session_hash = blake3::hash(root_session.as_bytes()).to_hex();
    let task_hash = blake3::hash(&task.into_bytes()).to_hex();
    let separator = std::path::MAIN_SEPARATOR;
    let relative = work.display(&format_args!(
        "sessions{separator}{session_hash}{separator}agents{separator}{task_hash}"
    ))?;
    work.join_path(&resources.data, Path::new(&relative.0))
        .map_err(acyclic_harness::Error::from)
}

pub(crate) fn native_view_options(
    resources: &crate::ServiceResources,
    root_session: &str,
    task: acyclic_harness::TaskId,
    maximum_volumes: u32,
    work_per_volume: WorkBudget,
    work: &NativeMaterializationWork,
) -> Result<(NativeViewOptions, acyclic_fs::NativeAllocation)> {
    if maximum_volumes == 0 || !work_per_volume.has_finite_allowances() {
        return Err(invalid(
            "Pi native options require their original finite composition allowances",
        ));
    }
    let task = native_task_directory(resources, root_session, task, work)?;
    let view = work.join_path(&task.0, Path::new("view"))?;
    Ok((
        NativeViewOptions {
            root: view.0,
            maximum_volumes,
            work_per_volume,
        },
        view.1,
    ))
}

pub(super) fn native_service_root<'a>(
    data: &Path,
    root_session: &str,
    task: acyclic_harness::TaskId,
    view: &'a Path,
    work: &NativeMaterializationWork,
) -> Result<&'a Path> {
    work.charge(WorkCounters {
        bytes_hashed: root_session.len() as u64 + 16,
        ..WorkCounters::default()
    })?;
    let session_hash = blake3::hash(root_session.as_bytes()).to_hex();
    let task_hash = blake3::hash(&task.into_bytes()).to_hex();
    let mut actual = view
        .strip_prefix(data)
        .map_err(|_| invalid("Pi native root escaped its protected service"))?
        .components();
    for expected in [
        "sessions",
        session_hash.as_str(),
        "agents",
        task_hash.as_str(),
        "view",
    ] {
        if actual.next() != Some(Component::Normal(std::ffi::OsStr::new(expected))) {
            return Err(invalid(
                "Pi native preparation changed its original root session or task namespace",
            ));
        }
    }
    if actual.next().is_some() {
        return Err(invalid(
            "Pi native preparation selected a descendant rather than its original view",
        ));
    }
    view.parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .ok_or_else(|| invalid("Pi native preparation has no protected original session root"))
}
