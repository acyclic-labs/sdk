//! Routes packet operations through the original single or shared projection.

use super::packet::{PacketInput, PacketOutput, PacketRequest};
use super::packet_dispatch::{PacketCompletion, PacketFailure};
use super::shared::{SharedViews, view_root_inode};
use super::*;

pub(super) enum PacketRouter {
    Single(FuseProjection),
    Shared(Arc<SharedViews>),
}

impl PacketRouter {
    pub(super) fn cache_capabilities(&self) -> Result<InitFlags, NativeMountError> {
        match self {
            Self::Single(projection) => Ok(projection.core.state()?.cache.capabilities()),
            Self::Shared(_) => Ok(ProjectionCache::Notifications.capabilities()),
        }
    }

    pub(super) fn initialize(
        &self,
    ) -> Result<Option<crate::NativeMaterializationWork>, PacketFailure> {
        match self {
            Self::Single(projection) => projection
                .core
                .request(|work| {
                    work.admit(crate::WorkCounters::UNCHARGED)?;
                    Ok(Some(work.clone()))
                })
                .map_err(|failure| PacketFailure::from_callback(&projection.core, failure)),
            Self::Shared(_) => Ok(None),
        }
    }

    pub(super) fn dispatch(
        &self,
        request: &PacketRequest<'_>,
        response: &mut [u8],
    ) -> Result<PacketCompletion, PacketFailure> {
        match self {
            Self::Single(projection) => {
                projection.dispatch_packet(request, response, request.owner)
            }
            Self::Shared(views) => {
                if request.opcode == 42 {
                    return shared_forget(views, request);
                }
                if request.inode == ROOT_INODE {
                    return shared_root(views, request, response);
                }
                // These arguments carry a second inode. Cross-view operations
                // retain the original EXDEV decision before entering either view.
                let mut input = PacketInput::new(request.body);
                let other = match request.opcode {
                    12 | 13 | 45 => Some(input.u64()?),
                    47 => {
                        input.take(16)?;
                        Some(input.u64()?)
                    }
                    _ => None,
                };
                if let Some(other) = other {
                    if let Err(errno) = views.same_view(INodeNo(request.inode), INodeNo(other)) {
                        return errno_reply(request, response, errno);
                    }
                }
                match views.view(INodeNo(request.inode)) {
                    Ok(view) => match view.dispatch_packet(request, response, request.owner) {
                        Ok(completion) => Ok(completion.retaining(view)),
                        Err(error) => Err(error.retaining_view(view)),
                    },
                    Err(_) if matches!(request.opcode, 2 | 42) => Ok(PacketCompletion::no_reply()),
                    Err(errno) => errno_reply(request, response, errno),
                }
            }
        }
    }

    pub(super) fn close(&self) {
        match self {
            Self::Single(projection) => projection.core.stop(),
            Self::Shared(views) => views.close(),
        }
    }
}

fn errno_reply(
    request: &PacketRequest<'_>,
    response: &mut [u8],
    errno: i32,
) -> Result<PacketCompletion, PacketFailure> {
    let mut output = PacketOutput::new(response, 0)?;
    Ok(PacketCompletion::reply(
        output.finish(request.unique, Some(errno))?,
    ))
}

fn shared_root(
    views: &SharedViews,
    request: &PacketRequest<'_>,
    response: &mut [u8],
) -> Result<PacketCompletion, PacketFailure> {
    if request.opcode == 2 {
        let mut input = PacketInput::new(request.body);
        input.u64()?;
        input.end()?;
        return Ok(PacketCompletion::no_reply());
    }
    let mut input = PacketInput::new(request.body);
    let result = (|| {
        let mut completion = PacketCompletion::no_reply();
        let mut output = PacketOutput::new(
            response,
            match request.opcode {
                1 => 128,
                3 => 104,
                27 => 16,
                17 => 80,
                _ => 0,
            },
        )?;
        match request.opcode {
            1 => {
                let name = input.name()?;
                input.end()?;
                let Some((serial, view)) = views.views.enter_name(name.as_bytes()) else {
                    return Err(libc::ENOENT.into());
                };
                let mut retained_work = None;
                let attributes = view.core.request(|work| {
                    retained_work = Some(work.clone());
                    let attributes = view.node_attributes(view_root_inode(serial), None, work)?;
                    Ok((attributes, work.clone()))
                });
                let ((attr, ttl), work) = match attributes {
                    Ok(value) => value,
                    Err(failure) => {
                        return Err(PacketFailure::from_callback(&view.core, failure)
                            .retaining_work(retained_work)
                            .retaining_view(view));
                    }
                };
                output.entry(&attr, &ttl)?;
                return Ok(
                    PacketCompletion::reply(output.finish(request.unique, None)?)
                        .retaining_work(work)
                        .retaining(view),
                );
            }
            3 => {
                input.take(16)?;
                input.end()?;
                output.attributes(&views.root_attr, &CACHE_TTL)?;
            }
            27 => {
                input.take(8)?;
                input.end()?;
                output.opened(0, FopenFlags::empty())?;
            }
            28 | 44 => {
                if let Err(error) = shared_listing(
                    views,
                    input,
                    &mut output,
                    &mut completion,
                    request.opcode == 44,
                ) {
                    return Err(error.retaining_completion(completion));
                }
            }
            29 | 30 => {
                input.take(if request.opcode == 29 { 24 } else { 16 })?;
                input.end()?;
            }
            17 => {
                input.end()?;
                output.unknown_capacity()?;
            }
            22 => {
                input.take(8)?;
                input.name()?;
                input.end()?;
                return Err(libc::ENODATA.into());
            }
            23 => {
                let size = input.u32()?;
                input.take(4)?;
                input.end()?;
                if size == 0 {
                    output.u64(0)?;
                }
            }
            4 | 5 | 6 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 18 | 20 | 21 | 24 | 25 | 35
            | 43 | 45 | 46 | 47 => return Err(libc::EACCES.into()),
            _ => return Err(libc::ENOSYS.into()),
        }
        completion.length = Some(output.finish(request.unique, None)?);
        Ok(completion)
    })();
    match result {
        Ok(value) => Ok(value),
        Err(error) if error.is_terminal() => Err(error),
        Err(error) => errno_reply(request, response, error.errno()),
    }
}

fn shared_forget(
    views: &SharedViews,
    request: &PacketRequest<'_>,
) -> Result<PacketCompletion, PacketFailure> {
    let mut input = PacketInput::new(request.body);
    let count = usize::try_from(input.u32()?).map_err(|_| libc::EOVERFLOW)?;
    input.take(4)?;
    if input.remaining().len() != count.checked_mul(16).ok_or(libc::EOVERFLOW)? {
        return Err(libc::EPROTO.into());
    }
    let mut completion = PacketCompletion::no_reply();
    for _ in 0..count {
        let inode = input.u64()?;
        let references = input.u64()?;
        let Ok(view) = views.view(INodeNo(inode)) else {
            continue;
        };
        let core = Arc::clone(&view.core);
        let mut retained_work = None;
        let result = core.request(|work| {
            retained_work = Some(work.clone());
            work.charge(Items.delta(1))?;
            // Keep the original view/work through used-ring publication,
            // even though FORGET has no FUSE response bytes.
            completion.retain_view(view, work)?;
            core.state_to_finish()
                .release_lookup_reference(inode, references);
            Ok(())
        });
        if let Err(failure) = result {
            return Err(PacketFailure::from_callback(&core, failure)
                .retaining_work(retained_work)
                .retaining_completion(completion));
        }
    }
    Ok(completion)
}

fn shared_listing(
    views: &SharedViews,
    mut input: PacketInput<'_>,
    output: &mut PacketOutput<'_>,
    completion: &mut PacketCompletion,
    plus: bool,
) -> Result<(), PacketFailure> {
    input.take(8)?;
    let mut offset = input.u64()?;
    let size = input.u32()? as usize;
    input.take(20)?;
    input.end()?;
    output.limit_payload(size)?;
    output.plus = plus;
    for (next, name) in [(1, "."), (2, "..")] {
        if offset < next
            && output.directory_entry(next, OsStr::new(name), &views.root_attr, &CACHE_TTL)?
        {
            return Ok(());
        }
    }
    loop {
        let batch = views.listed_after(offset, super::shared::ROOT_LISTING_BATCH);
        if batch.is_empty() {
            break;
        }
        for (serial, name) in batch {
            offset = serial + 2;
            if output.plus {
                let Some(view) = views.views.enter(serial) else {
                    continue;
                };
                let core = Arc::clone(&view.core);
                let mut retained_work = None;
                let result = core.request(|work| {
                    retained_work = Some(work.clone());
                    let (attr, ttl) = view.node_attributes(view_root_inode(serial), None, work)?;
                    if output.directory_entry(offset, OsStr::from_bytes(&name), &attr, &ttl)? {
                        return Ok(true);
                    }
                    completion.retain_view(view, work)?;
                    Ok(false)
                });
                match result {
                    Ok(true) => return Ok(()),
                    Ok(false) => {}
                    Err(failure)
                        if crate::native_mount::shared::outcome::stops_mount(&failure.0) =>
                    {
                        return Err(
                            PacketFailure::source(&core, failure).retaining_work(retained_work)
                        );
                    }
                    Err(_) => continue,
                }
            } else {
                let attr = FileAttr {
                    ino: INodeNo(view_root_inode(serial)),
                    ..views.root_attr
                };
                if output.directory_entry(
                    offset,
                    OsStr::from_bytes(&name),
                    &attr,
                    &Duration::ZERO,
                )? {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}
