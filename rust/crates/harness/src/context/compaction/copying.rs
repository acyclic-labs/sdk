use super::*;

pub(crate) fn clone_context(
    context: &Context,
    through: Option<usize>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Context, u64)> {
    let source = match through {
        Some(through) => context
            .messages
            .get(..through)
            .ok_or_else(|| Error::Invalid("summary extent exceeds source projection".into()))?,
        None => context.messages.as_slice(),
    };
    let current_input_index = context
        .current_input_index
        .filter(|index| through.is_none_or(|through| (*index as usize) < through));
    if work.is_none() {
        return Ok((
            Context {
                messages: source.to_vec(),
                metadata: context.metadata.clone(),
                current_input_index,
            },
            0,
        ));
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = Context::default();
    let copied = (|| {
        let (messages, claim) = clone_messages(source, None, &[], quota.work())?;
        result.messages = messages;
        quota.adopt(claim)?;
        let (metadata, claim) = clone_metadata(&context.metadata, quota.work())?;
        result.metadata = metadata;
        quota.adopt(claim)?;
        result.current_input_index = current_input_index;
        Ok(())
    })();
    if let Err(error) = copied {
        drop(result);
        return Err(error);
    }
    let claim = quota.retained();
    quota.transfer();
    Ok((result, claim))
}

pub(crate) fn clone_metadata(
    metadata: &std::collections::BTreeMap<String, crate::conversation::FileRef>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(
    std::collections::BTreeMap<String, crate::conversation::FileRef>,
    u64,
)> {
    if work.is_none() {
        return Ok((metadata.clone(), 0));
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = std::collections::BTreeMap::new();
    let copied = (|| {
        for (key, file) in metadata {
            let key = quota.observer()?.clone_text(key)?;
            quota.adopt(key.capacity() as u64)?;
            let file = file.clone_in_quota(&mut quota)?;
            let bytes = std::mem::size_of::<(String, crate::conversation::FileRef)>() as u64;
            let io = quota.observer()?;
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            quota.adopt(bytes)?;
            result.insert(key, file);
        }
        Ok(())
    })();
    if let Err(error) = copied {
        drop(result);
        return Err(error);
    }
    let claim = quota.retained();
    quota.transfer();
    Ok((result, claim))
}

pub(crate) fn clone_messages(
    first: &[crate::model::ModelMessage],
    user: Option<&crate::model::ModelContent>,
    second: &[crate::model::ModelMessage],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(Vec<crate::model::ModelMessage>, u64)> {
    use crate::model::{ModelMessage, ModelRole};
    let count = first
        .len()
        .checked_add(second.len())
        .and_then(|n| n.checked_add(usize::from(user.is_some())))
        .ok_or_else(|| Error::Invalid("context vector capacity overflows".into()))?;
    if work.is_none() {
        let mut result = Vec::with_capacity(count);
        result.extend_from_slice(first);
        if let Some(content) = user {
            result.push(ModelMessage {
                role: ModelRole::User,
                content: content.clone(),
            });
        }
        result.extend_from_slice(second);
        return Ok((result, 0));
    }
    let bytes = count
        .checked_mul(std::mem::size_of::<ModelMessage>())
        .ok_or_else(|| Error::Invalid("context vector capacity overflows".into()))?
        as u64;
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut result = Vec::new();
    let copied = (|| {
        let io = quota.observer()?;
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
        if let Err(cause) = result.try_reserve_exact(count) {
            return Err(quota.observer()?.allocation_failure(cause));
        }
        let actual = (result.capacity() * std::mem::size_of::<ModelMessage>()) as u64;
        if actual > bytes {
            let io = quota.observer()?;
            io.admit_owned(actual - bytes)?;
            io.retain_owned(actual - bytes)?;
            quota.adopt(actual - bytes)?;
        }
        for (role, content) in first
            .iter()
            .map(|m| (m.role, &m.content))
            .chain(user.into_iter().map(|content| (ModelRole::User, content)))
            .chain(second.iter().map(|m| (m.role, &m.content)))
        {
            result.push(clone_message(role, content, &mut quota)?);
        }
        Ok(())
    })();
    if let Err(error) = copied {
        drop(result);
        return Err(error);
    }
    let claim = quota.retained();
    quota.transfer();
    Ok((result, claim))
}

pub(super) fn clone_message(
    role: crate::model::ModelRole,
    content: &crate::model::ModelContent,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<crate::model::ModelMessage> {
    let slot = std::mem::size_of::<crate::model::ModelMessage>() as u64;
    if let Some(io) = quota.work().as_deref() {
        io.admit_copy(slot)?;
    }
    let (content, claim) = crate::model::clone_content(content, quota.work())?;
    quota.adopt(claim)?;
    if let Some(io) = quota.work().as_deref_mut() {
        io.copy_bytes(slot)?;
    }
    Ok(crate::model::ModelMessage { role, content })
}
