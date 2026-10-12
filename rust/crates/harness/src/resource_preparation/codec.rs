use super::*;
use crate::{
    executor::json::{
        JsonBounds, JsonCodecCharge, JsonValueSchema, canonical_json_digest as digest,
        json_from_value_bounded as from_value, json_typed_value_decode_owned_bytes as allowance,
    },
    wire_codec::CodecQuota,
};
impl ResourcePreparationRequest {
    pub(crate) fn decode(
        value: &Value,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Self, u64)> {
        charge(work, 0)?;
        let maximum = value
            .get("maximum_bytes")
            .and_then(Value::as_u64)
            .filter(|n| !matches!(*n, 0 | u64::MAX))
            .ok_or_else(|| diagnostic(work, Invalid, "preparation requires a finite byte bound"))?;
        let source_digest = digest(value, maximum, work)?;
        let mut quota = CodecQuota::from_retained(work, 0);
        let (request, reserved) = Self::decode_value(value, maximum, quota.work())?;
        quota.adopt(reserved)?;
        request.validate(quota.work())?;
        if source_digest != digest(&request, maximum, quota.work())? {
            return Err(diagnostic(
                quota.work(),
                Invalid,
                "preparation decode changes its identity",
            ));
        }
        let owned = add(
            std::mem::size_of::<Self>() as u64,
            request.logical_owned_bytes()?,
        )?;
        if owned > reserved {
            return Err(diagnostic(
                quota.work(),
                Invalid,
                "preparation exceeds its admitted inventory",
            ));
        }
        quota.replace(owned)?;
        quota.transfer();
        Ok((request, owned))
    }
    fn decode_value(
        value: &Value,
        maximum: u64,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Self, u64)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
            let io = crate::filesystem::PreparationIo::from_native_work(original)?;
            let (request, custody) = crate::filesystem::native_decoded(
                io.value_decode_preparation::<Self>(value, Invalid)?,
            )?;
            let owned = custody.bytes();
            if let Err(error) = work
                .as_deref_mut()
                .expect("original observer")
                .absorb_native_custody(custody)
            {
                drop(request);
                return Err(error);
            }
            return Ok((request, owned));
        }
        let mut quota = CodecQuota::from_retained(work, 0);
        let bounds = JsonBounds {
            maximum_bytes: maximum,
            maximum_items: maximum + 1,
            maximum_depth: 128,
        };
        let request = from_value(
            std::borrow::Cow::Borrowed(value),
            bounds,
            0,
            None,
            JsonValueSchema::ResourcePreparationRequest,
            &mut |next| {
                if let JsonCodecCharge::Admission { value_shape, .. } = next {
                    quota.replace(add(
                        std::mem::size_of::<Self>() as u64,
                        allowance(value_shape, std::mem::size_of::<Self>() as u64)?,
                    )?)
                } else if let Some(io) = quota.work().as_deref_mut() {
                    io.codec_charge(next)
                } else {
                    Ok(())
                }
            },
        )?;
        let retained = quota.retained();
        quota.transfer();
        Ok((request, retained))
    }
}
#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ResourcePreparationRequest {
    fn json_value_schema() -> JsonValueSchema {
        JsonValueSchema::ResourcePreparationRequest
    }
}
