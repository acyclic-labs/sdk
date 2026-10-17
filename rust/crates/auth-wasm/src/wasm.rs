use crate::{Snapshot, block_ids};
use acyclic_auth::{authority::decode_caveat, contract::*, verify::*};
use js_sys::{Array, BigInt, Uint8Array};
use prost::Message;
use wasm_bindgen::{JsCast, prelude::*};
use zeroize::Zeroizing;

fn error(value: VerifyError) -> JsValue {
    let error = js_sys::Error::new(&value.to_string());
    error.set_name("CapabilityError");
    error.into()
}

/// Trust supplied by the application from an authenticated current source.
#[wasm_bindgen]
pub struct TrustSnapshot(Snapshot);

#[wasm_bindgen]
impl TrustSnapshot {
    /// Key/id arrays are concatenated 32-byte values. No issuer is taken from
    /// the capability itself. Create a fresh snapshot after every trust update.
    #[wasm_bindgen(constructor)]
    pub fn new(
        environment: String,
        environment_keys: Uint8Array,
        account: String,
        issuer_keys: Uint8Array,
        minimum_key_epoch: BigInt,
        revoked_key_ids: Vec<String>,
        revoked_block_ids: Uint8Array,
    ) -> Result<Self, JsValue> {
        Ok(Self(Snapshot {
            environment,
            environment_keys: block_ids(&environment_keys.to_vec()).map_err(error)?,
            account,
            issuer_keys: block_ids(&issuer_keys.to_vec()).map_err(error)?,
            minimum_key_epoch: u64::try_from(minimum_key_epoch)
                .map_err(|_| error(VerifyError::Malformed))?,
            revoked_key_ids: revoked_key_ids.into_iter().collect(),
            revoked_block_ids: block_ids(&revoked_block_ids.to_vec())
                .map_err(error)?
                .into_iter()
                .collect(),
        }))
    }
}

/// Immutable authenticated holder. There is no mint or root-key API.
#[wasm_bindgen]
pub struct Capability(VerifiedCapability);

#[wasm_bindgen]
impl Capability {
    /// Input is copied directly into zeroizing storage, bypassing wasm-bindgen's
    /// unguarded slice temporaries. The caller owns its JavaScript input buffer.
    #[wasm_bindgen(js_name = verify)]
    pub fn verify_bytes(
        bytes: Uint8Array,
        trust: &TrustSnapshot,
        now_unix_seconds: BigInt,
    ) -> Result<Self, JsValue> {
        let mut input = Zeroizing::new(vec![0; bytes.length() as usize]);
        bytes.copy_to(&mut input);
        let now = u64::try_from(now_unix_seconds).map_err(|_| error(VerifyError::Malformed))?;
        verify(&input, &trust.0, now).map(Self).map_err(error)
    }

    /// Recheck current trust and time without repeating signatures.
    pub fn check(&self, trust: &TrustSnapshot, now_unix_seconds: BigInt) -> Result<(), JsValue> {
        let now = u64::try_from(now_unix_seconds).map_err(|_| error(VerifyError::Malformed))?;
        self.0.check(&trust.0, now).map_err(error)
    }

    /// Caveats use the generated protobuf contract; the Rust decoder admits all
    /// semantic values. Only the supplied fresh tip secret can be retained.
    pub fn attenuate(
        &self,
        caveats: Array,
        next_secret: Uint8Array,
    ) -> Result<Capability, JsValue> {
        if next_secret.length() != 32 {
            return Err(error(VerifyError::Malformed));
        }
        let mut secret = Zeroizing::new([0; 32]);
        next_secret.copy_to(secret.as_mut_slice());
        let caveats = caveats
            .iter()
            .map(|value| {
                let bytes = value
                    .dyn_into::<Uint8Array>()
                    .map_err(|_| error(VerifyError::Malformed))?;
                decode_caveat(&bytes.to_vec()).map_err(|_| error(VerifyError::Malformed))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.0.attenuate(caveats, &secret).map(Self).map_err(error)
    }

    /// Consumes this holder and its append proof.
    pub fn seal(self) -> Result<Capability, JsValue> {
        self.0.seal().map(Self).map_err(error)
    }

    /// Caller-owned transport bytes. Clear the JavaScript array after use.
    pub fn encode(&self) -> Uint8Array {
        Uint8Array::from(self.0.encode().as_slice())
    }

    #[wasm_bindgen(js_name = blockIds)]
    pub fn block_ids(&self) -> Array {
        self.0
            .block_ids()
            .iter()
            .map(|id| Uint8Array::from(id.as_slice()))
            .collect()
    }

    /// Issuer-certified identity with append-only delegation lineage applied.
    pub fn principal(&self) -> Uint8Array {
        let value = PrincipalProto::from(self.0.effective().principal().clone()).encode_to_vec();
        Uint8Array::from(value.as_slice())
    }

    /// Each grant set remains a conjunctive ceiling.
    pub fn grants(&self) -> Array {
        self.0
            .effective()
            .grants()
            .map(|value| {
                Uint8Array::from(
                    GrantSetProto::from(value.clone())
                        .encode_to_vec()
                        .as_slice(),
                )
            })
            .collect()
    }

    /// Preserve the declaring identity of each conjunctive filter.
    pub fn filters(&self) -> Array {
        self.0
            .effective()
            .filters()
            .map(|clause| {
                JsValue::from(FilterClause {
                    declaring_block: clause.declaring_block(),
                    subject_id: clause.subject_id().to_owned(),
                    lineage: clause.lineage().map(str::to_owned).collect(),
                    filter: FilterProto::from(clause.filter().clone()).encode_to_vec(),
                })
            })
            .collect()
    }

    pub fn limits(&self) -> Uint8Array {
        let value = LimitsProto::from(self.0.effective().limits().clone()).encode_to_vec();
        Uint8Array::from(value.as_slice())
    }

    #[wasm_bindgen(js_name = expiresAtUnixSeconds)]
    pub fn expires_at_unix_seconds(&self) -> u64 {
        self.0.effective().expires_at_unix_seconds()
    }

    #[wasm_bindgen(js_name = notBeforeUnixSeconds)]
    pub fn not_before_unix_seconds(&self) -> u64 {
        self.0.effective().not_before_unix_seconds()
    }
}

/// Owned display projection. Editing it never changes holder authority.
#[wasm_bindgen(getter_with_clone)]
pub struct FilterClause {
    /// Zero-based block index where references became fixed.
    pub declaring_block: usize,
    /// Acting identity at that block.
    pub subject_id: String,
    /// Ancestor-or-self identities through that block.
    pub lineage: Vec<String>,
    /// Canonical protobuf Filter.
    pub filter: Vec<u8>,
}
