//! Agent key custody and the published Global signup proof.

use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use sha2::{Digest as _, Sha256};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

// Blocking workers outlive the future awaiting them.
struct ProofCancellation(Arc<AtomicBool>);

impl Drop for ProofCancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum Agent {
    Pending {
        private_key: String,
    },
    Active {
        private_key: String,
        registration: Registration,
    },
}

#[derive(Deserialize, Serialize)]
pub(super) struct Registration {
    pub(super) user_id: String,
    pub(super) org_id: String,
    pub(super) key_id: String,
    account_id: String,
    birth: String,
    certificate: String,
    pub(super) tier: String,
    expires_at: u64,
}

impl Agent {
    pub(super) fn registration(&self) -> Result<&Registration, Error> {
        match self {
            Self::Active { registration, .. } => Ok(registration),
            Self::Pending { .. } => Err(Error::new(
                "signup_pending",
                "signup is pending; rerun signup --agent with these credentials to recover its receipt",
            )),
        }
    }

    pub(super) fn sign(&self, message: &str) -> Result<String, Error> {
        let private_key = match self {
            Self::Pending { private_key } | Self::Active { private_key, .. } => private_key,
        };
        let bytes = URL_SAFE_NO_PAD
            .decode(private_key)
            .map_err(|_| Error::new("credential_error", "invalid agent private key"))?;
        let key = Ed25519KeyPair::from_pkcs8(&bytes)
            .map_err(|_| Error::new("credential_error", "invalid agent private key"))?;
        Ok(URL_SAFE_NO_PAD.encode(key.sign(message.as_bytes()).as_ref()))
    }
}

#[derive(Deserialize)]
struct Challenge {
    challenge: String,
    difficulty: u32,
    expires_at: u64,
}

fn proof(challenge: &Challenge, public_key: &str, cancelled: &AtomicBool) -> Result<String, Error> {
    if challenge.difficulty > 28 || challenge.challenge.len() != 43 {
        return Err(Error::new("invalid_response", "invalid hashcash challenge"));
    }
    let prefix = format!("{}.{}.", challenge.challenge, public_key);
    let mut base = Sha256::new();
    base.update(prefix.as_bytes());
    for nonce in 0_u64.. {
        if nonce % 4096 == 0 {
            if cancelled.load(Ordering::Relaxed) {
                return Err(Error::new("cancelled", "agent signup cancelled"));
            }
            if now()? >= challenge.expires_at {
                return Err(Error::new(
                    "expired_challenge",
                    "hashcash challenge expired",
                ));
            }
        }
        let nonce = nonce.to_string();
        let mut hash = base.clone();
        hash.update(nonce.as_bytes());
        let digest = hash.finalize();
        if leading_zero_bits(&digest) >= challenge.difficulty {
            return Ok(nonce);
        }
    }
    Err(Error::new("proof_error", "hashcash nonce space exhausted"))
}

fn leading_zero_bits(bytes: &[u8]) -> u32 {
    let mut bits = 0;
    for byte in bytes {
        bits += byte.leading_zeros();
        if *byte != 0 {
            break;
        }
    }
    bits
}

impl Identity {
    async fn agent_challenge(&self) -> Result<Challenge, Error> {
        let value = Self::response(
            self.client
                .get(format!("{}/v1/identity/signup/challenge", self.endpoint)),
        )
        .await?;
        serde_json::from_value(value)
            .map_err(|_| Error::new("invalid_response", "invalid agent challenge"))
    }

    pub(super) async fn signup(&self, store: &Store) -> Result<Value, Error> {
        let (private_key, session) = if store.path.exists() {
            let credentials = store.credentials(&self.endpoint)?;
            match credentials.agent {
                Some(Agent::Pending { private_key }) => (private_key, credentials.session),
                _ => {
                    return Err(Error::new(
                        "credentials_exist",
                        "signup requires new or pending agent credentials",
                    ));
                }
            }
        } else {
            let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .map_err(|_| Error::new("key_error", "cannot generate agent key"))?;
            (URL_SAFE_NO_PAD.encode(document.as_ref()), None)
        };
        let bytes = URL_SAFE_NO_PAD
            .decode(&private_key)
            .map_err(|_| Error::new("credential_error", "invalid agent private key"))?;
        let key = Ed25519KeyPair::from_pkcs8(&bytes)
            .map_err(|_| Error::new("credential_error", "invalid agent private key"))?;
        let challenge = self.agent_challenge().await?;
        let public_key = URL_SAFE_NO_PAD.encode(key.public_key().as_ref());
        let cancellation = ProofCancellation(Arc::new(AtomicBool::new(false)));
        let cancelled = cancellation.0.clone();
        let nonce = tokio::task::spawn_blocking(move || {
            proof(&challenge, &public_key, &cancelled).map(|nonce| (challenge, public_key, nonce))
        })
        .await
        .map_err(|_| Error::new("proof_error", "hashcash worker failed"))??;
        drop(cancellation);
        // Persist before sending: a lost response must never lose the key to a created org.
        let mut credentials = Credentials {
            endpoint: self.endpoint.clone(),
            session,
            agent: Some(Agent::Pending {
                private_key: private_key.clone(),
            }),
        };
        store.save(&credentials)?;
        let signature = URL_SAFE_NO_PAD.encode(
            key.sign(
                &format!(
                    "acyclic-agent-signup-v1\n{}\n{}\n{}",
                    nonce.1, nonce.0.challenge, nonce.2
                )
                .into_bytes(),
            )
            .as_ref(),
        );
        let value = Self::response(self.client.post(format!("{}/v1/identity/signup", self.endpoint))
            .json(&json!({"challenge": nonce.0.challenge, "public_key": nonce.1, "nonce": nonce.2, "signature": signature}))).await?;
        let registration: Registration = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "invalid_response",
                "invalid signup response; agent key preserved",
            )
        })?;
        if [
            &registration.user_id,
            &registration.org_id,
            &registration.key_id,
            &registration.account_id,
            &registration.birth,
            &registration.certificate,
            &registration.tier,
        ]
        .iter()
        .any(|value| value.is_empty())
            || registration.expires_at == 0
        {
            return Err(Error::new(
                "invalid_response",
                "incomplete signup receipt; agent key preserved",
            ));
        }
        let result = serde_json::to_value(&registration)
            .map_err(|_| Error::new("invalid_response", "invalid agent registration"))?;
        credentials.agent = Some(Agent::Active {
            private_key,
            registration,
        });
        store.save(&credentials)?;
        Ok(result)
    }

    pub(super) async fn renew_agent(&self, store: &Store) -> Result<Value, Error> {
        let mut credentials = store.credentials(&self.endpoint)?;
        let agent = credentials.agent.as_mut().ok_or_else(|| {
            Error::new(
                "login_required",
                "run acyclic login or signup --agent first",
            )
        })?;
        let registration = agent.registration()?;
        let challenge = self.agent_challenge().await?;
        let signature = agent.sign(&format!(
            "acyclic-agent-renew-v1\n{}\n{}\n{}",
            registration.org_id, registration.key_id, challenge.challenge
        ))?;
        let value = Self::response(
            self.client
                .post(format!("{}/v1/identity/signup/renew", self.endpoint))
                .json(
                    &json!({"org_id": registration.org_id, "key_id": registration.key_id,
                "challenge": challenge.challenge, "signature": signature}),
                ),
        )
        .await?;
        #[derive(Deserialize)]
        struct Renewal {
            org_id: String,
            key_id: String,
            certificate: String,
            birth: String,
            expires_at: u64,
        }
        let renewal: Renewal = serde_json::from_value(value.clone())
            .map_err(|_| Error::new("invalid_response", "invalid agent renewal response"))?;
        if renewal.org_id != registration.org_id || renewal.key_id != registration.key_id {
            return Err(Error::new(
                "invalid_response",
                "agent renewal changed identity",
            ));
        }
        if renewal.certificate.is_empty() || renewal.birth.is_empty() || renewal.expires_at == 0 {
            return Err(Error::new(
                "invalid_response",
                "incomplete agent renewal receipt",
            ));
        }
        if let Agent::Active { registration, .. } = agent {
            registration.certificate = renewal.certificate;
            registration.birth = renewal.birth;
            registration.expires_at = renewal.expires_at;
        }
        store.save(&credentials)?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_uses_server_sha256_format_and_most_significant_bits() {
        let challenge = Challenge {
            challenge: "c".repeat(43),
            difficulty: 12,
            expires_at: u64::MAX,
        };
        let nonce = proof(&challenge, &"p".repeat(43), &AtomicBool::new(false));
        assert!(nonce.is_ok());
        if let Ok(nonce) = nonce {
            let hash = Sha256::digest(
                format!("{}.{}.{}", challenge.challenge, "p".repeat(43), nonce).as_bytes(),
            );
            assert!(leading_zero_bits(&hash) >= 12);
        }
        assert_eq!(leading_zero_bits(&[0, 0, 1]), 23);
        assert_eq!(leading_zero_bits(&[128]), 0);
    }

    #[test]
    fn dropped_proof_guard_stops_the_worker_at_its_checkpoint() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let guard = ProofCancellation(cancelled.clone());
        drop(guard);
        assert!(
            proof(
                &Challenge {
                    challenge: "c".repeat(43),
                    difficulty: 28,
                    expires_at: u64::MAX
                },
                "key",
                &cancelled
            )
            .is_err_and(|error| error.code == "cancelled")
        );
    }

    #[test]
    fn invalid_or_expired_challenges_are_refused() {
        assert!(
            proof(
                &Challenge {
                    challenge: "c".repeat(43),
                    difficulty: 29,
                    expires_at: u64::MAX
                },
                "key",
                &AtomicBool::new(false)
            )
            .is_err()
        );
        assert!(
            proof(
                &Challenge {
                    challenge: "c".repeat(43),
                    difficulty: 1,
                    expires_at: 0
                },
                "key",
                &AtomicBool::new(false)
            )
            .is_err()
        );
    }
}
