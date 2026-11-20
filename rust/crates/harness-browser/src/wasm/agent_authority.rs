mod platform;
use super::*;

fn optional_agent(agent: Option<String>) -> Result<Option<AgentId>, JsValue> {
    agent
        .map(|agent| AgentId::parse(&agent))
        .transpose()
        .map_err(js_error)
}

impl WasmAgentAuthority {
    pub(super) fn within(&self) -> Result<acyclic_actors::addressing::ActorReference, JsValue> {
        let recipient = &self.caller.audience;
        acyclic_actors::addressing::ActorReference::new(
            &recipient.account,
            &recipient.ns,
            &recipient.id,
        )
        .map_err(|error| js_error(crate::Error::Invalid(error.to_string())))
    }

    fn verifier(&self) -> Result<crate::core::AuthorityVerifier, JsValue> {
        self.issuer.verifier(&mut None).map_err(js_error)
    }
}

#[wasm_bindgen]
impl WasmAgentAuthority {
    /// Retains an opaque capability supplied by the authenticated platform.
    /// Signing and append secrets remain outside guest memory.
    #[wasm_bindgen(constructor)]
    pub fn new(
        #[wasm_bindgen(unchecked_param_type = "WasmAuthorityCapability")] platform: JsValue,
    ) -> Result<Self, JsValue> {
        platform::open(platform)
    }

    /// Issues a host grant, optionally bound to an acting agent.
    #[wasm_bindgen(js_name = issueScope)]
    pub async fn issue_scope(
        &self,
        id: String,
        capabilities: JsValue,
        agent: Option<String>,
    ) -> Result<JsValue, JsValue> {
        let capabilities = Capabilities::new(from_js::<Vec<String>>(capabilities)?);
        let scope = self
            .issuer
            .issue(None, &id, &capabilities, optional_agent(agent)?)
            .await
            .map_err(js_error)?;
        to_js(&scope)
    }

    /// Attenuates a scope without permitting capability expansion.
    pub async fn attenuate(
        &self,
        parent: JsValue,
        id: String,
        capabilities: JsValue,
    ) -> Result<JsValue, JsValue> {
        let parent: Scope = from_js(parent)?;
        let capabilities = Capabilities::new(from_js::<Vec<String>>(capabilities)?);
        to_js(
            &self
                .issuer
                .issue(Some(&parent), &id, &capabilities, parent.agent())
                .await
                .map_err(js_error)?,
        )
    }
}
