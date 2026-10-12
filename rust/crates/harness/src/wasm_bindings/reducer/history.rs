use super::*;

#[derive(Serialize)]
struct ConversationPage<'a> {
    agent: Option<AgentId>,
    event_revision: u64,
    total_messages: u64,
    messages: &'a [ConversationMessage],
    next_sequence: Option<u64>,
}

#[wasm_bindgen]
impl WasmReducer {
    /// Selects fork history against this reducer before provider preparation.
    #[wasm_bindgen(js_name = prepareForkRequest, unchecked_return_type = "ForkRequest")]
    pub fn prepare_fork_request(
        &self,
        #[wasm_bindgen(unchecked_param_type = "unknown")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ForkHistoryPolicy | null")] policy: JsValue,
    ) -> Result<JsValue, JsValue> {
        let request: ForkRequest = from_js(request)?;
        let policy: crate::fork::ForkHistoryPolicy = if policy.is_null() || policy.is_undefined() {
            crate::fork::ForkHistoryPolicy::default()
        } else {
            from_js(policy)?
        };
        to_js_admitted(
            &request
                .with_history_policy(&self.reducer, policy, &mut None)
                .map_err(js_error)?,
        )
    }

    /// Returns a bounded conversation page with exact revisions and cursors.
    #[wasm_bindgen(js_name = conversationPage)]
    pub fn conversation_page(&self, after_sequence: u64, limit: u32) -> Result<JsValue, JsValue> {
        if limit == 0 || limit as usize > crate::conversation::MAX_CONVERSATION_PAGE_MESSAGES {
            return Err(JsValue::from_str("conversation page limit is invalid"));
        }
        let conversation = self
            .reducer
            .conversation()
            .ok_or_else(|| JsValue::from_str("aggregate is not a conversation"))?;
        let total_messages = conversation.logical_revision();
        let messages = conversation
            .page(after_sequence, total_messages, limit as usize)
            .map_err(js_error)?;
        let end = messages
            .last()
            .map_or(after_sequence, |message| message.sequence);
        to_js_admitted(&ConversationPage {
            agent: conversation.agent,
            event_revision: self.reducer.revision(),
            total_messages,
            messages,
            next_sequence: (end < total_messages).then_some(end),
        })
    }

    /// Returns the retained context selection for an admitted operation.
    #[wasm_bindgen(js_name = contextSelectionForOperation)]
    pub fn context_selection_for_operation(&self, operation: JsValue) -> Result<JsValue, JsValue> {
        let operation = from_js(operation)?;
        to_js_admitted(&self.reducer.context_selection_for_operation(operation))
    }
}
