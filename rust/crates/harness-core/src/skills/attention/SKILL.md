---
name: attention
description: Delegate open ui/ asks to a separate triager without adding them to a worker's context.
---

Use a fresh triager role with a self-contained brief containing the scope, exact ask-file references and the effective policy/attention.md. Keep the worker's conversation out of its input. Use ordinary spawn and send; the triager owns its private ui/queue.md and policy/attention-signal.md. Give it only the authority needed for the selected asks.

The queue is an authored ranking of links to live asks, not a stored pending set. Resolve each ask against its original file generation before replying. Replies use one ordinary prompt carrying {ui,generation,answers,text}; approval decisions retain their trusted action binding and go through Decide. Neither markdown nor a recommendation grants permission.

Ask the triager to answer what policy permits, combine related questions into one higher-level ui/ ask, or escalate one merged summary when the parent worker must act. Include enough information in each delivered answer for the recipient to continue without pulling the parent's conversation. Record learned preferences in the signal file only when supported by the human's actual replies or overrides. Never inject queue contents, status or the signal into the worker's turns.

For an explicitly authorised recommendation deadline, publish the ask in ui/index.md (or link it from there) and call wait with {input:true,timeout_ms:T}; agents may be omitted. This parks on the actor's alarm and resumes earlier on inbox input. On resumption, inspect the delivered reply before acting; an alarm grants no additional authority. Without a deadline, use {input:true}. Keep the ask's generation and recommendation pinned across the wait.

While an ask is open, speculate only within the user's existing authority and reservations. Give each likely answer an ordinary worker fork with the ask's exact generation, that answer as a conditional assumption, and a self-contained task. Keep the branches isolated; a recommended= value expresses a preference, not permission. Wait with input:true when a human reply can select a branch. Match the reply to the pinned generation, inspect and verify the selected branch, and integrate only that branch. Archive the other children with archive. If the ask changes, discard the old assumptions before selecting or integrating anything. External actions still require their ordinary approvals.

For Explain, open an ordinary Fork at the settled conversation turn associated with the ask's cut, selecting the explanation role. Include the exact ask file generation and the human's question in its brief. That side-conversation inherits the pinned conversation and project generations, never the worker's later head. Send follow-up questions to the sidecar; it answers there without answering the original ask, resuming the worker, or merging files. Return to the original ask to deliver a decision through its normal reply binding. Archive the sidecar when the conversation is finished. If the ask's cut is unavailable, report that rather than substituting the current head.
