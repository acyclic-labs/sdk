---
name: attention
description: Delegate open ui/ asks to a separate triager without adding them to a worker's context.
---

Use a fresh triager role with a self-contained brief containing the scope, exact ask-file references and the effective policy/attention.md. Keep the worker's conversation out of its input. Use ordinary spawn and send; the triager owns its private ui/queue.md and policy/attention-signal.md. Give it only the authority needed for the selected asks.

The queue is an authored ranking of links to live asks, not a stored pending set. Resolve each ask against its original file generation before replying. Replies use one ordinary prompt carrying {ui,generation,answers,text}; approval decisions retain their trusted action binding and go through Decide. Neither markdown nor a recommendation grants permission.

Ask the triager to answer what policy permits, combine related questions into one higher-level ui/ ask, or escalate one merged summary when the parent worker must act. Include enough information in each delivered answer for the recipient to continue without pulling the parent's conversation. Record learned preferences in the signal file only when supported by the human's actual replies or overrides. Never inject queue contents, status or the signal into the worker's turns.
