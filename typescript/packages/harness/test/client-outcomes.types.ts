import type { AgentClient, AgentEvent, AgentFileRef, Phase, StatusRecord, TurnInput, UnknownAgentError } from "../src/client.js";

type Conversation = Extract<AgentEvent, { kind: "conversation" }>;
type Message = Extract<TurnInput, { message: unknown }>["message"];

const sequence: Conversation["sequence"] = 9_007_199_254_740_993n;
const inbox: Message["inbox"] = sequence;
const approval: Phase = { waiting_approval: { id: "approval-01010101010101010101010101010101" } };
const status: StatusRecord = { phase: approval, cursor: inbox };

declare const uncertain: UnknownAgentError;
const originalRequest: Uint8Array = uncertain.request;
const unknownCode: "agent_unknown" = uncertain.name;
// @ts-expect-error an indeterminate control retains bytes, not a string message
const erasedRequest: string = uncertain.request;

// @ts-expect-error full-width conversation sequences cannot become Number
const roundedSequence: Conversation["sequence"] = 1;
// @ts-expect-error input correlation is the original exact inbox receipt
const roundedInbox: Message["inbox"] = 1;
// @ts-expect-error an approval phase must identify the actual request
const missingApproval: Phase = { waiting_approval: {} };
// @ts-expect-error the exact serializer keeps file lengths as BigInt too
const roundedLength: AgentFileRef["descriptor"]["byte_length"] = 1;

const project = (client: AgentClient, agent: string): Conversation | null => {
  const event = client.agentRecord(agent, 0);
  return event?.kind === "conversation" ? event : null;
};
void [status, roundedSequence, roundedInbox, missingApproval, roundedLength, project,
  originalRequest, unknownCode, erasedRequest];
