import { piProvider, type PiProjectedMessage } from "../src/index.js";

type Assert<Condition extends true> = Condition;
type UserPart = Extract<PiProjectedMessage, { role: "system" | "user" }>["content"][number];
type AssistantPart = Extract<PiProjectedMessage, { role: "assistant" }>["content"][number];
type ToolPart = Extract<PiProjectedMessage, { role: "tool" }>["content"][number];

type UserRejectsToolCalls = Assert<Extract<UserPart, { type: "tool_call" }> extends never ? true : false>;
type AssistantRejectsToolResults = Assert<Extract<AssistantPart, { type: "tool_result" }> extends never ? true : false>;
type ToolRejectsToolCalls = Assert<Extract<ToolPart, { type: "tool_call" }> extends never ? true : false>;
type AssistantAcceptsToolCalls = Assert<Extract<AssistantPart, { type: "tool_call" }> extends never ? false : true>;
type ToolAcceptsToolResults = Assert<Extract<ToolPart, { type: "tool_result" }> extends never ? false : true>;

const customEvents = piProvider<{ model: string }, { kind: "done" }, string>({
  project: () => ({ model: "custom" }),
  run: async function* (request) {
    if (request.model !== "custom") throw new Error("unexpected model");
    yield { kind: "done" as const };
  },
  event: (event) => ({ type: "complete", metadata: event.kind }),
  reconcile: async () => [{ kind: "done" }],
});
void customEvents;
