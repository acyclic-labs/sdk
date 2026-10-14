import type { SwarmDetail } from "../src/index.js";
import response from "./fixtures/detail-role-spend.json" with { type: "json" };

const detail: Pick<SwarmDetail, "swarm_id" | "spend_by_role"> = response;
const root = detail.spend_by_role?.root;
if (root) {
  const cost: number = root.inference_usd;
  const agents: number = root.agents;
  void cost;
  void agents;
  // @ts-expect-error Cloud flattens AgentSpend into each role; no nested spend object exists.
  root.spend;
}
