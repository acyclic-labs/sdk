import type { ClientViews, CommandOutputView, FileKey, OutputOwner } from "../src/client.js";

const owner: OutputOwner = { Task: "task" };
const outcome: CommandOutputView = {
  sequence: 9_007_199_254_740_993n,
  completion: "completed",
  text: "answer",
  error: null,
  task: null,
};
const project = (views: ClientViews): [FileKey, CommandOutputView][] =>
  views.outcomes("swarm/a", owner, 0n);
// @ts-expect-error full-width sequences must retain bigint identity
const rounded: CommandOutputView = { ...outcome, sequence: 1 };
void project;
void rounded;
