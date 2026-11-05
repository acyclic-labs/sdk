import { ActorsClient, ActorId } from "@acyclic-labs/actors";

const actors = new ActorsClient({
  endpoint: process.env.ACTORS_ENDPOINT!, token: process.env.ACYCLIC_API_KEY!,
});
console.log(await actors.inspectActor({
  actorId: await ActorId(process.env.ACTOR_ID!),
}));
