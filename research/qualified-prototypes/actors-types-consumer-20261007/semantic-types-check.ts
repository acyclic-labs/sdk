import { ActorsClient, type ReadonlySemantic, semantic } from '@acyclic-labs/actors';
const cursor: semantic.SubscriptionStart = { start: { case: 'cursor', value: 0n } };
const head: semantic.SubscriptionStart = { start: { case: 'currentHead', value: true } };
// @ts-expect-error the Rust semantic contract exposes a literal true current-head marker.
const invalid: semantic.SubscriptionStart = { start: { case: 'currentHead', value: false } };
const actorId = 'a' as semantic.ActorId;
const client = new ActorsClient({ endpoint: 'http://127.0.0.1:1', token: 'fixture' });
const result: Promise<ReadonlySemantic<semantic.AddSubscriptionResponse>> = client.addSubscription({ actorId, subscription: { subscriptionId: 's', streamPath: 'events', start: cursor, placementAnchor: false }, idempotencyKey: 'i' });
async function readonlyCheck() {
  const value = await result;
  // @ts-expect-error public result is recursively readonly.
  value.actor = null;
  // @ts-expect-error nested array is readonly.
  value.actor?.subscriptions.push({});
  return { head, value };
}
void readonlyCheck();
