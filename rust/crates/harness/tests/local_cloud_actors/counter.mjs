let state = {counter: 0, version: 1, events: []};
export const actor = {
  restore(bytes) {state = bytes ? JSON.parse(new TextDecoder().decode(bytes)) : {counter: 0, version: 1, events: []};},
  async event(event, context) {
    state.events.push(`v${state.version}:${event.sequence}:${atob(event.value)}`);
    context.markDirty();
  },
  checkpoint() {return new TextEncoder().encode(JSON.stringify(state));}
};
export default {async fetch(request, context) {
  if (request.method === 'POST') {
    const {add} = await request.json();
    state.counter += add;
    context.markDirty();
  }
  return Response.json(state);
}};
