let state = {events: []};
export const actor = {
  restore(bytes) {state = bytes ? JSON.parse(new TextDecoder().decode(bytes)) : {events: []};},
  async event() {throw new Error('sdk deterministic poison');},
  checkpoint() {return new TextEncoder().encode(JSON.stringify(state));}
};
export default {async fetch() {return Response.json(state);}};
