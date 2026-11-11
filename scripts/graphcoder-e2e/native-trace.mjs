import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {join} from 'node:path';
import {createHash} from 'node:crypto';

export function verifyNativeTrace(workspace, expected) {
      const lines = path => readFileSync(join(workspace, path), "utf8").trim().split(/\r?\n/).filter(Boolean).map(JSON.parse);
      const agents = lines(join(expected.directory, "agents.jsonl"));
      assert.equal(agents.length, expected.agents, "trace agent count");
      const ids = new Set(agents.map(agent => agent.id));
      assert.equal(ids.size, agents.length, "trace repeats an agent identity");
      assert.equal(agents.filter(agent => agent.parent === null).length, 1, "trace root count");
      for (const agent of agents) {
        assert.equal(agent.version, 1);
        assert.deepEqual(agent.topology, expected.topology);
        assert.equal(agent.role, agent.parent === null ? (expected.root_role ?? "orchestrator") : "worker");
        assert.equal(typeof agent.model, "string");
        if (agent.parent !== null) assert.ok(ids.has(agent.parent), "trace parent is absent");
      }
      const depths = new Map(), widths = new Map();
      for (const agent of agents) {
        const depth = agent.parent === null ? 0 : depths.get(agent.parent) + 1;
        assert.ok(Number.isSafeInteger(depth), 'trace discovers a child before its parent');
        depths.set(agent.id, depth);
        if (agent.parent !== null) widths.set(agent.parent, (widths.get(agent.parent) ?? 0) + 1);
      }
      let width = 0, depth = 0;
      for (const value of widths.values()) width = Math.max(width, value);
      for (const value of depths.values()) depth = Math.max(depth, value);
      if (expected.width !== undefined) assert.equal(width, expected.width, 'trace width');
      if (expected.depth !== undefined) assert.equal(depth, expected.depth, 'trace depth');
      const missing = new Set(expected.assistant_text ?? []);
      for (const agent of agents) {
        if (!missing.size) break;
        const payloads = new Map();
        for (const row of lines(join(expected.directory, agent.id + '.jsonl'))) {
          if (row.kind !== 'execution_payload' || row.field !== 'event') continue;
          assert.equal(row.agent, agent.agent_path, 'native payload belongs to another agent');
          const key = JSON.stringify([row.input, row.turn, row.sequence]);
          const payload = payloads.get(key) ?? {chunks: [], bytes: 0};
          assert.equal(row.offset, payload.bytes, 'native event payload loses a chunk');
          const bytes = Buffer.from(row.data, 'base64');
          payload.chunks.push(bytes); payload.bytes += bytes.length;
          if (payload.bytes === row.file.descriptor.byte_length) {
            const bytes = Buffer.concat(payload.chunks);
            assert.deepEqual([...createHash('sha256').update(bytes).digest()], row.file.descriptor.sha256, 'native event payload digest');
            const event = JSON.parse(bytes);
            if (event.kind === 'message_completed' || event.kind === 'text_part') missing.delete(event.text);
            payloads.delete(key);
          } else {
            assert.ok(payload.bytes < row.file.descriptor.byte_length, 'native event payload exceeds its declared size');
            payloads.set(key, payload);
          }
        }
        assert.equal(payloads.size, 0, 'native event payload is incomplete');
      }
      assert.equal(missing.size, 0, 'native assistant messages missing: ' + [...missing].join(', '));
      const usage = lines(expected.usage);
      const byAgent = new Map();
      for (const row of usage) {
        if (!row.usage.completed) continue;
        const calls = byAgent.get(row.usage.agent_path) ?? [];
        calls.push(row); byAgent.set(row.usage.agent_path, calls);
      }
      for (const agent of agents) {
        const calls = byAgent.get(agent.agent_path) ?? [];
        assert.ok(calls.length, "trace has no completed call for an agent");
        for (const row of calls) {
          assert.ok(Number.isSafeInteger(row.usage.tokens.input));
          assert.ok(Number.isSafeInteger(row.usage.tokens.output));
          assert.ok(Object.hasOwn(row.usage.tokens, "cached"));
          assert.ok(Object.hasOwn(row, "estimated_cost"), "trace lost cost observation");
          if (expected.price_revision) {
            assert.equal(row.estimated_cost.price_revision, expected.price_revision);
            assert.equal(row.estimated_cost.currency, "USD");
            assert.ok(Number.isSafeInteger(row.estimated_cost.token_nano_units), "trace lost selected token cost");
          }
        }
      }
}
