<script lang="ts">
  import { clientReadable } from "@acyclic-labs/harness/svelte";
  let { store } = $props();
  const view = clientReadable(store);
</script>
<p data-svelte>{$view.value.count}:{$view.branch === null ? 'authoritative' : 'hypothesis'}</p>
<p data-svelte-outcome role="status">{$view.hypotheses.map(status => status.outcome).join(',')}</p>
