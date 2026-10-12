<script lang="ts">
  import { clientReadable } from "@acyclic-labs/harness/svelte";
  let { store, failedStore, conflictStore } = $props();
  const view = clientReadable(store);
  const failed = clientReadable(failedStore);
  const conflict = clientReadable(conflictStore);
</script>
<input value="draft" aria-label="draft" />
<p data-svelte>{$view.value.count}:{$view.branch === null ? 'authoritative' : 'hypothesis'}</p>
<p data-svelte-outcome role="status" aria-live="polite" aria-atomic="true">{$view.hypotheses.map(status => `${status.prediction}:${status.outcome}`).join(',')}</p>
<p data-failed role="status" aria-live="polite" aria-atomic="true">{$failed.hypotheses.map(status => `${status.prediction}:${status.outcome}`).join(',')}</p>
<p data-conflict role="status" aria-live="polite" aria-atomic="true">{$conflict.hypotheses.map(status => `${status.prediction}:${status.outcome}`).join(',')}</p>
<ol data-scroll style="height:80px;overflow-y:scroll;margin:0">
  {#each Array.from({ length: 20 }, (_, i) => i) as i (i)}
    <li data-anchor={i} style="height:24px">{i}:{$view.value.count}</li>
  {/each}
</ol>
