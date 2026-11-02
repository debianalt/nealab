<script lang="ts">
	// The live chart selection (histogram, Moran, bivariate, profile…) with the one
	// thing that turns it into something usable: save it as a zone, so it gets the
	// zone CSV / GeoJSON / comparison like a lasso.
	import type { HexStore } from '$lib/stores/hex.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';

	let {
		hexStore,
		onSave,
		onClear,
	}: {
		hexStore: HexStore;
		onSave: () => Promise<string | null>;
		onClear: () => void;
	} = $props();

	const sel = $derived(hexStore.chartSelection);
	let savedAs: string | null = $state(null);
	let saving = $state(false);
	// A new selection replaces the «saved as» note.
	$effect(() => { if (sel) savedAs = null; });

	async function save() {
		if (saving) return;
		saving = true;
		try { savedAs = await onSave(); } finally { saving = false; }
	}
</script>

{#if sel}
	<div class="csb" role="status">
		<div class="csb-text">
			<strong>{sel.h3s.length.toLocaleString(i18n.locale)}</strong> {i18n.t('zone.sel.hexes')}
			{#if sel.origin}<span class="csb-origin">· {sel.origin}</span>{/if}
		</div>
		<div class="csb-actions">
			<button class="csb-save" onclick={save} disabled={saving}>{i18n.t('zone.sel.save')}</button>
			<button class="csb-clear" onclick={onClear} title={i18n.t('zone.sel.clear')} aria-label={i18n.t('zone.sel.clear')}>×</button>
		</div>
	</div>
{:else if savedAs}
	<div class="csb csb-done" role="status">{i18n.t('zone.sel.saved').replace('{id}', savedAs)}</div>
{/if}

<style>
	.csb {
		position: sticky; top: 0; z-index: 2;
		display: flex; align-items: center; justify-content: space-between; gap: 8px;
		margin: 6px 0 8px; padding: 7px 8px;
		border-left: 3px solid var(--s-cat-1); border-radius: 3px;
		background: var(--s-surface);
		box-shadow: 0 0 0 1px rgba(255,255,255,0.06);
		font-size: 12.5px; color: var(--s-ink-1);
	}
	.csb-text { min-width: 0; line-height: 1.4; }
	.csb-origin { color: var(--s-ink-2); }
	.csb-actions { display: flex; gap: 6px; flex-shrink: 0; }
	.csb-save {
		font: inherit; font-size: 12px; padding: 4px 9px; border-radius: 4px; cursor: pointer;
		color: var(--s-ink-1); background: rgba(196, 87, 47, 0.18); border: 1px solid var(--s-cat-1);
	}
	.csb-save:hover { background: rgba(196, 87, 47, 0.3); }
	.csb-save:disabled { opacity: 0.6; cursor: wait; }
	.csb-clear {
		font: inherit; font-size: 15px; line-height: 1; padding: 2px 6px; cursor: pointer;
		color: var(--s-ink-2); background: none; border: 1px solid rgba(var(--c-fg-rgb), 0.12); border-radius: 4px;
	}
	.csb-clear:hover { color: var(--s-ink-1); }
	.csb-done { position: static; border-left-color: var(--s-ink-3); color: var(--s-ink-2); }
</style>
