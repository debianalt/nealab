<script lang="ts">
	// Saved zones outside the open department (8-nov-2026): one row per department with
	// its letters, «ir» opens it in the active layer and frames them, ✕ removes them.
	// The zones live in hexStore.savedZones (sessionStorage) until the user removes them.
	import type { HexStore } from '$lib/stores/hex.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { formatDept } from '$lib/utils/format';
	import { territoryRef } from '$lib/utils/territoryRef';
	import { TERRITORY_REGISTRY } from '$lib/config';

	let {
		hexStore,
		onGo,
		onClear,
		coverage,
	}: {
		hexStore: HexStore;
		onGo: (key: string) => void;
		onClear: (key: string) => void;
		/** The active analysis' coverage: no «ir» to a province where the layer does not exist. */
		coverage?: Record<string, string>;
	} = $props();

	const groups = $derived(hexStore.otherZoneGroups);
	const tidOf = (prefix: string) => Object.values(TERRITORY_REGISTRY).find(t => t.parquetPrefix === prefix)?.id ?? '';
	// «ir» only where the active layer exists and the zone has a department to open.
	function canGo(prefix: string, name: string): boolean {
		const tid = tidOf(prefix);
		return !!hexStore.activeLayer && !!name && !!tid && coverage?.[tid] !== 'unavailable';
	}
	function label(prefix: string, name: string): string {
		const tid = tidOf(prefix);
		const where = name ? formatDept(name) : i18n.t('zone.saved.whole');
		// Another province: say which.
		return tid && prefix !== hexStore.territoryPrefix ? `${where} (${territoryRef(tid, i18n.locale)})` : where;
	}
</script>

{#if groups.length > 0}
	<div class="szl">
		<div class="szl-title">{i18n.t('zone.saved.others')}</div>
		{#each groups as g (g.key)}
			<div class="szl-row">
				<span class="szl-name">{label(g.prefix, g.name)}</span>
				<span class="szl-ids">
					{#each g.zones as z (z.id)}<span class="szl-id" style:color={z.color} title={z.name ?? ''}>{z.id}{#if z.name}<span class="szl-zname">{z.name}</span>{/if}</span>{/each}
				</span>
				{#if canGo(g.prefix, g.name)}
					<button class="szl-go" onclick={() => onGo(g.key)} title={i18n.t('zone.saved.goTitle')}>{i18n.t('zone.saved.go')} →</button>
				{/if}
				<button class="szl-x" onclick={() => onClear(g.key)} title={i18n.t('zone.saved.clearTitle')} aria-label={i18n.t('zone.saved.clearTitle')}>&#10005;</button>
			</div>
		{/each}
		<!-- With zones of this department open, their own note already says it. -->
		{#if hexStore.hexZones.length === 0}<p class="szl-note">{i18n.t('zone.saved.note')}</p>{/if}
	</div>
{/if}

<style>
	.szl { font-size: 12.5px; margin: 4px 0 10px; padding-bottom: 6px; border-bottom: 1px solid var(--c-line-2); }
	.szl-title { font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; color: var(--c-ink-3); margin-bottom: 4px; }
	.szl-row { display: flex; align-items: center; gap: 8px; padding: 3px 0; border-top: 1px solid var(--c-line); }
	.szl-name { flex: 1; color: var(--c-ink-2); min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.szl-ids { display: flex; gap: 6px; font-weight: 700; min-width: 0; overflow: hidden; }
	.szl-id { white-space: nowrap; min-width: 0; overflow: hidden; text-overflow: ellipsis; max-width: 140px; }
	.szl-zname { font-weight: 400; color: var(--c-ink-2); margin-left: 3px; }
	.szl-go, .szl-x {
		background: none; border: 1px solid var(--c-line-2); border-radius: 4px;
		color: var(--c-ink-3); font-size: 11.5px; padding: 1px 6px; cursor: pointer;
	}
	.szl-go:hover { border-color: var(--c-accent); color: var(--c-accent); }
	.szl-x:hover { border-color: #ef4444; color: #ef4444; }
	.szl-note { font-size: 11.5px; color: var(--c-ink-4); margin: 6px 0 0; line-height: 1.35; }
</style>
