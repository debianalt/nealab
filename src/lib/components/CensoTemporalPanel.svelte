<script lang="ts">
	// Isolated panel for the `censo_temporal` layer: on hexagon selection, queries
	// the per-year population/housing totals for the selected cells and renders the
	// 1991→2022 trajectory. Fully self-contained — it does NOT touch the shared hex
	// load path; it runs its own DuckDB query against the same (cached) parquet.
	import { query } from '$lib/stores/duckdb';
	import { getSatGlobalUrl } from '$lib/config';
	import { i18n } from '$lib/stores/i18n.svelte';
	import type { HexStore } from '$lib/stores/hex.svelte';

	let { hexStore }: { hexStore: HexStore } = $props();

	const YEARS = ['1991', '2001', '2010', '2022'] as const;

	let pob = $state<number[] | null>(null);
	let viv = $state<number[] | null>(null);
	let loading = $state(false);
	let error = $state(false);
	let metric = $state<'pob' | 'viv'>('pob');

	// Plain var (not $state): the selection signature already loaded — guards the
	// $effect against re-querying on unrelated store changes. Mirrors RadioCensusPanel.
	let loadedKey: string | null = null;

	$effect(() => {
		const keys = [...hexStore.selectedHexes.keys()].sort();
		const sig = `${hexStore.territoryPrefix}|${keys.join(',')}`;
		if (!keys.length) { pob = null; viv = null; loadedKey = null; return; }
		if (sig === loadedKey) return;
		loadedKey = sig;
		loading = true; error = false; pob = null; viv = null;

		const url = getSatGlobalUrl('censo_temporal', hexStore.territoryPrefix);
		const inList = keys.map((k) => `'${k}'`).join(',');
		const cols = YEARS.flatMap((y) => [`SUM(pob_cnt_${y}) AS p${y}`, `SUM(viv_cnt_${y}) AS v${y}`]).join(', ');
		query(`SELECT ${cols} FROM '${url}' WHERE h3index IN (${inList})`)
			.then((t) => {
				if (sig !== loadedKey) return;
				pob = YEARS.map((y) => Number(t.getChild(`p${y}`)?.get(0) ?? 0));
				viv = YEARS.map((y) => Number(t.getChild(`v${y}`)?.get(0) ?? 0));
				loading = false;
			})
			.catch(() => { if (sig === loadedKey) { error = true; loading = false; } });
	});

	const series = $derived(metric === 'pob' ? pob : viv);
	const maxV = $derived(series ? Math.max(...series, 1) : 1);
	const total22 = $derived(series ? series[series.length - 1] : 0);
	const hasData = $derived(!!series && series.some((v) => v > 0));
	const pct = $derived(
		series && series[0] > 0 ? ((series[3] - series[0]) / series[0]) * 100 : null
	);

	function fmt(n: number): string {
		return Math.round(n).toLocaleString('es-AR');
	}
</script>

<div class="ct-root">
	<div class="ct-header">
		<span class="ct-title">{i18n.t('side.censoTemporal.title')}</span>
		<span class="ct-count">{hexStore.selectedHexes.size} hex</span>
	</div>
	<p class="ct-subtitle">{i18n.t('side.censoTemporal.subtitle')}</p>

	{#if error}
		<p class="ct-msg">{i18n.t('side.censoTemporal.error')}</p>
	{:else if loading}
		<p class="ct-msg">{i18n.t('side.censoTemporal.loading')}</p>
	{:else if !hasData}
		<p class="ct-msg">{i18n.t('side.censoTemporal.empty')}</p>
	{:else if series}
		<div class="ct-toggle">
			<button class="ct-tab" class:active={metric === 'pob'} onclick={() => (metric = 'pob')}>
				{i18n.t('side.censoTemporal.population')}
			</button>
			<button class="ct-tab" class:active={metric === 'viv'} onclick={() => (metric = 'viv')}>
				{i18n.t('side.censoTemporal.dwellings')}
			</button>
		</div>

		<div class="ct-bars">
			{#each YEARS as y, i}
				<div class="ct-bar-col">
					<span class="ct-bar-val">{fmt(series[i])}</span>
					<div class="ct-bar-track">
						<div class="ct-bar-fill" style="height: {(series[i] / maxV) * 100}%"></div>
					</div>
					<span class="ct-bar-year">{y}</span>
				</div>
			{/each}
		</div>

		{#if pct !== null}
			<div class="ct-change">
				<span class="ct-change-label">{i18n.t('side.censoTemporal.change')}</span>
				<span class="ct-change-val" class:up={pct >= 0} class:down={pct < 0}>
					{pct >= 0 ? '+' : ''}{pct.toFixed(0)}%
				</span>
			</div>
		{/if}
	{/if}
</div>

<style>
	.ct-root { font-size: 11px; line-height: 1.3; padding-top: 8px; margin-top: 8px; border-top: 1px solid rgba(255,255,255,0.07); }
	.ct-header { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 2px; }
	.ct-title { font-size: 10px; font-weight: 600; color: #e2e8f0; }
	.ct-count { font-size: 8px; color: rgba(255,255,255,0.40); }
	.ct-subtitle { font-size: 9px; color: rgba(255,255,255,0.45); margin: 0 0 8px; }
	.ct-msg { font-size: 10px; color: #94a3b8; padding: 10px 0; text-align: center; }
	.ct-toggle { display: flex; gap: 2px; background: rgba(255,255,255,0.04); border-radius: 6px; padding: 2px; margin-bottom: 10px; }
	.ct-tab { flex: 1; padding: 3px 6px; background: none; border: none; border-radius: 4px; color: #737373; font-size: 9px; font-weight: 600; cursor: pointer; transition: all 0.15s; font-family: inherit; }
	.ct-tab:hover { color: #a3a3a3; }
	.ct-tab.active { background: rgba(255,255,255,0.10); color: #e2e8f0; }
	.ct-bars { display: flex; align-items: flex-end; gap: 6px; height: 88px; }
	.ct-bar-col { flex: 1; display: flex; flex-direction: column; align-items: center; height: 100%; justify-content: flex-end; gap: 3px; }
	.ct-bar-val { font-size: 8px; color: rgba(226,232,240,0.75); white-space: nowrap; }
	.ct-bar-track { width: 60%; flex: 1; display: flex; align-items: flex-end; }
	.ct-bar-fill { width: 100%; background: linear-gradient(to top, #2563eb, #60a5fa); border-radius: 2px 2px 0 0; min-height: 1px; }
	.ct-bar-year { font-size: 8px; color: rgba(255,255,255,0.45); }
	.ct-change { display: flex; justify-content: space-between; align-items: center; margin-top: 10px; padding-top: 6px; border-top: 1px solid rgba(255,255,255,0.06); }
	.ct-change-label { font-size: 9px; color: #a3a3a3; }
	.ct-change-val { font-size: 11px; font-weight: 700; }
	.ct-change-val.up { color: #4ade80; }
	.ct-change-val.down { color: #f87171; }
</style>
