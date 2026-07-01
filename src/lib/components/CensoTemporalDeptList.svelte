<script lang="ts">
	// Department navigator for the censo_temporal layer: EXACT official INDEC
	// totals per census year (sat_censo_temporal_dept.parquet, built by
	// pipeline/build_censo_temporal_dept.py). Companion to the hex apportionment —
	// these are the published aggregates, the hexagons remain the fine estimate.
	import ChartFrame from './ChartFrame.svelte';
	import { ensureArBoundaries, getArFeatures } from '$lib/utils/deptBoundaries';
	import { query } from '$lib/stores/duckdb';
	import { getCensoTemporalDeptUrl } from '$lib/config';
	import { i18n } from '$lib/stores/i18n.svelte';
	import type { HexStore } from '$lib/stores/hex.svelte';

	let { hexStore, onSelectDept }: {
		hexStore: HexStore;
		onSelectDept?: (feature: any | null) => void;
	} = $props();

	const YEARS = ['1991', '2001', '2010', '2022'] as const;
	const PROV_BY_PREFIX: Record<string, string> = { '': '54', 'corrientes/': '18', 'chaco/': '22', 'formosa/': '34' };

	type Row = { code: string; name: string; pob: (number | null)[]; viv: (number | null)[] };

	let rows = $state<Row[] | null>(null);
	let loading = $state(false);
	let error = $state(false);
	let metric = $state<'pob' | 'viv'>('pob');
	let expandedCode = $state<string | null>(null);

	let loadedPrefix: string | null = null;
	$effect(() => {
		const tp = hexStore.territoryPrefix;
		if (tp === loadedPrefix) return;
		loadedPrefix = tp;
		loading = true; error = false; rows = null; expandedCode = null;
		onSelectDept?.(null);
		query(`SELECT * FROM '${getCensoTemporalDeptUrl(tp)}'`)
			.then((t) => {
				if (tp !== loadedPrefix) return;
				const out: Row[] = [];
				for (let i = 0; i < t.numRows; i++) {
					const get = (c: string) => { const v = t.getChild(c)?.get(i); return v == null ? null : Number(v); };
					out.push({
						code: String(t.getChild('dept_code')?.get(i) ?? ''),
						name: String(t.getChild('dept_name')?.get(i) ?? ''),
						pob: YEARS.map((y) => get(`pob_${y}`)),
						viv: YEARS.map((y) => get(`viv_${y}`)),
					});
				}
				rows = out; loading = false;
			})
			.catch(() => { if (tp === loadedPrefix) { error = true; loading = false; } });
	});

	// Clear the dept outline on the map when the panel unmounts (layer change).
	// Warm the AR dept polygons (deferred out of the bundle) so a dept-row click's
	// synchronous deptFeature() lookup has them ready. Runs once on mount.
	$effect(() => { ensureArBoundaries().catch(() => {}); });

	$effect(() => () => onSelectDept?.(null));

	const sorted = $derived(rows ? [...rows].sort((a, b) => (b[metric][3] ?? 0) - (a[metric][3] ?? 0)) : null);

	function pct(r: Row): number | null {
		const s = r[metric];
		const a = s[0], b = s[3];
		return a != null && a > 0 && b != null ? ((b - a) / a) * 100 : null;
	}

	// Boundary lookup by NAME: the redcodes in ar_dept_boundaries.json are not
	// INDEC codes for Chaco/Formosa (sequential 22001…), so code joins mismatch.
	// Accent/punctuation-insensitive fallback covers spelling drift (Laishí/Laishi).
	function norm(s: string): string {
		return s.normalize('NFD').replace(/\p{M}/gu, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
	}
	function deptFeature(name: string): any | null {
		const prov = PROV_BY_PREFIX[hexStore.territoryPrefix];
		if (!prov) return null;
		const feats = (getArFeatures() ?? []).filter((f: any) => String(f.properties.redcode).startsWith(prov));
		return feats.find((f: any) => f.properties.nombre === name)
			?? feats.find((f: any) => norm(f.properties.nombre) === norm(name))
			?? null;
	}

	function toggleRow(r: Row) {
		if (expandedCode === r.code) { expandedCode = null; onSelectDept?.(null); return; }
		expandedCode = r.code;
		onSelectDept?.(deptFeature(r.name));
	}

	function fmt(n: number): string {
		return Math.round(n).toLocaleString('es-AR');
	}

	function csvRows() {
		return (rows ?? []).map((r) => ({
			departamento: r.name,
			codigo_indec: r.code,
			...Object.fromEntries(YEARS.flatMap((y, i) => [[`pob_${y}`, r.pob[i] ?? ''], [`viv_${y}`, r.viv[i] ?? '']])),
		}));
	}
</script>

<ChartFrame title={i18n.t('side.censoDept.title')} csvRows={csvRows} csvFilename="spatia_censo_deptos">
	<div class="cd-panel">
		<p class="cd-subtitle">{i18n.t('side.censoDept.subtitle')}</p>

		{#if error}
			<p class="cd-msg">{i18n.t('side.censoTemporal.error')}</p>
		{:else if loading || !sorted}
			<p class="cd-msg">{i18n.t('side.censoTemporal.loading')}</p>
		{:else}
			<div class="cd-toggle">
				<button class="cd-tab" class:active={metric === 'pob'} onclick={() => (metric = 'pob')}>
					{i18n.t('side.censoTemporal.population')}
				</button>
				<button class="cd-tab" class:active={metric === 'viv'} onclick={() => (metric = 'viv')}>
					{i18n.t('side.censoTemporal.dwellings')}
				</button>
			</div>

			<div class="cd-list">
				{#each sorted as r (r.code)}
					{@const p = pct(r)}
					{@const series = r[metric]}
					{@const max = Math.max(...series.map((v) => v ?? 0), 1)}
					<button class="cd-row" class:active={expandedCode === r.code} onclick={() => toggleRow(r)}>
						<span class="cd-name">{r.name}</span>
						<span class="cd-val">{series[3] != null ? fmt(series[3]) : '—'}</span>
						<span class="cd-pct" class:up={p !== null && p >= 0} class:down={p !== null && p < 0}>
							{p === null ? '—' : `${p >= 0 ? '+' : ''}${p.toFixed(0)}%`}
						</span>
					</button>
					{#if expandedCode === r.code}
						<div class="cd-bars">
							{#each YEARS as y, i}
								<div class="cd-bar-col">
									<span class="cd-bar-val">{series[i] != null ? fmt(series[i]!) : '—'}</span>
									<div class="cd-bar-track">
										<div class="cd-bar-fill" style="height: {((series[i] ?? 0) / max) * 100}%"></div>
									</div>
									<span class="cd-bar-year">{y}</span>
								</div>
							{/each}
						</div>
					{/if}
				{/each}
			</div>
		{/if}
	</div>
</ChartFrame>

<style>
	.cd-panel { font-size: 11px; line-height: 1.3; }
	.cd-subtitle { font-size: 9px; color: rgba(255,255,255,0.45); margin: 0 0 8px; }
	.cd-msg { font-size: 10px; color: #94a3b8; padding: 10px 0; text-align: center; }
	.cd-toggle { display: flex; gap: 2px; background: rgba(255,255,255,0.04); border-radius: 6px; padding: 2px; margin-bottom: 8px; }
	.cd-tab { flex: 1; padding: 3px 6px; background: none; border: none; border-radius: 4px; color: #737373; font-size: 9px; font-weight: 600; cursor: pointer; transition: all 0.15s; font-family: inherit; }
	.cd-tab:hover { color: #a3a3a3; }
	.cd-tab.active { background: rgba(255,255,255,0.10); color: #e2e8f0; }
	.cd-list { display: flex; flex-direction: column; gap: 1px; max-height: 320px; overflow-y: auto; }
	.cd-row { display: flex; align-items: baseline; gap: 8px; width: 100%; padding: 4px 6px; background: none; border: none; border-radius: 4px; cursor: pointer; font-family: inherit; text-align: left; transition: background 0.12s; }
	.cd-row:hover { background: rgba(255,255,255,0.05); }
	.cd-row.active { background: rgba(255,255,255,0.09); }
	.cd-name { flex: 1; font-size: 10px; color: #cbd5e1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
	.cd-val { font-size: 10px; color: rgba(226,232,240,0.85); font-variant-numeric: tabular-nums; }
	.cd-pct { width: 42px; text-align: right; font-size: 9px; font-weight: 700; color: #737373; font-variant-numeric: tabular-nums; }
	.cd-pct.up { color: #4ade80; }
	.cd-pct.down { color: #f87171; }
	.cd-bars { display: flex; align-items: flex-end; gap: 6px; height: 76px; padding: 6px 6px 4px; margin: 1px 0 4px; background: rgba(255,255,255,0.03); border-radius: 4px; }
	.cd-bar-col { flex: 1; display: flex; flex-direction: column; align-items: center; height: 100%; justify-content: flex-end; gap: 3px; }
	.cd-bar-val { font-size: 8px; color: rgba(226,232,240,0.75); white-space: nowrap; }
	.cd-bar-track { width: 60%; flex: 1; display: flex; align-items: flex-end; }
	.cd-bar-fill { width: 100%; background: linear-gradient(to top, #2563eb, #60a5fa); border-radius: 2px 2px 0 0; min-height: 1px; }
	.cd-bar-year { font-size: 8px; color: rgba(255,255,255,0.45); }
</style>
