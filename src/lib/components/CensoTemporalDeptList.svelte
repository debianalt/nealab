<script lang="ts">
	// Department ranking for the censo_temporal layer: EXACT official INDEC
	// totals per census year (sat_censo_temporal_dept.parquet, built by
	// pipeline/build_censo_temporal_dept.py). Companion to the hex apportionment —
	// these are the published aggregates, the hexagons remain the fine estimate.
	// Same look as the deptRank ranking of the perDepartment layers, but without
	// the province dept fill: here the territory's hexagons are already on the map.
	import ChartFrame from './ChartFrame.svelte';
	import { ensureArBoundaries, getArFeatures } from '$lib/utils/deptBoundaries';
	import { query } from '$lib/stores/duckdb';
	import { getCensoTemporalDeptUrl, TERRITORY_REGISTRY } from '$lib/config';
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
	let expandedCode = $state<string | null>(null);

	// Three readings, 2010→2022 (the pair the map's change mode shows). The
	// default answers the layer's question: where population changed the most.
	type Reading = { tabKey: string; titleKey: string; noteKey: string; unitKey: string; decimals: number; signed: boolean;
		value: (r: Row) => number | null; extra?: (r: Row) => number | null };
	const delta = (r: Row) => (r.pob[2] != null && r.pob[3] != null ? r.pob[3] - r.pob[2] : null);
	const READINGS: Reading[] = [
		{ tabKey: 'rank.censo.tab.pct', titleKey: 'rank.censo.title.pct', noteKey: 'rank.censo.note.pct', unitKey: 'rank.unit.pct', decimals: 1, signed: true,
		  value: (r) => { const d = delta(r); return d != null && r.pob[2]! > 0 ? (d / r.pob[2]!) * 100 : null; }, extra: (r) => r.pob[3] },
		{ tabKey: 'rank.censo.tab.people', titleKey: 'rank.censo.title.people', noteKey: 'rank.censo.note.people', unitKey: 'rank.unit.people', decimals: 0, signed: true,
		  value: delta },
		{ tabKey: 'rank.censo.tab.total', titleKey: 'rank.censo.title.total', noteKey: 'rank.censo.note.total', unitKey: 'rank.unit.people', decimals: 0, signed: false,
		  value: (r) => r.pob[3], extra: (r) => r.viv[3] },
	];
	const EXTRA_UNIT = ['rank.unit.people', '', 'rank.unit.dwellings'];
	let readingIdx = $state(0);
	const reading = $derived(READINGS[readingIdx]);

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

	const territoryLabel = $derived(Object.values(TERRITORY_REGISTRY).find((t) => t.parquetPrefix === hexStore.territoryPrefix)?.label ?? '');

	// No value → bottom of the list.
	const sorted = $derived(rows
		? [...rows].sort((a, b) => (reading.value(b) ?? -Infinity) - (reading.value(a) ?? -Infinity))
		: null);
	const rankMax = $derived(sorted ? Math.max(...sorted.map((r) => Math.abs(reading.value(r) ?? 0)), 1e-9) : 1);
	const selectedPos = $derived.by(() => {
		if (!sorted || !expandedCode) return null;
		const i = sorted.findIndex((r) => r.code === expandedCode);
		return i < 0 ? null : { pos: i + 1, total: sorted.length };
	});

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

	// The scope line above the charts can drop the department filter on its own:
	// unmark the row so list and map agree.
	$effect(() => {
		if (!hexStore.deptFilter && expandedCode) expandedCode = null;
	});

	function toggleRow(r: Row) {
		if (expandedCode === r.code) { expandedCode = null; onSelectDept?.(null); return; }
		expandedCode = r.code;
		onSelectDept?.(deptFeature(r.name));
	}

	function fmt(n: number): string {
		return Math.round(n).toLocaleString(i18n.locale);
	}
	const fmtRank = (v: number | null | undefined, decimals: number, signed = false) =>
		v == null ? '—' : (signed && v > 0 ? '+' : '') + v.toLocaleString(i18n.locale, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });

	function csvRows() {
		return (rows ?? []).map((r) => {
			const d = delta(r);
			return {
				departamento: r.name,
				codigo_indec: r.code,
				...Object.fromEntries(YEARS.flatMap((y, i) => [[`pob_${y}`, r.pob[i] ?? ''], [`viv_${y}`, r.viv[i] ?? '']])),
				cambio_pob_2010_2022: d ?? '',
				cambio_pob_2010_2022_pct: d != null && r.pob[2]! > 0 ? +((d / r.pob[2]!) * 100).toFixed(2) : '',
			};
		});
	}
</script>

<ChartFrame title={`${i18n.t(reading.titleKey)}${territoryLabel ? ` · ${territoryLabel}` : ''}`} csvRows={csvRows} csvFilename="spatia_censo_deptos">
	<div class="cd-panel">
		{#if error}
			<p class="cd-msg">{i18n.t('side.censoTemporal.error')}</p>
		{:else if loading || !sorted}
			<p class="cd-msg">{i18n.t('side.censoTemporal.loading')}</p>
		{:else}
			<div class="rank-tabs">
				{#each READINGS as m, i}
					<button class="rank-tab" class:active={readingIdx === i} onclick={() => (readingIdx = i)}>{i18n.t(m.tabKey)}</button>
				{/each}
			</div>

			{#if selectedPos}
				<div class="rank-selected">
					<span class="rank-selected-pos">{selectedPos.pos}.º {i18n.t('rank.of')} {selectedPos.total}</span>
				</div>
			{/if}

			<div class="cd-list">
				{#each sorted as r, i (r.code)}
					{@const v = reading.value(r)}
					{@const x = reading.extra?.(r)}
					<button class="rank-row" class:active={expandedCode === r.code} onclick={() => toggleRow(r)}>
						<span class="rank-pos">{i + 1}</span>
						<span class="rank-name">{r.name}</span>
						<span class="rank-bar-wrap">
							<span class="rank-bar" class:down={reading.signed && (v ?? 0) < 0} style:width="{(100 * Math.abs(v ?? 0)) / rankMax}%"></span>
						</span>
						<span class="rank-value">{fmtRank(v, reading.decimals, reading.signed)} {v == null ? '' : i18n.t(reading.unitKey)}</span>
						{#if reading.extra}
							<span class="rank-extra">{x == null ? '—' : fmt(x)} {i18n.t(EXTRA_UNIT[readingIdx])}</span>
						{/if}
					</button>
					{#if expandedCode === r.code}
						{#each [{ key: 'side.censoTemporal.population', series: r.pob, cls: 'pob' }, { key: 'side.censoTemporal.dwellings', series: r.viv, cls: 'viv' }] as s}
							{@const max = Math.max(...s.series.map((n) => n ?? 0), 1)}
							<div class="cd-series-label">{i18n.t(s.key)}</div>
							<div class="cd-bars">
								{#each YEARS as y, j}
									<div class="cd-bar-col">
										<span class="cd-bar-val">{s.series[j] != null ? fmt(s.series[j]!) : '—'}</span>
										<div class="cd-bar-track">
											<div class="cd-bar-fill {s.cls}" style="height: {((s.series[j] ?? 0) / max) * 100}%"></div>
										</div>
										<span class="cd-bar-year">{y}</span>
									</div>
								{/each}
							</div>
						{/each}
					{/if}
				{/each}
			</div>
			<p class="rank-note">{i18n.t(reading.noteKey)}</p>
		{/if}
	</div>
</ChartFrame>

<style>
	.cd-panel { font-size: 12.5px; line-height: 1.3; }
	.cd-msg { font-size: 12px; color: #94a3b8; padding: 10px 0; text-align: center; }
	.cd-list { display: flex; flex-direction: column; gap: 1px; max-height: 340px; overflow-y: auto; }

	/* Same look as the deptRank ranking in OvertureAnalysis (rank-*). Growth green,
	   decline red, as in the map's change mode. */
	.rank-tabs { display: flex; gap: 4px; margin: 0 0 8px; flex-wrap: wrap; }
	.rank-tab { font-size: 12px; padding: 3px 8px; border-radius: 4px; border: 1px solid rgba(148,163,184,0.3); background: none; color: #94a3b8; cursor: pointer; font-family: inherit; }
	.rank-tab.active { background: rgba(220,38,38,0.15); border-color: #dc2626; color: #fecaca; }
	.rank-selected { font-size: 12.5px; color: #cbd5e1; margin: -4px 0 8px; }
	.rank-selected-pos { font-weight: 700; color: var(--s-ink-1); }
	.rank-row { display: flex; align-items: center; gap: 5px; width: 100%; padding: 3px 2px; margin-bottom: 1px; background: none; border: none; border-radius: 4px; cursor: pointer; font-family: inherit; text-align: left; transition: background 0.15s; }
	.rank-row:hover { background: rgba(96,165,250,0.1); }
	.rank-row.active { background: rgba(255,255,255,0.09); }
	.rank-pos { font-size: 11.5px; color: #64748b; width: 14px; text-align: right; flex-shrink: 0; }
	.rank-name { font-size: 11.5px; color: #d4d4d4; width: 100px; flex-shrink: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.rank-bar-wrap { flex: 1; height: 6px; background: rgba(100,116,139,0.15); border-radius: 2px; overflow: hidden; }
	.rank-bar { display: block; height: 100%; border-radius: 2px; background: var(--s-cat-2); transition: width 0.3s; }   /* growth: río, as the map's change ramp */
	.rank-bar.down { background: var(--s-cat-1); }   /* loss: tierra */
	.rank-value { font-size: 12px; font-weight: 600; color: #e2e8f0; min-width: 40px; text-align: right; flex-shrink: 0; font-variant-numeric: tabular-nums; }
	.rank-extra { font-size: 11.5px; color: #64748b; min-width: 48px; text-align: right; flex-shrink: 0; font-variant-numeric: tabular-nums; }
	.rank-note { font-size: 11.5px; color: #94a3b8; line-height: 1.4; margin: 6px 0 0; }

	.cd-series-label { font-size: 11px; color: rgba(255,255,255,0.55); margin: 3px 6px 0; }
	.cd-bars { display: flex; align-items: flex-end; gap: 6px; height: 64px; padding: 4px 6px; margin: 1px 0 3px; background: rgba(255,255,255,0.03); border-radius: 4px; }
	.cd-bar-col { flex: 1; display: flex; flex-direction: column; align-items: center; height: 100%; justify-content: flex-end; gap: 3px; }
	.cd-bar-val { font-size: 11px; color: rgba(226,232,240,0.75); white-space: nowrap; }
	.cd-bar-track { width: 60%; flex: 1; display: flex; align-items: flex-end; }
	.cd-bar-fill { width: 100%; background: var(--s-cat-2); border-radius: 2px 2px 0 0; min-height: 1px; }
	.cd-bar-fill.viv { background: var(--s-cat-3); }
	.cd-bar-year { font-size: 11px; color: rgba(255,255,255,0.55); }
</style>
