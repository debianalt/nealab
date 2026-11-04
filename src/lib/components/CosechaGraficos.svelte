<script lang="ts">
	// Charts of «¿Dónde hay pino y eucalipto para cortar?» (macizo layer, no per-hex data): harvest calendar,
	// planting year and satellite felling year, for the open department or the whole province. They read the
	// `calendario` / `plantacion` / `talado` blocks the summaries carry (pipeline/forestal/exportar_cosecha_web.py);
	// a picked column becomes hexStore.macizoChartFocus, the same tile filter the map paints with.
	import { untrack, onDestroy } from 'svelte';
	import MacizoBarras from './MacizoBarras.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { loadDeptSummary } from '$lib/utils/deptSummaries';
	import { TERRITORY_REGISTRY, type MacizoFocus } from '$lib/config';
	import { formatDept } from '$lib/utils/format';
	import type { HexStore, DeptFill } from '$lib/stores/hex.svelte';

	type Gen = 'pino' | 'euca' | 'otras';
	type Serie = { rotulos: string[]; ha: Record<Gen, number[]> };
	type Chart = 'calendario' | 'plantacion' | 'talado' | 'hoy';
	const GENERO: Record<Gen, string> = { pino: 'Pinus', euca: 'Eucalyptus', otras: 'otras' };

	let { hexStore }: { hexStore: HexStore } = $props();

	let summary = $state<any>(null);
	$effect(() => {
		const layer = hexStore.activeLayer;
		const prefix = hexStore.territoryPrefix;
		summary = null;
		if (!layer?.macizos) return;
		loadDeptSummary(layer.id, prefix).then((s) => { summary = s; });
	});

	const scope = $derived.by(() => {
		if (!summary) return null;
		const key = hexStore.selectedParquetKey;
		const dep = key ? summary.departments?.find((d: any) => d.parquetKey === key) : null;
		if (dep?.calendario) return { name: formatDept(dep.dpto), b: dep };
		if (!key && summary.province?.calendario) return { name: TERRITORY_REGISTRY[hexStore.territoryId]?.label ?? '', b: summary.province };
		return null;
	});

	// The pick belongs to one scope: a new department (or the province) starts clean.
	let picked = $state<{ chart: Chart; col: number; gen: Gen } | null>(null);
	$effect(() => {
		void scope?.name;
		untrack(() => { picked = null; hexStore.setMacizoChartFocus(null); });
	});
	// Cleared from outside (tab change, Esc): drop the highlighted segment too.
	onDestroy(() => { hexStore.setMacizoChartFocus(null); hexStore.setDeptFillPick(null); });
	$effect(() => { if (!hexStore.macizoChartFocus) { picked = null; untrack(() => hexStore.setDeptFillPick(null)); } });

	const cal = $derived<Serie | null>(scope?.b.calendario ?? null);
	// «Hoy» is a stock (everything at or past rotation), the other columns are yearly flows: on one axis the
	// stock flattens the years. The bars show the flows; the stock is a line of its own that also paints.
	const calFlow = $derived<Serie | null>(cal ? {
		rotulos: cal.rotulos.slice(1),
		ha: { pino: cal.ha.pino.slice(1), euca: cal.ha.euca.slice(1), otras: cal.ha.otras.slice(1) },
	} : null);
	const hoyHa = $derived(cal ? sumCol(cal, 0, 1) : 0);
	const hoyOn = $derived(picked?.chart === 'hoy');
	function pickHoy() {
		if (hoyOn) { picked = null; hexStore.setMacizoChartFocus(null); return; }
		picked = { chart: 'hoy', col: 0, gen: 'pino' };
		const label = i18n.t('harvest.chart.cal.hoyAll');
		hexStore.setMacizoChartFocus({ sit: [3, 4], label });
		hexStore.setDeptFillPick(provinceFill('hoy', 0, 'pino', label));
	}
	const pla = $derived<Serie | null>(scope?.b.plantacion ?? null);
	const tal = $derived<Serie | null>(scope?.b.talado ?? null);

	const t = (key: string, a = '') => i18n.t(key).replace('{a}', a);
	const yearOf = (r: string) => parseInt(r.replace(/\D/g, ''), 10);

	function colName(chart: Chart, r: string): string {
		if (chart === 'calendario') return r === 'hoy' ? t('harvest.chart.cal.hoy') : t(r.endsWith('+') ? 'harvest.chart.cal.desde' : 'harvest.chart.cal.anio', String(yearOf(r)));
		if (chart === 'plantacion') return r.startsWith('<') ? t('harvest.chart.pla.antes', String(yearOf(r))) : t('harvest.chart.pla.anio', r);
		return t('harvest.chart.tal.anio', r);
	}

	function focusOf(chart: Chart, r: string, gen: Gen): MacizoFocus {
		const genero = GENERO[gen];
		const label = `${i18n.t(`harvest.gen.${gen}`)} · ${colName(chart, r)}`;
		const y = yearOf(r);
		if (chart === 'calendario') {
			if (r === 'hoy') return { genero, sit: [3, 4], label };
			return { genero, sit: [1, 2], anioTurno: r.endsWith('+') ? [y, 9999] : [y, y], label };
		}
		if (chart === 'plantacion') return { genero, anioPlant: r.startsWith('<') ? [1901, y - 1] : [y, y], label };
		return { genero, sit: [5], anioPerdida: [y, y], label };
	}

	// Province view (no department open): the macizos are not drawn at that zoom, so a pick paints the
	// departments with that column's hectares instead (same fill and legend as the ranking).
	function provinceFill(chart: Chart, col: number, gen: Gen, label: string): DeptFill | null {
		if (hexStore.selectedParquetKey || !summary?.departments) return null;
		const offset = chart === 'calendario' ? 1 : 0;   // the bars skip the calendar's «hoy» stock
		const val = (d: any) => {
			if (chart === 'hoy') return (['pino', 'euca', 'otras'] as Gen[]).reduce((a, g) => a + (d.calendario?.ha[g]?.[0] ?? 0), 0);
			return d[chart]?.ha[gen]?.[col + offset] ?? null;
		};
		const values = summary.departments.map((d: any) => ({ name: d.dpto, value: val(d) }))
			.sort((a: any, b: any) => (b.value ?? -1) - (a.value ?? -1));
		const max = Math.max(1, ...values.map((v: any) => v.value ?? 0));
		return { territory: hexStore.territoryId, values, max, signed: false, decimals: 0, unit: 'ha', title: label };
	}

	function pick(chart: Chart, s: Serie, col: number, gen: Gen) {
		if (picked && picked.chart === chart && picked.col === col && picked.gen === gen) {
			picked = null; hexStore.setMacizoChartFocus(null); return;
		}
		picked = { chart, col, gen };
		const f = focusOf(chart, s.rotulos[col], gen);
		hexStore.setMacizoChartFocus(f);
		hexStore.setDeptFillPick(provinceFill(chart, col, gen, f.label ?? ''));
	}
	const sel = (chart: Chart) => (picked?.chart === chart ? { col: picked.col, gen: picked.gen } : null);

	const fmt = (v: number) => Math.round(v).toLocaleString(i18n.locale === 'en' ? 'en' : 'es');
	const sumCol = (s: Serie, from: number, to: number) =>
		(['pino', 'euca', 'otras'] as Gen[]).reduce((a, g) => a + s.ha[g].slice(from, to).reduce((x, v) => x + v, 0), 0);
	// Ticks on round years (every `cada`), the first and the last column; none that would crowd the last one.
	const ticks = (s: Serie, cada: number) => s.rotulos.map((r, i) => {
		const last = s.rotulos.length - 1;
		if (i === 0) return r === 'hoy' ? i18n.t('harvest.chart.cal.hoyTick') : r;
		if (i === last) return r;
		return yearOf(r) % cada === 0 && last - i > 2 && i > 2 ? r : '';
	});

	const calSummary = $derived(cal ? i18n.t('harvest.chart.cal.sum').replace('{c}', fmt(sumCol(cal, 1, 6))) : '');
	const talPeak = $derived.by(() => {
		if (!tal) return '';
		const tot = tal.rotulos.map((_, i) => sumCol(tal, i, i + 1));
		const all = tot.reduce((a, v) => a + v, 0);
		if (!all) return i18n.t('harvest.chart.tal.none');
		const i = tot.indexOf(Math.max(...tot));
		return i18n.t('harvest.chart.tal.sum').replace('{t}', fmt(all)).replace('{a}', tal.rotulos[i]).replace('{p}', fmt(tot[i]));
	});
	const plaSummary = $derived.by(() => {
		if (!pla) return '';
		const tot = pla.rotulos.map((_, i) => sumCol(pla, i, i + 1));
		const i = tot.indexOf(Math.max(...tot));
		return i18n.t('harvest.chart.pla.sum').replace('{a}', pla.rotulos[i]).replace('{p}', fmt(tot[i]));
	});
	const where = $derived(scope?.name ? ` · ${scope.name}` : '');
</script>

{#if scope && cal && pla && tal}
	<p class="cg-hint">{i18n.t(hexStore.selectedParquetKey ? 'harvest.chart.hint' : 'harvest.chart.hintProv')}</p>
	{#if calFlow}
	<button class="cg-hoy" class:on={hoyOn} onclick={pickHoy}>
		<b>{fmt(hoyHa)} ha</b> {i18n.t('harvest.chart.cal.hoyLine')} <span class="cg-ver">{i18n.t(hoyOn ? 'harvest.chart.cal.hoyOff' : 'harvest.chart.cal.hoyVer')}</span>
	</button>
	<MacizoBarras title={i18n.t('harvest.chart.cal.title') + where} rotulos={ticks(calFlow, 5)} ha={calFlow.ha}
		selected={sel('calendario')} onPick={(c, g) => pick('calendario', calFlow, c, g)} summary={calSummary}
		colName={(c) => colName('calendario', calFlow.rotulos[c])} note={i18n.t('harvest.chart.cal.note')} csvFilename="spatia_cosecha_calendario" />
	{/if}
	<MacizoBarras title={i18n.t('harvest.chart.pla.title') + where} rotulos={ticks(pla, 10)} ha={pla.ha}
		selected={sel('plantacion')} onPick={(c, g) => pick('plantacion', pla, c, g)} summary={plaSummary}
		colName={(c) => colName('plantacion', pla.rotulos[c])} note={i18n.t('harvest.chart.pla.note')} csvFilename="spatia_cosecha_plantacion" />
	<MacizoBarras title={i18n.t('harvest.chart.tal.title') + where} rotulos={ticks(tal, 5)} ha={tal.ha}
		selected={sel('talado')} onPick={(c, g) => pick('talado', tal, c, g)} summary={talPeak}
		colName={(c) => colName('talado', tal.rotulos[c])} note={i18n.t('harvest.chart.tal.note')} csvFilename="spatia_cosecha_talado" />
{:else if !summary}
	<p class="cg-hint">{i18n.t('uso.serie.loading')}</p>
{/if}

<style>
	.cg-hint { margin: 2px 0 6px; font-size: 11px; color: var(--s-ink-3); line-height: 1.35; }
	.cg-hoy { display: block; width: 100%; text-align: left; margin: 0 0 6px; padding: 6px 8px; font: inherit; font-size: 11.5px;
		color: var(--s-ink-1); background: var(--s-surface); border: 1px solid var(--s-grid); border-radius: 4px; cursor: pointer; }
	.cg-hoy.on { border-color: var(--s-ink-2); }
	.cg-ver { color: var(--s-ink-3); white-space: nowrap; }
</style>
