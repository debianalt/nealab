<script lang="ts">
	// «Evolución» of the land-use layer: hectares by class every 5 years, 1985–2025, for the open
	// department or, with none open, the whole province. Reads the `serie` block the summaries carry
	// (pipeline/hexoma/exportar_uso_web.py): no parquet query. Stacked columns in the map's colors.
	import ChartFrame from './ChartFrame.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { themeStore } from '$lib/stores/theme.svelte';
	import { classColor, USO_HEX } from '$lib/utils/sello';
	import { CHART_PX, INK, svgScale, dockBoost, measure } from '$lib/utils/chartType';
	import { loadDeptSummary } from '$lib/utils/deptSummaries';
	import { TERRITORY_REGISTRY, USO_CLASSES } from '$lib/config';
	import { formatDept } from '$lib/utils/format';
	import type { HexStore } from '$lib/stores/hex.svelte';

	let { hexStore }: { hexStore: HexStore } = $props();

	// Panel classes of the summary → the 8 map classes (same grouping as the map: pastura + cultivo anual,
	// urbano + sin vegetación). Order = bottom → top of each column.
	const GROUPS: { key: string; from: string[]; color: string }[] = [
		{ key: 'monte', from: ['monte'], color: USO_HEX[0] },
		{ key: 'plantacion', from: ['plantacion'], color: USO_HEX[1] },
		{ key: 'perenne', from: ['perenne'], color: USO_HEX[2] },
		{ key: 'agro', from: ['cultivo', 'pastura'], color: USO_HEX[3] },
		{ key: 'pastizal', from: ['pastizal'], color: USO_HEX[4] },
		{ key: 'estero', from: ['estero'], color: USO_HEX[5] },
		{ key: 'agua', from: ['agua'], color: USO_HEX[6] },
		{ key: 'urbano', from: ['urbano', 'otro'], color: USO_HEX[7] },
	];
	const labelOf = (i: number) => i18n.t(USO_CLASSES.steps[i].labelKey);

	let summary = $state<any>(null);
	$effect(() => {
		const layer = hexStore.activeLayer;
		const prefix = hexStore.territoryPrefix;
		summary = null;
		if (!layer?.classSeries) return;
		loadDeptSummary(layer.id, prefix).then((s) => { summary = s; });
	});

	const scope = $derived.by(() => {
		if (!summary) return null;
		const key = hexStore.selectedParquetKey;
		const dep = key ? summary.departments?.find((d: any) => d.parquetKey === key) : null;
		if (dep?.serie) return { name: formatDept(dep.dpto), serie: dep.serie };
		if (summary.province?.serie) return { name: TERRITORY_REGISTRY[hexStore.territoryId]?.label ?? '', serie: summary.province.serie };
		return null;
	});

	const anios = $derived<number[]>(scope?.serie.anios ?? []);
	// ha[g][y]
	const ha = $derived<number[][]>(
		scope ? GROUPS.map((g) => anios.map((_, y) => g.from.reduce((a, c) => a + (scope.serie.ha[c]?.[y] ?? 0), 0))) : []
	);
	const totals = $derived(anios.map((_, y) => ha.reduce((a, g) => a + g[y], 0)));
	const maxTotal = $derived(Math.max(1, ...totals));
	const colors = $derived(GROUPS.map((g) => classColor(g.color, themeStore.tema)));

	const SVG_W = 300, PAD_L = 4, PAD_R = 4, PAD_T = 8, PLOT_H = 160;
	let renderedW = $state(0), renderedH = $state(0);
	let docked = $state(false);
	const k = $derived(svgScale(SVG_W, PAD_T + PLOT_H + 20, renderedW, renderedH) * dockBoost(docked));
	const SVG_H = $derived(PAD_T + PLOT_H + (CHART_PX.tick + 8) * k);
	const colW = $derived(anios.length ? (SVG_W - PAD_L - PAD_R) / anios.length : 0);
	const barW = $derived(colW * 0.72);

	let hover = $state<{ g: number; y: number } | null>(null);
	const fmt = (v: number) => Math.round(v).toLocaleString(i18n.locale === 'en' ? 'en' : 'es');
	const pct = (v: number, t: number) => (t > 0 ? (100 * v / t).toLocaleString(i18n.locale === 'en' ? 'en' : 'es', { maximumFractionDigits: 1 }) : '0');

	// One line under the chart: the hovered segment, else the change of the two main classes.
	const lineText = $derived.by(() => {
		if (!anios.length) return '';
		if (hover) {
			const v = ha[hover.g][hover.y];
			return `${anios[hover.y]} · ${labelOf(hover.g)}: ${fmt(v)} ha (${pct(v, totals[hover.y])} %)`;
		}
		const last = anios.length - 1;
		return i18n.t('uso.serie.change')
			.replace('{a}', String(anios[0])).replace('{b}', String(anios[last]))
			.replace('{m0}', fmt(ha[0][0])).replace('{m1}', fmt(ha[0][last]))
			.replace('{p0}', fmt(ha[1][0])).replace('{p1}', fmt(ha[1][last]));
	});

	function csvRows() {
		const rows: Record<string, unknown>[] = [];
		anios.forEach((a, y) => GROUPS.forEach((g, i) => rows.push({ anio: a, clase: labelOf(i), ha: Math.round(ha[i][y]) })));
		return rows;
	}
</script>

{#if hexStore.activeLayer?.classSeries}
<ChartFrame bind:expanded={docked} title={`${i18n.t('flow.title')} ${anios[0] ?? ''}–${anios[anios.length - 1] ?? ''}${scope?.name ? ` · ${scope.name}` : ''}`} csvRows={csvRows} csvFilename="spatia_uso_serie">
	{#if scope}
		<div use:measure={(w, h) => { renderedW = w; renderedH = h; }}>
			<svg width="100%" viewBox="0 0 {SVG_W} {SVG_H}" preserveAspectRatio="xMidYMid meet" style="display:block" role="img" aria-label={i18n.t('uso.serie.aria')}>
				{#each anios as a, y}
					{@const x = PAD_L + y * colW + (colW - barW) / 2}
					{@const ys = (() => { let acc = PAD_T + PLOT_H; return ha.map((g) => { const h = (g[y] / maxTotal) * PLOT_H; acc -= h; return { y0: acc, h }; }); })()}
					{#each GROUPS as _, i}
						{#if ys[i].h > 0.2}
							<!-- svelte-ignore a11y_no_static_element_interactions -->
							<rect x={x} y={ys[i].y0 + 0.5} width={barW} height={Math.max(0, ys[i].h - 1)} fill={colors[i]}
								opacity={hover && (hover.g !== i) ? 0.45 : 1}
								onmouseenter={() => (hover = { g: i, y })} onmouseleave={() => (hover = null)} />
						{/if}
					{/each}
					<text x={x + barW / 2} y={PAD_T + PLOT_H + (CHART_PX.tick + 3) * k} text-anchor="middle"
						font-size={CHART_PX.tick * k} fill={INK.tick} font-family="'IBM Plex Sans', system-ui, sans-serif">{a}</text>
				{/each}
			</svg>
		</div>
		<p class="us-line">{lineText}</p>
		<ul class="us-legend">
			{#each GROUPS as g, i}
				{#if ha[i]?.some((v) => v > 0)}
					<li><span class="us-sw" style:background={colors[i]}></span>{labelOf(i)} <span class="us-n">{fmt(ha[i][0])} → {fmt(ha[i][anios.length - 1])} ha</span></li>
				{/if}
			{/each}
		</ul>
		<p class="us-note">{i18n.t('uso.serie.note')}</p>
	{:else}
		<p class="us-line">{i18n.t('uso.serie.loading')}</p>
	{/if}
</ChartFrame>
{/if}

<style>
	.us-line { margin: 4px 0 2px; font-size: 11.5px; color: var(--s-ink-1); min-height: 1.3em; }
	.us-legend { list-style: none; margin: 4px 0 0; padding: 0; display: grid; grid-template-columns: 1fr; gap: 2px; font-size: 11px; color: var(--s-ink-2); }
	.us-legend li { display: flex; align-items: center; gap: 6px; }
	.us-sw { width: 10px; height: 10px; border-radius: 2px; flex: none; }
	.us-n { margin-left: auto; font-family: 'IBM Plex Mono', ui-monospace, monospace; font-size: 10.5px; color: var(--s-ink-3); }
	.us-note { margin: 6px 0 0; font-size: 10.5px; color: var(--s-ink-3); line-height: 1.35; }
</style>
