<script lang="ts">
	// Stacked columns (pine · eucalyptus · other genera) for the macizo layer's charts (CosechaGraficos).
	// Clicking a segment picks that column and genus: the parent turns it into a tile filter and the map
	// paints those stands. Same drawing rules as UsoSerie (real px through ChartFrame).
	import ChartFrame from './ChartFrame.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { CHART_PX, INK, svgScale, dockBoost, measure } from '$lib/utils/chartType';
	import { SELVA, RIO, ORO } from '$lib/utils/sello';

	type Gen = 'pino' | 'euca' | 'otras';
	let {
		title, rotulos, ha, selected = null, onPick, summary = '', note = '', colName, csvFilename,
	}: {
		title: string;
		rotulos: string[];                      // tick text per column (already localized; '' = no tick)
		ha: Record<Gen, number[]>;
		selected?: { col: number; gen: Gen } | null;
		onPick: (col: number, gen: Gen) => void;
		summary?: string;                       // line shown when nothing is hovered
		note?: string;
		colName: (col: number) => string;       // «llega al turno en 2031», for the hover line
		csvFilename: string;
	} = $props();

	const GENS: Gen[] = ['pino', 'euca', 'otras'];
	const COLOR: Record<Gen, string> = { pino: SELVA, euca: RIO, otras: ORO };
	const genLabel = (g: Gen) => i18n.t(`harvest.gen.${g}`);

	const n = $derived(rotulos.length);
	const totals = $derived(rotulos.map((_, i) => GENS.reduce((a, g) => a + (ha[g]?.[i] ?? 0), 0)));
	const maxTotal = $derived(Math.max(1, ...totals));
	const presentes = $derived(GENS.filter((g) => ha[g]?.some((v) => v > 0)));

	const SVG_W = 300, PAD_L = 4, PAD_R = 4, PAD_T = 8, PLOT_H = 130;
	let renderedW = $state(0), renderedH = $state(0);
	let docked = $state(false);
	const k = $derived(svgScale(SVG_W, PAD_T + PLOT_H + 20, renderedW, renderedH) * dockBoost(docked));
	const SVG_H = $derived(PAD_T + PLOT_H + (CHART_PX.tick + 8) * k);
	const colW = $derived(n ? (SVG_W - PAD_L - PAD_R) / n : 0);
	const barW = $derived(colW * (n > 24 ? 0.82 : 0.72));

	let hover = $state<{ col: number; gen: Gen } | null>(null);
	const fmt = (v: number) => Math.round(v).toLocaleString(i18n.locale === 'en' ? 'en' : 'es');
	const lineText = $derived.by(() => {
		const h = hover ?? selected;
		if (!h) return summary;
		return `${genLabel(h.gen)} · ${colName(h.col)}: ${fmt(ha[h.gen]?.[h.col] ?? 0)} ha`;
	});
	const showTick = (i: number) => rotulos[i] !== '';

	function csvRows() {
		const rows: Record<string, unknown>[] = [];
		rotulos.forEach((r, i) => GENS.forEach((g) => rows.push({ columna: r, genero: genLabel(g), ha: Math.round(ha[g]?.[i] ?? 0) })));
		return rows;
	}
</script>

<ChartFrame bind:expanded={docked} {title} {csvRows} {csvFilename}>
	<div use:measure={(w, h) => { renderedW = w; renderedH = h; }}>
		<svg width="100%" viewBox="0 0 {SVG_W} {SVG_H}" preserveAspectRatio="xMidYMid meet" style="display:block" role="img" aria-label={title}>
			<line x1={PAD_L} x2={SVG_W - PAD_R} y1={PAD_T + PLOT_H + 0.5} y2={PAD_T + PLOT_H + 0.5} stroke="var(--s-axis)" stroke-width="0.6" />
			{#each rotulos as r, i}
				{@const x = PAD_L + i * colW + (colW - barW) / 2}
				{@const ys = (() => { let acc = PAD_T + PLOT_H; return GENS.map((g) => { const h = ((ha[g]?.[i] ?? 0) / maxTotal) * PLOT_H; acc -= h; return { y0: acc, h }; }); })()}
				{#each GENS as g, gi}
					{#if ys[gi].h > 0.15}
						{@const on = !selected || (selected.col === i && selected.gen === g)}
						<!-- svelte-ignore a11y_click_events_have_key_events -->
						<!-- svelte-ignore a11y_no_static_element_interactions -->
						<rect x={x} y={ys[gi].y0 + 0.4} width={barW} height={Math.max(0.6, ys[gi].h - 0.8)} fill={COLOR[g]}
							opacity={on ? (hover && !(hover.col === i && hover.gen === g) && !selected ? 0.55 : 1) : 0.3}
							stroke={selected && selected.col === i && selected.gen === g ? 'var(--s-ink-1)' : 'none'} stroke-width="0.8"
							style="cursor:pointer"
							onmouseenter={() => (hover = { col: i, gen: g })} onmouseleave={() => (hover = null)}
							onclick={() => onPick(i, g)} />
					{/if}
				{/each}
				{#if showTick(i)}
					<text x={x + barW / 2} y={PAD_T + PLOT_H + (CHART_PX.tick + 3) * k} text-anchor={i === 0 ? 'start' : i === n - 1 ? 'end' : 'middle'}
						font-size={CHART_PX.tick * k} fill={INK.tick} font-family="'IBM Plex Sans', system-ui, sans-serif">{r}</text>
				{/if}
			{/each}
		</svg>
	</div>
	<p class="mb-line">{lineText}</p>
	<ul class="mb-legend">
		{#each presentes as g}
			<li><span class="mb-sw" style:background={COLOR[g]}></span>{genLabel(g)} <span class="mb-n">{fmt((ha[g] ?? []).reduce((a, v) => a + v, 0))} ha</span></li>
		{/each}
	</ul>
	{#if note}<p class="mb-note">{note}</p>{/if}
</ChartFrame>

<style>
	.mb-line { margin: 4px 0 2px; font-size: 11.5px; color: var(--s-ink-1); min-height: 1.3em; }
	.mb-legend { list-style: none; margin: 4px 0 0; padding: 0; display: grid; gap: 2px; font-size: 11px; color: var(--s-ink-2); }
	.mb-legend li { display: flex; align-items: center; gap: 6px; }
	.mb-sw { width: 10px; height: 10px; border-radius: 2px; flex: none; }
	.mb-n { margin-left: auto; font-family: 'IBM Plex Mono', ui-monospace, monospace; font-size: 10.5px; color: var(--s-ink-3); }
	.mb-note { margin: 6px 0 0; font-size: 10.5px; color: var(--s-ink-3); line-height: 1.35; }
</style>
