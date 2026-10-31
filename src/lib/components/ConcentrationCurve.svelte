<script lang="ts">
	import { RIO, SELECT, INK_3, SURFACE } from '$lib/utils/sello';
	import { i18n } from '$lib/stores/i18n.svelte';
	import ChartFrame from './ChartFrame.svelte';
	import { CHART_PX, INK, svgScale, fitLabel, dockBoost, measure } from '$lib/utils/chartType';

	let {
		data = new Map() as Map<string, Record<string, any>>,
		variable = '',
		label = '',
		onBrushSelect = (_h3s: string[]) => {},
	}: {
		data: Map<string, Record<string, any>>;
		variable: string;
		// What is being summed, without unit ("Edificios en zona baja").
		label?: string;
		onBrushSelect?: (h3s: string[]) => void;
	} = $props();

	const PAD_L = 34, PAD_R = 10, PAD_T = 10, PAD_B = 38;
	const SVG_W = 260, SVG_H = 190;
	const plotW = SVG_W - PAD_L - PAD_R;
	const plotH = SVG_H - PAD_T - PAD_B;

	let renderedW = $state(0), renderedH = $state(0);
	let docked = $state(false);
	const k = $derived(svgScale(SVG_W, SVG_H, renderedW, renderedH) * dockBoost(docked));

	// Hexagons with a positive value, largest first; cum[i] = share of the total
	// held by the first i+1 hexagons. Equal-area cells, so the curve reads as
	// "share of the territory" vs "share of the total".
	const sorted = $derived.by(() => {
		const rows: { h3: string; v: number }[] = [];
		for (const [h3, row] of data) {
			const v = Number(row[variable]);
			if (isFinite(v) && v > 0) rows.push({ h3, v });
		}
		rows.sort((a, b) => b.v - a.v);
		return rows;
	});
	const total = $derived(sorted.reduce((s, r) => s + r.v, 0));
	// All hexagons (zeros included) count for the x axis: "10 % of the hexagons".
	const nAll = $derived.by(() => {
		let n = 0;
		for (const row of data.values()) if (isFinite(Number(row[variable]))) n++;
		return n;
	});
	const cum = $derived.by(() => {
		const out = new Array(sorted.length);
		let s = 0;
		for (let i = 0; i < sorted.length; i++) { s += sorted[i].v; out[i] = total > 0 ? s / total : 0; }
		return out as number[];
	});

	// Share of the total held by the top `f` fraction of all hexagons.
	function shareAt(f: number): number {
		const n = Math.floor(f * nAll);
		if (n <= 0) return 0;
		return n >= cum.length ? 1 : cum[n - 1];
	}
	// Smallest fraction of hexagons holding `share` of the total.
	function hexFracFor(share: number): number {
		const i = cum.findIndex(c => c >= share);
		return i < 0 ? 1 : (i + 1) / nAll;
	}

	const pts = $derived.by(() => {
		if (nAll === 0 || total <= 0) return '';
		const N = 200;
		const p: string[] = [];
		for (let i = 0; i <= N; i++) {
			const f = i / N;
			p.push(`${svgX(f).toFixed(1)},${svgY(shareAt(f)).toFixed(1)}`);
		}
		return p.join(' ');
	});

	const pct = (x: number) => (x * 100).toLocaleString(i18n.locale, { maximumFractionDigits: x < 0.1 ? 1 : 0 });
	const headTop10 = $derived(shareAt(0.1));
	const headHalf = $derived(hexFracFor(0.5));

	function svgX(f: number) { return PAD_L + f * plotW; }
	function svgY(s: number) { return PAD_T + (1 - s) * plotH; }

	// Cut: the top `cut` fraction of hexagons is selected on the map.
	let cut: number | null = $state(null);
	const cutCount = $derived(cut === null ? 0 : Math.min(sorted.length, Math.floor(cut * nAll)));

	function fracFromClient(clientX: number, rect: DOMRect): number {
		const lx = (clientX - rect.left) * (SVG_W / rect.width);
		return Math.max(0, Math.min(1, (lx - PAD_L) / plotW));
	}
	function applyCut(f: number) {
		cut = f;
		onBrushSelect(sorted.slice(0, cutCount).map(r => r.h3));
	}
	function startDrag(e: MouseEvent) {
		if (total <= 0) return;
		const rect = (e.currentTarget as SVGElement).getBoundingClientRect();
		cut = fracFromClient(e.clientX, rect);
		const onMove = (ev: MouseEvent) => { cut = fracFromClient(ev.clientX, rect); };
		const onUp = () => {
			window.removeEventListener('mousemove', onMove);
			window.removeEventListener('mouseup', onUp);
			if (cut !== null) applyCut(cut);
		};
		window.addEventListener('mousemove', onMove);
		window.addEventListener('mouseup', onUp);
	}
	function clearCut() {
		cut = null;
		onBrushSelect([]);
	}

	$effect(() => {
		void data;
		void variable;
		cut = null;
	});

	function csvRows() {
		const N = 100;
		return Array.from({ length: N + 1 }, (_, i) => ({ pct_hex: i, pct_total: (shareAt(i / N) * 100).toFixed(2) }));
	}
</script>

<ChartFrame bind:expanded={docked} title={i18n.t('chart.conc.title')} csvRows={csvRows} csvFilename="spatia_concentracion">
	<div class="cc-panel">
		{#if total > 0}
			<p class="cc-head">
				{#if headTop10 >= 0.999}
					<!-- Fewer than 10 % of the cells hold anything: say where it all is. -->
					{@html i18n.t('chart.conc.all').replace('{p}', `<strong>${pct(sorted.length / nAll)} %</strong>`)}
				{:else}
					{@html i18n.t('chart.conc.top10').replace('{p}', `<strong>${pct(headTop10)} %</strong>`)}
				{/if}
				<br />
				{@html i18n.t('chart.conc.half').replace('{p}', `<strong>${pct(headHalf)} %</strong>`)}
			</p>
			<div class="cc-sub">
				{#if cut !== null}
					<span class="cc-sel">{i18n.t('chart.conc.sel').replace('{k}', pct(cut)).replace('{n}', cutCount.toLocaleString(i18n.locale)).replace('{p}', pct(shareAt(cut)))}</span>
				{:else}
					<span class="cc-hint">{i18n.t('chart.conc.hint')}</span>
				{/if}
			</div>
			<div use:measure={(w, h) => { renderedW = w; renderedH = h; }}>
				<!-- svelte-ignore a11y_no_static_element_interactions -->
				<svg width="100%" viewBox="0 0 {SVG_W} {SVG_H}" preserveAspectRatio="xMidYMid meet"
					onmousedown={startDrag} ondblclick={clearCut} style="cursor: crosshair; display: block;">
					<!-- Even split reference -->
					<line x1={svgX(0)} y1={svgY(0)} x2={svgX(1)} y2={svgY(1)}
						stroke={INK_3} stroke-width="1" stroke-dasharray="3 2" />
					<text x={svgX(0.62)} y={svgY(0.62) + 12 * k} fill={INK.hint}
						font-size={CHART_PX.tick * k} font-family="'IBM Plex Sans', system-ui, sans-serif"
						transform={`rotate(${-Math.atan2(plotH, plotW) * 180 / Math.PI}, ${svgX(0.62)}, ${svgY(0.62) + 12 * k})`}
					>{i18n.t('chart.conc.even')}</text>

					<!-- Selected top share -->
					{#if cut !== null}
						<rect x={svgX(0)} y={PAD_T} width={svgX(cut) - svgX(0)} height={plotH}
							fill={SELECT} opacity="0.1" pointer-events="none" />
						<line x1={svgX(cut)} x2={svgX(cut)} y1={PAD_T} y2={PAD_T + plotH}
							stroke={SELECT} stroke-width="1.2" pointer-events="none" />
						<circle cx={svgX(cut)} cy={svgY(shareAt(cut))} r={4 * k} fill={SELECT} stroke={SURFACE} stroke-width={2 * k} pointer-events="none" />
					{/if}

					<polyline points={pts} fill="none" stroke={RIO} stroke-width={2 * k} stroke-linejoin="round" />

					<!-- Axes ticks -->
					{#each [0, 0.25, 0.5, 0.75, 1] as f}
						<text x={svgX(f)} y={PAD_T + plotH + (CHART_PX.tick + 2) * k}
							text-anchor={f === 0 ? 'start' : f === 1 ? 'end' : 'middle'} fill={INK.tick}
							font-size={CHART_PX.tick * k} font-family="'IBM Plex Sans', system-ui, sans-serif">{f * 100}%</text>
						<text x={PAD_L - 3 * k} y={svgY(f) + 4 * k} text-anchor="end" fill={INK.tick}
							font-size={CHART_PX.tick * k} font-family="'IBM Plex Sans', system-ui, sans-serif">{f * 100}%</text>
					{/each}
					<text x={PAD_L + plotW / 2} y={PAD_T + plotH + (CHART_PX.tick + CHART_PX.label + 6) * k}
						text-anchor="middle" fill={INK.label} font-size={CHART_PX.label * k} font-family="'IBM Plex Sans', system-ui, sans-serif"
					>{fitLabel(i18n.t('chart.conc.axisX'), plotW / k, CHART_PX.label)}</text>
					<!-- Y title bottom-right: a concentration curve never goes below the diagonal. -->
					<text x={svgX(1) - 4 * k} y={PAD_T + plotH - 6 * k} text-anchor="end" fill={INK.label}
						font-size={CHART_PX.label * k} font-family="'IBM Plex Sans', system-ui, sans-serif"
					><title>{label}</title>{fitLabel(i18n.t('chart.conc.axisY').replace('{v}', label), plotW / k * 0.9, CHART_PX.label)}</text>
				</svg>
			</div>
		{:else}
			<div class="cc-empty">{i18n.t('chart.conc.empty')}</div>
		{/if}
	</div>
</ChartFrame>

<style>
	.cc-panel { padding: 2px 0; }
	.cc-head {
		margin: 2px 2px 6px;
		font-size: 13px;
		line-height: 1.5;
		color: var(--s-ink-1);
	}
	.cc-head :global(strong) { color: var(--s-ink-1); font-family: var(--font-serif); font-variant-numeric: tabular-nums; font-size: 16px; font-weight: 600; }
	.cc-sub { padding: 0 2px; margin-bottom: 2px; min-height: 16px; }
	.cc-sel { font-size: 11px; color: var(--s-ink-1); }
	.cc-hint { font-size: 11px; color: var(--s-ink-3); font-style: italic; }
	.cc-empty { font-size: 11px; color: var(--s-ink-3); padding: 10px 2px; font-style: italic; }
	:global(.chart-frame-expanded) .cc-head { font-size: 16px; }
	:global(.chart-frame-expanded) .cc-head :global(strong) { font-size: 19px; }
</style>
