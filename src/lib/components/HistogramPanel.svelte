<script lang="ts">
	import { i18n } from '$lib/stores/i18n.svelte';
	import ChartFrame from './ChartFrame.svelte';
	import { CHART_PX, INK, svgScale, median, fitLabel } from '$lib/utils/chartType';

	let {
		data = new Map() as Map<string, Record<string, any>>,
		variable = '',
		xLabel = 'score',
		onBrushSelect = (_h3s: string[]) => {},
		markers = [] as Array<{ value: number; color: string }>,
	}: {
		data: Map<string, Record<string, any>>;
		variable: string;
		xLabel?: string;
		onBrushSelect?: (h3s: string[]) => void;
		// Vertical lines at given values (e.g. the census tracts selected on the map).
		markers?: Array<{ value: number; color: string }>;
	} = $props();

	function csvRows() {
		const [lo, hi] = domain;
		const span = (hi - lo) / BINS;
		return histogram.map((count, i) => ({
			bin_lo: fmtTick(lo + i * span),
			bin_hi: fmtTick(lo + (i + 1) * span),
			count,
		}));
	}

	const BINS = 20;
	const PAD_L = 12, PAD_R = 12, PAD_T = 14, PAD_B = 32;
	const SVG_W = 260, SVG_H = 120;
	const plotW = SVG_W - PAD_L - PAD_R;
	const plotH = SVG_H - PAD_T - PAD_B;

	// Data-driven domain of the primary variable: the chart plots whatever physical
	// unit the layer carries (tC/ha, min, µg/m³, 0–1 fractions…), not a fixed 0–100.
	// Fallback [0,100] when empty,
	// and a +1 span guard avoids divide-by-zero when all values are equal.
	const domain = $derived.by(() => {
		let lo = Infinity, hi = -Infinity;
		for (const row of data.values()) {
			const v = Number(row[variable]);
			if (!isFinite(v)) continue;
			if (v < lo) lo = v;
			if (v > hi) hi = v;
		}
		if (lo === Infinity) return [0, 100] as [number, number];
		if (hi === lo) return [lo, lo + 1] as [number, number];
		return [lo, hi] as [number, number];
	});

	// Physical value at a given 0–100 percentage of the plot width, and back.
	function pctToVal(pct: number): number {
		const [lo, hi] = domain;
		return lo + (pct / 100) * (hi - lo);
	}

	function fmtTick(v: number): string {
		const a = Math.abs(v);
		if (a >= 100) return v.toFixed(0);
		if (a >= 1) return v.toFixed(1);
		return v.toFixed(2);
	}

	const histogram = $derived.by(() => {
		const [lo, hi] = domain;
		const span = hi - lo;
		const bins = new Array(BINS).fill(0);
		for (const row of data.values()) {
			const v = Number(row[variable]);
			if (!isFinite(v)) continue;
			const bin = Math.min(Math.max(Math.floor(((v - lo) / span) * BINS), 0), BINS - 1);
			bins[bin]++;
		}
		return bins;
	});

	const maxCount = $derived(Math.max(...histogram, 1));
	const totalHexes = $derived(data.size);

	// Rendered width → viewBox units per pixel, so text stays at real pixel size.
	let renderedW = $state(0), renderedH = $state(0);
	const k = $derived(svgScale(SVG_W, SVG_H, renderedW, renderedH));

	// Median as the reference line (the old line sat at mid-range, which means nothing).
	const medianPct = $derived.by(() => {
		const vals: number[] = [];
		for (const row of data.values()) { const v = Number(row[variable]); if (isFinite(v)) vals.push(v); }
		const m = median(vals);
		if (!isFinite(m)) return null;
		const [lo, hi] = domain;
		return Math.max(0, Math.min(100, ((m - lo) / (hi - lo)) * 100));
	});

	let brushLo: number | null = $state(null);
	let brushHi: number | null = $state(null);
	let selectedCount = $state(0);

	function svgX(pct: number): number {
		return PAD_L + (pct / 100) * plotW;
	}

	const BIN_W = 100 / BINS; // 5 units per bin

	// Account for SVG viewBox scaling: PAD_L/plotW are in viewBox units, clientX in screen px
	function pctFromClientX(clientX: number, rect: DOMRect): number {
		const scale = SVG_W / rect.width; // viewBox units per screen pixel
		const localX = (clientX - rect.left) * scale;
		return Math.max(0, Math.min(100, ((localX - PAD_L) / plotW) * 100));
	}

	function startBrush(e: MouseEvent) {
		const rect = (e.currentTarget as SVGElement).getBoundingClientRect();
		const start = pctFromClientX(e.clientX, rect);
		let end = start;
		brushLo = start;
		brushHi = start;

		const onMove = (ev: MouseEvent) => {
			end = pctFromClientX(ev.clientX, rect);
			brushLo = Math.min(start, end);
			brushHi = Math.max(start, end);
		};

		const onUp = () => {
			window.removeEventListener('mousemove', onMove);
			window.removeEventListener('mouseup', onUp);
			let lo = Math.min(start, end);
			let hi = Math.max(start, end);
			// Click or tiny drag → snap to the containing bin
			if (hi - lo < BIN_W * 0.8) {
				const binIdx = Math.min(Math.floor(lo / BIN_W), BINS - 1);
				lo = binIdx * BIN_W;
				hi = (binIdx + 1) * BIN_W;
			}
			brushLo = lo;
			brushHi = hi;
			// brushLo/brushHi are 0–100 plot percentages; convert to physical values
			// before filtering against the raw variable.
			const vLo = pctToVal(lo);
			const vHi = pctToVal(hi);
			const h3s: string[] = [];
			for (const [h3, row] of data) {
				const v = Number(row[variable]);
				if (isFinite(v) && v >= vLo && v <= vHi) h3s.push(h3);
			}
			selectedCount = h3s.length;
			onBrushSelect(h3s);
		};

		window.addEventListener('mousemove', onMove);
		window.addEventListener('mouseup', onUp);
	}

	function clearBrush() {
		brushLo = null;
		brushHi = null;
		selectedCount = 0;
		onBrushSelect([]);
	}

	// Clear brush when data changes (dept switch, analysis change)
	$effect(() => {
		void data.size;
		void variable;
		brushLo = null;
		brushHi = null;
		selectedCount = 0;
	});
</script>

<ChartFrame title="Distribución" csvRows={csvRows} csvFilename="spatia_histogram">
	<div class="hpanel">
	<div class="hpanel-subheader">
		{#if brushLo !== null}
			<span class="hpanel-count">{selectedCount.toLocaleString()} {i18n.t('chart.hexDblClick')}</span>
		{:else}
			<span class="hpanel-hint">{i18n.t('chart.histogram.dragRange')}</span>
		{/if}
		<span class="hpanel-unit">{totalHexes.toLocaleString()} {i18n.t('chart.hexShort')}</span>
	</div>
	<div bind:clientWidth={renderedW} bind:clientHeight={renderedH}>
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<svg
		width="100%"
		viewBox="0 0 {SVG_W} {SVG_H}"
		preserveAspectRatio="xMidYMid meet"
		onmousedown={startBrush}
		ondblclick={clearBrush}
		style="cursor: crosshair; display: block;"
	>
		<!-- Median reference line -->
		{#if medianPct !== null}
			<line
				x1={svgX(medianPct)} x2={svgX(medianPct)}
				y1={PAD_T} y2={PAD_T + plotH}
				stroke="rgba(255,255,255,0.45)" stroke-width="1" stroke-dasharray="3 2"
			/>
			<text
				x={svgX(medianPct)} y={PAD_T - 3 * k}
				text-anchor={medianPct > 80 ? 'end' : medianPct < 20 ? 'start' : 'middle'}
				fill={INK.tick} font-size={CHART_PX.tick * k} font-family="system-ui, sans-serif"
				pointer-events="none"
			>{i18n.t('chart.median')}</text>
		{/if}

		<!-- Bars -->
		{#each histogram as count, i}
			{@const bw = plotW / BINS}
			{@const bx = PAD_L + i * bw}
			{@const bh = (count / maxCount) * plotH}
			{@const by = PAD_T + plotH - bh}
			{@const binLo = i * (100 / BINS)}
			{@const binHi = (i + 1) * (100 / BINS)}
			{@const inBrush = brushLo !== null && brushHi !== null && binHi > brushLo && binLo < brushHi}
			<rect
				x={bx + 0.5} y={by}
				width={bw - 1} height={bh}
				fill={inBrush ? '#34d399' : '#94a3b8'}
				opacity={brushLo !== null && !inBrush ? 0.2 : 0.85}
			/>
		{/each}

		<!-- Brush rect overlay -->
		{#if brushLo !== null && brushHi !== null}
			<rect
				x={svgX(brushLo)} y={PAD_T}
				width={Math.max(0, svgX(brushHi) - svgX(brushLo))} height={plotH}
				fill="#34d399" opacity="0.07"
				stroke="#34d399" stroke-width="1" stroke-opacity="0.35"
				pointer-events="none"
			/>
		{/if}

		<!-- Markers: where the selected items fall in the distribution -->
		{#each markers as m}
			{@const pct = Math.max(0, Math.min(100, ((m.value - domain[0]) / (domain[1] - domain[0])) * 100))}
			<line
				x1={svgX(pct)} x2={svgX(pct)}
				y1={PAD_T - 2} y2={PAD_T + plotH}
				stroke={m.color} stroke-width="1.5"
				pointer-events="none"
			/>
		{/each}

		<!-- X axis labels (physical values at 0/25/50/75/100 % of the domain) -->
		{#each [0, 25, 50, 75, 100] as tick}
			<text
				x={svgX(tick)} y={PAD_T + plotH + (CHART_PX.tick + 2) * k}
				text-anchor={tick === 0 ? 'start' : tick === 100 ? 'end' : 'middle'} fill={INK.tick}
				font-size={CHART_PX.tick * k} font-family="system-ui, sans-serif"
			>{fmtTick(pctToVal(tick))}</text>
		{/each}

		<!-- Axis title: what the bars measure, inside the SVG so exports carry it -->
		<text
			x={PAD_L + plotW / 2} y={PAD_T + plotH + (CHART_PX.tick + CHART_PX.label + 6) * k}
			text-anchor="middle" fill={INK.label}
			font-size={CHART_PX.label * k} font-family="system-ui, sans-serif"
		><title>{xLabel}</title>{fitLabel(xLabel, plotW / k, CHART_PX.label)}</text>
	</svg>
	</div>
	</div>
</ChartFrame>

<style>
	.hpanel {
		padding: 2px 0;
	}
	.hpanel-subheader {
		display: flex;
		align-items: baseline;
		gap: 6px;
		margin-bottom: 2px;
		padding: 0 2px;
	}
	.hpanel-count {
		font-size: 11px;
		color: #34d399;
	}
	.hpanel-unit {
		font-size: 11px;
		color: rgba(255,255,255,0.5);
		margin-left: auto;
	}
	.hpanel-hint {
		font-size: 11px;
		color: rgba(255,255,255,0.5);
		font-style: italic;
	}
</style>
