<script lang="ts">
	import ChartFrame from './ChartFrame.svelte';
	import type { FlowBands } from '$lib/config';

	let {
		data = new Map() as Map<string, Record<string, any>>,
		temporalPeriods = null as { baseline: string; current: string } | null,
		bands = null as FlowBands | null,
		onBrushSelect = (_h3s: string[]) => {},
	}: {
		data: Map<string, Record<string, any>>;
		temporalPeriods?: { baseline: string; current: string } | null;
		bands?: FlowBands | null;
		onBrushSelect?: (h3s: string[]) => void;
	} = $props();

	// Default banding: composite score 0-100 with 33/67 cuts (legacy behaviour).
	const SCORE_BANDS: FlowBands = {
		col: 'score',
		baselineCol: 'score_baseline',
		breaks: [33, 67],
		labels: ['Bajo', 'Medio', 'Alto'],
		colors: ['#fb923c', '#f59e0b', '#22d3ee'],
		higherIsWorse: false,
		note: 'Flujo de hexágonos entre bandas: Bajo (<33) · Medio (33–67) · Alto (≥67) · Clic en un flujo o banda para ver esos hexágonos en el mapa',
	};

	const cfg = $derived(bands ?? SCORE_BANDS);
	const N = $derived(cfg.labels.length);

	const SVG_W = 260, SVG_H = 164;
	const BAND_W = 40, PAD_T = 22, PAD_B = 22;
	const LEFT_X = 18, RIGHT_X = SVG_W - 18 - BAND_W;
	const MID_X = SVG_W / 2;
	const plotH = SVG_H - PAD_T - PAD_B;

	// pd.cut semantics (right-closed): value lands in the lowest band whose
	// upper break it does not exceed — keeps chart bands == parquet typology.
	function toBand(v: number): number {
		let b = 0;
		for (const br of cfg.breaks) if (v > br) b++;
		return b;
	}

	type FlowEntry = { h3: string; from: number; to: number };

	const entries = $derived.by(() => {
		const result: FlowEntry[] = [];
		for (const [h3, row] of data) {
			const s  = Number(row[cfg.col]);
			const sb = Number(row[cfg.baselineCol]);
			if (!isFinite(s) || !isFinite(sb)) continue;
			result.push({ h3, from: toBand(sb), to: toBand(s) });
		}
		return result;
	});

	const matrix = $derived.by(() => {
		const m: string[][][] = Array.from({ length: N }, () =>
			Array.from({ length: N }, () => [] as string[])
		);
		for (const e of entries) m[e.from][e.to].push(e.h3);
		return m;
	});

	const total      = $derived(entries.length);
	const bandIdx    = $derived(Array.from({ length: N }, (_, i) => i));
	const fromTotals = $derived(bandIdx.map(i => entries.filter(e => e.from === i).length));
	const toTotals   = $derived(bandIdx.map(i => entries.filter(e => e.to   === i).length));
	// "improved" = moved toward the better end, which depends on direction
	const isBetter   = $derived((to: number, from: number) =>
		cfg.higherIsWorse ? to < from : to > from);
	const improved   = $derived(entries.filter(e => isBetter(e.to, e.from)).length);
	const worsened   = $derived(entries.filter(e => e.to !== e.from && !isBetter(e.to, e.from)).length);
	const stable     = $derived(entries.filter(e => e.to === e.from).length);

	// Band Y positions: highest band index at top, stacked downward
	function bandYs(totals: number[]): number[] {
		const y: number[] = new Array(N).fill(PAD_T);
		if (total === 0) {
			for (let i = 0; i < N; i++) y[i] = PAD_T + ((N - 1 - i) * plotH) / N;
			return y;
		}
		let acc = PAD_T;
		for (let i = N - 1; i >= 0; i--) {
			y[i] = acc;
			acc += (totals[i] / total) * plotH;
		}
		return y;
	}
	const leftBandY  = $derived(bandYs(fromTotals));
	const rightBandY = $derived(bandYs(toTotals));
	const leftBandH  = $derived(bandIdx.map(i => total > 0 ? (fromTotals[i] / total) * plotH : plotH / N));
	const rightBandH = $derived(bandIdx.map(i => total > 0 ? (toTotals[i] / total) * plotH : plotH / N));

	type FlowVis = {
		from: number; to: number; count: number; h3s: string[];
		y1: number; y2: number; strokeW: number; color: string;
	};

	const visFlows = $derived.by(() => {
		if (total === 0) return [] as FlowVis[];
		const lc = [...leftBandY];
		const rc = [...rightBandY];
		const result: FlowVis[] = [];
		for (let from = N - 1; from >= 0; from--) {
			for (let to = N - 1; to >= 0; to--) {
				const h3s = matrix[from][to];
				if (!h3s.length) continue;
				const fh = (h3s.length / total) * plotH;
				const color = to === from
					? 'rgba(148,163,184,0.55)'
					: isBetter(to, from) ? '#22d3ee' : '#f87171';
				result.push({
					from, to, count: h3s.length, h3s,
					y1: lc[from] + fh / 2,
					y2: rc[to]   + fh / 2,
					strokeW: Math.max(1.5, fh * 0.85),
					color,
				});
				lc[from] += fh;
				rc[to]   += fh;
			}
		}
		return result;
	});

	let activeFlow = $state<[number, number] | null>(null);
	let activeSide = $state<'from' | 'to' | null>(null);
	let activeBand = $state<number | null>(null);

	function selectFlow(from: number, to: number) {
		if (activeFlow?.[0] === from && activeFlow?.[1] === to) {
			activeFlow = null; onBrushSelect([]); return;
		}
		activeFlow = [from, to]; activeSide = null; activeBand = null;
		onBrushSelect(matrix[from][to]);
	}

	function selectBand(side: 'from' | 'to', band: number) {
		if (activeSide === side && activeBand === band) {
			activeSide = null; activeBand = null; onBrushSelect([]); return;
		}
		activeSide = side; activeBand = band; activeFlow = null;
		const h3s = side === 'from'
			? entries.filter(e => e.from === band).map(e => e.h3)
			: entries.filter(e => e.to   === band).map(e => e.h3);
		onBrushSelect(h3s);
	}

	$effect(() => {
		void data.size;
		void cfg.col;
		activeFlow = null; activeSide = null; activeBand = null;
	});

	function csvRows() {
		return entries.map(e => ({
			h3index: e.h3,
			band_from: cfg.labels[e.from],
			band_to: cfg.labels[e.to],
		}));
	}

	const reverseBands = $derived([...bandIdx].reverse());
</script>

<ChartFrame title="Evolución" csvRows={csvRows} csvFilename="spatia_flow">
	<div class="fc-panel">
	<div class="fc-subheader">
		{#if activeFlow !== null}
			<span class="fc-active">
				{cfg.labels[activeFlow[0]]}→{cfg.labels[activeFlow[1]]}: {matrix[activeFlow[0]][activeFlow[1]].length.toLocaleString()} hex
				<button class="fc-clear" onclick={() => { activeFlow = null; onBrushSelect([]); }}>× quitar</button>
			</span>
		{:else if activeSide !== null && activeBand !== null}
			<span class="fc-active">
				{cfg.labels[activeBand]} ({activeSide === 'from' ? 'baseline' : 'actual'}): {(activeSide === 'from' ? fromTotals : toTotals)[activeBand].toLocaleString()} hex
				<button class="fc-clear" onclick={() => { activeSide = null; activeBand = null; onBrushSelect([]); }}>× quitar</button>
			</span>
		{:else if total === 0}
			<span class="fc-hint">seleccioná un departamento</span>
		{/if}
	</div>

	{#if total > 0}
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<svg
			width="100%"
			viewBox="0 0 {SVG_W} {SVG_H}"
			preserveAspectRatio="xMidYMid meet"
			style="display:block"
		>
			<!-- Flows (rendered behind bands) -->
			{#each visFlows as f}
				{@const isActive = activeFlow?.[0] === f.from && activeFlow?.[1] === f.to}
				<!-- svelte-ignore a11y_no_static_element_interactions -->
				<path
					d="M {LEFT_X + BAND_W} {f.y1} C {MID_X} {f.y1}, {MID_X} {f.y2}, {RIGHT_X} {f.y2}"
					fill="none"
					stroke={f.color}
					stroke-width={f.strokeW}
					opacity={activeFlow !== null ? (isActive ? 0.9 : 0.07) : 0.5}
					style="cursor:pointer"
					onclick={() => selectFlow(f.from, f.to)}
				/>
			{/each}

			<!-- Left bands (baseline) -->
			{#each reverseBands as band}
				{#if leftBandH[band] > 0.5}
					{@const cy = leftBandY[band] + leftBandH[band] / 2}
					<!-- svelte-ignore a11y_no_static_element_interactions -->
					<rect
						x={LEFT_X} y={leftBandY[band]}
						width={BAND_W} height={leftBandH[band]}
						fill={cfg.colors[band]}
						opacity={activeSide === 'from' && activeBand === band ? 1 : 0.6}
						rx="2"
						style="cursor:pointer"
						onclick={() => selectBand('from', band)}
					/>
					{#if leftBandH[band] > 18}
						<text
							x={LEFT_X + BAND_W / 2} y={cy - 1.5}
							text-anchor="middle" fill="rgba(0,0,0,0.75)"
							font-size="5.5" font-weight="700" font-family="system-ui,sans-serif"
							pointer-events="none"
						>{cfg.labels[band]}</text>
						<text
							x={LEFT_X + BAND_W / 2} y={cy + 5}
							text-anchor="middle" fill="rgba(0,0,0,0.65)"
							font-size="4.5" font-weight="600" font-family="system-ui,sans-serif"
							pointer-events="none"
						>{fromTotals[band].toLocaleString()} · {Math.round(fromTotals[band] / total * 100)}%</text>
					{:else if leftBandH[band] > 10}
						<text
							x={LEFT_X + BAND_W / 2} y={cy + 2.5}
							text-anchor="middle" fill="rgba(0,0,0,0.75)"
							font-size="5.5" font-weight="700" font-family="system-ui,sans-serif"
							pointer-events="none"
						>{cfg.labels[band]}</text>
					{/if}
				{/if}
			{/each}

			<!-- Right bands (actual) -->
			{#each reverseBands as band}
				{#if rightBandH[band] > 0.5}
					{@const cy = rightBandY[band] + rightBandH[band] / 2}
					<!-- svelte-ignore a11y_no_static_element_interactions -->
					<rect
						x={RIGHT_X} y={rightBandY[band]}
						width={BAND_W} height={rightBandH[band]}
						fill={cfg.colors[band]}
						opacity={activeSide === 'to' && activeBand === band ? 1 : 0.6}
						rx="2"
						style="cursor:pointer"
						onclick={() => selectBand('to', band)}
					/>
					{#if rightBandH[band] > 18}
						<text
							x={RIGHT_X + BAND_W / 2} y={cy - 1.5}
							text-anchor="middle" fill="rgba(0,0,0,0.75)"
							font-size="5.5" font-weight="700" font-family="system-ui,sans-serif"
							pointer-events="none"
						>{cfg.labels[band]}</text>
						<text
							x={RIGHT_X + BAND_W / 2} y={cy + 5}
							text-anchor="middle" fill="rgba(0,0,0,0.65)"
							font-size="4.5" font-weight="600" font-family="system-ui,sans-serif"
							pointer-events="none"
						>{toTotals[band].toLocaleString()} · {Math.round(toTotals[band] / total * 100)}%</text>
					{:else if rightBandH[band] > 10}
						<text
							x={RIGHT_X + BAND_W / 2} y={cy + 2.5}
							text-anchor="middle" fill="rgba(0,0,0,0.75)"
							font-size="5.5" font-weight="700" font-family="system-ui,sans-serif"
							pointer-events="none"
						>{cfg.labels[band]}</text>
					{/if}
				{/if}
			{/each}

			<!-- Period labels -->
			<text x={LEFT_X + BAND_W / 2} y={PAD_T - 7} text-anchor="middle"
				fill="rgba(255,255,255,0.55)" font-size="6" font-family="system-ui,sans-serif"
			>{temporalPeriods?.baseline ?? 'Baseline'}</text>
			<text x={RIGHT_X + BAND_W / 2} y={PAD_T - 7} text-anchor="middle"
				fill="rgba(255,255,255,0.55)" font-size="6" font-family="system-ui,sans-serif"
			>{temporalPeriods?.current ?? 'Actual'}</text>

			<!-- Summary inside the SVG so PNG/SVG exports are self-contained -->
			<text x={SVG_W / 2} y={SVG_H - 6} text-anchor="middle"
				font-size="5.5" font-family="system-ui,sans-serif"
			><tspan fill="#22d3ee">↑{improved.toLocaleString()} mejoraron</tspan><tspan fill="rgba(255,255,255,0.35)"> · </tspan><tspan fill="#f87171">↓{worsened.toLocaleString()} empeoraron</tspan><tspan fill="rgba(255,255,255,0.35)"> · </tspan><tspan fill="rgba(148,163,184,0.9)">={stable.toLocaleString()} estables</tspan></text>
		</svg>
		<div class="fc-note">{cfg.note}</div>
	{/if}
	</div>
</ChartFrame>

<style>
	.fc-panel {
		padding: 2px 0;
	}
	.fc-subheader {
		display: flex;
		align-items: baseline;
		gap: 6px;
		margin-bottom: 2px;
		padding: 0 2px;
		min-height: 14px;
		flex-wrap: wrap;
	}
	.fc-active {
		font-size: 8px;
		color: #a78bfa;
		display: flex;
		align-items: baseline;
		gap: 5px;
	}
	.fc-hint {
		font-size: 8px;
		color: rgba(255,255,255,0.25);
		font-style: italic;
	}
	.fc-clear {
		font-size: 7.5px;
		color: rgba(255,255,255,0.3);
		background: none;
		border: none;
		cursor: pointer;
		padding: 0;
		line-height: 1;
	}
	.fc-clear:hover { color: #f87171; }
	.fc-note {
		font-size: 7.5px;
		color: rgba(255,255,255,0.22);
		line-height: 1.5;
		padding: 3px 2px 2px;
		border-top: 1px solid rgba(255,255,255,0.06);
		margin-top: 1px;
	}
</style>
