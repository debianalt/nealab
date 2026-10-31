<script lang="ts">
	import { i18n } from '$lib/stores/i18n.svelte';
	import * as d3 from 'd3';
	import { gridDisk, getResolution } from 'h3-js';
	import ChartFrame from './ChartFrame.svelte';
	import { CHART_PX, INK, fitLabel, dockBoost } from '$lib/utils/chartType';

	let {
		data = new Map() as Map<string, Record<string, any>>,
		variable = '',
		label = '',
		boundaryCache = new Map() as Map<string, number[][]>,
		onShowLisa = (_entries: { h3index: string; value: number; boundary?: number[][] }[]) => {},
		onBrushSelect = (_h3s: string[]) => {},
		defaultCollapsed = true,
	} = $props();

	const allH3s = $derived([...data.keys()]);
	let docked = $state(false);

	// The viewBox is fixed at draw time, so docking the chart beside the map would
	// just magnify it. Redraw when the box changes size noticeably (clears the brush).
	let drawnW = 0;
	$effect(() => {
		if (!container) return;
		const ro = new ResizeObserver(() => {
			const w = container!.clientWidth;
			if (w > 0 && drawnW > 0 && Math.abs(w - drawnW) / drawnW > 0.15) draw();
		});
		ro.observe(container);
		return () => ro.disconnect();
	});

	let container: HTMLDivElement | undefined = $state();
	let computing = $state(false);
	let brushCount = $state(0);
	let lisaMode = $state(false);
	let result = $state<{
		I: number; expected: number; variance: number; z: number; p: number; n: number; res: number;
		points: { h3: string; std: number; lag: number; quad: string }[];
	} | null>(null);

	// Plain-language reading of I (row-standardised weights, contiguous H3 cells).
	function readingKey(I: number): string {
		if (I >= 0.5) return 'chart.moran.read.strong';
		if (I >= 0.3) return 'chart.moran.read.moderate';
		if (I >= 0.1) return 'chart.moran.read.weak';
		if (I > -0.1) return 'chart.moran.read.none';
		return 'chart.moran.read.dispersed';
	}

	$effect(() => {
		if (variable && data.size > 0 && allH3s.length > 0) compute();
	});

	function csvRows() {
		if (!result) return [];
		return result.points.map(p => ({
			h3index: p.h3,
			std: p.std.toFixed(4),
			lag: p.lag.toFixed(4),
			quadrant: p.quad,
		}));
	}

	$effect(() => {
		if (result && container) draw();
	});

	function compute() {
		computing = true;
		result = null;

		requestAnimationFrame(() => {
			const vals = new Map<string, number>();
			let sum = 0, n = 0;

			for (const h3 of allH3s) {
				const row = data.get(h3);
				const v = row?.[variable];
				if (typeof v === 'number' && !isNaN(v)) { vals.set(h3, v); sum += v; n++; }
			}
			if (n < 10) { computing = false; return; }

			const mean = sum / n;
			let variance = 0;
			for (const v of vals.values()) variance += (v - mean) ** 2;
			variance /= n;
			const sd = Math.sqrt(variance);
			if (sd < 1e-10) { computing = false; return; }

			const std = new Map<string, number>();
			for (const [h3, v] of vals) std.set(h3, (v - mean) / sd);

			// Row-standardised contiguity weights (w_ij = 1/k_i, as GeoDa): the spatial
			// lag is the neighbours' mean and I is the slope of the Moran scatterplot.
			const nbrs = new Map<string, string[]>();
			for (const h3 of vals.keys()) {
				const nb = gridDisk(h3, 1).filter(c => c !== h3 && vals.has(c));
				if (nb.length > 0) nbrs.set(h3, nb);
			}
			const lag = new Map<string, number>();
			let S0 = 0, S1 = 0, S2 = 0, num = 0;
			for (const [h3, nb] of nbrs) {
				const k = nb.length;
				let lagSum = 0, colSum = 0;
				for (const c of nb) {
					lagSum += std.get(c)!;
					const kc = nbrs.get(c)!.length;       // symmetric contiguity: c has h3 as neighbour
					S1 += (1 / k + 1 / kc) ** 2;          // ½ Σ_ij (w_ij + w_ji)², summed over ordered pairs
					colSum += 1 / kc;                     // w_·i
				}
				const li = lagSum / k;
				lag.set(h3, li);
				num += std.get(h3)! * li;
				S0 += 1;                                  // each row sums to 1
				S2 += (1 + colSum) ** 2;                  // (w_i· + w_·i)²
			}
			S1 /= 2;

			let sumZ2 = 0;
			for (const z of std.values()) sumZ2 += z * z;

			const I = S0 > 0 ? (n / S0) * num / sumZ2 : 0;
			const expected = -1 / (n - 1);
			// Variance under the normality assumption (Cliff & Ord 1981).
			const varI = S0 > 0
				? (n * n * S1 - n * S2 + 3 * S0 * S0) / ((n * n - 1) * S0 * S0) - expected * expected
				: 0;
			const z = varI > 0 ? (I - expected) / Math.sqrt(varI) : 0;
			const p = 2 * (1 - normalCDF(Math.abs(z)));
			let res = 9;
			for (const h3 of vals.keys()) { try { res = getResolution(h3); } catch {} break; }

			const points: { h3: string; std: number; lag: number; quad: string }[] = [];
			for (const h3 of vals.keys()) {
				const zi = std.get(h3)!;
				const li = lag.get(h3);
				if (li === undefined) continue;
				let quad: string;
				if (zi > 0 && li > 0) quad = 'HH';
				else if (zi < 0 && li < 0) quad = 'LL';
				else if (zi > 0 && li < 0) quad = 'HL';
				else quad = 'LH';
				points.push({ h3, std: zi, lag: li, quad });
			}

			result = { I, expected, variance: varI, z, p, n, res, points };
			computing = false;
		});
	}

	function normalCDF(x: number): number {
		const a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741, a4 = -1.453152027, a5 = 1.061405429;
		const p = 0.3275911;
		const sign = x < 0 ? -1 : 1;
		x = Math.abs(x) / Math.sqrt(2);
		const t = 1 / (1 + p * x);
		const y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);
		return 0.5 * (1 + sign * y);
	}

	const QUAD_COLORS: Record<string, string> = {
		'HH': '#ef4444', 'LL': '#3b82f6', 'HL': '#f97316', 'LH': '#60a5fa'
	};

	function draw() {
		if (!result || !container) return;
		brushCount = 0;
		onBrushSelect([]);
		const { I, points } = result;

		// Use container size at first draw as the canonical viewBox, then render the SVG
		// at 100% width/height with preserveAspectRatio. The viewBox stays fixed so labels
		// + dots scale UNIFORMLY when the container grows (expand to floating panel) — the
		// same approach the other 5 charts use declaratively.
		const W = container.clientWidth || 250;
		const H = container.clientHeight || 180;
		drawnW = container.clientWidth;
		const fs = (px: number) => `${px * dockBoost(docked)}px`;
		const m = { top: 10, right: 12, bottom: 36, left: 46 };
		const w = W - m.left - m.right;
		const h = H - m.top - m.bottom;

		d3.select(container).selectAll('*').remove();
		const svg = d3.select(container).append('svg')
			.attr('width', '100%')
			.attr('height', '100%')
			.attr('viewBox', `0 0 ${W} ${H}`)
			.attr('preserveAspectRatio', 'xMidYMid meet')
			.style('display', 'block');
		const g = svg.append('g').attr('transform', `translate(${m.left},${m.top})`);

		const sample = points.length > 2000
			? points.filter((_, i) => i % Math.ceil(points.length / 2000) === 0)
			: points;

		const xExt = d3.extent(sample, p => p.std) as [number, number];
		const yExt = d3.extent(sample, p => p.lag) as [number, number];
		const ext = Math.max(Math.abs(xExt[0]), Math.abs(xExt[1]), Math.abs(yExt[0]), Math.abs(yExt[1])) * 1.1;

		const x = d3.scaleLinear().domain([-ext, ext]).range([0, w]);
		const y = d3.scaleLinear().domain([-ext, ext]).range([h, 0]);

		g.append('rect').attr('x', x(0)).attr('y', 0).attr('width', w - x(0)).attr('height', y(0))
			.attr('fill', '#ef44441a');
		g.append('rect').attr('x', 0).attr('y', y(0)).attr('width', x(0)).attr('height', h - y(0))
			.attr('fill', '#3b82f61a');
		g.append('rect').attr('x', x(0)).attr('y', y(0)).attr('width', w - x(0)).attr('height', h - y(0))
			.attr('fill', '#f973161a');
		g.append('rect').attr('x', 0).attr('y', 0).attr('width', x(0)).attr('height', y(0))
			.attr('fill', '#60a5fa1a');

		g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(0)).attr('y2', y(0))
			.attr('stroke', '#334155').attr('stroke-width', 0.5);
		g.append('line').attr('x1', x(0)).attr('x2', x(0)).attr('y1', 0).attr('y2', h)
			.attr('stroke', '#334155').attr('stroke-width', 0.5);

		g.append('line')
			.attr('x1', x(-ext)).attr('y1', y(-ext * I))
			.attr('x2', x(ext)).attr('y2', y(ext * I))
			.attr('stroke', '#f59e0b').attr('stroke-width', 1.5)
			.attr('stroke-dasharray', '6,4').attr('opacity', 0.7);

		const dots = g.selectAll('circle').data(sample).join('circle')
			.attr('cx', d => x(d.std)).attr('cy', d => y(d.lag))
			.attr('r', 2).attr('fill', d => QUAD_COLORS[d.quad]).attr('opacity', 0.5);

		const fontSize = fs(CHART_PX.tick);
		g.append('text').attr('x', w - 4).attr('y', 14)
			.attr('text-anchor', 'end').attr('fill', '#ef4444').attr('font-size', fontSize).text(i18n.t('chart.moran.qHH'));
		g.append('text').attr('x', 4).attr('y', h - 5)
			.attr('fill', '#3b82f6').attr('font-size', fontSize).text(i18n.t('chart.moran.qLL'));
		g.append('text').attr('x', w - 4).attr('y', h - 5)
			.attr('text-anchor', 'end').attr('fill', '#f97316').attr('font-size', fontSize).text(i18n.t('chart.moran.qHL'));
		g.append('text').attr('x', 4).attr('y', 14)
			.attr('fill', '#60a5fa').attr('font-size', fontSize).text(i18n.t('chart.moran.qLH'));

		// Axis titles trimmed to the plot: the panel font is wide and labels are long.
		const family = getComputedStyle(container).fontFamily;
		const xTitle = label || variable;
		svg.append('text').attr('x', m.left + w / 2).attr('y', H - 4)
			.attr('text-anchor', 'middle').attr('fill', INK.label).attr('font-size', fs(CHART_PX.label))
			.text(fitLabel(xTitle, w, CHART_PX.label * dockBoost(docked), family, ` ${i18n.t('chart.moran.std')}`))
			.append('title').text(xTitle);
		svg.append('text').attr('x', 12).attr('y', m.top + h / 2)
			.attr('text-anchor', 'middle').attr('fill', INK.label).attr('font-size', fs(CHART_PX.label))
			.attr('transform', `rotate(-90,12,${m.top + h / 2})`)
			.text(fitLabel(i18n.t('chart.moran.lag'), h, CHART_PX.label * dockBoost(docked), family));

		g.append('g').attr('transform', `translate(0,${h})`)
			.call(d3.axisBottom(x).ticks(5))
			.call(g => g.selectAll('text').attr('fill', INK.tick).attr('font-size', fs(CHART_PX.tick)))
			.call(g => g.selectAll('line,path').attr('stroke', '#334155'));
		g.append('g')
			.call(d3.axisLeft(y).ticks(5))
			.call(g => g.selectAll('text').attr('fill', INK.tick).attr('font-size', fs(CHART_PX.tick)))
			.call(g => g.selectAll('line,path').attr('stroke', '#334155'));

		// Manual brush — d3.brush can be blocked by overflow:auto scroll containers
		let bx0 = 0, by0 = 0, bx1 = 0, by1 = 0;
		const brushRect = g.append('rect')
			.attr('class', 'brush-rect')
			.attr('fill', 'rgba(251,191,36,0.1)')
			.attr('stroke', '#fbbf24')
			.attr('stroke-width', 0.8)
			.attr('pointer-events', 'none')
			.style('display', 'none');

		g.append('rect')
			.attr('class', 'brush-overlay')
			.attr('x', 0).attr('y', 0)
			.attr('width', w).attr('height', h)
			.attr('fill', 'none')
			.attr('pointer-events', 'all')
			.attr('cursor', 'crosshair')
			.on('mousedown', function(event: MouseEvent) {
				event.preventDefault();
				[bx0, by0] = d3.pointer(event);
				bx1 = bx0; by1 = by0;
				brushRect.style('display', null).attr('x', bx0).attr('y', by0).attr('width', 0).attr('height', 0);

				function onMove(e: MouseEvent) {
					const [mx, my] = d3.pointer(e, g.node()!);
					bx1 = Math.max(0, Math.min(w, mx));
					by1 = Math.max(0, Math.min(h, my));
					brushRect
						.attr('x', Math.min(bx0, bx1)).attr('y', Math.min(by0, by1))
						.attr('width', Math.abs(bx1 - bx0)).attr('height', Math.abs(by1 - by0));
				}

				function onUp() {
					window.removeEventListener('mousemove', onMove);
					window.removeEventListener('mouseup', onUp);
					if (Math.abs(bx1 - bx0) < 3 && Math.abs(by1 - by0) < 3) {
						brushRect.style('display', 'none');
						dots.attr('opacity', 0.5).attr('r', 2);
						brushCount = 0;
						onBrushSelect([]);
						return;
					}
					const x0 = x.invert(Math.min(bx0, bx1));
					const x1 = x.invert(Math.max(bx0, bx1));
					const y0 = y.invert(Math.max(by0, by1));
					const y1 = y.invert(Math.min(by0, by1));
					const selectedSet = new Set(
						result!.points
							.filter(p => p.std >= x0 && p.std <= x1 && p.lag >= y0 && p.lag <= y1)
							.map(p => p.h3)
					);
					dots.each(function(d: any) {
						const inside = selectedSet.has(d.h3);
						d3.select(this).attr('opacity', inside ? 1 : 0.08).attr('r', inside ? 3 : 2);
					});
					brushCount = selectedSet.size;
					onBrushSelect([...selectedSet]);
				}

				window.addEventListener('mousemove', onMove);
				window.addEventListener('mouseup', onUp);
			});
	}

	function clearBrush() {
		if (container) {
			d3.select(container).selectAll<SVGCircleElement, unknown>('circle')
				.attr('opacity', 0.5).attr('r', 2);
			d3.select(container).select('.brush-rect').style('display', 'none');
			brushCount = 0;
			onBrushSelect([]);
		}
	}

	function toggleLisa() {
		if (lisaMode) {
			lisaMode = false;
			onShowLisa([]);
			return;
		}
		if (!result) return;
		const LISA_COLORS: Record<string, number> = { 'HH': 4, 'LL': 1, 'HL': 3, 'LH': 2 };
		const entries = result.points.map(p => ({
			h3index: p.h3,
			value: LISA_COLORS[p.quad] ?? 0,
			boundary: boundaryCache.get(p.h3),
		}));
		lisaMode = true;
		onShowLisa(entries);
	}

</script>

<ChartFrame bind:expanded={docked} title={i18n.t('chart.moran.title')} csvRows={csvRows} csvFilename="spatia_moran" {defaultCollapsed}>
	<div class="moran-body">
			{#if computing}
				<div class="moran-loading">
					<div class="moran-spinner"></div>
					{i18n.t('chart.moran.computing')}
				</div>
			{:else if result}
				<div class="moran-stats">
					<span class="moran-I">I = {result.I.toFixed(4)}</span>
					<span class="moran-dim">E[I] = {result.expected.toFixed(4)}</span>
					<span class="moran-dim">z = {result.z.toFixed(2)}</span>
					<span class="{result.p < 0.05 ? 'moran-sig' : 'moran-insig'}">p {result.p < 0.001 ? '< 0.001' : '= ' + result.p.toFixed(3)}</span>
					<span class="moran-dim">n = {result.n.toLocaleString(i18n.locale)} {result.res < 9 ? i18n.t('chart.moran.nAggregated').replace('{res}', String(result.res)) : i18n.t('chart.moran.nHex')}</span>
					{#if brushCount > 0}
						<span class="moran-sel">{brushCount} sel.</span>
						<button class="moran-btn-clear" onclick={clearBrush}>✕</button>
					{/if}
				</div>
				<p class="moran-read">{i18n.t(readingKey(result.I))} <span class="moran-dim">{i18n.t('chart.moran.pNote')}</span></p>
				<div class="moran-mapview">
					<span class="moran-dim">{i18n.t('chart.moran.map')}</span>
					<button class="moran-view-btn" class:moran-view-btn-active={!lisaMode}
						onclick={() => { if (lisaMode) { lisaMode = false; onShowLisa([]); } }}>
						{i18n.t('chart.moran.choropleth')}
					</button>
					<button class="moran-view-btn" class:moran-view-btn-active={lisaMode}
						onclick={() => { if (!lisaMode) toggleLisa(); }} title={i18n.t('chart.moran.quadrantsHint')}>
						{i18n.t('chart.moran.quadrants')}
					</button>
				</div>
				<div bind:this={container} class="moran-plot"></div>
				<div class="moran-quads">
					{#each ['HH', 'LL', 'HL', 'LH'] as q}
						<span style="color: {QUAD_COLORS[q]}">{i18n.t(`chart.moran.q${q}`)}: {result.points.filter(p => p.quad === q).length.toLocaleString(i18n.locale)}</span>
					{/each}
				</div>
			{:else if data.size > 0}
				<div class="moran-empty">{i18n.t('chart.moran.insufficient')}</div>
			{:else}
				<div class="moran-empty">{i18n.t('chart.moran.loadLayer')}</div>
			{/if}
	</div>
</ChartFrame>

<style>
	.moran-read {
		font-size: 12px; color: #cbd5e1; line-height: 1.4;
		margin: 2px 0 6px;
	}
	.moran-wrap {
		margin-top: 10px;
		border-top: 1px solid rgba(255,255,255,0.07);
		padding-top: 6px;
	}
	.moran-toggle {
		display: flex;
		align-items: center;
		gap: 5px;
		font-size: 12px;
		color: #a3a3a3;
		background: none;
		border: none;
		cursor: pointer;
		padding: 2px 0;
		width: 100%;
		text-align: left;
		transition: color 0.15s;
	}
	.moran-toggle:hover { color: #d4d4d4; }
	.moran-arrow { font-size: 11px; }
	.moran-badge {
		margin-left: auto;
		font-family: var(--font-mono);
		font-size: 11px;
		color: #f59e0b;
	}
	.moran-body {
		margin-top: 6px;
		display: flex;
		flex-direction: column;
		gap: 4px;
	}
	.moran-loading {
		display: flex;
		align-items: center;
		gap: 6px;
		font-size: 12px;
		color: #9ca3af;
		padding: 8px 0;
	}
	.moran-spinner {
		width: 10px;
		height: 10px;
		border: 2px solid #6366f1;
		border-top-color: transparent;
		border-radius: 50%;
		animation: spin 0.7s linear infinite;
	}
	@keyframes spin { to { transform: rotate(360deg); } }
	.moran-stats {
		display: flex;
		align-items: center;
		gap: 6px;
		flex-wrap: wrap;
		font-size: 12px;
		font-family: var(--font-mono);
	}
	.moran-I { color: #f59e0b; font-weight: bold; }
	.moran-dim { color: #9ca3af; }
	.moran-sig { color: #4ade80; }
	.moran-insig { color: #f87171; }
	.moran-sel { color: #fbbf24; font-size: 11px; }
	.moran-btn-clear {
		font-size: 11px;
		color: #fbbf24;
		background: none;
		border: none;
		cursor: pointer;
		padding: 0 2px;
		transition: color 0.15s;
	}
	.moran-btn-clear:hover { color: #fde68a; }
	.moran-btn-csv {
		margin-left: auto;
		font-size: 11px;
		color: #9ca3af;
		background: none;
		border: none;
		cursor: pointer;
		padding: 0 2px;
		transition: color 0.15s;
	}
	.moran-btn-csv:hover { color: #6366f1; }
	.moran-mapview {
		display: flex;
		align-items: center;
		gap: 5px;
	}
	.moran-view-btn {
		font-size: 11px;
		padding: 2px 8px;
		border: 1px solid #334155;
		border-radius: 3px;
		background: none;
		color: #9ca3af;
		cursor: pointer;
		font-family: var(--font-sans);
		transition: all 0.12s;
	}
	.moran-view-btn:hover { color: #d4d4d4; border-color: #4b5563; }
	.moran-view-btn-active {
		background: rgba(99,102,241,0.15);
		border-color: #6366f1;
		color: #a5b4fc;
	}
	.moran-plot {
		width: 100%;
		height: 220px;
		user-select: none;
	}
	/* When inside ChartFrame's expanded floating panel, let the plot grow */
	:global(.chart-frame-expanded) .moran-body {
		display: flex;
		flex-direction: column;
		height: 100%;
		gap: 6px;
	}
	:global(.chart-frame-expanded) .moran-plot {
		flex: 1;
		height: auto;
		min-height: 300px;
	}
	.moran-quads {
		display: flex;
		flex-wrap: wrap;
		gap: 2px 12px;
		font-size: 11px;
	}
	.moran-quads span { white-space: nowrap; }
	.moran-empty {
		font-size: 12px;
		color: #9ca3af;
		padding: 6px 0;
	}
</style>
