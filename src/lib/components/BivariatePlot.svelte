<script lang="ts">
	import { query } from '$lib/stores/duckdb';
	import { ANALYSIS_REGISTRY, HEX_LAYER_REGISTRY, getSatGlobalUrl, getHexAxisLabel } from '$lib/config';
	import { cellToParent, getResolution } from 'h3-js';
	import { loadDeptSummary } from '$lib/utils/deptSummaries';
	import { normDeptName } from '$lib/utils/deptBoundaries';
	import { i18n } from '$lib/stores/i18n.svelte';
	import ChartFrame from './ChartFrame.svelte';
	import { CHART_PX, INK, svgScale, median, fitLabel, dockBoost, measure, quantile, BIVAR_COLORS } from '$lib/utils/chartType';
	import { untrack, onDestroy } from 'svelte';

	let {
		data = new Map() as Map<string, Record<string, any>>,
		variable = '',
		xLabel = 'score',
		analysisId = '',
		territoryPrefix = '',
		territoryId = '',
		deptName = null as string | null,
		deptKey = null as string | null,
		deptUrl = (_id: string, _key: string) => null as string | null,
		onBrushSelect = (_h3s: string[]) => {},
		onShowBivariateMap = (_entries: { h3index: string; value: number | null; nodata?: boolean }[]) => {},
		defaultCollapsed = true,
	}: {
		data: Map<string, Record<string, any>>;
		variable: string;
		xLabel?: string;
		analysisId?: string;
		territoryPrefix?: string;
		territoryId?: string;
		deptName?: string | null;
		deptKey?: string | null;
		// Per-department parquet URL of a layer in the active territory.
		deptUrl?: (layerId: string, key: string) => string | null;
		onBrushSelect?: (h3s: string[]) => void;
		// Paints the map with the 3×3 classes ([] restores the layer's choropleth).
		onShowBivariateMap?: (entries: { h3index: string; value: number | null; nodata?: boolean }[]) => void;
		defaultCollapsed?: boolean;
	} = $props();

	const PAD_L = 44, PAD_R = 8, PAD_T = 8, PAD_B = 38;
	const SVG_W = 260, SVG_H = 236;
	const plotW = SVG_W - PAD_L - PAD_R;
	const plotH = SVG_H - PAD_T - PAD_B;

	// Y candidates: comparable hexagon layers available in this territory.
	const comparableAnalyses = $derived(
		ANALYSIS_REGISTRY.filter(a => a.comparable && a.id !== analysisId && HEX_LAYER_REGISTRY[a.id]
			&& (a.coverage as Record<string, string> | undefined)?.[territoryId] !== 'unavailable')
	);
	$effect(() => { if (selectedB && !comparableAnalyses.some(a => a.id === selectedB)) selectedB = ''; });

	// Resolution of the cells the panel shows: a large department is aggregated to a
	// coarser H3 level (hexStore LOD), so Y has to be averaged to that same level or
	// no key ever matches.
	const xRes = $derived.by(() => {
		for (const h3 of data.keys()) { try { return getResolution(h3); } catch { return 9; } }
		return 9;
	});

	// Y's parquet for the open department (its own key: census layers name some
	// departments differently), else the territory-wide parquet.
	async function yUrl(b: string): Promise<string> {
		if (deptName && deptKey) {
			const summary = await loadDeptSummary(b, territoryPrefix);
			const want = normDeptName(deptName);
			const d = (summary?.departments as any[] | undefined)?.find(
				(x) => normDeptName(x.dpto ?? x.distrito ?? x.municipio) === want);
			const url = deptUrl(b, d?.parquetKey ?? deptKey);
			if (url) return url;
		}
		return getSatGlobalUrl(b, territoryPrefix);
	}

	let selectedB = $state('');
	let bVar = $state('');
	let bData = $state(new Map<string, number>());
	let bLoading = $state(false);
	let bError = $state(false);

	$effect(() => {
		const b = selectedB;
		if (!b) { bData = new Map(); return; }
		const layer = HEX_LAYER_REGISTRY[b];
		if (!layer) return;
		const res = xRes;
		void deptKey; void deptName; // reload Y when another department opens
		bLoading = true;
		bError = false;
		(async () => {
			const url = await yUrl(b);
			// The configured primary can be missing from a territory's parquet (hexStore
			// substitutes one); fall back to the first declared variable present.
			const desc = await query(`DESCRIBE SELECT * FROM read_parquet('${url}')`);
			const cols = new Set<string>();
			for (let i = 0; i < desc.numRows; i++) cols.add(String(desc.get(i)!.toJSON().column_name));
			const col = cols.has(layer.primaryVariable) ? layer.primaryVariable
				: layer.variables.map(v => v.col).find(c => cols.has(c) && !c.endsWith('_label') && c !== 'type');
			if (!col) throw new Error('no Y column');
			const result = await query(`SELECT h3index, "${col}" as val FROM read_parquet('${url}')`);
			const sum = new Map<string, number>(), cnt = new Map<string, number>();
			for (let i = 0; i < result.numRows; i++) {
				const row = result.get(i)!.toJSON();
				const v = Number(row.val);
				if (!isFinite(v)) continue;
				let h3 = String(row.h3index);
				if (res < 9) { try { h3 = cellToParent(h3, res); } catch { continue; } }
				sum.set(h3, (sum.get(h3) ?? 0) + v);
				cnt.set(h3, (cnt.get(h3) ?? 0) + 1);
			}
			const m = new Map<string, number>();
			for (const [h3, s] of sum) m.set(h3, s / cnt.get(h3)!);
			return { m, col };
		})()
			.then(({ m, col }) => {
				if (selectedB !== b) return; // superseded by another pick
				bData = m;
				bVar = col;
				bLoading = false;
			})
			.catch(() => {
				bError = true;
				bLoading = false;
			});
	});

	const yLabel = $derived.by(() => {
		if (!selectedB) return '';
		return getHexAxisLabel(HEX_LAYER_REGISTRY[selectedB], bVar || undefined);
	});

	let renderedW = $state(0), renderedH = $state(0);
	let docked = $state(false);
	const k = $derived(svgScale(SVG_W, SVG_H, renderedW, renderedH) * dockBoost(docked));

	const allPoints = $derived.by(() => {
		if (!selectedB || bData.size === 0) return [] as { x: number; y: number; h3: string }[];
		const pts: { x: number; y: number; h3: string }[] = [];
		for (const [h3, row] of data) {
			const x = Number(row[variable]);
			const y = bData.get(h3);
			if (y !== undefined && isFinite(x) && isFinite(y)) {
				pts.push({ x, y, h3 });
			}
		}
		return pts;
	});

	const renderedPoints = $derived.by(() => {
		const MAX = 2500;
		if (allPoints.length <= MAX) return allPoints;
		const stride = Math.ceil(allPoints.length / MAX);
		return allPoints.filter((_, i) => i % stride === 0);
	});

	// Data-driven domains over the plotted points, so each axis fills the canvas in
	// whatever physical unit its layer carries (not a fixed 0–100). Fallback [0,100]
	// when empty; +1 span guard avoids divide-by-zero when all values are equal.
	const xDom = $derived.by(() => {
		let lo = Infinity, hi = -Infinity;
		for (const p of allPoints) { if (p.x < lo) lo = p.x; if (p.x > hi) hi = p.x; }
		if (lo === Infinity) return [0, 100] as [number, number];
		if (hi === lo) return [lo, lo + 1] as [number, number];
		return [lo, hi] as [number, number];
	});
	const yDom = $derived.by(() => {
		let lo = Infinity, hi = -Infinity;
		for (const p of allPoints) { if (p.y < lo) lo = p.y; if (p.y > hi) hi = p.y; }
		if (lo === Infinity) return [0, 100] as [number, number];
		if (hi === lo) return [lo, lo + 1] as [number, number];
		return [lo, hi] as [number, number];
	});

	function fmtTick(v: number): string {
		const a = Math.abs(v);
		if (a >= 100) return v.toFixed(0);
		if (a >= 1) return v.toFixed(1);
		return v.toFixed(2);
	}

	// Quadrants split at each axis median (mid-range splits mean nothing on skewed data).
	const xMed = $derived(median(allPoints.map(p => p.x)));
	const yMed = $derived(median(allPoints.map(p => p.y)));

	// Value at a fraction (0..1) of each axis, for ticks.
	function xAt(f: number): number { const [lo, hi] = xDom; return lo + f * (hi - lo); }
	function yAt(f: number): number { const [lo, hi] = yDom; return lo + f * (hi - lo); }

	function svgX(v: number): number { const [lo, hi] = xDom; return PAD_L + ((v - lo) / (hi - lo)) * plotW; }
	function svgY(v: number): number { const [lo, hi] = yDom; return PAD_T + (1 - (v - lo) / (hi - lo)) * plotH; }

	function clientToData(clientX: number, clientY: number, rect: DOMRect): [number, number] {
		const [xLo, xHi] = xDom;
		const [yLo, yHi] = yDom;
		const scaleX = SVG_W / rect.width;
		const scaleY = SVG_H / rect.height;
		const lx = (clientX - rect.left) * scaleX;
		const ly = (clientY - rect.top) * scaleY;
		const fx = Math.max(0, Math.min(1, (lx - PAD_L) / plotW));
		const fy = Math.max(0, Math.min(1, 1 - (ly - PAD_T) / plotH));
		return [xLo + fx * (xHi - xLo), yLo + fy * (yHi - yLo)];
	}

	let brushRect: { x0: number; y0: number; x1: number; y1: number } | null = $state(null);
	let selectedCount = $state(0);

	function startBrush(e: MouseEvent) {
		if (!selectedB || bData.size === 0) return;
		activeQuad = null;
		const svgEl = e.currentTarget as SVGElement;
		const rect = svgEl.getBoundingClientRect();
		const [sx, sy] = clientToData(e.clientX, e.clientY, rect);
		let ex = sx, ey = sy;
		brushRect = { x0: sx, y0: sy, x1: sx, y1: sy };

		const onMove = (ev: MouseEvent) => {
			[ex, ey] = clientToData(ev.clientX, ev.clientY, rect);
			brushRect = {
				x0: Math.min(sx, ex), y0: Math.min(sy, ey),
				x1: Math.max(sx, ex), y1: Math.max(sy, ey),
			};
		};

		const onUp = () => {
			window.removeEventListener('mousemove', onMove);
			window.removeEventListener('mouseup', onUp);
			const br = brushRect;
			// Tiny drag → treat as a click and clear. Threshold is 2% of each axis span
			// so it works regardless of physical unit (tC/ha vs 0–1 fractions).
			const xEps = (xDom[1] - xDom[0]) * 0.02;
			const yEps = (yDom[1] - yDom[0]) * 0.02;
			if (!br || (br.x1 - br.x0 < xEps && br.y1 - br.y0 < yEps)) {
				clearBrush();
				return;
			}
			const h3s: string[] = [];
			for (const pt of allPoints) {
				if (pt.x >= br.x0 && pt.x <= br.x1 && pt.y >= br.y0 && pt.y <= br.y1) {
					h3s.push(pt.h3);
				}
			}
			selectedCount = h3s.length;
			onBrushSelect(h3s);
		};

		window.addEventListener('mousemove', onMove);
		window.addEventListener('mouseup', onUp);
	}

	function clearBrush() {
		brushRect = null;
		activeQuad = null;
		activeCell = null;
		selectedCount = 0;
		onBrushSelect([]);
	}

	// Corner labels select their whole quadrant (relative to the medians).
	type Quad = 'hh' | 'lh' | 'hl' | 'll'; // first letter X, second Y
	let activeQuad: Quad | null = $state(null);
	const QUADS: { q: Quad; xHi: boolean; yHi: boolean }[] = [
		{ q: 'lh', xHi: false, yHi: true }, { q: 'hh', xHi: true, yHi: true },
		{ q: 'll', xHi: false, yHi: false }, { q: 'hl', xHi: true, yHi: false },
	];
	function quadLabel(xHi: boolean, yHi: boolean): string {
		const hi = i18n.t('chart.bivariate.hi'), lo = i18n.t('chart.bivariate.lo');
		return `X ${xHi ? hi : lo} · Y ${yHi ? hi : lo}`;
	}
	// The high side is strict: zero-inflated layers have median 0, and "high"
	// must not swallow every zero.
	function inQuad(pt: { x: number; y: number }, q: Quad): boolean {
		const xOk = q[0] === 'h' ? pt.x > xMed : pt.x <= xMed;
		const yOk = q[1] === 'h' ? pt.y > yMed : pt.y <= yMed;
		return xOk && yOk;
	}
	function selectQuadrant(q: Quad, xHi: boolean, yHi: boolean) {
		if (activeQuad === q) { clearBrush(); return; }
		const br = {
			x0: xHi ? xMed : xDom[0], x1: xHi ? xDom[1] : xMed,
			y0: yHi ? yMed : yDom[0], y1: yHi ? yDom[1] : yMed,
		};
		brushRect = br;
		activeQuad = q;
		const h3s = allPoints.filter(pt => inQuad(pt, q)).map(pt => pt.h3);
		selectedCount = h3s.length;
		onBrushSelect(h3s);
	}

	$effect(() => {
		void data.size;
		void variable;
		void selectedB;
		brushRect = null;
		activeQuad = null;
		selectedCount = 0;
	});

	// ── 3×3 bivariate map: terciles of each axis → 9 classes ──────────────
	// When a value repeats over more than a third of the hexagons (zero-inflated
	// layers: two thirds with no buildings in low ground) both cuts coincide and the
	// middle class would be empty: keep the repeated value as "low" and split the
	// rest at its median.
	function cuts(sorted: number[]): number[] {
		const c1 = quantile(sorted, 1 / 3), c2 = quantile(sorted, 2 / 3);
		if (c2 > c1) return [c1, c2];
		const above = sorted.filter(v => v > c1);
		return above.length > 1 ? [c1, quantile(above, 0.5)] : [c1, c2];
	}
	const tert = $derived.by(() => ({
		x: cuts(allPoints.map(p => p.x).sort((a, b) => a - b)),
		y: cuts(allPoints.map(p => p.y).sort((a, b) => a - b)),
	}));
	// Right-closed like pd.cut: ties at a cut (zero-inflated layers) stay in the lower class.
	function tercile(v: number, cuts: number[]): number {
		let c = 0;
		for (const b of cuts) if (v > b) c++;
		return c;
	}
	function classOf(pt: { x: number; y: number }): number {
		return 1 + tercile(pt.y, tert.y) * 3 + tercile(pt.x, tert.x);
	}
	const classCounts = $derived.by(() => {
		const n = new Array(9).fill(0);
		if (mapOn) for (const p of allPoints) n[classOf(p) - 1]++;
		return n;
	});

	let mapOn = $state(false);
	let activeCell: number | null = $state(null);

	function paintMap() {
		const withY = new Set<string>();
		const entries: { h3index: string; value: number | null; nodata?: boolean }[] = [];
		for (const p of allPoints) { entries.push({ h3index: p.h3, value: classOf(p) }); withY.add(p.h3); }
		// Hexagons without a Y value stay visible as "no data", not as holes.
		for (const h3 of data.keys()) if (!withY.has(h3)) entries.push({ h3index: h3, value: null, nodata: true });
		onShowBivariateMap(entries);
	}
	function toggleMap() {
		if (mapOn) { mapOn = false; activeCell = null; onShowBivariateMap([]); clearBrush(); return; }
		mapOn = true;
		paintMap();
	}
	function selectCell(cls: number) {
		if (activeCell === cls) { clearBrush(); return; }
		brushRect = null;
		activeQuad = null;
		activeCell = cls;
		const h3s = allPoints.filter(p => classOf(p) === cls).map(p => p.h3);
		selectedCount = h3s.length;
		onBrushSelect(h3s);
	}

	// New data, layer or Y: the classes no longer describe the map → switch off.
	$effect(() => {
		void data; void variable; void selectedB;
		untrack(() => {
			if (!mapOn) return;
			mapOn = false;
			activeCell = null;
			onShowBivariateMap([]);
		});
	});
	onDestroy(() => { if (mapOn) onShowBivariateMap([]); });

	function csvRows() {
		return allPoints.map(p => ({ h3index: p.h3, x: p.x.toFixed(2), y: p.y.toFixed(2), ...(mapOn ? { clase_3x3: classOf(p) } : {}) }));
	}
</script>

<ChartFrame bind:expanded={docked} title="Bivariado" csvRows={csvRows} csvFilename="spatia_bivariate" {defaultCollapsed}>
	<div class="bvpanel">
	<div class="bvpanel-subheader">
		{#if brushRect !== null || activeCell !== null}
			<span class="bvpanel-count">{selectedCount.toLocaleString()} {i18n.t('chart.hexDblClick')}</span>
		{:else if selectedB && allPoints.length > 0}
			<span class="bvpanel-hint">{allPoints.length.toLocaleString()} {i18n.t('chart.bivariate.pts')}</span>
		{:else}
			<span class="bvpanel-hint">{i18n.t('chart.bivariate.pickY')}</span>
		{/if}
	</div>

	<div class="bvpanel-picker">
		<span class="picker-label">{i18n.t('chart.bivariate.axisX')}</span>
		<span class="picker-fixed" title={xLabel}>{xLabel}</span>
	</div>
	<div class="bvpanel-picker">
		<span class="picker-label">{i18n.t('chart.bivariate.axisY')}</span>
		<select bind:value={selectedB} class="picker-select">
			<option value="">{i18n.t('chart.bivariate.pick')}</option>
			{#each comparableAnalyses as a}
				<option value={a.id}>{i18n.t(a.titleKey)}</option>
			{/each}
		</select>
	</div>

	{#if bLoading}
		<div class="bvpanel-state">cargando…</div>
	{:else if bError}
		<div class="bvpanel-state bvpanel-error">{i18n.t('chart.loadError')}</div>
	{:else if !selectedB}
		<div class="bvpanel-empty-prompt">
			{i18n.t('chart.bivariate.intro')}
		</div>
	{:else if selectedB && allPoints.length === 0 && bData.size > 0}
		<div class="bvpanel-state">{i18n.t('chart.bivariate.noCommon')}</div>
	{:else if selectedB && allPoints.length > 0}
		<div class="bv-svgwrap" use:measure={(w, h) => { renderedW = w; renderedH = h; }}>
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<svg
			width="100%"
			viewBox="0 0 {SVG_W} {SVG_H}"
			preserveAspectRatio="xMidYMid meet"
			onmousedown={startBrush}
			ondblclick={clearBrush}
			style="cursor: crosshair; display: block;"
		>
			<!-- Quadrant guide lines at each axis median -->
			{#if isFinite(xMed) && isFinite(yMed)}
				<line x1={svgX(xMed)} x2={svgX(xMed)} y1={PAD_T} y2={PAD_T + plotH}
					stroke="rgba(255,255,255,0.32)" stroke-width="1" stroke-dasharray="3 2" />
				<line x1={PAD_L} x2={PAD_L + plotW} y1={svgY(yMed)} y2={svgY(yMed)}
					stroke="rgba(255,255,255,0.32)" stroke-width="1" stroke-dasharray="3 2" />
			{/if}

			<!-- Scatter points -->
			{#each renderedPoints as pt}
				{@const inBrush = activeCell ? classOf(pt) === activeCell : activeQuad ? inQuad(pt, activeQuad) : brushRect !== null && pt.x >= brushRect.x0 && pt.x <= brushRect.x1 && pt.y >= brushRect.y0 && pt.y <= brushRect.y1}
				<circle
					cx={svgX(pt.x)} cy={svgY(pt.y)}
					r="1.4"
					fill={inBrush ? '#fb923c' : mapOn ? BIVAR_COLORS[classOf(pt) - 1] : '#94a3b8'}
					opacity={(brushRect !== null || activeCell) && !inBrush ? 0.1 : mapOn ? 0.85 : 0.5}
				/>
			{/each}

			<!-- Brush overlay rect -->
			{#if brushRect !== null}
				<rect
					x={svgX(brushRect.x0)} y={svgY(brushRect.y1)}
					width={Math.max(0, svgX(brushRect.x1) - svgX(brushRect.x0))}
					height={Math.max(0, svgY(brushRect.y0) - svgY(brushRect.y1))}
					fill="#fb923c" opacity="0.07"
					stroke="#fb923c" stroke-width="1" stroke-opacity="0.4"
					pointer-events="none"
				/>
			{/if}

			<!-- Corner labels: a click selects that quadrant on the map -->
			{#if isFinite(xMed) && isFinite(yMed)}
				{#each QUADS as c}
					<!-- svelte-ignore a11y_click_events_have_key_events -->
					<text
						x={c.xHi ? PAD_L + plotW - 3 * k : PAD_L + 3 * k}
						y={c.yHi ? PAD_T + (CHART_PX.tick + 1) * k : PAD_T + plotH - 4 * k}
						text-anchor={c.xHi ? 'end' : 'start'}
						fill={activeQuad === c.q ? '#fb923c' : INK.tick}
						font-size={CHART_PX.tick * k} font-family="system-ui, sans-serif"
						stroke="rgba(15,23,42,0.92)" stroke-width={3 * k} stroke-linejoin="round" paint-order="stroke"
						class="bv-quad"
						onmousedown={(e) => e.stopPropagation()}
						onclick={() => selectQuadrant(c.q, c.xHi, c.yHi)}
					>{quadLabel(c.xHi, c.yHi)}</text>
				{/each}
			{/if}

			<!-- X axis ticks (physical values at 0/50/100 % of the domain) -->
			{#each [0, 0.5, 1] as f}
				<text x={svgX(xAt(f))} y={PAD_T + plotH + (CHART_PX.tick + 2) * k}
					text-anchor={f === 0 ? 'start' : f === 1 ? 'end' : 'middle'} fill={INK.tick}
					font-size={CHART_PX.tick * k} font-family="system-ui, sans-serif">{fmtTick(xAt(f))}</text>
			{/each}

			<!-- Y axis ticks (physical values at 0/50/100 % of the domain) -->
			{#each [0, 0.5, 1] as f}
				<text x={PAD_L - 3 * k} y={svgY(yAt(f)) + 4 * k}
					text-anchor="end" fill={INK.tick}
					font-size={CHART_PX.tick * k} font-family="system-ui, sans-serif">{fmtTick(yAt(f))}</text>
			{/each}

			<!-- X axis title -->
			<text x={PAD_L + plotW / 2} y={PAD_T + plotH + (CHART_PX.tick + CHART_PX.label + 6) * k}
				text-anchor="middle" fill={INK.label}
				font-size={CHART_PX.label * k} font-family="system-ui, sans-serif"
			><title>{xLabel}</title>{fitLabel(`X: ${xLabel}`, plotW / k, CHART_PX.label)}</text>

			<!-- Y axis title (rotated) -->
			<text
				x={CHART_PX.label * k} y={PAD_T + plotH / 2}
				text-anchor="middle" fill={INK.label}
				font-size={CHART_PX.label * k} font-family="system-ui, sans-serif"
				transform={`rotate(-90, ${CHART_PX.label * k}, ${PAD_T + plotH / 2})`}
			><title>{yLabel}</title>{fitLabel(`Y: ${yLabel}`, plotH / k, CHART_PX.label)}</text>
		</svg>
		</div>
		<div class="bvpanel-note">{i18n.t('chart.bivariate.medianNote')}</div>
		<button class="bv-mapbtn" class:bv-mapbtn-on={mapOn} onclick={toggleMap}>
			{mapOn ? i18n.t('chart.bivariate.paintMapOff') : i18n.t('chart.bivariate.paintMap')}
		</button>
		{#if mapOn}
			<div class="bv-legend">
				<div class="bv-legend-y">Y ↑</div>
				<div class="bv-legend-grid">
					{#each [2, 1, 0] as yi}
						{#each [0, 1, 2] as xi}
							{@const cls = 1 + yi * 3 + xi}
							<button class="bv-cell" class:bv-cell-on={activeCell === cls}
								style="background: {BIVAR_COLORS[cls - 1]}"
								title="{classCounts[cls - 1].toLocaleString()} hex"
								onclick={() => selectCell(cls)}>{classCounts[cls - 1].toLocaleString()}</button>
						{/each}
					{/each}
				</div>
				<div class="bv-legend-x">X →</div>
				<p class="bv-legend-note">{i18n.t('chart.bivariate.legendNote')}</p>
			</div>
		{/if}
	{/if}
	</div>
</ChartFrame>

<style>
	.bvpanel {
		padding: 2px 0;
	}
	.bvpanel-subheader {
		display: flex;
		align-items: baseline;
		gap: 6px;
		margin-bottom: 4px;
		padding: 0 2px;
	}
	.bvpanel-count {
		font-size: 11px;
		color: #fb923c;
	}
	.bvpanel-hint {
		font-size: 11px;
		color: rgba(255,255,255,0.5);
		font-style: italic;
	}
	.bvpanel-picker {
		display: flex;
		align-items: center;
		gap: 6px;
		margin-bottom: 4px;
		padding: 0 2px;
	}
	.picker-label {
		font-size: 11px;
		color: rgba(255,255,255,0.6);
		white-space: nowrap;
	}
	.picker-fixed {
		flex: 1;
		min-width: 0;
		font-size: 12px;
		color: rgba(255,255,255,0.85);
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.picker-select {
		flex: 1;
		background: rgba(255,255,255,0.04);
		border: 1px solid rgba(255,255,255,0.12);
		border-radius: 3px;
		color: rgba(255,255,255,0.85);
		font-size: 12px;
		font-family: inherit;
		padding: 2px 4px;
		cursor: pointer;
		min-width: 0;
	}
	.picker-select:focus {
		outline: none;
		border-color: rgba(255,255,255,0.28);
	}
	.picker-select option {
		background: #1e293b;
		color: #e2e8f0;
	}
	.bvpanel-state {
		font-size: 11px;
		color: rgba(255,255,255,0.5);
		text-align: center;
		padding: 16px 0;
		font-style: italic;
	}
	.bvpanel-error { color: #f87171; }
	.bvpanel-note {
		font-size: 11px;
		color: rgba(255,255,255,0.5);
		line-height: 1.45;
		padding: 4px 2px 0;
	}
	.bv-quad { cursor: pointer; }
	.bv-quad:hover { fill: #fdba74; }
	/* Docked beside the map the scatter grows with the width: cap it so the 3×3
	   legend stays in view (measure() reads the capped box). */
	:global(.chart-frame-expanded) .bv-svgwrap :global(svg) { max-height: 52vh; }
	.bv-mapbtn {
		margin-top: 8px;
		width: 100%;
		padding: 6px 8px;
		font-size: 12px;
		font-family: inherit;
		color: #e2e8f0;
		background: rgba(99, 102, 241, 0.14);
		border: 1px solid rgba(129, 140, 248, 0.45);
		border-radius: 4px;
		cursor: pointer;
	}
	.bv-mapbtn:hover { background: rgba(99, 102, 241, 0.26); }
	.bv-mapbtn-on { background: rgba(99, 102, 241, 0.32); border-color: #818cf8; }
	.bv-legend {
		display: grid;
		grid-template-columns: auto auto 1fr;
		grid-template-rows: auto auto;
		gap: 4px 6px;
		align-items: center;
		margin-top: 8px;
	}
	.bv-legend-y { font-size: 11px; color: rgba(255,255,255,0.6); }
	.bv-legend-grid {
		display: grid;
		grid-template-columns: repeat(3, 44px);
		grid-auto-rows: 26px;
		gap: 2px;
	}
	.bv-legend-x { grid-column: 2; font-size: 11px; color: rgba(255,255,255,0.6); text-align: center; }
	.bv-legend-note {
		grid-column: 3;
		grid-row: 1;
		margin: 0;
		font-size: 11px;
		line-height: 1.45;
		color: rgba(255,255,255,0.55);
	}
	.bv-cell {
		border: 2px solid transparent;
		border-radius: 2px;
		font-size: 10px;
		font-family: inherit;
		color: rgba(15, 23, 42, 0.85);
		cursor: pointer;
		padding: 0;
	}
	.bv-cell:hover { border-color: rgba(255,255,255,0.7); }
	.bv-cell-on { border-color: #fb923c; }
	.bvpanel-empty-prompt {
		font-size: 11px;
		color: rgba(255,255,255,0.55);
		padding: 8px 2px 10px;
		line-height: 1.5;
		border: 1px dashed rgba(255,255,255,0.1);
		border-radius: 4px;
		padding: 10px 8px;
		margin: 2px 0 4px;
		font-style: italic;
	}
</style>
