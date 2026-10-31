<script lang="ts">
	import { query } from '$lib/stores/duckdb';
	import { cellToParent, getResolution } from 'h3-js';
	import { i18n } from '$lib/stores/i18n.svelte';
	import ChartFrame from './ChartFrame.svelte';
	import { CHART_PX, INK, svgScale, fitLabel, dockBoost, measure } from '$lib/utils/chartType';
	import { layerDeptUrl } from '$lib/utils/layerDeptUrl';

	let {
		data = new Map() as Map<string, Record<string, any>>,
		variable = '',
		xLabel = '',
		valueUnit = '',
		territoryPrefix = '',
		deptName = null as string | null,
		deptKey = null as string | null,
		deptUrl = (_id: string, _key: string) => null as string | null,
		onBrushSelect = (_h3s: string[]) => {},
	}: {
		data: Map<string, Record<string, any>>;
		variable: string;
		// Active variable: axis title (with unit) and bare unit for the headline.
		xLabel?: string;
		valueUnit?: string;
		territoryPrefix?: string;
		deptName?: string | null;
		deptKey?: string | null;
		deptUrl?: (layerId: string, key: string) => string | null;
		onBrushSelect?: (h3s: string[]) => void;
	} = $props();

	// Physical distance/time columns of the accessibility layer. dist_primary_m_raw
	// is in km despite its name (measured: median 5.2 in Guaraní).
	const DISTANCES = [
		{ col: 'dist_primary_m_raw', key: 'radio.distPrimaria', unit: 'km' },
		{ col: 'dist_nearest_hospital_km_raw', key: 'radio.distHospital', unit: 'km' },
		{ col: 'dist_nearest_secundaria_km_raw', key: 'radio.distSecundaria', unit: 'km' },
		{ col: 'travel_min_cabecera_raw', key: 'radio.travelCabecera', unit: 'min' },
		{ col: 'travel_min_capital_raw', key: 'radio.travelCapital', unit: 'min' },
	] as const;
	// Fixed, readable band edges per unit (upper bounds; last band is open).
	const BREAKS: Record<string, number[]> = { km: [1, 2, 5, 10, 20], min: [15, 30, 60, 120, 180] };

	let distIdx = $state(0);
	const dist = $derived(DISTANCES[distIdx]);

	let distMap = $state(new Map<string, number>());
	let loading = $state(false);
	let loadError = $state(false);
	let mostlyZero = $state(false);

	// Resolution of the chart cells (res-9, or coarse in giant districts): the
	// distance is averaged to that level so keys match.
	const res = $derived.by(() => {
		for (const h3 of data.keys()) { try { return getResolution(h3); } catch { return 9; } }
		return 9;
	});

	$effect(() => {
		const col = dist.col;
		const r = res;
		void deptKey; void deptName;
		loading = true;
		loadError = false;
		(async () => {
			const url = await layerDeptUrl('accessibility', territoryPrefix, deptName, deptKey, deptUrl);
			const result = await query(`SELECT h3index, "${col}" AS d FROM read_parquet('${url}')`);
			const sum = new Map<string, number>(), cnt = new Map<string, number>();
			let zeros = 0, n = 0;
			for (let i = 0; i < result.numRows; i++) {
				const row = result.get(i)!.toJSON();
				const v = Number(row.d);
				if (!isFinite(v)) continue;
				n++; if (v === 0) zeros++;
				let h3 = String(row.h3index);
				if (r < 9) { try { h3 = cellToParent(h3, r); } catch { continue; } }
				sum.set(h3, (sum.get(h3) ?? 0) + v);
				cnt.set(h3, (cnt.get(h3) ?? 0) + 1);
			}
			const m = new Map<string, number>();
			for (const [h3, s] of sum) m.set(h3, s / cnt.get(h3)!);
			// Territories without this target computed store 0 everywhere.
			return { m, mostlyZero: n > 0 && zeros / n > 0.8 };
		})()
			.then(({ m, mostlyZero: z }) => {
				if (DISTANCES[distIdx].col !== col) return; // superseded
				distMap = m;
				mostlyZero = z;
				loading = false;
			})
			.catch(() => { loadError = true; loading = false; });
	});

	type Band = { label: string; h3s: string[]; sum: number; n: number; mean: number };
	const bands = $derived.by(() => {
		const br = BREAKS[dist.unit];
		const out: Band[] = br.map((b, i) => ({ label: `${i === 0 ? 0 : br[i - 1]}–${b}`, h3s: [], sum: 0, n: 0, mean: NaN }));
		out.push({ label: `>${br[br.length - 1]}`, h3s: [], sum: 0, n: 0, mean: NaN });
		for (const [h3, row] of data) {
			const v = Number(row[variable]);
			const d = distMap.get(h3);
			if (!isFinite(v) || d === undefined) continue;
			let i = br.findIndex(b => d < b);
			if (i < 0) i = br.length;
			out[i].h3s.push(h3); out[i].sum += v; out[i].n++;
		}
		for (const b of out) b.mean = b.n > 0 ? b.sum / b.n : NaN;
		return out.filter(b => b.n > 0);
	});
	const overall = $derived.by(() => {
		let s = 0, n = 0;
		for (const b of bands) { s += b.sum; n += b.n; }
		return n > 0 ? s / n : NaN;
	});

	const PAD_L = 8, PAD_R = 8, PAD_T = 18, PAD_B = 46;
	const SVG_W = 260, SVG_H = 180;
	const plotW = SVG_W - PAD_L - PAD_R;
	const plotH = SVG_H - PAD_T - PAD_B;
	let renderedW = $state(0), renderedH = $state(0);
	let docked = $state(false);
	const k = $derived(svgScale(SVG_W, SVG_H, renderedW, renderedH) * dockBoost(docked));

	// Value scale includes 0 so bars read as magnitudes (delta layers can go negative).
	const vDom = $derived.by(() => {
		let lo = 0, hi = 0;
		for (const b of bands) { if (b.mean < lo) lo = b.mean; if (b.mean > hi) hi = b.mean; }
		return hi === lo ? [lo, lo + 1] : [lo, hi];
	});
	const vy = (v: number) => PAD_T + (1 - (v - vDom[0]) / (vDom[1] - vDom[0])) * plotH;

	const fmt = (v: number) => {
		const a = Math.abs(v);
		return v.toLocaleString(i18n.locale, { maximumFractionDigits: a >= 100 ? 0 : a >= 1 ? 1 : 2 });
	};
	const fmtN = (n: number) => (n >= 1000 ? `${(n / 1000).toLocaleString(i18n.locale, { maximumFractionDigits: 1 })}k` : String(n));

	let activeBand: number | null = $state(null);
	function selectBand(i: number) {
		if (activeBand === i) { clearBand(); return; }
		activeBand = i;
		onBrushSelect(bands[i].h3s);
	}
	function clearBand() {
		activeBand = null;
		onBrushSelect([]);
	}
	$effect(() => {
		void data; void variable; void distIdx;
		activeBand = null;
	});

	function csvRows() {
		return bands.map(b => ({ banda: `${b.label} ${dist.unit}`, hexagonos: b.n, promedio: b.mean.toFixed(3) }));
	}
</script>

<ChartFrame bind:expanded={docked} title={i18n.t('chart.dist.title')} csvRows={csvRows} csvFilename="spatia_perfil_distancia">
	<div class="dp-panel">
		<div class="dp-picker">
			<span class="dp-label">{i18n.t('chart.dist.to')}</span>
			<select bind:value={distIdx} class="dp-select">
				{#each DISTANCES as d, i}
					<option value={i}>{i18n.t(d.key)}</option>
				{/each}
			</select>
		</div>

		{#if loading}
			<div class="dp-state">{i18n.t('chart.dist.loading')}</div>
		{:else if loadError}
			<div class="dp-state dp-error">{i18n.t('chart.loadError')}</div>
		{:else if mostlyZero || bands.length < 2}
			<div class="dp-state">{i18n.t('chart.dist.noData')}</div>
		{:else}
			{@const first = bands[0]}
			{@const last = bands[bands.length - 1]}
			{@const peak = bands.reduce((a, b) => (b.mean > a.mean ? b : a), first)}
			<p class="dp-head">
				{i18n.t('chart.dist.near')} ({first.label} {dist.unit}): <strong>{fmt(first.mean)}</strong> {valueUnit}
				· {i18n.t('chart.dist.far')} ({last.label} {dist.unit}): <strong>{fmt(last.mean)}</strong> {valueUnit}
				{#if peak !== first && peak !== last}
					<br />{i18n.t('chart.dist.peak')} ({peak.label} {dist.unit}): <strong>{fmt(peak.mean)}</strong> {valueUnit}
				{/if}
			</p>
			<div class="dp-sub">
				{#if activeBand !== null && bands[activeBand]}
					<span class="dp-sel">{bands[activeBand].label} {dist.unit}: {bands[activeBand].n.toLocaleString(i18n.locale)} hex · {i18n.t('chart.dist.clear')}</span>
				{:else}
					<span class="dp-hint">{i18n.t('chart.dist.hint')}</span>
				{/if}
			</div>
			<div use:measure={(w, h) => { renderedW = w; renderedH = h; }}>
				<svg width="100%" viewBox="0 0 {SVG_W} {SVG_H}" preserveAspectRatio="xMidYMid meet" style="display: block;">
					{#each bands as b, i}
						{@const bw = plotW / bands.length}
						{@const x = PAD_L + i * bw}
						{@const y0 = vy(0)}
						{@const y1 = vy(b.mean)}
						<!-- svelte-ignore a11y_click_events_have_key_events -->
						<!-- svelte-ignore a11y_no_static_element_interactions -->
						<g class="dp-band" onclick={() => selectBand(i)}>
							<rect x={x} y={PAD_T} width={bw} height={plotH} fill="transparent" />
							<rect x={x + bw * 0.15} y={Math.min(y0, y1)} width={bw * 0.7} height={Math.max(1, Math.abs(y1 - y0))}
								fill={activeBand === i ? '#34d399' : '#38bdf8'} opacity={activeBand !== null && activeBand !== i ? 0.35 : 0.85} rx="1.5" />
							<text x={x + bw / 2} y={Math.min(y0, y1) - 3 * k} text-anchor="middle" fill={INK.label}
								font-size={CHART_PX.tick * k} font-family="system-ui, sans-serif">{fmt(b.mean)}</text>
							<text x={x + bw / 2} y={PAD_T + plotH + (CHART_PX.tick + 2) * k} text-anchor="middle" fill={INK.tick}
								font-size={CHART_PX.tick * k} font-family="system-ui, sans-serif">{b.label}</text>
							<text x={x + bw / 2} y={PAD_T + plotH + (2 * CHART_PX.tick + 4) * k} text-anchor="middle" fill={INK.hint}
								font-size={(CHART_PX.tick - 1) * k} font-family="system-ui, sans-serif">{fmtN(b.n)}</text>
						</g>
					{/each}
					{#if isFinite(overall)}
						<line x1={PAD_L} x2={PAD_L + plotW} y1={vy(overall)} y2={vy(overall)}
							stroke="rgba(255,255,255,0.4)" stroke-width="1" stroke-dasharray="3 2" pointer-events="none" />
					{/if}
					<text x={PAD_L + plotW / 2} y={PAD_T + plotH + (2 * CHART_PX.tick + CHART_PX.label + 9) * k} text-anchor="middle"
						fill={INK.label} font-size={CHART_PX.label * k} font-family="system-ui, sans-serif"
					>{fitLabel(`${i18n.t(dist.key)} (${dist.unit})`, plotW / k, CHART_PX.label)}</text>
				</svg>
			</div>
			<div class="dp-note" title={xLabel}>{i18n.t('chart.dist.note').replace('{v}', xLabel)}</div>
		{/if}
	</div>
</ChartFrame>

<style>
	.dp-panel { padding: 2px 0; }
	.dp-picker { display: flex; align-items: center; gap: 6px; margin-bottom: 6px; padding: 0 2px; }
	.dp-label { font-size: 11px; color: rgba(255,255,255,0.6); white-space: nowrap; }
	.dp-select {
		flex: 1; min-width: 0;
		background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.12); border-radius: 3px;
		color: rgba(255,255,255,0.85); font-size: 12px; font-family: inherit; padding: 2px 4px; cursor: pointer;
	}
	.dp-select option { background: #1e293b; color: #e2e8f0; }
	.dp-head { margin: 2px 2px 4px; font-size: 13px; line-height: 1.5; color: rgba(255,255,255,0.85); }
	.dp-head strong { color: #38bdf8; font-size: 15px; }
	.dp-sub { padding: 0 2px; min-height: 16px; }
	.dp-sel { font-size: 11px; color: #34d399; }
	.dp-hint { font-size: 11px; color: rgba(255,255,255,0.5); font-style: italic; }
	.dp-state { font-size: 11px; color: rgba(255,255,255,0.5); padding: 12px 2px; font-style: italic; }
	.dp-error { color: #f87171; }
	.dp-note { font-size: 11px; line-height: 1.45; color: rgba(255,255,255,0.5); padding: 4px 2px 0; }
	.dp-band { cursor: pointer; }
	.dp-band:hover rect:nth-child(2) { opacity: 1; }
	:global(.chart-frame-expanded) .dp-head { font-size: 16px; }
	:global(.chart-frame-expanded) .dp-head strong { font-size: 19px; }
</style>
