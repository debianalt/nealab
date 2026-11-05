<script lang="ts">
	import { displayTypeLabel } from '$lib/utils/typeLabel';
	import type { HexStore } from '$lib/stores/hex.svelte';
	import PetalChart from './PetalChart.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { withRef } from '$lib/utils/territoryRef';
	import { unitLabel } from '$lib/utils/units';
	import { downloadCsvFromRows, downloadGeoJsonFromHexList } from '$lib/utils/data-export';
	import { radioUnit, radioSelectionOn, isRadioSupport, fmtRedcode } from '$lib/stores/radioUnit.svelte';
	import type { HexVariable } from '$lib/config';

	let {
		hexStore,
	}: {
		hexStore: HexStore;
	} = $props();

	const CENSUS_ANALYSES = new Set(['service_deprivation', 'health_access', 'education_capital', 'education_flow', 'economic_activity', 'accessibility']);
	const selected = $derived([...hexStore.selectedHexes.entries()]);
	const layer = $derived(hexStore.activeLayer);
	const isCensus = $derived(layer ? CENSUS_ANALYSES.has(layer.id) : false);
	// service_deprivation keeps the petal: six household rates, each read against
	// the provincial average (visual hex-to-hex comparison).
	const PETAL_HIDDEN = new Set([...CENSUS_ANALYSES].filter(id => id !== 'service_deprivation'));
	const showPetals = $derived(layer ? !PETAL_HIDDEN.has(layer.id) : true);
	const variables = $derived(layer?.variables ?? []);
	const componentVars = $derived(
		// type_label is surfaced as the classification chip (gated, hidden when null) —
		// excluding it here avoids a duplicate "Clasificación" row that renders
		// "Sin cobertura" (legend.noData) on unscored hexes (e.g. forestry on water).
		variables.filter(v => !v.hidden && !['score', 'type', 'type_label', 'pca_1', 'pca_2'].includes(v.col))
	);
	const petalLabels = $derived(hexStore.petalLabels);

	// Census layers: the rates belong to the INDEC radio, so a click picks the cell's radio and
	// the panel shows each radio once (radioUnit.groupSelection). A second cell of the same radio
	// only adds its buildings. Satellite layers, Paraguay, the aggregated view and the
	// territory comparison keep one block per cell. Same gate as the map effect in +page.svelte.
	const byRadio = $derived(
		radioSelectionOn(layer?.id, hexStore.territoryPrefix, hexStore.analysisResLevel != null)
		&& hexStore.compareVisibleData.size === 0
	);
	$effect(() => {
		if (!byRadio) return;
		radioUnit.ensure(hexStore.territoryPrefix);
		radioUnit.ensureCellPop(hexStore.territoryPrefix);
	});

	/** Estimated people: one decimal under 10, "<1" under one, never "0" next to buildings. */
	function fmtPeople(v: number): string {
		if (v > 0 && v < 1) return '<1';
		return v < 10 && !Number.isInteger(v) ? v.toFixed(1) : fmtInt(v);
	}
	const groups = $derived(byRadio ? radioUnit.groupSelection(hexStore.territoryPrefix, hexStore.selectedHexes) : null);
	const radios = $derived(groups?.radios ?? []);
	const looseSet = $derived(new Set(groups?.loose ?? []));
	/** Cells shown as cells: every one without radios, only those outside any radio with them. */
	const cellEntries = $derived(groups ? selected.filter(([h]) => looseSet.has(h)) : selected);
	const radioVars = $derived(groups ? componentVars.filter(v => isRadioSupport(layer?.id, v.col)) : []);
	/** Variables measured on the cell (population density, night lights): per cell even by radio. */
	const cellVars = $derived(groups ? componentVars.filter(v => !isRadioSupport(layer?.id, v.col)) : componentVars);

	// One petal per radio (its first cell) plus the loose cells; selectionPetalLayers follows
	// the selection order.
	const petalLayers = $derived.by(() => {
		const all = hexStore.selectionPetalLayers;
		if (!groups) return all;
		const keys = [...hexStore.selectedHexes.keys()];
		const colorOf = new Map(radios.map(r => [r.hexes[0], r.color]));
		const keep = new Set([...colorOf.keys(), ...looseSet]);
		return all.flatMap((p, i) => keep.has(keys[i]) ? [{ ...p, color: colorOf.get(keys[i]) ?? p.color }] : []);
	});

	/** Chip/column label of a cell: the index digits that change (every res-9 cell here reads 89a9…ffff). */
	function hexLabel(h3: string): string {
		return h3.slice(4, 11);
	}

	function isNum(v: unknown): v is number {
		return typeof v === 'number' && Number.isFinite(v);
	}

	function fmtVar(v: HexVariable, val: number): string {
		return fmtValue(val * (v.scale ?? 1), v.unit) + (v.unit ? ` ${unitLabel(v.unit)}` : '');
	}

	/** Rate of the selected radios together: each radio weighs by its base (households or people);
	 *  a variable without base (travel minutes) weighs by the radio's population. */
	function pooled(v: HexVariable): number | null {
		let num = 0, den = 0;
		for (const r of radios) {
			const x = r.row[v.col];
			const w = r.row[v.baseCol ?? 'pob_total'];
			if (!isNum(x) || !isNum(w) || w <= 0) continue;
			num += x * w; den += w;
		}
		return den > 0 ? num / den : null;
	}

	function sumRadios(col: string): number | null {
		let s = 0, any = false;
		for (const r of radios) { const x = r.row[col]; if (isNum(x)) { s += x; any = true; } }
		return any ? s : null;
	}

	function deselectRadio(hexes: string[]) {
		for (const h of hexes) hexStore.deselectHex(h);
	}

	/**
	 * Adaptive number formatter — picks precision based on magnitude so that
	 * fractional values like NDVI (0.85) do not collapse to "1" via toFixed(0).
	 */
	function fmtSmart(v: unknown): string {
		if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
		if (v === 0) return '0';
		const abs = Math.abs(v);
		if (abs < 0.01) return v.toExponential(1);
		if (abs < 1) return v.toFixed(2);
		if (abs < 10) return v.toFixed(2);
		if (abs < 100) return v.toFixed(1);
		return v.toFixed(0);
	}

	/** Integer formatter with thousands separators (es-AR) for counts/population. */
	function fmtInt(v: unknown): string {
		if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
		return Math.round(v).toLocaleString('es-AR');
	}

	/** Estimated households/people behind a rate: never "0" next to a rate. */
	function fmtBase(v: unknown): string {
		if (typeof v === 'number' && v > 0 && v < 1) return '<1';
		return fmtInt(v);
	}

	// Rates, minutes, densities and people per household read best with one
	// decimal (6.6 %, not 6.60 %), none from 100 up; other units keep the adaptive
	// precision.
	const ONE_DECIMAL = new Set(['%', 'min', 'personas', 'hab/km²']);
	function fmtValue(v: number, unit: string | undefined): string {
		if (!unit || !ONE_DECIMAL.has(unit) || !Number.isFinite(v)) return fmtSmart(v);
		return v.toFixed(Math.abs(v) >= 100 ? 0 : 1);
	}

	// Show the magnitude table when the layer carries dasymetric magnitude
	// (n_edificios) or the bases its rates are over (sociodemographic has no
	// building count, only hog_total).
	const hasBuildings = $derived(cellEntries.some(([, d]) => d.data?.n_edificios != null));
	// Households/people each % is computed over (census layers: hog_total,
	// pob_18_mas, …), one table row per distinct base column.
	const baseCols = $derived(
		[...new Set(componentVars.map(v => v.baseCol).filter((c): c is string => !!c))]
	);
	// Only show the classification chip for layers whose K-means typology is
	// meaningful (declare a type_label variable in config). Layers where it doesn't
	// work were stripped of the variable, so the parquet's residual type_label
	// (e.g. "Sin cobertura censal") is not surfaced.
	const hasClassification = $derived(variables.some(v => v.col === 'type_label'));

	function downloadRadiosCsv() {
		const rows = radios.map(r => ({
			redcode: r.redcode,
			hexagonos_marcados: r.hexes.length,
			edificios_marcados: radioUnit.cellPeople(hexStore.territoryPrefix, r.hexes)?.edificios ?? null,
			personas_estimadas_marcadas: radioUnit.cellPeople(hexStore.territoryPrefix, r.hexes)?.personas ?? null,
			personas_por_area: radioUnit.cellPeople(hexStore.territoryPrefix, r.hexes)?.porArea ?? null,
			...r.row,
		}));
		if (rows.length === 0) return;
		const allCols = new Set<string>();
		for (const row of rows) for (const k of Object.keys(row)) allCols.add(k);
		downloadCsvFromRows(rows, [...allCols], `spatia_${layer?.id ?? 'layer'}_radios_seleccionados.csv`);
	}

	function downloadSelectedCsv() {
		const radioOf = new Map<string, string>();
		for (const r of radios) for (const h of r.hexes) radioOf.set(h, r.redcode);
		const rows = [...hexStore.selectedHexes.entries()].map(([h3, sel]) => ({
			h3index: h3, ...(groups ? { redcode: radioOf.get(h3) ?? '' } : {}), ...sel.data,
		}));
		if (rows.length === 0) return;
		const allCols = new Set<string>(['h3index']);
		for (const row of rows) for (const k of Object.keys(row)) allCols.add(k);
		const layerId = hexStore.activeLayer?.id ?? 'layer';
		downloadCsvFromRows(rows, [...allCols], `spatia_${layerId}_hexes_seleccionados.csv`);
	}

	function downloadSelectedGeoJson() {
		const h3s = [...hexStore.selectedHexes.keys()];
		if (h3s.length === 0) return;
		const layerId = hexStore.activeLayer?.id ?? 'layer';
		downloadGeoJsonFromHexList(h3s, hexStore.visibleData, `spatia_${layerId}_hexes_seleccionados.geojson`);
	}
</script>

<div class="hex-comparison">
	<div class="hc-header">
		<span class="hc-title">{i18n.t('hex.comparison')}</span>
		<button class="hc-clear" onclick={() => hexStore.clearSelection()}>
			&#10005; {i18n.t('ctrl.clear')}
		</button>
	</div>

	<!-- Hex chips -->
	<div class="hc-chips">
		{#each radios as r (r.redcode)}
			<span class="hc-chip">
				<span class="hc-dot" style:background={r.color}></span>
				<span class="hc-label">{fmtRedcode(r.redcode)}</span>
				<button class="hc-remove" onclick={() => deselectRadio(r.hexes)}>&#10005;</button>
			</span>
		{/each}
		{#each cellEntries as [h3index, data]}
			<span class="hc-chip">
				<span class="hc-dot" style:background={data.color}></span>
				<span class="hc-label">{hexLabel(h3index)}</span>
				<button class="hc-remove" onclick={() => hexStore.deselectHex(h3index)}>&#10005;</button>
			</span>
		{/each}
	</div>

	<!-- Petal chart (normalized vs provincial avg) -->
	{#if petalLayers.length > 0 && petalLabels.length >= 3 && showPetals}
		<PetalChart layers={petalLayers} labels={petalLabels} size={340} />
		<div class="hc-ref-note">
			<span class="hc-ref-dash"></span> {i18n.t('hex.petalRelative')} 50 = {withRef(i18n.t('hex.avgOf'), hexStore.territoryId, i18n.locale)}
		</div>
	{/if}

	<!-- Census layers: one block per selected radio (its rates once), then the radios together -->
	{#if groups && radios.length > 0}
		{#each radios as r (r.redcode)}
			<div class="cd-hex-block">
				<div class="cd-hex-id">
					<span class="hc-dot" style:background={r.color}></span>
					{i18n.t('hex.radio')} {fmtRedcode(r.redcode)}
					<span class="cd-cells">· {r.hexes.length} {i18n.t(r.hexes.length === 1 ? 'hex.cellMarked' : 'hex.cellsMarked')}</span>
				</div>
				{#each radioVars as v}
					{@const val = r.row[v.col]}
					<div class="cd-row">
						<span class="cd-label">{i18n.t(v.labelKey)}</span>
						{#if isNum(val)}
							<span class="cd-val-data">{fmtVar(v, val)}</span>
						{:else}
							<span class="cd-val-data cd-val-nodata">{i18n.t('legend.noData')}</span>
						{/if}
					</div>
				{/each}
				{#each cellVars as v}
					<div class="cd-row">
						<span class="cd-label">{i18n.t(v.labelKey)}</span>
						<span class="cd-val-data">{r.hexes.map(h => { const x = hexStore.selectedHexes.get(h)?.data?.[v.col]; return isNum(x) ? fmtVar(v, x) : '—'; }).join(' · ')}</span>
					</div>
				{/each}
				{#each baseCols as col}
					<div class="cd-row cd-row-mag">
						<span class="cd-label">{i18n.t(`hex.radioBase.${col}`)}</span>
						<span class="cd-val-data">{fmtInt(r.row[col])}</span>
					</div>
				{/each}
				{#if radioUnit.cellPeople(hexStore.territoryPrefix, r.hexes)}
					{@const pop = radioUnit.cellPeople(hexStore.territoryPrefix, r.hexes)!}
					<div class="cd-row cd-row-mag">
						<span class="cd-label">{i18n.t('hex.buildingsMarked')}</span>
						<span class="cd-val-data">{fmtInt(pop.edificios)}</span>
					</div>
					<div class="cd-row cd-row-mag">
						<span class="cd-label">{i18n.t('hex.peopleMarked')}</span>
						<span class="cd-val-data">{fmtPeople(pop.personas)}</span>
					</div>
					{#if pop.porArea}
						<div class="cd-area-note">{i18n.t('hex.byAreaNote')}</div>
					{/if}
				{/if}
			</div>
		{/each}
		{#if radios.length >= 2}
			{@const allHexes = radios.flatMap(r => r.hexes)}
			<div class="cd-hex-block cd-total">
				<div class="cd-hex-id">{i18n.t('hex.selectionTotal')} · {radios.length} {i18n.t('hex.radiosN')}</div>
				{#each radioVars as v}
					{@const p = pooled(v)}
					<div class="cd-row">
						<span class="cd-label">{i18n.t(v.labelKey)}</span>
						{#if p != null}
							<span class="cd-val-data">{fmtVar(v, p)}</span>
						{:else}
							<span class="cd-val-data cd-val-nodata">{i18n.t('legend.noData')}</span>
						{/if}
					</div>
				{/each}
				{#each baseCols as col}
					<div class="cd-row cd-row-mag">
						<span class="cd-label">{i18n.t(`hex.radiosBase.${col}`)}</span>
						<span class="cd-val-data">{fmtInt(sumRadios(col))}</span>
					</div>
				{/each}
				{#if radioUnit.cellPeople(hexStore.territoryPrefix, allHexes)}
					{@const pop = radioUnit.cellPeople(hexStore.territoryPrefix, allHexes)!}
					<div class="cd-row cd-row-mag">
						<span class="cd-label">{i18n.t('hex.buildingsMarked')}</span>
						<span class="cd-val-data">{fmtInt(pop.edificios)}</span>
					</div>
					<div class="cd-row cd-row-mag">
						<span class="cd-label">{i18n.t('hex.peopleMarked')}</span>
						<span class="cd-val-data">{fmtPeople(pop.personas)}</span>
					</div>
				{/if}
			</div>
		{/if}
		<div class="mag-note">{i18n.t('hex.radioNote')}</div>
	{/if}

	<!-- Data rows per cell: every selected cell, or (census layers) the ones outside the radios -->
	{#if componentVars.length > 0}
		{#each cellEntries as [h3index, hexData]}
			<div class="cd-hex-block">
				<div class="cd-hex-id">
					<span class="hc-dot" style:background={hexData.color}></span>
					{hexLabel(h3index)}
					{#if hasClassification && displayTypeLabel(hexData.data?.type_label, i18n.locale)}
						<span class="cd-type">{displayTypeLabel(hexData.data?.type_label, i18n.locale)}</span>
					{/if}
				</div>
				{#each componentVars as v}
					{@const val = hexData.data?.[v.col]}
					{@const rawVal = v.rawCol ? hexData.data?.[v.rawCol] : null}
					{@const rawLoaded = !!v.rawCol && !!hexData.data && v.rawCol in hexData.data}
					<!-- When the layer has a raw column and it was loaded, it alone decides: a NULL raw
					     with a filled normalized value is a placeholder (Chaco and Formosa carry
					     dist_primary_m = 50 in every cell and no road distance). -->
					{@const hasData = rawLoaded
						? (typeof rawVal === 'number' && Number.isFinite(rawVal))
						: ((typeof val === 'string' && val.length > 0)
							|| (typeof val === 'number' && Number.isFinite(val)))}
					{@const isStr = typeof val === 'string' && val.length > 0}
					{@const numVal = typeof val === 'number' ? val : 0}
					{@const displayVal = (rawVal != null && typeof rawVal === 'number') ? rawVal : numVal}
					<div class="cd-row">
						<span class="cd-label">{i18n.t(v.labelKey)}</span>
						{#if !hasData}
							<span class="cd-val-data cd-val-nodata">{i18n.t('legend.noData')}</span>
						{:else}
							<span class="cd-val-data">{isStr ? val : fmtValue(displayVal * (v.scale ?? 1), v.unit) + (v.unit ? ` ${unitLabel(v.unit)}` : '')}</span>
						{/if}
					</div>
				{/each}
			</div>
		{/each}
	{/if}

	<!-- Magnitude (buildings, est. population) + estimated % per variable -->
	{#if cellEntries.length > 0 && (hasBuildings || baseCols.length > 0)}
		<div class="mag-section">
			<div class="mag-title">{i18n.t('hex.magTable')}</div>
			<table class="mag-table">
				<thead>
					<tr>
						<th></th>
						{#each cellEntries as [h3index, d]}
							<th>
								<span class="hc-dot" style:background={d.color}></span>{hexLabel(h3index)}
							</th>
						{/each}
					</tr>
				</thead>
				<tbody>
					{#if hasBuildings}
						<tr>
							<td>{i18n.t('hex.buildings')}</td>
							{#each cellEntries as [, d]}<td>{fmtInt(d.data?.n_edificios)}</td>{/each}
						</tr>
					{/if}
					{#each baseCols as col}
						<tr>
							<td>{i18n.t(`hex.base.${col}`)}</td>
							{#each cellEntries as [, d]}<td>{fmtBase(d.data?.[col])}</td>{/each}
						</tr>
					{/each}
				</tbody>
			</table>
			<div class="mag-note">{i18n.t('hex.estPctNote')}</div>
			{#if baseCols.length > 0}
				<div class="mag-note">{i18n.t('hex.baseNote')}</div>
			{/if}
		</div>
	{/if}

	{#if selected.length > 0}
		<div class="hc-download-row">
			{#if radios.length > 0}
				<button class="hc-dl-btn" onclick={downloadRadiosCsv}>↓ {i18n.t('hex.csvRadios')}</button>
			{/if}
			<button class="hc-dl-btn" onclick={downloadSelectedCsv}>↓ CSV</button>
			<button class="hc-dl-btn" onclick={downloadSelectedGeoJson}>↓ GeoJSON</button>
		</div>
	{/if}

</div>

<style>
	.hex-comparison { font-size: 12.5px; }
	.cd-hex-block { margin: 8px 0; padding: 6px 0; border-top: 1px solid rgba(var(--c-fg-rgb), 0.06); }
	.cd-hex-id { display: flex; align-items: center; gap: 5px; font-size: 12px; color: var(--c-ink-1); font-family: var(--font-mono); margin-bottom: 5px; }
	.cd-type { font-size: 12px; color: var(--c-ink-1); font-weight: 500; background: rgba(var(--c-fg-rgb), 0.08); padding: 1px 6px; border-radius: 3px; font-family: sans-serif; }
	.cd-row { display: flex; align-items: center; gap: 6px; margin-bottom: 2px; }
	.cd-label { font-size: 11.5px; color: var(--c-ink-1n); flex: 0 1 auto; min-width: 100px; }
	.cd-bar-track { flex: 1; height: 5px; background: var(--c-line); border-radius: 3px; overflow: hidden; }
	.cd-bar-fill { height: 100%; border-radius: 3px; transition: width 0.3s; min-width: 2px; }
	.cd-val { font-size: 11px; font-weight: 600; color: var(--c-ink-2); width: 24px; text-align: right; flex-shrink: 0; }
	.cd-val-data { font-size: 12px; font-weight: 600; color: var(--c-ink-1); text-align: right; margin-left: auto; white-space: nowrap; }
	.cd-val-nodata { color: var(--c-ink-3); font-style: italic; font-weight: 500; }
	.cd-cells { font-family: sans-serif; font-size: 11.5px; color: var(--c-ink-2); }
	.cd-row-mag .cd-label, .cd-row-mag .cd-val-data { color: var(--c-ink-2); font-weight: 500; }
	.cd-total { border-top: 1px solid rgba(var(--c-fg-rgb), 0.18); }
	.cd-area-note { font-size: 11px; color: var(--c-ink-2); font-style: italic; margin-top: 2px; }
	.hc-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 8px;
	}
	.hc-title {
		font-size: 13px;
		font-weight: 600;
		color: var(--c-ink-1);
	}
	.hc-clear {
		font-size: 11.5px;
		padding: 2px 6px;
		border-radius: 4px;
		background: rgba(var(--c-fg-rgb), 0.06);
		border: 1px solid var(--c-line-2);
		color: var(--c-ink-1n);
		cursor: pointer;
		transition: all 0.15s;
	}
	.hc-clear:hover { border-color: #ef4444; color: #ef4444; }

	.hc-chips {
		display: flex;
		flex-wrap: wrap;
		gap: 4px;
		margin-bottom: 8px;
	}
	.hc-chip {
		display: flex;
		align-items: center;
		gap: 4px;
		padding: 2px 6px;
		border-radius: 4px;
		background: rgba(var(--c-fg-rgb), 0.06);
		border: 1px solid var(--c-line-2);
		font-size: 12px;
		color: var(--c-ink-1);
	}
	.hc-dot {
		width: 8px;
		height: 8px;
		border-radius: 50%;
		flex-shrink: 0;
	}
	.hc-label { font-family: var(--font-sans); font-weight: 500; }
	.hc-remove {
		background: none;
		border: none;
		color: var(--c-ink-2n);
		cursor: pointer;
		font-size: 11.5px;
		padding: 0 2px;
		line-height: 1;
	}
	.hc-remove:hover { color: #ef4444; }

	.mag-section { margin: 10px 0 4px; padding-top: 8px; border-top: 1px solid rgba(var(--c-fg-rgb), 0.06); }
	.mag-title { font-size: 12px; font-weight: 600; color: var(--c-ink-1); margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.03em; }
	.mag-table { width: 100%; border-collapse: collapse; }
	.mag-table th, .mag-table td { padding: 3px 4px; text-align: right; font-size: 12px; color: var(--c-ink-1); white-space: nowrap; }
	.mag-table th:first-child, .mag-table td:first-child { text-align: left; color: var(--c-ink-1n); font-size: 11.5px; font-weight: 400; }
	.mag-table thead th { font-family: var(--font-sans); font-weight: 600; border-bottom: 1px solid rgba(var(--c-fg-rgb), 0.08); }
	.mag-table thead th .hc-dot { width: 7px; height: 7px; margin-right: 3px; vertical-align: middle; }
	.mag-table tbody tr:not(:last-child) td { border-bottom: 1px solid rgba(var(--c-fg-rgb), 0.04); }
	.mag-note { font-size: 11px; color: rgba(var(--c-fg-rgb), 0.55); font-style: italic; margin-top: 5px; line-height: 1.3; }

	.hc-download-row { margin-top: 8px; display: flex; gap: 6px; }
	.hc-dl-btn { flex: 1; padding: 6px 10px; background: rgba(var(--c-accent-rgb), 0.15); border: 1px solid rgba(var(--c-accent-rgb), 0.3); border-radius: 4px; color: var(--c-accent); font-size: 11.5px; font-weight: 600; cursor: pointer; font-family: inherit; transition: all 0.15s; }
	.hc-dl-btn:hover { background: rgba(var(--c-accent-rgb), 0.25); border-color: rgba(var(--c-accent-rgb), 0.5); }

	.hc-ref-note {
		display: flex;
		align-items: center;
		gap: 6px;
		font-size: 11.5px;
		color: rgba(var(--c-fg-rgb), 0.55);
		margin: -4px 0 8px;
		justify-content: center;
	}
	.hc-ref-dash {
		display: inline-block;
		width: 16px;
		border-top: 1px dashed rgba(var(--c-fg-rgb), 0.5);
	}

	.hc-bars-section { margin-top: 4px; }
	.hc-var-row { margin-bottom: 6px; }
	.hc-var-label {
		font-size: 11.5px;
		color: var(--c-ink-1n);
		margin-bottom: 2px;
	}
	.hc-var-bars {
		display: flex;
		flex-direction: column;
		gap: 2px;
	}
	.hc-bar-row {
		display: flex;
		align-items: center;
		gap: 4px;
	}
	.hc-bar-track {
		flex: 1;
		height: 5px;
		background: var(--c-line);
		border-radius: 3px;
		overflow: hidden;
	}
	.hc-bar-fill {
		height: 100%;
		border-radius: 3px;
		transition: width 0.3s ease;
		min-width: 2px;
	}
	.hc-bar-val {
		font-size: 11px;
		font-weight: 600;
		color: var(--c-ink-2);
		width: 32px;
		text-align: right;
		flex-shrink: 0;
	}
</style>
