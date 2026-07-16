<script lang="ts">
	import type { HexStore } from '$lib/stores/hex.svelte';
	import type { TerritoryStore } from '$lib/stores/territory.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { lp } from '$lib/utils/locale-path';
	import { formatDept } from '$lib/utils/format';
	import CTADiagnostic from '$lib/components/CTADiagnostic.svelte';
	import PetalChart from '$lib/components/PetalChart.svelte';
	import TemporalToggle from '$lib/components/TemporalToggle.svelte';
	import { HEX_LAYER_REGISTRY, DATA_FRESHNESS, getSatDptoUrl, getFloodDptoUrl, getScoresDptoUrl, getReportUrl, getTemporalCol, getDeptSummaryUrl, TERRITORY_REGISTRY, getSatGlobalUrl, type AnalysisConfig, type TemporalMode, type TerritoryConfig, type CountryId } from '$lib/config';
	import { loadDeptSummary } from '$lib/utils/deptSummaries';
	import { query } from '$lib/stores/duckdb';
	import { downloadCsvFromQuery, downloadGeoJsonFromHexQuery } from '$lib/utils/data-export';
	import { ANALYSIS_CONTENT } from '$lib/content/methodology';

	let {
		analysis,
		hexStore,
		territoryStore,
		onSelectDpto,
	}: {
		analysis: AnalysisConfig;
		hexStore: HexStore;
		territoryStore: TerritoryStore;
		onSelectDpto?: (dpto: string, parquetKey: string, centroid: [number, number]) => void;
	} = $props();

	const layerCfg = $derived(HEX_LAYER_REGISTRY[analysis.id]);
	const freshness = $derived(layerCfg ? DATA_FRESHNESS[layerCfg.parquet] : null);
	const loading = $derived(hexStore.loading);
	const selectedHexes = $derived(hexStore.selectedHexes);
	const isPerDept = $derived(layerCfg?.perDepartment === true);
	const selectedDpto = $derived(hexStore.selectedDpto);
	const selectedHex = $derived.by(() => {
		if (selectedHexes.size === 0) return null;
		const [h3index, sel] = [...selectedHexes.entries()][0];
		return { h3index, ...sel.data };
	});

	// Census-based analyses: hide petals (radio-level data → identical within radio, not informative)
	const CENSUS_ANALYSES = new Set(['service_deprivation', 'health_access', 'education_capital', 'education_flow', 'economic_activity', 'accessibility', 'carbon_stock']);

	// Department summaries for perDepartment layers
	let deptSummary = $state<any>(null);

	$effect(() => {
		if (!isPerDept || !layerCfg) return;
		const prefix = hexStore.territoryPrefix;
		deptSummary = null;
		loadDeptSummary(layerCfg.id, prefix).then(s => { deptSummary = s; });
	});

	// Territory tab state — multi-territory dept list
	let allSummaries = $state(new Map<string, any>());
	let activeTab = $state('');
	let _summaryLoad = 0;

	// Effect 1: keep activeTab in sync with current territory
	$effect(() => {
		if (!isPerDept || !layerCfg) return;
		activeTab = territoryStore.activeTerritory.id;
	});

	// Effect 2: load dept summaries for all territories when analysis changes (not on territory change)
	$effect(() => {
		if (!isPerDept || !layerCfg) return;
		const id = layerCfg.id;
		allSummaries = new Map();
		const myLoad = ++_summaryLoad;
		const available = Object.values(TERRITORY_REGISTRY).filter(t => t.available);
		Promise.all(
			available.map(t => loadDeptSummary(id, t.parquetPrefix).then(s => ({ t, s })))
		).then(results => {
			if (_summaryLoad !== myLoad) return;
			const m = new Map<string, any>();
			for (const { t, s } of results) {
				if (s?.departments?.length > 0) m.set(t.id, s);
			}
			allSummaries = m;
		});
	});

	const tabTerritories = $derived.by(() => {
		const available = Object.values(TERRITORY_REGISTRY).filter(t => t.available);
		return available.filter(t => allSummaries.has(t.id));
	});

	const COUNTRY_ORDER: CountryId[] = ['ar', 'py', 'br'];
	const COUNTRY_FLAGS: Record<CountryId, string> = { ar: '🇦🇷', py: '🇵🇾', br: '🇧🇷' };

	const tabsByCountry = $derived.by(() => {
		const groups = new Map<CountryId, TerritoryConfig[]>();
		for (const t of tabTerritories) {
			const list = groups.get(t.country) ?? [];
			list.push(t);
			groups.set(t.country, list);
		}
		return groups;
	});

	function handleTabClick(territoryId: string) {
		activeTab = territoryId;
		if (territoryId !== territoryStore.activeTerritory.id) {
			territoryStore.setTerritory(territoryId);
		}
	}

	const activeSummaryData = $derived(allSummaries.get(activeTab) ?? deptSummary);

	// Flood layer only: per-territory Sentinel-1 SAR acquisition date for the
	// "current extent" component. The pipeline injects it into each
	// *_flood_dept_summary.json under metadata.sar_date (ISO). Null elsewhere
	// or until the territory has been re-processed → the SAR line is hidden.
	const sarDate = $derived(
		analysis.id === 'flood_risk' ? (activeSummaryData?.metadata?.sar_date ?? null) : null
	);
	// ISO (2026-03-21) → DD/MM/YYYY to match the processedDate format shown alongside.
	const sarDateDisplay = $derived.by(() => {
		if (!sarDate) return null;
		const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(sarDate);
		return m ? `${m[3]}/${m[2]}/${m[1]}` : sarDate;
	});

	const deptList = $derived.by(() => {
		const summary = allSummaries.size > 0 ? allSummaries.get(activeTab) : deptSummary;
		if (!summary?.departments) return [];
		return [...summary.departments].sort((a: any, b: any) => b.avg_score - a.avg_score);
	});

	// Dept filter — BR states list 300-500 municipalities; without a filter the
	// list buries every chart below it. Reset on territory tab switch.
	let deptFilter = $state('');
	$effect(() => { activeTab; deptFilter = ''; });
	const normalize = (s: string) => s.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
	const filteredDeptList = $derived.by(() => {
		if (!deptFilter.trim()) return deptList;
		const q = normalize(deptFilter.trim());
		return deptList.filter((d: any) =>
			normalize(String(d.dpto ?? d.distrito ?? d.municipio ?? '')).includes(q)
		);
	});

	const colorScale = $derived(layerCfg?.colorScale ?? 'sequential');
	const isCategorical = $derived(colorScale === 'categorical');
	const PALETTE = ['#1565c0', '#7e57c2', '#4db6ac', '#66bb6a', '#c0ca33', '#ffb74d', '#e65100', '#78909c'];

	function getTypeColor(type: number): string {
		return PALETTE[(type - 1) % PALETTE.length];
	}

	function getScoreColor(score: number): string {
		if (isCategorical) return getTypeColor(Math.round(score));
		if (colorScale === 'flood') {
			if (score >= 10) return '#dc2626';
			if (score >= 4)  return '#eab308';
			return '#3b82f6';
		}
		if (colorScale === 'green') {
			if (score >= 70) return '#22c55e';
			if (score >= 40) return '#166534';
			return '#1e293b';
		}
		if (colorScale === 'warm') {
			if (score >= 70) return '#fde725';
			if (score >= 40) return '#f59e0b';
			return '#0f172a';
		}
		// Sequential (viridis)
		if (score >= 70) return '#fde725';
		if (score >= 40) return '#21918c';
		return '#440154';
	}

	function getScoreLevel(score: number): string {
		if (colorScale === 'flood') {
			if (score >= 10) return i18n.t('legend.high');
			if (score >= 4)  return i18n.t('legend.medium');
			if (score >= 2)  return i18n.t('legend.low');
			return i18n.t('legend.veryLow');
		}
		if (score >= 70) return i18n.t('legend.high');
		if (score >= 40) return i18n.t('legend.medium');
		if (score >= 20) return i18n.t('legend.low');
		return i18n.t('legend.veryLow');
	}

	const legendGradient = $derived(
		colorScale === 'flood'
			? 'linear-gradient(to right, #3b82f6, #eab308, #dc2626)'
			: colorScale === 'green'
			? 'linear-gradient(to right, #1e293b, #166534, #22c55e, #bbf7d0)'
			: colorScale === 'warm'
			? 'linear-gradient(to right, #0f172a, #f59e0b, #fde725)'
			: 'linear-gradient(to right, #440154, #21918c, #fde725)'
	);

	const legendLabels = $derived(
		[i18n.t('legend.low'), i18n.t('legend.medium'), i18n.t('legend.high')]
	);

	const scoreDirection = $derived(
		colorScale === 'flood' ? 'danger' : isCategorical ? 'categorical' : 'good'
	);

	// Auto-select top department on first load
	// Department list loads, user picks manually

	function handleDptoClick(dept: any) {
		if (onSelectDpto) {
			// Support both 'dpto' (Misiones) and 'distrito' (Itapúa) admin column names
			const name = dept.dpto ?? dept.distrito ?? dept.municipio ?? '';
			onSelectDpto(name, dept.parquetKey, dept.centroid as [number, number]);
		}
	}

	function handleBackToDepts() {
		hexStore.setLayer(analysis.id);
	}

	let downloadState = $state<'idle' | 'csv' | 'geojson'>('idle');

	function currentParquetKey(): string {
		return hexStore.selectedParquetKey || 'data';
	}

	async function handleDownloadCsv() {
		if (!dataUrl || downloadState !== 'idle') return;
		downloadState = 'csv';
		try {
			await downloadCsvFromQuery(
				`SELECT * FROM '${dataUrl}'`,
				`spatia_${layerCfg?.id}_${currentParquetKey()}.csv`
			);
		} catch (e) {
			console.warn('CSV download failed:', e);
		} finally {
			downloadState = 'idle';
		}
	}

	async function handleDownloadGeoJson() {
		if (!dataUrl || downloadState !== 'idle') return;
		downloadState = 'geojson';
		try {
			await downloadGeoJsonFromHexQuery(
				`SELECT * FROM '${dataUrl}'`,
				`spatia_${layerCfg?.id}_${currentParquetKey()}.geojson`
			);
		} catch (e) {
			console.warn('GeoJSON download failed:', e);
		} finally {
			downloadState = 'idle';
		}
	}

	function urlForAnalysis(id: string, parquetKey: string): string {
		const tp = hexStore.territoryPrefix;
		if (id === 'flood_risk') return getFloodDptoUrl(parquetKey, tp);
		if (id === 'territorial_scores') return getScoresDptoUrl(parquetKey, tp);
		return getSatDptoUrl(id, parquetKey, tp);
	}

	// Data download URL for selected department.
	// Uses hexStore.selectedParquetKey (set by loadDepartment) instead of looking up in
	// deptList — avoids race conditions where allSummaries is still loading async and
	// deptList is momentarily empty (manifested as missing CSV/GeoJSON buttons for Itapúa).
	const dataUrl = $derived.by(() => {
		if (!selectedDpto || !layerCfg || !hexStore.selectedParquetKey) return null;
		return urlForAnalysis(layerCfg.id, hexStore.selectedParquetKey);
	});

	// Component variables (skip score, type, type_label, pca)
	const componentVars = $derived(
		layerCfg?.variables.filter(v =>
			!['score', 'flood_risk_score', 'risk_score', 'type', 'type_label', 'territorial_type', 'pca_1', 'pca_2'].includes(v.col)
		) ?? []
	);

	// Petal chart data for selected hex
	const petalLabels = $derived(componentVars.map(v => i18n.t(v.labelKey)));
	const hexPetalLayers = $derived.by(() => {
		if (!selectedHex || componentVars.length === 0) return [];
		const values = componentVars.map(v => {
			const val = selectedHex[v.col];
			return typeof val === 'number' ? Math.min(100, Math.max(0, val)) : 0;
		});
		return [{ values, color: getTypeColor(selectedHex.type ?? 1) }];
	});

	// ── Cross-analysis profile for selected hex ──
	const CROSS_ANALYSIS_IDS = ['carbon_stock', 'agri_potential', 'accessibility', 'deforestation_dynamics', 'land_use', 'flood_risk'];
	const CROSS_TITLE_KEYS: Record<string, string> = { land_use: 'sat.landUse.title' };
	const CROSS_ANALYSES = $derived(
		CROSS_ANALYSIS_IDS.map(id => {
			const cfg = HEX_LAYER_REGISTRY[id];
			const titleKey = cfg?.titleKey ?? CROSS_TITLE_KEYS[id] ?? id;
			return { id, label: i18n.t(titleKey) };
		})
	);

	let crossProfile = $state<{ label: string; typeLabel: string }[]>([]);
	let _crossProfileGen = 0;

	$effect(() => {
		const hex = selectedHex;
		const dpto = selectedDpto;
		if (!hex || !dpto || !deptList.length) { crossProfile = []; return; }

		const dept = deptList.find((d: any) => (d.dpto ?? d.distrito ?? d.municipio) === dpto);
		if (!dept) { crossProfile = []; return; }

		const gen = ++_crossProfileGen;
		const h3 = hex.h3index;
		const promises = CROSS_ANALYSES
			.filter(a => a.id !== analysis.id)
			.map(async (a) => {
				try {
					let url: string;
					if (a.id === 'flood_risk') url = getFloodDptoUrl(dept.parquetKey);
					else if (a.id === 'territorial_scores') url = getScoresDptoUrl(dept.parquetKey);
					else url = getSatDptoUrl(a.id, dept.parquetKey);
					const r = await query(`SELECT type_label FROM '${url}' WHERE h3index = '${h3}'`);
					if (r.numRows > 0) {
						const row = r.get(0)!.toJSON() as Record<string, any>;
						return { label: a.label, typeLabel: String(row.type_label || '—') };
					}
				} catch { /* skip unavailable */ }
				return { label: a.label, typeLabel: '—' };
			});

		Promise.all(promises).then(results => {
			if (_crossProfileGen !== gen) return; // newer dept selected
			crossProfile = results;
		});
	});

	// ── Type distribution for selected department ──
	let typeDistribution = $state<{ type: number; label: string; count: number; pct: number; avgScore: number | null }[]>([]);
	let _typeDistGen = 0;

	$effect(() => {
		const dpto = selectedDpto;
		if (!dpto || !dataUrl) { typeDistribution = []; return; }

		// Only categorical analyses (clustered with type labels) have the type/type_label columns.
		// For continuous analyses the query would fail with a Binder Error.
		if (!isCategorical) { typeDistribution = []; return; }

		const gen = ++_typeDistGen;
		const pv = layerCfg?.primaryVariable ?? 'score';
		const scoreCol = pv === 'type' || pv === 'territorial_type' ? '' : `, AVG(${pv}) as avg_score`;
		query(`SELECT type, type_label, COUNT(*) as n${scoreCol} FROM '${dataUrl}' GROUP BY type, type_label ORDER BY n DESC`)
			.then(r => {
				if (_typeDistGen !== gen) return; // newer dept selected
				const total = Array.from({ length: r.numRows }, (_, i) => Number(r.get(i)!.toJSON().n)).reduce((a, b) => a + b, 0);
				typeDistribution = Array.from({ length: r.numRows }, (_, i) => {
					const row = r.get(i)!.toJSON() as Record<string, any>;
					return {
						type: Number(row.type),
						label: String(row.type_label || `Tipo ${row.type}`),
						count: Number(row.n),
						pct: Math.round(Number(row.n) / total * 100),
						avgScore: row.avg_score != null ? Number(row.avg_score) : null,
					};
				});
			})
			.catch(() => { typeDistribution = []; });
	});

	// ── Diagnostic: dominant type per analysis for department ──
	let showDiagnostic = $state(false);
	let diagnosticData = $state<{ label: string; dominant: string; pct: number }[]>([]);

	async function loadDiagnostic() {
		if (!selectedDpto || !deptList.length) return;
		const dept = deptList.find((d: any) => (d.dpto ?? d.distrito ?? d.municipio) === selectedDpto);
		if (!dept) return;

		showDiagnostic = true;
		const allAnalyses = [...CROSS_ANALYSES, { id: analysis.id, label: i18n.t(analysis.titleKey).replace(/ \(Misiones\)/, '') }];

		const results = await Promise.all(allAnalyses.map(async (a) => {
			try {
				let url: string;
				if (a.id === 'flood_risk') url = getFloodDptoUrl(dept.parquetKey);
				else if (a.id === 'territorial_scores') url = getScoresDptoUrl(dept.parquetKey);
				else url = getSatDptoUrl(a.id, dept.parquetKey);
				const r = await query(`SELECT type_label, COUNT(*) as n FROM '${url}' GROUP BY type_label ORDER BY n DESC LIMIT 1`);
				if (r.numRows > 0) {
					const row = r.get(0)!.toJSON() as Record<string, any>;
					const total_r = await query(`SELECT COUNT(*) as t FROM '${url}'`);
					const total = Number(total_r.get(0)!.toJSON().t);
					return { label: a.label, dominant: String(row.type_label), pct: Math.round(Number(row.n) / total * 100) };
				}
			} catch { /* skip */ }
			return { label: a.label, dominant: '—', pct: 0 };
		}));

		diagnosticData = results;
	}

	// PDF report URL for selected department
	const reportUrl = $derived.by(() => {
		if (!selectedDpto || !layerCfg || !deptList.length) return null;
		const dept = deptList.find((d: any) => (d.dpto ?? d.distrito ?? d.municipio) === selectedDpto);
		if (!dept) return null;
		return getReportUrl(layerCfg.id, dept.parquetKey);
	});

	const content = $derived(ANALYSIS_CONTENT[analysis.id] ?? null);

	// ── Temporal toggle support ──
	const isTemporal = $derived(layerCfg?.temporal === true);
	const tMode = $derived(hexStore.temporalMode);

	function getDisplayCol(col: string): string {
		if (!isTemporal || tMode === 'current') return col;
		return getTemporalCol(col, tMode);
	}

	function getDisplayVal(hex: Record<string, any>, col: string): number | null {
		const tCol = getDisplayCol(col);
		return hex[tCol] !== undefined ? (hex[tCol] as number) : null;
	}

	const effectiveLegendGradient = $derived(
		isTemporal && tMode === 'delta'
			? 'linear-gradient(to right, #dc2626, #737373, #22c55e)'
			: legendGradient
	);
	const effectiveLegendLabels = $derived(
		isTemporal && tMode === 'delta'
			? [i18n.t('temporal.legend.worse'), i18n.t('temporal.legend.noChange'), i18n.t('temporal.legend.better')]
			: legendLabels
	);

	const displayScore = $derived(selectedHex ? (getDisplayVal(selectedHex, hexStore.effectivePrimary) ?? 0) : 0);

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

	let reportCopied = $state(false);
	async function copyReportEmail() {
		await navigator.clipboard.writeText('nealab@spatia.ar');
		reportCopied = true;
		setTimeout(() => { reportCopied = false; }, 2000);
	}

	// ── Calibration distribution ──
	interface CalibRow { territory: TerritoryConfig; p25: number; p50: number; p75: number; avg: number }
	let calibrationData = $state<CalibRow[]>([]);
	let calibrationLoading = $state(false);
	let calibrationLoaded = $state(false);

	$effect(() => {
		// Reset calibration when analysis changes
		analysis.id;
		calibrationLoaded = false;
		calibrationLoading = false;
		calibrationData = [];
	});

	async function loadCalibration() {
		if (calibrationLoaded || calibrationLoading || !layerCfg) return;
		calibrationLoading = true;
		const col = layerCfg.primaryVariable ?? 'score';
		const available = Object.values(TERRITORY_REGISTRY).filter((t: TerritoryConfig) =>
			t.available && (layerCfg.coverage?.[t.id] ?? 'available') === 'available'
		);
		const results = await Promise.all(available.map(async (t: TerritoryConfig) => {
			const url = getSatGlobalUrl(layerCfg!.id, t.parquetPrefix);
			try {
				const r = await query(`SELECT quantile_disc("${col}", 0.25) as p25, quantile_disc("${col}", 0.5) as p50, quantile_disc("${col}", 0.75) as p75, avg("${col}") as avg_score FROM '${url}' WHERE "${col}" IS NOT NULL AND "${col}" > 0`);
				if (r.numRows === 0) return null;
				const row = r.get(0)!.toJSON() as Record<string, any>;
				return { territory: t, p25: Number(row.p25), p50: Number(row.p50), p75: Number(row.p75), avg: Number(row.avg_score) } as CalibRow;
			} catch { return null; }
		}));
		calibrationData = results.filter(Boolean) as CalibRow[];
		calibrationLoaded = true;
		calibrationLoading = false;
	}
</script>

<!-- Flood "current extent": Sentinel-1 SAR snapshot date + revisit cadence.
     Hoisted snippet rendered inside each freshness/source box; renders nothing
     unless the active territory's summary carries metadata.sar_date. -->
{#snippet sarFreshness()}
	{#if sarDateDisplay}
		<div><strong>{i18n.t('section.sarImage')}:</strong> {sarDateDisplay} · {i18n.t('section.sarRevisit')}</div>
	{/if}
{/snippet}

{#if selectedHex && selectedDpto && isPerDept}
	<!-- ═══ HEX DETAIL VIEW ═══ -->
	<div class="view">
		<button class="back-btn" onclick={handleBackToDepts}>{i18n.t('analysis.flood.topDepts')}</button>

		<div class="hex-header">
			<div class="hex-id" title={selectedHex.h3index}>
				{selectedHex.h3index.slice(0, 4)}...{selectedHex.h3index.slice(-4)}
			</div>
			{#if selectedHex.type_label}
				<div class="risk-badge" style:background={getTypeColor(selectedHex.type ?? 1)}>
					{selectedHex.type_label}
				</div>
			{:else if !isCategorical}
				<div class="risk-badge" style:background={getScoreColor(displayScore)}>
					{getScoreLevel(displayScore)}
				</div>
			{/if}
		</div>

		{#if isTemporal}
			<TemporalToggle {hexStore} layerId={layerCfg?.id ?? ''} />
		{/if}

		{#if hexPetalLayers.length > 0}
			<div class="petal-section">
				<div class="petal-wrapper">
					<PetalChart layers={hexPetalLayers} labels={petalLabels} size={240} />
				</div>
				<p class="petal-hint">{i18n.t('analysis.petalHint')}</p>
			</div>
		{/if}
		{#if CENSUS_ANALYSES.has(analysis.id) && componentVars.length > 0}
			<div class="census-detail">
				{#each componentVars as v}
					{@const val = selectedHex[v.col]}
					{@const rawVal = v.rawCol ? selectedHex[v.rawCol] : null}
					{@const displayVal = (rawVal != null && typeof rawVal === 'number') ? rawVal : val}
					<!-- fmtSmart renders '—' for a missing value; coercing to 0 here used to
					     display a fake "0 <unit>" when the column wasn't in the loaded data
					     (stale cache / schema drift). A real 0 still shows as 0. -->
					<div class="cd-row">
						<span class="cd-label">{i18n.t(v.labelKey)}</span>
						<span class="cd-val-data">{fmtSmart(displayVal)}{v.unit ? ` ${v.unit}` : ' /100'}</span>
					</div>
				{/each}
			</div>
		{/if}

		{#if crossProfile.length > 0}
			<div class="cross-profile">
				<div class="cross-title">{i18n.t('section.territorialProfile')}</div>
				{#each crossProfile as cp}
					<div class="cross-row">
						<span class="cross-label">{cp.label}</span>
						<span class="cross-value">{cp.typeLabel}</span>
					</div>
				{/each}
			</div>
		{/if}

		{#if dataUrl}
			<div class="download-row">
				<button class="download-btn" onclick={handleDownloadCsv} disabled={downloadState !== 'idle'} title="CSV del departamento (todos los hexágonos)">
					{downloadState === 'csv' ? '…' : 'CSV'}
				</button>
				<button class="download-btn download-secondary" onclick={handleDownloadGeoJson} disabled={downloadState !== 'idle'} title="GeoJSON del departamento (polígonos H3)">
					{downloadState === 'geojson' ? '…' : 'GeoJSON'}
				</button>
			</div>
		{/if}

		{#if freshness}
			<div class="source-note-box">
				<div><strong>{i18n.t('section.source')}:</strong> {i18n.t(freshness.sourceKey)}</div>
				<div><strong>{i18n.t('section.processed')}:</strong> {freshness.processedDate}</div>
				{@render sarFreshness()}
			</div>
		{/if}
	</div>

{:else if selectedDpto && isPerDept}
	<!-- ═══ DEPARTMENT SELECTED (minimal, like FloodRisk) ═══ -->
	<div class="view">
		<button class="back-btn" onclick={handleBackToDepts}>{i18n.t('analysis.flood.topDepts')}</button>

		<div class="dept-active-title">{formatDept(selectedDpto)}</div>

		{#if dataUrl}
			<div class="download-row">
				<button
					class="download-btn"
					onclick={handleDownloadCsv}
					disabled={downloadState !== 'idle'}
					title="CSV · todas las variables por hexágono H3"
				>
					{downloadState === 'csv' ? '…' : '↓ CSV'}
				</button>
				<button
					class="download-btn download-secondary"
					onclick={handleDownloadGeoJson}
					disabled={downloadState !== 'idle'}
					title="GeoJSON · polígonos H3 para QGIS / ArcGIS"
				>
					{downloadState === 'geojson' ? '…' : '↓ GeoJSON'}
				</button>
			</div>
		{/if}

		{#if content}
			<a class="methodology-link" href={lp(`/metodologia/${analysis.id}`, i18n.locale)} target="_blank" rel="noopener">
				¿Cómo se calcula? →
			</a>
		{/if}

		{#if isTemporal}
			<TemporalToggle {hexStore} layerId={layerCfg?.id ?? ''} />
		{/if}

		{#if loading}
			<div class="loading">{i18n.t('analysis.loading')}</div>
		{:else}
			<div class="hint">{i18n.t('analysis.flood.clickHint')}</div>
		{/if}

		{#if typeDistribution.length > 0}
			<div class="type-dist">
				<div class="cross-title">{i18n.t('section.typeDistribution')}</div>
				{#each typeDistribution as td}
					<div class="cross-row">
						<span class="cross-label">{td.label}</span>
						<span class="cross-value">{td.count.toLocaleString()} ({td.pct}%)</span>
					</div>
				{/each}
			</div>
		{/if}

		<div class="action-row">
			<button class="action-btn" onclick={copyReportEmail} style="text-align:center;cursor:pointer;">
				{reportCopied ? 'nealab@spatia.ar ✓' : i18n.t('section.requestReport')}
			</button>
		</div>

		{#if content}
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.howToRead')}</summary>
				<div class="method-body">
					<p class="explain-text">{content.howToRead[i18n.locale] ?? content.howToRead.es}</p>
				</div>
			</details>
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.implications')}</summary>
				<div class="method-body">
					<p class="explain-text">{content.implications[i18n.locale] ?? content.implications.es}</p>
				</div>
			</details>
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.methodology')}</summary>
				<div class="method-body">
					<p class="explain-text">{content.method[i18n.locale === 'pt' || i18n.locale === 'gn' ? 'es' : i18n.locale] ?? content.method.es}</p>
				</div>
			</details>
		{/if}

		{#if freshness}
			<div class="source-note-box">
				<div><strong>{i18n.t('section.source')}:</strong> {i18n.t(freshness.sourceKey)}</div>
				<div><strong>{i18n.t('section.processed')}:</strong> {freshness.processedDate}</div>
				{@render sarFreshness()}
			</div>
		{/if}
	</div>

{:else if isPerDept}
	<!-- ═══ DEPARTMENT LIST ═══ -->
	<div class="view">
		<p class="desc">{i18n.t(analysis.descKey)}</p>

		{#if !isCategorical}
			<div class="score-info-box">
				<span class="score-range">{i18n.t('legend.range')}</span>
				{#if scoreDirection === 'danger'}
					<span class="score-dir score-dir-danger">{i18n.t('legend.highMeansDanger')}</span>
				{:else}
					<span class="score-dir score-dir-good">{i18n.t('legend.highMeansGood')}</span>
				{/if}
			</div>
		{/if}

		{#if activeSummaryData}
			<div class="summary-cards">
				<div class="summary-card">
					<div class="card-value">{activeSummaryData.province.total_hexes?.toLocaleString()}</div>
					<div class="card-label">{i18n.t('section.zonesAnalyzed')}</div>
				</div>
			</div>
		{/if}

		{#if isTemporal}
			<TemporalToggle {hexStore} layerId={layerCfg?.id ?? ''} />
		{/if}

		{#if tabTerritories.length > 1}
			<div class="territory-tabs-wrapper">
				{#each COUNTRY_ORDER as country}
					{#if tabsByCountry.has(country)}
						<div class="territory-country-row">
							<span class="country-flag">{COUNTRY_FLAGS[country]}</span>
							<div class="territory-tabs">
								{#each tabsByCountry.get(country)! as t}
									<button
										class="territory-tab"
										class:active={activeTab === t.id}
										onclick={() => handleTabClick(t.id)}
									>{t.shortLabel}</button>
								{/each}
							</div>
						</div>
					{/if}
				{/each}
			</div>
		{/if}

		<div class="dept-section">
			<div class="section-title">{i18n.t('analysis.flood.topDepts')}</div>
			{#if deptList.length === 0 && isPerDept}
				<div class="dept-row" style="color: var(--text-secondary); font-style: italic; padding: 8px 0;">
					{hexStore.territoryPrefix ? 'No hay datos departamentales para este territorio.' : 'Cargando…'}
				</div>
			{:else}
				{#if !selectedDpto}
					<p class="dept-select-hint">▼ Seleccioná un sector para ver el análisis</p>
				{/if}
				{#if deptList.length > 12}
					<input
						class="dept-filter"
						type="search"
						placeholder="Filtrar entre {deptList.length}…"
						bind:value={deptFilter}
					/>
				{/if}
				<div class="dept-list-scroll">
					{#each filteredDeptList as dept}
						<button class="dept-row dept-clickable" onclick={() => handleDptoClick(dept)}>
							<div class="dept-name">{formatDept(dept.dpto ?? dept.distrito ?? dept.municipio)}</div>
							<div class="dept-score">
								{dept.hex_count?.toLocaleString() ?? ''} hex
							</div>
						</button>
					{:else}
						<div class="dept-row" style="color: var(--text-secondary); font-style: italic;">Sin coincidencias</div>
					{/each}
				</div>
			{/if}
		</div>

		{#if analysis.comparable}
			<details class="method-details" ontoggle={(e) => { if ((e.target as HTMLDetailsElement).open) loadCalibration(); }}>
				<summary class="method-summary">Distribución por territorio /100</summary>
				<div class="method-body">
					{#if calibrationLoading}
						<p class="explain-text">Cargando…</p>
					{:else if calibrationData.length > 0}
						<table class="calib-table">
							<thead><tr><th>Territorio</th><th>P25</th><th>Med</th><th>P75</th><th>Avg</th></tr></thead>
							<tbody>
							{#each calibrationData as d}
								<tr>
									<td>{d.territory.flag} {d.territory.shortLabel}</td>
									<td>{d.p25.toFixed(0)}</td>
									<td>{d.p50.toFixed(0)}</td>
									<td>{d.p75.toFixed(0)}</td>
									<td>{d.avg.toFixed(1)}</td>
								</tr>
							{/each}
							</tbody>
						</table>
						<p class="explain-text" style="margin-top:4px">Distribuciones similares entre territorios confirman que los goalposts están bien calibrados.</p>
					{/if}
				</div>
			</details>
		{/if}

		{#if content}
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.howToRead')}</summary>
				<div class="method-body">
					<p class="explain-text">{content.howToRead[i18n.locale] ?? content.howToRead.es}</p>
				</div>
			</details>

			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.implications')}</summary>
				<div class="method-body">
					<p class="explain-text">{content.implications[i18n.locale] ?? content.implications.es}</p>
				</div>
			</details>

			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.methodology')}</summary>
				<div class="method-body">
					<p class="explain-text">{content.method[i18n.locale === 'pt' || i18n.locale === 'gn' ? 'es' : i18n.locale] ?? content.method.es}</p>
					<div class="method-components">
						{#each componentVars as v}
							<div class="method-item">
								<span class="method-term">{i18n.t(v.labelKey)}</span>
							</div>
						{/each}
					</div>
				</div>
			</details>
		{/if}


		{#if freshness}
			<div class="source-note-box">
				<div><strong>{i18n.t('section.source')}:</strong> {i18n.t(freshness.sourceKey)}</div>
				<div><strong>{i18n.t('section.processed')}:</strong> {freshness.processedDate}</div>
				{@render sarFreshness()}
			</div>
		{/if}

		<CTADiagnostic analysisName={i18n.t(analysis.titleKey)} />
	</div>

{:else}
	<!-- ═══ NON-perDepartment (Overture layers) ═══ -->
	<div class="view">
		<p class="desc">{i18n.t(analysis.descKey)}</p>

		{#if freshness}
			<div class="freshness">
				<span class="freshness-label">{i18n.t('data.updatedAt')}: {freshness.processedDate}</span>
				<span class="freshness-source">{i18n.t(freshness.sourceKey)}</span>
				{#if sarDateDisplay}
					<span class="freshness-label">{i18n.t('section.sarImage')}: {sarDateDisplay} · {i18n.t('section.sarRevisit')}</span>
				{/if}
			</div>
		{/if}

		{#if loading}
			<div class="loading">{i18n.t('lens.loading')}</div>
		{:else if layerCfg}
			<div class="variables-hint">
				{#each layerCfg.variables as v}
					<div class="variable-tag">{i18n.t(v.labelKey)}</div>
				{/each}
			</div>

			<!-- Temporal toggle for non-perDepartment temporal layers (censo_temporal).
			     Guarded by isTemporal: the only non-perDept temporal layer is censo_temporal,
			     so this renders for that layer only and is inert for every other global layer. -->
			{#if isTemporal}
				<TemporalToggle {hexStore} layerId={layerCfg?.id ?? ''} />
			{/if}

			{#if selectedHexes.size === 0}
				<p class="hint">{i18n.t('lens.selectRadio')}</p>
			{:else}
				<div class="selected-hexes">
					{#each [...selectedHexes] as [h3index, sel]}
						<div class="hex-card">
							<div class="hex-id">{h3index.slice(0, 4)}...{h3index.slice(-4)}</div>
							{#if analysis.id === 'sociodemographic' && componentVars.length > 0}
								{@const cardPetalVals = componentVars.map(v => { const n = Number(sel.data[v.col]); return Number.isFinite(n) ? Math.min(100, Math.max(0, n)) : 0; })}
								{@const cardPetalColor = getTypeColor(Number(sel.data['type']) || 1)}
								<div class="hex-petal">
									<PetalChart layers={[{values: cardPetalVals, color: cardPetalColor}]} labels={petalLabels} size={160} />
								</div>
							{/if}
							<div class="hex-values">
								{#each layerCfg.variables as v}
									{@const val = sel.data[v.col]}
									{#if val != null && !(v.hideIfZero && val === 0)}
										<div class="hex-val">
											<span class="hex-val-label">{i18n.t(v.labelKey)}</span>
											<span class="hex-val-num">{typeof val === 'number' ? (Number.isInteger(val) ? val.toLocaleString() : val.toFixed(1)) : val}</span>
										</div>
									{/if}
								{/each}
							</div>
						</div>
					{/each}
				</div>
			{/if}
		{/if}

		{#if content}
			<a class="methodology-link" href={lp(`/metodologia/${analysis.id}`, i18n.locale)} target="_blank" rel="noopener">
				¿Cómo se calcula? →
			</a>
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.howToRead')}</summary>
				<div class="method-body">
					<p class="explain-text">{content.howToRead[i18n.locale] ?? content.howToRead.es}</p>
				</div>
			</details>
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.implications')}</summary>
				<div class="method-body">
					<p class="explain-text">{content.implications[i18n.locale] ?? content.implications.es}</p>
				</div>
			</details>
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.methodology')}</summary>
				<div class="method-body">
					<p class="explain-text">{content.method[i18n.locale === 'pt' || i18n.locale === 'gn' ? 'es' : i18n.locale] ?? content.method.es}</p>
				</div>
			</details>
		{/if}

		<CTADiagnostic analysisName={i18n.t(analysis.titleKey)} />
	</div>
{/if}

<style>
	.view { font-size: 11px; }
	.desc { color: #a3a3a3; margin: 0 0 8px; line-height: 1.4; }
	.score-info-box { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; padding: 5px 8px; background: rgba(255,255,255,0.04); border-radius: 4px; border: 1px solid rgba(255,255,255,0.06); }
	.score-range { font-size: 9px; color: #737373; font-weight: 600; }
	.score-dir { font-size: 9px; font-weight: 500; }
	.score-dir-danger { color: #f87171; }
	.score-dir-good { color: #4ade80; }
	.freshness { display: flex; flex-direction: column; gap: 2px; margin-bottom: 8px; padding: 4px 6px; background: rgba(255,255,255,0.03); border-radius: 4px; }
	.freshness-label { color: #737373; font-size: 9px; }
	.freshness-source { color: #525252; font-size: 9px; }
	.loading { color: #d4d4d4; font-size: 10px; text-align: center; padding: 20px 0; }
	.hint { font-size: 9px; color: #a3a3a3; text-align: center; margin-top: 8px; }

	/* ── Summary cards ── */
	.summary-cards { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-bottom: 12px; }
	.summary-card { background: rgba(100,116,139,0.08); border-radius: 6px; padding: 8px 6px; text-align: center; }
	.card-value { font-size: 15px; font-weight: 700; color: #e2e8f0; }
	.card-label { font-size: 8px; color: #d4d4d4; margin-top: 2px; }

	/* ── Department list ── */
	.dept-section { margin-bottom: 10px; }
	.dept-filter {
		width: 100%;
		background: rgba(255,255,255,0.05);
		border: 1px solid rgba(255,255,255,0.10);
		border-radius: 4px;
		color: #e2e8f0;
		font-size: 10px;
		font-family: inherit;
		padding: 4px 8px;
		margin-bottom: 6px;
		outline: none;
	}
	.dept-filter:focus { border-color: rgba(96,165,250,0.5); }
	.dept-filter::placeholder { color: rgba(255,255,255,0.30); }
	.dept-list-scroll {
		max-height: 264px; /* ~11 rows — keeps charts reachable below 300-500 row BR lists */
		overflow-y: auto;
		scrollbar-width: thin;
		scrollbar-color: #334155 transparent;
	}
	.section-title { font-size: 10px; font-weight: 600; color: #cbd5e1; margin-bottom: 6px; }
	.dept-select-hint { font-size: 9px; color: #60a5fa; margin: 0 0 6px; font-style: italic; opacity: 0.8; }
	.dept-row { display: flex; align-items: center; gap: 6px; margin-bottom: 3px; }
	.dept-clickable { background: none; border: none; width: 100%; padding: 4px 2px; border-radius: 4px; cursor: pointer; transition: background 0.15s; }
	.dept-clickable:hover { background: rgba(96,165,250,0.1); }
	.dept-name { font-size: 9px; color: #d4d4d4; width: 72px; text-align: left; flex-shrink: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.dept-bar-wrap { flex: 1; height: 4px; background: rgba(100,116,139,0.15); border-radius: 2px; overflow: hidden; }
	.dept-bar { height: 100%; border-radius: 2px; transition: width 0.3s; }
	.dept-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
	.dept-score { font-size: 9px; font-weight: 600; min-width: 24px; text-align: right; }
	.dept-active-title { font-size: 14px; font-weight: 700; color: #e2e8f0; margin-bottom: 8px; }

	/* ── Navigation ── */
	.back-btn { background: none; border: none; color: #60a5fa; font-size: 10px; cursor: pointer; padding: 0; margin-bottom: 8px; }
	.back-btn:hover { text-decoration: underline; }

	/* ── Hex detail ── */
	.hex-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
	.hex-id { font-family: monospace; font-size: 10px; color: #d4d4d4; }
	.risk-badge { font-size: 9px; font-weight: 700; color: #000; padding: 2px 8px; border-radius: 9999px; }
	.score-bar { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
	.score-label { font-size: 9px; color: #d4d4d4; white-space: nowrap; }
	.score-track { flex: 1; height: 6px; background: rgba(100,116,139,0.2); border-radius: 3px; overflow: hidden; }
	.score-fill { height: 100%; border-radius: 3px; transition: width 0.3s; }
	.score-value { font-size: 13px; font-weight: 700; min-width: 32px; text-align: right; }
	.petal-section { margin: 6px 0; }
	.petal-hint { font-size: 8px; color: rgba(255,255,255,0.35); text-align: center; margin: 2px 0 0; line-height: 1.3; }
	.census-detail { display: flex; flex-direction: column; gap: 4px; margin: 8px 0; }
	.cd-row { display: flex; align-items: center; gap: 6px; }
	.cd-label { font-size: 9px; color: #d4d4d4; flex: 0 0 auto; min-width: 100px; }
	.cd-bar-track { flex: 1; height: 6px; background: #1e293b; border-radius: 3px; overflow: hidden; }
	.cd-bar-fill { height: 100%; border-radius: 3px; transition: width 0.3s; min-width: 2px; }
	.cd-val { font-size: 9px; font-weight: 600; color: #cbd5e1; width: 28px; text-align: right; flex-shrink: 0; }
	.cd-val-data { font-size: 10px; font-weight: 600; color: #e2e8f0; text-align: right; margin-left: auto; white-space: nowrap; }
	.petal-wrapper { display: flex; justify-content: center; margin: 0 auto; max-width: 260px; }

	/* Cross-analysis profile + type distribution + diagnostic */
	.cross-profile, .type-dist, .diagnostic { margin: 10px 0; padding: 8px 0; border-top: 1px solid rgba(255,255,255,0.06); }
	.cross-title { font-size: 9px; font-weight: 600; color: #a3a3a3; text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 6px; }
	.cross-row { display: flex; align-items: center; gap: 6px; padding: 2px 0; font-size: 10px; }
	.cross-label { color: #737373; flex-shrink: 0; min-width: 90px; }
	.cross-value { color: #d4d4d4; font-weight: 500; }
	.type-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
	.action-row { display: flex; gap: 6px; margin: 10px 0; }
	.action-btn { flex: 1; padding: 6px 8px; background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08); border-radius: 4px; color: #a3a3a3; font-size: 9px; cursor: pointer; transition: all 0.15s; }
	.action-btn:hover { background: rgba(255,255,255,0.08); color: #d4d4d4; }
	.detail-label { font-size: 9px; color: #d4d4d4; margin-bottom: 2px; }
	.detail-value { font-size: 14px; font-weight: 700; color: #e2e8f0; }
	.detail-desc { font-size: 8px; color: #a3a3a3; margin-top: 2px; }

	/* ── Legend ── */
	.flood-legend { margin: 12px 0; }
	.legend-title { font-size: 9px; font-weight: 600; color: #d4d4d4; margin-bottom: 4px; }
	.legend-bar { height: 8px; border-radius: 4px; }
	.legend-labels { display: flex; justify-content: space-between; font-size: 8px; color: #a3a3a3; margin-top: 2px; }

	/* ── Collapsibles ── */
	.method-details { margin-top: 10px; border: 1px solid rgba(100,116,139,0.15); border-radius: 6px; overflow: hidden; }
	.method-summary { font-size: 9px; font-weight: 600; color: #d4d4d4; padding: 6px 8px; cursor: pointer; user-select: none; list-style: none; display: flex; align-items: center; gap: 4px; }
	.method-summary::before { content: '\25B8'; font-size: 8px; transition: transform 0.15s; }
	.method-details[open] > .method-summary::before { transform: rotate(90deg); }
	.method-summary::-webkit-details-marker { display: none; }
	.method-body { padding: 4px 8px 8px; }
	.method-item { margin-bottom: 4px; }
	.method-term { font-size: 9px; font-weight: 600; color: #cbd5e1; }
	.explain-text { font-size: 9px; color: #a3a3a3; line-height: 1.5; margin: 2px 0 0; }
	.mini-legend { margin-top: 6px; }
	.method-components { margin-top: 6px; }

	/* ── Methodology link ── */
	.methodology-link { display: inline-block; font-size: 9px; color: #94a3b8; text-decoration: none; padding: 4px 0; margin: 4px 0 10px; transition: color 0.15s; }
	.methodology-link:hover { color: #60a5fa; text-decoration: underline; }

	/* ── Download button ── */
	.download-btn { display: block; text-align: center; padding: 6px 10px; margin: 0; background: rgba(59,130,246,0.15); border: 1px solid rgba(59,130,246,0.3); border-radius: 4px; color: #60a5fa; font-size: 9px; font-weight: 600; text-decoration: none; transition: all 0.15s; cursor: pointer; font-family: inherit; }
	.download-btn:hover:not(:disabled) { background: rgba(59,130,246,0.25); border-color: rgba(59,130,246,0.5); }
	.download-btn:disabled { cursor: wait; opacity: 0.6; }
	.download-row { display: flex; gap: 6px; margin: 10px 0; }
	.download-row .download-btn { flex: 1; }
	.download-secondary { background: rgba(255,255,255,0.05); border-color: rgba(255,255,255,0.15); color: #a3a3a3; }
	.download-secondary:hover:not(:disabled) { background: rgba(255,255,255,0.1); border-color: rgba(255,255,255,0.25); color: #d4d4d4; }
	.download-date { font-weight: 400; font-size: 8px; opacity: 0.7; }

	/* ── Source box ── */
	.source-note-box { margin-top: 10px; padding: 8px 10px; background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.08); border-radius: 6px; font-size: 9px; color: #e2e8f0; line-height: 1.5; }
	.source-note-box strong { color: #f8fafc; }

	/* ── Non-perDept hex cards ── */
	.variables-hint { display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 10px; }
	.variable-tag { background: rgba(255,255,255,0.06); color: #d4d4d4; padding: 2px 6px; border-radius: 3px; font-size: 9px; }
	.selected-hexes { display: flex; flex-direction: column; gap: 6px; }
	.hex-card { background: rgba(255,255,255,0.04); border-radius: 6px; padding: 6px 8px; }
	.hex-petal { margin: 4px 0 6px; }
	.hex-values { display: flex; flex-direction: column; gap: 2px; }
	.hex-val { display: flex; justify-content: space-between; align-items: baseline; }
	.hex-val-label { color: #a3a3a3; }
	.hex-val-num { color: #e5e5e5; font-weight: 500; font-variant-numeric: tabular-nums; }

	/* ── Territory tabs ── */
	.territory-tabs-wrapper { display: flex; flex-direction: column; gap: 3px; margin-bottom: 10px; }
	.territory-country-row { display: flex; align-items: flex-start; gap: 4px; }
	.country-flag { font-size: 11px; line-height: 20px; flex-shrink: 0; }
	.territory-tabs { display: flex; flex-wrap: wrap; gap: 2px; flex: 1; }
	.territory-tab { background: rgba(255,255,255,0.04); border: 1px solid rgba(100,116,139,0.18); border-radius: 3px; color: #a3a3a3; font-size: 9px; font-weight: 500; padding: 3px 5px; cursor: pointer; transition: all 0.15s; font-family: inherit; }
	.territory-tab.active { background: rgba(59,130,246,0.15); color: #60a5fa; font-weight: 700; border-color: rgba(59,130,246,0.3); }
	.territory-tab:hover:not(.active) { background: rgba(255,255,255,0.07); color: #d4d4d4; }

	/* ── Temporal toggle ── */
	.temporal-toggle { display: flex; gap: 0; margin-bottom: 10px; border-radius: 6px; overflow: hidden; border: 1px solid rgba(100,116,139,0.2); }
	.temporal-toggle button { flex: 1; background: rgba(255,255,255,0.03); border: none; color: #a3a3a3; font-size: 9px; font-weight: 500; padding: 5px 4px; cursor: pointer; transition: all 0.15s; }
	.temporal-toggle button:not(:last-child) { border-right: 1px solid rgba(100,116,139,0.15); }
	.temporal-toggle button.active { background: rgba(59,130,246,0.15); color: #60a5fa; font-weight: 700; }
	.temporal-toggle button:hover:not(.active) { background: rgba(255,255,255,0.06); }

	/* ── Calibration table ── */
	.calib-table { width: 100%; border-collapse: collapse; font-size: 9px; margin-top: 4px; }
	.calib-table th { color: #737373; font-weight: 600; text-align: right; padding: 2px 4px; }
	.calib-table th:first-child { text-align: left; }
	.calib-table td { color: #d4d4d4; text-align: right; padding: 2px 4px; border-top: 1px solid rgba(255,255,255,0.04); font-variant-numeric: tabular-nums; }
	.calib-table td:first-child { text-align: left; color: #a3a3a3; }
</style>
