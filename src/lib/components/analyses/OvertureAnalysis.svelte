<script lang="ts">
	import { MAP_TIERRA, MAP_SELVA, MAP_DIV, rampGradient, TIERRA as RANK_TOWARD, RIO as RANK_AWAY } from '$lib/utils/sello';
	import { untrack, onDestroy } from 'svelte';
	import type { HexStore } from '$lib/stores/hex.svelte';
	import type { TerritoryStore } from '$lib/stores/territory.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { unitLabel } from '$lib/utils/units';
	import { lp } from '$lib/utils/locale-path';
	import { formatDept } from '$lib/utils/format';
	import RequestDiagnostic from '$lib/components/RequestDiagnostic.svelte';
	import PetalChart from '$lib/components/PetalChart.svelte';
	import TemporalToggle from '$lib/components/TemporalToggle.svelte';
	import CensoTemporalDeptList from '$lib/components/CensoTemporalDeptList.svelte';
	import { HEX_LAYER_REGISTRY, DATA_FRESHNESS, getSatDptoUrl, getFloodDptoUrl, getScoresDptoUrl, TERRITORY_REGISTRY, type AnalysisConfig, type TerritoryConfig, type CountryId } from '$lib/config';
	import { loadDeptSummary } from '$lib/utils/deptSummaries';
	import { withRef, territoryRef } from '$lib/utils/territoryRef';
	import { query } from '$lib/stores/duckdb';
	import { downloadCsvFromQuery, downloadGeoJsonFromHexQuery } from '$lib/utils/data-export';
	import { ANALYSIS_CONTENT, resolveMethod } from '$lib/content/methodology';

	let {
		analysis,
		hexStore,
		territoryStore,
		onSelectDpto,
		onCensoDeptSelect,
	}: {
		analysis: AnalysisConfig;
		hexStore: HexStore;
		territoryStore: TerritoryStore;
		onSelectDpto?: (dpto: string, parquetKey: string, centroid: [number, number]) => void;
		onCensoDeptSelect?: (feature: any | null) => void;
	} = $props();

	const layerCfg = $derived(HEX_LAYER_REGISTRY[analysis.id]);
	const freshness = $derived(layerCfg ? DATA_FRESHNESS[layerCfg.parquet] : null);
	const loading = $derived(hexStore.loading);
	const selectedHexes = $derived(hexStore.selectedHexes);
	const isPerDept = $derived(layerCfg?.perDepartment === true);
	const selectedDpto = $derived(hexStore.selectedDpto);


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
	// "current extent" component. The weekly NEA refresh (pipeline/
	// update_flood_current.py) publishes it in data/flood_meta.json, read at
	// runtime so a new image shows its date without a redeploy; territories the
	// refresh does not cover fall back to metadata.sar_date of the bundled
	// *_flood_dept_summary.json. Null → the SAR line is hidden.
	let floodMeta = $state<Record<string, { sar_date?: string; processed?: string }> | null>(null);
	$effect(() => {
		if (analysis.id !== 'flood_risk' || floodMeta) return;
		fetch(`https://cdn.spatia.ar/data/flood_meta.json?h=${Math.floor(Date.now() / 3_600_000)}`)
			.then(r => (r.ok ? r.json() : null))
			.then(j => { if (j) floodMeta = j; })
			.catch(() => {});
	});
	const sarDate = $derived(
		analysis.id === 'flood_risk'
			? (floodMeta?.[activeTab]?.sar_date ?? activeSummaryData?.metadata?.sar_date ?? null)
			: null
	);
	// ISO (2026-03-21) → DD/MM/YYYY to match the processedDate format shown alongside.
	const isoToDmy = (iso: string) => {
		const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
		return m ? `${m[3]}/${m[2]}/${m[1]}` : iso;
	};
	const sarDateDisplay = $derived(sarDate ? isoToDmy(sarDate) : null);
	// Processed date: the refresh's own date where it ran, the static one otherwise.
	const processedDisplay = $derived(
		analysis.id === 'flood_risk' && floodMeta?.[activeTab]?.processed
			? isoToDmy(floodMeta[activeTab].processed!)
			: (freshness?.processedDate ?? '')
	);

	// Department ranking (layerCfg.deptRank): physical metric per department from
	// the summary's `rank` block. Only when the active territory's summary has it.
	// The pipeline flags territories where the metric does not separate departments
	// (flood HAND < 5 m on the Chaco plain: medians 85–100 %). There a note replaces
	// the bars (the radar ranking for the plain was retired: Sentinel-1 overestimates
	// water on the Chaco plain).
	const rankSummary = $derived(allSummaries.size > 0 ? allSummaries.get(activeTab) : deptSummary);
	const rankSaturatedRaw = $derived(!!rankSummary?.metadata?.rank?.saturated);
	// deptRank.metrics: several readings of the layer, switched from tabs.
	// Only readings with data in the active territory get a tab (accessibility has
	// no cabecera times in Chaco/Formosa, no hospital distances in Brazil).
	let rankMetricIdx = $state(0);
	$effect(() => { layerCfg?.id; rankMetricIdx = 0; });
	const rankMetrics = $derived.by(() => {
		const base = layerCfg?.deptRank;
		if (!base?.metrics) return [];
		// A reading also gets a tab when only its territory-wide block exists
		// (deptRank.province): the change tab where the department order is not stable.
		return base.metrics.filter(m =>
			rankSummary?.departments?.some((d: any) => d.rank?.[m.field ?? base.field] != null)
			|| (!!(m.province ?? base.province) && !!rankSummary?.metadata?.rank?.[(m.province ?? base.province)!]?.province?.label));
	});
	$effect(() => { if (rankMetricIdx >= rankMetrics.length) rankMetricIdx = 0; });
	const rankCfg = $derived.by(() => {
		const base = layerCfg?.deptRank ?? null;
		if (!base) return null;
		const m = rankMetrics[rankMetricIdx];
		return m ? { ...base, ...m } : base;
	});
	const rankUnit = $derived(rankCfg ? (rankCfg.unitKey ? i18n.t(rankCfg.unitKey) : rankCfg.unit) : '');
	// «Cómo leer» opens with the department view on every ranked layer; flood explains
	// it in its own text (tab by tab).
	const showDeptView = $derived(!!layerCfg?.deptRank && layerCfg.id !== 'flood_risk');
	// A reading that exists only by department: say what the hexagons show instead.
	const hexNoneNote = $derived.by(() => {
		if (!rankCfg?.hexNone || !layerCfg) return '';
		const pv = hexStore.effectivePrimary;
		const v = layerCfg.variables?.find((x: any) => x.col === pv || x.rawCol === pv);
		return v ? i18n.t('rank.hexNone').replace('{v}', i18n.t(v.labelKey)) : '';
	});
	// rankBars: departments with a value for the active reading. rankProvince: the
	// territory-wide block (deptRank.province), shown above the bars or instead of them.
	const rankBars = $derived(!!rankCfg && !!rankSummary?.departments?.some((d: any) => d.rank?.[rankCfg.field] != null));
	const rankProvince = $derived.by(() => {
		const key = rankCfg?.province;
		const p = key ? rankSummary?.metadata?.rank?.[key]?.province : null;
		return p?.label ? p : null;
	});
	const hasRank = $derived.by(() => {
		if (!rankCfg) return false;
		if (rankSaturatedRaw) return false;
		return rankBars || !!rankProvince;
	});
	const activeTerritoryLabel = $derived(TERRITORY_REGISTRY[activeTab]?.label ?? '');
	const rankSaturated = $derived(!!rankCfg?.saturatedKey && rankSaturatedRaw);
	// The map paints the active reading's per-hex variable (deptRank.hexVariable), so
	// an open department reads like its bar. No ranking (saturated plain) → primary.
	$effect(() => {
		const v = hasRank ? (rankCfg?.hexVariable ?? null) : null;
		const f = hasRank ? (rankCfg?.macizoFocus ?? null) : null;
		untrack(() => { hexStore.setColorVariable(v); hexStore.setMacizoFocus(f); });
	});

	const deptList = $derived.by(() => {
		const summary = allSummaries.size > 0 ? allSummaries.get(activeTab) : deptSummary;
		if (!summary?.departments) return [];
		if (hasRank && rankBars) {
			const f = rankCfg!.field;
			const missing = rankCfg!.signed ? -Infinity : -1;   // no value → bottom of the list
			return [...summary.departments].sort((a: any, b: any) => (b.rank?.[f] ?? missing) - (a.rank?.[f] ?? missing));
		}
		return [...summary.departments].sort((a: any, b: any) => b.avg_score - a.avg_score);
	});


	const rankMax = $derived(hasRank && rankBars ? Math.max(...deptList.map((d: any) => Math.abs(d.rank?.[rankCfg!.field] ?? 0)), 1e-9) : 1);
	const fmtRank = (v: number | null | undefined, decimals: number, signed = false) =>
		v == null ? '—' : (signed && v > 0 ? '+' : '') + v.toLocaleString(i18n.locale, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
	const selectedRankPos = $derived.by(() => {
		if (!hasRank || !rankBars || !selectedDpto) return null;
		const i = deptList.findIndex((d: any) => (d.dpto ?? d.distrito ?? d.municipio) === selectedDpto);
		return i < 0 ? null : { pos: i + 1, total: deptList.length, dept: deptList[i] };
	});

	// Province view: the map paints the departments with the bars' values and scale.
	// Only while the bars are shown; an open department shows its hexagons instead.
	$effect(() => {
		const show = hasRank && rankBars && !!rankCfg && !selectedDpto && !!activeTab;
		const f = show ? {
			territory: activeTab,
			values: deptList.map((d: any) => ({ name: d.dpto ?? d.distrito ?? d.municipio, value: d.rank?.[rankCfg!.field] ?? null })),
			max: rankMax,
			signed: !!rankCfg!.signed,
			decimals: rankCfg!.decimals,
			unit: rankUnit,
			title: i18n.t(rankCfg!.titleKey),
		} : null;
		untrack(() => hexStore.setDeptFill(f));
	});
	onDestroy(() => hexStore.setDeptFill(null));

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



	const legendGradient = $derived(
		colorScale === 'flood'
			? rampGradient(MAP_TIERRA)
			: colorScale === 'green'
			? rampGradient(MAP_SELVA)
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

	// Range shown in the score box. Same rule as MapLegend's badge: the colour comes
	// from the layer's primary variable, which is often a physical unit (µg/m³, min,
	// hab/km²…), not a 0–100 score. Only score/percentile units read "Score 0–100".
	const SCORE_UNITS = new Set(['/100', 'percentil', 'pctl']);
	const scoreRangeLabel = $derived.by(() => {
		const pv = hexStore.effectivePrimary;
		const v = layerCfg?.variables?.find((x: any) => x.col === pv || x.rawCol === pv);
		const u = v?.unit;
		// A fixed colour domain is what the legend shows (EUDR 0–40 %), not the unit's full range.
		const fd = layerCfg?.fixedColorDomain;
		if (fd && pv === layerCfg?.primaryVariable) return `${fd[0]}–${fd[1]}${u === '%' ? ' %' : u ? ` ${unitLabel(u)}` : ''}`;
		if (!u || SCORE_UNITS.has(u)) return i18n.t('legend.range');
		if (u === '%') return '0–100 %';
		return unitLabel(u);
	});
	// What a high value means for the column the map is painting (the active tab's,
	// via variableLegend), same rule as MapLegend. No layer-wide "Score alto = …"
	// fallback any more: most layers publish physical values, not scores.
	const highValueLabel = $derived.by(() => {
		const key = layerCfg?.variableLegend?.[hexStore.effectivePrimary]?.high ?? layerCfg?.legendHighKey;
		return key ? `${i18n.t('legend.highValue')}: ${i18n.t(key)}` : '';
	});

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
		layerCfg?.variables.filter(v => !v.hidden &&
			!['score', 'flood_risk_score', 'risk_score', 'type', 'type_label', 'territorial_type', 'pca_1', 'pca_2'].includes(v.col)
		) ?? []
	);

	// Petal chart data for selected hex
	const petalLabels = $derived(componentVars.map(v => i18n.t(v.labelKey)));

	// ── Cross-analysis profile for selected hex ──
	const CROSS_ANALYSIS_IDS = ['accessibility', 'deforestation_dynamics', 'land_use', 'flood_risk'];
	const CROSS_TITLE_KEYS: Record<string, string> = { land_use: 'sat.landUse.title' };
	const CROSS_ANALYSES = $derived(
		CROSS_ANALYSIS_IDS.map(id => {
			const cfg = HEX_LAYER_REGISTRY[id];
			const titleKey = cfg?.titleKey ?? CROSS_TITLE_KEYS[id] ?? id;
			return { id, label: i18n.t(titleKey) };
		})
	);

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

	const content = $derived(ANALYSIS_CONTENT[analysis.id] ?? null);

	// ── Temporal toggle support ──
	const isTemporal = $derived(layerCfg?.temporal === true);
	const tMode = $derived(hexStore.temporalMode);



	const effectiveLegendGradient = $derived(
		isTemporal && tMode === 'delta'
			? rampGradient(MAP_DIV)
			: legendGradient
	);
	const effectiveLegendLabels = $derived(
		isTemporal && tMode === 'delta'
			? [i18n.t('temporal.legend.worse'), i18n.t('temporal.legend.noChange'), i18n.t('temporal.legend.better')]
			: legendLabels
	);


	/**
	 * Adaptive number formatter — picks precision based on magnitude so that
	 * fractional values like NDVI (0.85) do not collapse to "1" via toFixed(0).
	 */

	// «Pedir un diagnóstico a medida»: this layer + the open department, or the territory.
	const layerRequestCtx = $derived({
		layer: i18n.t(analysis.titleKey),
		place: [selectedDpto ? formatDept(selectedDpto) : '', territoryRef(hexStore.territoryId, i18n.locale)].filter(Boolean).join(' · '),
	});

</script>

<!-- Flood "current extent": Sentinel-1 SAR snapshot date + revisit cadence.
     Hoisted snippet rendered inside each freshness/source box; renders nothing
     unless the active territory's summary carries metadata.sar_date. -->
{#snippet sarFreshness()}
	{#if sarDateDisplay}
		<div><strong>{i18n.t('section.sarImage')}:</strong> {sarDateDisplay} · {i18n.t('section.sarRevisit')}</div>
	{/if}
{/snippet}

{#if selectedDpto && isPerDept}
	<!-- ═══ DEPARTMENT SELECTED (minimal, like FloodRisk) ═══ -->
	<div class="view">
		<button class="back-btn" onclick={handleBackToDepts}>{i18n.t('nav.backToDepts')}</button>

		<div class="dept-active-title">{formatDept(selectedDpto)}</div>

		{#if hasRank && rankMetrics.length > 1 && (rankCfg?.hexVariable || layerCfg?.deptRank?.metrics?.some((m) => m.hexNone))}
			<div class="rank-tabs rank-tabs-dept">
				{#each rankMetrics as m, i}
					<button class="rank-tab" class:active={rankMetricIdx === i} onclick={() => (rankMetricIdx = i)}>{i18n.t(m.tabKey)}</button>
				{/each}
			</div>
		{/if}
		{#if hexNoneNote}<p class="explain-text rank-hexnone">{hexNoneNote}</p>{/if}
		{#if selectedRankPos && rankCfg}
			<div class="rank-selected">
				<span class="rank-selected-pos">{selectedRankPos.pos}.º {i18n.t('rank.of')} {selectedRankPos.total}</span>
				· {fmtRank(selectedRankPos.dept.rank?.[rankCfg.field], rankCfg.decimals, rankCfg.signed)} {rankUnit}
				{#if rankCfg.extraField}({fmtRank(selectedRankPos.dept.rank?.[rankCfg.extraField], rankCfg.extraDecimals ?? 0)} {i18n.t(rankCfg.extraUnitKey ?? '')}){/if}
				<div class="rank-selected-q">{i18n.t(rankCfg.titleKey)}</div>
			</div>
		{/if}

		<!-- Macizo layers have no per-hex parquet: nothing to export as hexagons. -->
		{#if dataUrl && !layerCfg?.macizos}
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
				{i18n.t('analysis.howCalc')}
			</a>
		{/if}

		{#if isTemporal}
			<TemporalToggle {hexStore} layerId={layerCfg?.id ?? ''} />
		{/if}

		{#if loading}
			<div class="loading">{i18n.t('analysis.loading')}</div>
		{:else}
			<div class="hint">{i18n.t(layerCfg?.macizos ? 'harvest.hint' : 'analysis.hex.clickHint')}</div>
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

		<RequestDiagnostic ctx={layerRequestCtx} />

		{#if content}
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.howToRead')}</summary>
				<div class="method-body">
					{#if showDeptView}<p class="explain-text">{i18n.t('analysis.howToRead.deptView')}</p>{/if}
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
					{#if resolveMethod(content.method, i18n.locale).fallbackFrom}
						<p class="method-notice">{i18n.t('section.method_es_only')}</p>
					{/if}
					<p class="explain-text">{resolveMethod(content.method, i18n.locale).text}</p>
				</div>
			</details>
		{/if}

		{#if freshness}
			<div class="source-note-box">
				<div><strong>{i18n.t('section.source')}:</strong> {i18n.t(freshness.sourceKey)}</div>
				<div><strong>{i18n.t('section.processed')}:</strong> {processedDisplay}</div>
				{@render sarFreshness()}
			</div>
		{/if}
	</div>

{:else if isPerDept}
	<!-- ═══ DEPARTMENT LIST ═══ -->
	<div class="view">
		<p class="desc">{i18n.t(analysis.descKey)}</p>

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

		{#if hasRank && rankCfg}
			<div class="dept-section rank-section">
				<div class="rank-title">{i18n.t(rankCfg.titleKey)}{#if activeTerritoryLabel} <span class="rank-territory">· {activeTerritoryLabel}</span>{/if}</div>
				{#if rankMetrics.length > 1}
					<div class="rank-tabs">
						{#each rankMetrics as m, i}
							<button class="rank-tab" class:active={rankMetricIdx === i} onclick={() => (rankMetricIdx = i)}>{i18n.t(m.tabKey)}</button>
						{/each}
					</div>
				{/if}
				{#if rankProvince}
					<div class="rank-province" class:rank-province-toward={rankProvince.label === 'toward'} class:rank-province-away={rankProvince.label === 'away'}>
						<div class="rank-province-head"><span class="rank-province-val">{fmtRank(rankProvince.tilt_pts, rankCfg.decimals, rankCfg.signed)} {rankUnit}</span> · {i18n.t(`rank.change.label.${rankProvince.label}`)}</div>
						{#if rankProvince.share_new_pct != null && rankProvince.share16_pct != null}
							<div class="rank-province-detail">{i18n.t('rank.change.prov.whole')} {activeTerritoryLabel}, {fmtRank(rankProvince.share_new_pct, 1)} % {i18n.t('rank.change.prov.built')} {fmtRank(rankProvince.share16_pct, 1)} % {i18n.t('rank.change.prov.stock')}</div>
						{/if}
					</div>
				{/if}
				{#if rankBars}
				{#if deptList.length > 12}
					<input
						class="dept-filter"
						type="search"
						placeholder="Filtrar entre {deptList.length}…"
						bind:value={deptFilter}
					/>
				{/if}
				<div class="dept-list-scroll rank-scroll">
					{#each filteredDeptList as dept}
						{@const v = dept.rank?.[rankCfg.field]}
						<button class="dept-row dept-clickable rank-row" onclick={() => handleDptoClick(dept)}>
							<div class="rank-pos">{deptList.indexOf(dept) + 1}</div>
							<div class="dept-name rank-name">{formatDept(dept.dpto ?? dept.distrito ?? dept.municipio)}</div>
							<div class="dept-bar-wrap rank-bar-wrap">
								<div class="dept-bar" style:width="{100 * Math.abs(v ?? 0) / rankMax}%" style:background={rankCfg.signed ? ((v ?? 0) < 0 ? RANK_AWAY : RANK_TOWARD) : 'var(--s-rank)'}></div>
							</div>
							<div class="rank-value">{fmtRank(v, rankCfg.decimals, rankCfg.signed)} {v == null ? '' : rankUnit}</div>
							{#if rankCfg.extraField}
								<div class="rank-extra">{fmtRank(dept.rank?.[rankCfg.extraField], rankCfg.extraDecimals ?? 0)} {i18n.t(rankCfg.extraUnitKey ?? '')}</div>
							{/if}
						</button>
					{:else}
						<div class="dept-row" style="color: var(--text-secondary); font-style: italic;">{i18n.t('common.noMatches')}</div>
					{/each}
				</div>
				{:else}
					<p class="rank-nobars">{withRef(i18n.t('rank.change.noBars'), activeTab || hexStore.territoryId, i18n.locale)}</p>
				{/if}
				<p class="rank-note">{withRef(i18n.t(rankCfg.noteKey), activeTab || hexStore.territoryId, i18n.locale)}</p>
			</div>
		{/if}

		{#if rankSaturated && rankCfg?.saturatedKey}
			<div class="dept-section rank-section rank-saturated">
				<div class="rank-title">{i18n.t(rankCfg.titleKey)}{#if activeTerritoryLabel} <span class="rank-territory">· {activeTerritoryLabel}</span>{/if}</div>
				<div class="rank-saturated-head">{withRef(i18n.t('rank.saturated.head'), activeTab || hexStore.territoryId, i18n.locale)}</div>
				<p class="rank-note">{i18n.t(rankCfg.saturatedKey)}</p>
			</div>
		{/if}


		{#if !isCategorical && !layerCfg?.colorClasses?.nominal}
			<div class="score-info-box">
				<span class="score-range">{scoreRangeLabel}</span>
				{#if highValueLabel}
					<span class="score-dir {scoreDirection === 'danger' ? 'score-dir-danger' : 'score-dir-good'}">{highValueLabel}</span>
				{/if}
			</div>
		{/if}

		{#if activeSummaryData}
			<div class="summary-cards">
				<div class="summary-card">
					{#if layerCfg?.macizos}
						<div class="card-value">{activeSummaryData.province.rank?.macizos?.toLocaleString()}</div>
						<div class="card-label">{i18n.t('harvest.card.macizos')}</div>
					{:else}
						<div class="card-value">{activeSummaryData.province.total_hexes?.toLocaleString()}</div>
						<div class="card-label">{i18n.t('section.zonesAnalyzed')}</div>
					{/if}
				</div>
			</div>
		{/if}

		{#if isTemporal}
			<TemporalToggle {hexStore} layerId={layerCfg?.id ?? ''} />
		{/if}


		{#if !hasRank || !rankBars}
		<div class="dept-section">
			<div class="section-title">{i18n.t('analysis.flood.topDepts')}</div>
			{#if deptList.length === 0 && isPerDept}
				<div class="dept-row" style="color: var(--text-secondary); font-style: italic; padding: 8px 0;">
					{hexStore.territoryPrefix ? withRef(i18n.t('analysis.noDeptData'), hexStore.territoryId, i18n.locale) : i18n.t('common.loading')}
				</div>
			{:else}
				{#if !selectedDpto}
					<p class="dept-select-hint">{i18n.t('analysis.selectSector')}</p>
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
						<div class="dept-row" style="color: var(--text-secondary); font-style: italic;">{i18n.t('common.noMatches')}</div>
					{/each}
				</div>
			{/if}
		</div>
		{/if}

		{#if content}
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.howToRead')}</summary>
				<div class="method-body">
					{#if showDeptView}<p class="explain-text">{i18n.t('analysis.howToRead.deptView')}</p>{/if}
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
					{#if resolveMethod(content.method, i18n.locale).fallbackFrom}
						<p class="method-notice">{i18n.t('section.method_es_only')}</p>
					{/if}
					<p class="explain-text">{resolveMethod(content.method, i18n.locale).text}</p>
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
				<div><strong>{i18n.t('section.processed')}:</strong> {processedDisplay}</div>
				{@render sarFreshness()}
			</div>
		{/if}

		<RequestDiagnostic ctx={layerRequestCtx} />
	</div>

{:else}
	<!-- ═══ NON-perDepartment (Overture layers) ═══ -->
	<div class="view">
		<p class="desc">{i18n.t(analysis.descKey)}</p>

		{#if freshness}
			<div class="freshness">
				<span class="freshness-label">{i18n.t('data.updatedAt')}: {processedDisplay}</span>
				<span class="freshness-source">{i18n.t('section.source')}: {i18n.t(freshness.sourceKey)}</span>
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

			<!-- Department ranking, first data block as in the perDepartment layers. -->
			{#if analysis.id === 'censo_temporal'}
				<CensoTemporalDeptList {hexStore} onSelectDept={onCensoDeptSelect} />
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
				{i18n.t('analysis.howCalc')}
			</a>
			<details class="method-details">
				<summary class="method-summary">{i18n.t('section.howToRead')}</summary>
				<div class="method-body">
					{#if showDeptView}<p class="explain-text">{i18n.t('analysis.howToRead.deptView')}</p>{/if}
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
					{#if resolveMethod(content.method, i18n.locale).fallbackFrom}
						<p class="method-notice">{i18n.t('section.method_es_only')}</p>
					{/if}
					<p class="explain-text">{resolveMethod(content.method, i18n.locale).text}</p>
				</div>
			</details>
		{/if}

		<RequestDiagnostic ctx={layerRequestCtx} />
	</div>
{/if}

<style>
	.view { font-size: 12.5px; }
	.desc { color: var(--c-ink-2n); margin: 0 0 8px; line-height: 1.4; }
	.score-info-box { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; padding: 5px 8px; background: rgba(var(--c-fg-rgb), 0.04); border-radius: 4px; border: 1px solid rgba(var(--c-fg-rgb), 0.06); }
	.score-range { font-size: 11.5px; color: var(--c-ink-3n); font-weight: 600; }
	.score-dir { font-size: 11.5px; font-weight: 500; }
	.score-dir-danger { color: var(--c-t-f87171); }
	.score-dir-good { color: var(--c-t-4ade80); }
	.freshness { display: flex; flex-direction: column; gap: 2px; margin-bottom: 8px; padding: 4px 6px; background: rgba(var(--c-fg-rgb), 0.03); border-radius: 4px; }
	.freshness-label { color: var(--c-ink-3n); font-size: 11.5px; }
	.freshness-source { color: var(--c-ink-3n); font-size: 11.5px; }
	.loading { color: var(--c-ink-1n); font-size: 12px; text-align: center; padding: 20px 0; }
	.hint { font-size: 11.5px; color: var(--c-ink-2n); text-align: center; margin-top: 8px; }

	/* ── Summary cards ── */
	.summary-cards { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-bottom: 12px; }
	.summary-card { background: rgba(100,116,139,0.08); border-radius: 6px; padding: 8px 6px; text-align: center; }
	.card-value { font-size: 15px; font-weight: 700; color: var(--c-ink-1); }
	.card-label { font-size: 11px; color: var(--c-ink-1n); margin-top: 2px; }

	/* ── Department list ── */
	.dept-section { margin-bottom: 10px; }
	.dept-filter {
		width: 100%;
		background: rgba(var(--c-fg-rgb), 0.05);
		border: 1px solid rgba(var(--c-fg-rgb), 0.10);
		border-radius: 4px;
		color: var(--c-ink-1);
		font-size: 12px;
		font-family: inherit;
		padding: 4px 8px;
		margin-bottom: 6px;
		outline: none;
	}
	.dept-filter:focus { border-color: rgba(var(--c-accent-rgb), 0.5); }
	.dept-filter::placeholder { color: rgba(var(--c-fg-rgb), 0.55); }
	.dept-list-scroll {
		max-height: 264px; /* ~11 rows — keeps charts reachable below 300-500 row BR lists */
		overflow-y: auto;
		scrollbar-width: thin;
		scrollbar-color: var(--c-line-2) transparent;
	}
	.section-title { font-size: 12px; font-weight: 600; color: var(--c-ink-2); margin-bottom: 6px; }
	.dept-select-hint { font-size: 11.5px; color: var(--c-accent); margin: 0 0 6px; font-style: italic; opacity: 0.8; }
	.dept-row { display: flex; align-items: center; gap: 6px; margin-bottom: 3px; }
	.dept-clickable { background: none; border: none; width: 100%; padding: 4px 2px; border-radius: 4px; cursor: pointer; transition: background 0.15s; }
	.dept-clickable:hover { background: rgba(var(--c-accent-rgb), 0.1); }
	.dept-name { font-size: 11.5px; color: var(--c-ink-1n); width: 72px; text-align: left; flex-shrink: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.dept-bar-wrap { flex: 1; height: 4px; background: rgba(100,116,139,0.15); border-radius: 2px; overflow: hidden; }
	.dept-bar { height: 100%; border-radius: 2px; transition: width 0.3s; }
	.dept-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
	.dept-score { font-size: 11.5px; font-weight: 600; min-width: 24px; text-align: right; }
	.dept-active-title { font-size: 14px; font-weight: 700; color: var(--c-ink-1); margin-bottom: 8px; }

	/* ── Department ranking (layerCfg.deptRank) ── */
	.rank-section { margin-bottom: 12px; }
	.rank-title { font-size: 13px; font-weight: 700; color: var(--c-ink-1); margin-bottom: 6px; }
	.rank-scroll { max-height: 340px; }
	.rank-row { gap: 5px; margin-bottom: 1px; padding: 3px 2px; }
	.rank-pos { font-size: 11.5px; color: var(--c-ink-4); width: 14px; text-align: right; flex-shrink: 0; }
	.rank-name { width: 100px; }
	.rank-bar-wrap { height: 6px; }
	.rank-value { font-size: 12px; font-weight: 600; color: var(--c-ink-1); min-width: 40px; text-align: right; flex-shrink: 0; }
	.rank-extra { font-size: 11px; color: var(--c-ink-3); min-width: 48px; text-align: right; flex-shrink: 0; }
	.rank-note { font-size: 11.5px; color: var(--c-ink-3); line-height: 1.4; margin: 6px 0 0; }
	.rank-province { border-left: 3px solid var(--c-ink-4); background: rgba(148,163,184,0.08); border-radius: 4px; padding: 6px 8px; margin: 0 0 8px; }
	.rank-province-toward { border-left-color: var(--s-cat-1); }
	.rank-province-away { border-left-color: var(--s-cat-2); }
	.rank-province-head { font-size: 12.5px; color: var(--c-ink-1); line-height: 1.35; }
	.rank-province-val { font-weight: 600; }
	.rank-province-toward .rank-province-val { color: var(--s-ink-1); }
	.rank-province-away .rank-province-val { color: var(--s-ink-1); }
	.rank-province-detail { font-size: 12px; color: var(--c-ink-3); line-height: 1.4; margin-top: 3px; }
	.rank-nobars { font-size: 12px; color: var(--c-ink-2); line-height: 1.4; margin: 0 0 4px; }
	.rank-territory { font-weight: 400; color: var(--c-ink-3); }
	.rank-tabs { display: flex; gap: 4px; margin: 0 0 8px; flex-wrap: wrap; }
	.rank-tab { font-size: 12px; padding: 3px 8px; border-radius: 4px; border: 1px solid rgba(148,163,184,0.3); background: none; color: var(--c-ink-3); cursor: pointer; }
	.rank-tab.active { background: rgba(220,38,38,0.15); border-color: #dc2626; color: var(--c-t-fecaca); }
	.rank-saturated { padding: 10px; border: 1px solid rgba(148,163,184,0.25); border-radius: 6px; background: rgba(148,163,184,0.06); }
	.rank-saturated-head { font-size: 12.5px; font-weight: 600; color: var(--c-t-fbbf24); margin-bottom: 4px; }
	.rank-saturated .rank-note { font-size: 12px; color: var(--c-ink-2); margin: 0; }
	.rank-selected { font-size: 12.5px; color: var(--c-ink-2); margin: -4px 0 10px; }
	.rank-selected-pos { font-weight: 700; color: var(--c-t-f87171); }
	.rank-selected-q { font-size: 11.5px; color: var(--c-ink-4); margin-top: 2px; }

	/* ── Navigation ── */
	.back-btn { background: none; border: none; color: var(--c-accent); font-size: 12px; cursor: pointer; padding: 0; margin-bottom: 8px; }
	.back-btn:hover { text-decoration: underline; }

	/* ── Hex detail ── */
	.hex-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
	.hex-id { font-family: var(--font-mono); font-size: 12px; color: var(--c-ink-1n); }
	.risk-badge { font-size: 11.5px; font-weight: 700; color: #000; padding: 2px 8px; border-radius: 9999px; }
	.score-bar { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
	.score-label { font-size: 11.5px; color: var(--c-ink-1n); white-space: nowrap; }
	.score-track { flex: 1; height: 6px; background: rgba(100,116,139,0.2); border-radius: 3px; overflow: hidden; }
	.score-fill { height: 100%; border-radius: 3px; transition: width 0.3s; }
	.score-value { font-size: 13px; font-weight: 700; min-width: 32px; text-align: right; }
	.petal-section { margin: 6px 0; }
	.petal-hint { font-size: 11px; color: rgba(var(--c-fg-rgb), 0.55); text-align: center; margin: 2px 0 0; line-height: 1.3; }
	.census-detail { display: flex; flex-direction: column; gap: 4px; margin: 8px 0; }
	.cd-row { display: flex; align-items: center; gap: 6px; }
	.cd-label { font-size: 11.5px; color: var(--c-ink-1n); flex: 0 0 auto; min-width: 100px; }
	.cd-bar-track { flex: 1; height: 6px; background: var(--c-line); border-radius: 3px; overflow: hidden; }
	.cd-bar-fill { height: 100%; border-radius: 3px; transition: width 0.3s; min-width: 2px; }
	.cd-val { font-size: 11.5px; font-weight: 600; color: var(--c-ink-2); width: 28px; text-align: right; flex-shrink: 0; }
	.cd-val-data { font-size: 12px; font-weight: 600; color: var(--c-ink-1); text-align: right; margin-left: auto; white-space: nowrap; }
	.petal-wrapper { display: flex; justify-content: center; margin: 0 auto; max-width: 260px; }

	/* Cross-analysis profile + type distribution + diagnostic */
	.cross-profile, .type-dist, .diagnostic { margin: 10px 0; padding: 8px 0; border-top: 1px solid rgba(var(--c-fg-rgb), 0.06); }
	.cross-title { font-size: 11.5px; font-weight: 600; color: var(--c-ink-2n); text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 6px; }
	.cross-row { display: flex; align-items: center; gap: 6px; padding: 2px 0; font-size: 12px; }
	.cross-label { color: var(--c-ink-3n); flex-shrink: 0; min-width: 90px; }
	.cross-value { color: var(--c-ink-1n); font-weight: 500; }
	.type-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
	.detail-label { font-size: 11.5px; color: var(--c-ink-1n); margin-bottom: 2px; }
	.detail-value { font-size: 14px; font-weight: 700; color: var(--c-ink-1); }
	.detail-desc { font-size: 11px; color: var(--c-ink-2n); margin-top: 2px; }

	/* ── Legend ── */
	.flood-legend { margin: 12px 0; }
	.legend-title { font-size: 11.5px; font-weight: 600; color: var(--c-ink-1n); margin-bottom: 4px; }
	.legend-bar { height: 8px; border-radius: 4px; }
	.legend-labels { display: flex; justify-content: space-between; font-size: 11px; color: var(--c-ink-2n); margin-top: 2px; }

	/* ── Collapsibles ── */
	.method-details { margin-top: 10px; border: 1px solid rgba(100,116,139,0.15); border-radius: 6px; overflow: hidden; }
	.method-summary { font-size: 11.5px; font-weight: 600; color: var(--c-ink-1n); padding: 6px 8px; cursor: pointer; user-select: none; list-style: none; display: flex; align-items: center; gap: 4px; }
	.method-summary::before { content: '\25B8'; font-size: 11px; transition: transform 0.15s; }
	.method-details[open] > .method-summary::before { transform: rotate(90deg); }
	.method-summary::-webkit-details-marker { display: none; }
	.method-body { padding: 4px 8px 8px; }
	.method-item { margin-bottom: 4px; }
	.method-term { font-size: 11.5px; font-weight: 600; color: var(--c-ink-2); }
	.explain-text { font-size: 11.5px; color: var(--c-ink-2n); line-height: 1.5; margin: 2px 0 0; }
	.method-notice { font-size: 11.5px; color: #7b8794; font-style: italic; line-height: 1.5; margin: 2px 0 0; padding-left: 6px; border-left: 2px solid #3a4553; }
	.mini-legend { margin-top: 6px; }
	.method-components { margin-top: 6px; }

	/* ── Methodology link ── */
	.methodology-link { display: inline-block; font-size: 11.5px; color: var(--c-ink-3); text-decoration: none; padding: 4px 0; margin: 4px 0 10px; transition: color 0.15s; }
	.methodology-link:hover { color: var(--c-accent); text-decoration: underline; }

	/* ── Download button ── */
	.download-btn { display: block; text-align: center; padding: 6px 10px; margin: 0; background: rgba(var(--c-accent-rgb), 0.15); border: 1px solid rgba(var(--c-accent-rgb), 0.3); border-radius: 4px; color: var(--c-accent); font-size: 11.5px; font-weight: 600; text-decoration: none; transition: all 0.15s; cursor: pointer; font-family: inherit; }
	.download-btn:hover:not(:disabled) { background: rgba(var(--c-accent-rgb), 0.25); border-color: rgba(var(--c-accent-rgb), 0.5); }
	.download-btn:disabled { cursor: wait; opacity: 0.6; }
	.download-row { display: flex; gap: 6px; margin: 10px 0; }
	.download-row .download-btn { flex: 1; }
	.download-secondary { background: rgba(var(--c-fg-rgb), 0.05); border-color: rgba(var(--c-fg-rgb), 0.15); color: var(--c-ink-2n); }
	.download-secondary:hover:not(:disabled) { background: rgba(var(--c-fg-rgb), 0.1); border-color: rgba(var(--c-fg-rgb), 0.25); color: var(--c-ink-1n); }
	.download-date { font-weight: 400; font-size: 11px; opacity: 0.7; }

	/* ── Source box ── */
	.source-note-box { margin-top: 10px; padding: 8px 10px; background: rgba(var(--c-fg-rgb), 0.05); border: 1px solid rgba(var(--c-fg-rgb), 0.08); border-radius: 6px; font-size: 11.5px; color: var(--c-ink-1); line-height: 1.5; }
	.source-note-box strong { color: var(--c-t-f8fafc); }

	/* ── Non-perDept hex cards ── */
	.variables-hint { display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 10px; }
	.variable-tag { background: rgba(var(--c-fg-rgb), 0.06); color: var(--c-ink-1n); padding: 2px 6px; border-radius: 3px; font-size: 11.5px; }
	.selected-hexes { display: flex; flex-direction: column; gap: 6px; }
	.hex-card { background: rgba(var(--c-fg-rgb), 0.04); border-radius: 6px; padding: 6px 8px; }
	.hex-petal { margin: 4px 0 6px; }
	.hex-values { display: flex; flex-direction: column; gap: 2px; }
	.hex-val { display: flex; justify-content: space-between; align-items: baseline; }
	.hex-val-label { color: var(--c-ink-2n); }
	.hex-val-num { color: var(--c-t-e5e5e5); font-weight: 500; font-variant-numeric: tabular-nums; }

	/* ── Territory tabs ── */
	.territory-tabs-wrapper { display: flex; flex-direction: column; gap: 3px; margin-bottom: 10px; }
	.territory-country-row { display: flex; align-items: flex-start; gap: 4px; }
	.country-flag { font-size: 12.5px; line-height: 20px; flex-shrink: 0; }
	.territory-tabs { display: flex; flex-wrap: wrap; gap: 2px; flex: 1; }
	.territory-tab { background: rgba(var(--c-fg-rgb), 0.04); border: 1px solid rgba(100,116,139,0.18); border-radius: 3px; color: var(--c-ink-2n); font-size: 11.5px; font-weight: 500; padding: 3px 5px; cursor: pointer; transition: all 0.15s; font-family: inherit; }
	.territory-tab.active { background: rgba(var(--c-accent-rgb), 0.15); color: var(--c-accent); font-weight: 700; border-color: rgba(var(--c-accent-rgb), 0.3); }
	.territory-tab:hover:not(.active) { background: rgba(var(--c-fg-rgb), 0.07); color: var(--c-ink-1n); }

	/* ── Temporal toggle ── */
	.temporal-toggle { display: flex; gap: 0; margin-bottom: 10px; border-radius: 6px; overflow: hidden; border: 1px solid rgba(100,116,139,0.2); }
	.temporal-toggle button { flex: 1; background: rgba(var(--c-fg-rgb), 0.03); border: none; color: var(--c-ink-2n); font-size: 11.5px; font-weight: 500; padding: 5px 4px; cursor: pointer; transition: all 0.15s; }
	.temporal-toggle button:not(:last-child) { border-right: 1px solid rgba(100,116,139,0.15); }
	.temporal-toggle button.active { background: rgba(var(--c-accent-rgb), 0.15); color: var(--c-accent); font-weight: 700; }
	.temporal-toggle button:hover:not(.active) { background: rgba(var(--c-fg-rgb), 0.06); }

	/* ── Calibration table ── */
</style>
