<script lang="ts">
	import { getAnalysesForLens, LENS_CONFIG, type AnalysisConfig, type TerritoryConfig } from '$lib/config';
	import type { LensStore } from '$lib/stores/lens.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';

	let {
		lensStore,
		activeTerritory,
		onSelectAnalysis,
	}: {
		lensStore: LensStore;
		activeTerritory?: TerritoryConfig;
		onSelectAnalysis: (analysis: AnalysisConfig) => void;
	} = $props();

	const lens = $derived(lensStore.activeLens);
	const cfg = $derived(lens ? LENS_CONFIG[lens] : null);
	const analyses = $derived(lens ? getAnalysesForLens(lens) : []);
	// Layers without coverage for the active territory stay VISIBLE but
	// disabled (🚫 badge) — hiding them reads as "the layers disappeared"
	// when switching territory.
	const comparableGroup = $derived(analyses.filter(a => a.comparable));
	const localGroup = $derived(analyses.filter(a => !a.comparable));

	// Sub-theme grouping within each domain. Values are dict keys, not text — these
	// headers render on every menu open and used to be Spanish in all four locales.
	const SUBGROUPS: Record<string, string> = {
		// Ambiente y riesgo
		flood_risk: 'menu.sub.waterRisk',
		deforestation_dynamics: 'menu.sub.naturalCover', carbon_stock: 'menu.sub.naturalCover',
		pm25_drivers: 'menu.sub.airQuality',
		// Producción y suelo
		agri_potential: 'menu.sub.soils', forestry_aptitude: 'menu.sub.soils',
		land_use: 'menu.sub.landUse',
		// Población y servicios
		accessibility: 'menu.sub.accessibility',
		sociodemographic: 'menu.sub.population', censo_temporal: 'menu.sub.population',
		service_deprivation: 'menu.sub.basicServices', health_access: 'menu.sub.basicServices',
		education_capital: 'menu.sub.education', education_flow: 'menu.sub.education',
		// Economía e infraestructura
		economic_activity: 'menu.sub.activity',
		eudr: 'menu.sub.trade',
	};

	// Keyed off the closed rigorBadge union (config.ts:1002). Was an inline ternary
	// duplicated across both group blocks, each with the Spanish text baked in.
	const RIGOR_KEYS: Record<NonNullable<AnalysisConfig['rigorBadge']>, string> = {
		physical: 'analysis.rigor.physical',
		modeled: 'analysis.rigor.modeled',
		census: 'analysis.rigor.census',
	};

	function getCoverage(analysis: AnalysisConfig): 'available' | 'pending' | 'unavailable' {
		if (!activeTerritory) return 'available';
		if (!analysis.coverage) {
			// No coverage map: available for Misiones (backwards compat), pending for new territories
			return activeTerritory.id === 'misiones' ? 'available' : 'pending';
		}
		return analysis.coverage[activeTerritory.id] ?? (activeTerritory.id === 'misiones' ? 'available' : 'pending');
	}

	// Group a list of analyses by sub-theme while preserving original order
	// of first appearance per sub-theme (more predictable than alphabetical).
	// Buckets are keyed by dict key; the markup resolves it through t().
	function groupBySubtheme(list: AnalysisConfig[]): Array<{ labelKey: string; items: AnalysisConfig[] }> {
		const order: string[] = [];
		const buckets: Record<string, AnalysisConfig[]> = {};
		for (const a of list) {
			const labelKey = SUBGROUPS[a.id] ?? 'menu.sub.other';
			if (!(labelKey in buckets)) { buckets[labelKey] = []; order.push(labelKey); }
			buckets[labelKey].push(a);
		}
		return order.map(labelKey => ({ labelKey, items: buckets[labelKey] }));
	}

	const comparableBuckets = $derived(groupBySubtheme(comparableGroup));
	const localBuckets = $derived(groupBySubtheme(localGroup));
</script>

{#if cfg && lens}
	<div class="analysis-menu">
		<div class="menu-header">
			<span class="header-label">{cfg.label[i18n.locale as 'es' | 'en' | 'gn' | 'pt']}</span>
		</div>

		<div class="analysis-list">
			{#if comparableGroup.length > 0}
				<div class="group-label">{i18n.t('menu.group.comparable')}</div>
				{#each comparableBuckets as bucket}
					<div class="subgroup-label">{i18n.t(bucket.labelKey)}</div>
					{#each bucket.items as analysis}
						{@const coverage = getCoverage(analysis)}
						<button
							class="analysis-item"
							class:available={analysis.status === 'available' && coverage === 'available'}
							class:coming-soon={analysis.status === 'coming_soon' || coverage !== 'available'}
							disabled={analysis.status === 'coming_soon' || coverage !== 'available'}
							onclick={() => onSelectAnalysis(analysis)}
						>
							<div class="item-title">{i18n.t(analysis.titleKey)}</div>
							<div class="item-desc">{i18n.t(analysis.descKey)}</div>
							{#if analysis.rigorBadge}
								<span class="item-rigor"
									class:rigor-physical={analysis.rigorBadge === 'physical'}
									class:rigor-modeled={analysis.rigorBadge === 'modeled'}
									class:rigor-census={analysis.rigorBadge === 'census'}>
									{i18n.t(RIGOR_KEYS[analysis.rigorBadge])}
								</span>
							{/if}
							{#if analysis.status === 'coming_soon'}
								<span class="item-badge">{i18n.t('analysis.status.comingSoon')}</span>
							{:else if coverage === 'pending'}
								<span class="item-badge">⏳ {i18n.t('analysis.coverage.pending')}</span>
							{:else if coverage === 'unavailable'}
								<span class="item-badge">🚫 {i18n.t('analysis.coverage.unavailable')}</span>
							{/if}
						</button>
					{/each}
				{/each}
			{/if}

			{#if localGroup.length > 0}
				<div class="group-label local">
					{activeTerritory?.flag ?? ''}
					{i18n
						.t('menu.group.localOnly')
						.replace('{territory}', activeTerritory?.label ?? i18n.t('menu.group.thisTerritory'))}
				</div>
				{#each localBuckets as bucket}
					<div class="subgroup-label">{i18n.t(bucket.labelKey)}</div>
					{#each bucket.items as analysis}
						{@const coverage = getCoverage(analysis)}
						<button
							class="analysis-item"
							class:available={analysis.status === 'available' && coverage === 'available'}
							class:coming-soon={analysis.status === 'coming_soon' || coverage !== 'available'}
							disabled={analysis.status === 'coming_soon' || coverage !== 'available'}
							onclick={() => onSelectAnalysis(analysis)}
						>
							<div class="item-title">{i18n.t(analysis.titleKey)}</div>
							<div class="item-desc">{i18n.t(analysis.descKey)}</div>
							{#if analysis.rigorBadge}
								<span class="item-rigor"
									class:rigor-physical={analysis.rigorBadge === 'physical'}
									class:rigor-modeled={analysis.rigorBadge === 'modeled'}
									class:rigor-census={analysis.rigorBadge === 'census'}>
									{i18n.t(RIGOR_KEYS[analysis.rigorBadge])}
								</span>
							{/if}
							{#if analysis.status === 'coming_soon'}
								<span class="item-badge">{i18n.t('analysis.status.comingSoon')}</span>
							{:else if coverage === 'pending'}
								<span class="item-badge">⏳ {i18n.t('analysis.coverage.pending')}</span>
							{:else if coverage === 'unavailable'}
								<span class="item-badge">🚫 {i18n.t('analysis.coverage.unavailable')}</span>
							{/if}
						</button>
					{/each}
				{/each}
			{/if}
		</div>
	</div>
{/if}

<style>
	.analysis-menu {
		font-size: 11px;
	}
	.menu-header {
		margin-bottom: 10px;
		padding-bottom: 6px;
		border-bottom: 1px solid rgba(255,255,255,0.08);
	}
	.header-label {
		font-size: 13px;
		font-weight: 700;
		color: #e2e8f0;
		letter-spacing: 0.02em;
	}
	.analysis-list {
		display: flex;
		flex-direction: column;
		gap: 2px;
		max-height: calc(100vh - 200px);
		overflow-y: auto;
		scrollbar-width: thin;
		scrollbar-color: #334155 transparent;
	}
	.analysis-list::-webkit-scrollbar { width: 4px; }
	.analysis-list::-webkit-scrollbar-track { background: transparent; }
	.analysis-list::-webkit-scrollbar-thumb { background: #334155; border-radius: 2px; }
	.group-label {
		font-size: 8px;
		font-weight: 700;
		color: rgba(255,255,255,0.30);
		text-transform: uppercase;
		letter-spacing: 0.07em;
		padding: 10px 10px 4px;
		border-top: 1px solid rgba(255,255,255,0.06);
		margin-top: 4px;
	}
	.group-label:first-child {
		border-top: none;
		padding-top: 0;
		margin-top: 0;
	}
	.group-label.local { color: rgba(255,255,255,0.22); }
	.subgroup-label {
		font-size: 9px;
		font-weight: 600;
		color: rgba(255,255,255,0.42);
		text-transform: uppercase;
		letter-spacing: 0.05em;
		padding: 8px 10px 2px;
		margin-top: 2px;
	}
	.subgroup-label:first-of-type { padding-top: 4px; }

	.analysis-item {
		display: flex;
		flex-direction: column;
		gap: 3px;
		padding: 10px 10px;
		border-radius: 4px;
		border: none;
		background: transparent;
		cursor: pointer;
		transition: background 0.12s;
		text-align: left;
		width: 100%;
		border-left: 2px solid transparent;
	}
	.analysis-item.available:hover {
		background: rgba(255,255,255,0.05);
		border-left-color: rgba(255,255,255,0.3);
	}
	.analysis-item.coming-soon {
		opacity: 0.45;
		cursor: not-allowed;
	}
	.analysis-item.coming-soon:hover {
		opacity: 0.6;
	}
	.item-title {
		font-size: 11px;
		font-weight: 600;
		color: #ffffff;
		line-height: 1.3;
	}
	.item-desc {
		font-size: 9px;
		color: rgba(255,255,255,0.5);
		line-height: 1.45;
	}
	.item-rigor {
		display: inline-block;
		font-size: 8px;
		font-style: italic;
		margin-top: 2px;
		opacity: 0.65;
	}
	.rigor-physical { color: #60a5fa; }
	.rigor-modeled  { color: #4ade80; }
	.rigor-census   { color: #fbbf24; }
	.item-badge {
		display: inline-block;
		font-size: 8px;
		color: #737373;
		font-style: italic;
		margin-top: 1px;
	}
</style>
