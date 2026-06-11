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
	const comparableGroup = $derived(analyses.filter(a => a.comparable && getCoverage(a) !== 'unavailable'));
	const localGroup = $derived(analyses.filter(a => !a.comparable && getCoverage(a) !== 'unavailable'));

	// Sub-theme grouping within each domain.
	const SUBGROUPS: Record<string, string> = {
		// Ambiente y riesgo
		flood_risk: 'Riesgo hídrico',
		deforestation_dynamics: 'Cobertura natural', carbon_stock: 'Cobertura natural',
		pm25_drivers: 'Calidad del aire',
		// Producción y suelo
		agri_potential: 'Suelos', forestry_aptitude: 'Suelos',
		land_use: 'Uso del suelo',
		// Población y servicios
		accessibility: 'Accesibilidad',
		sociodemographic: 'Población', censo_temporal: 'Población',
		service_deprivation: 'Servicios básicos', health_access: 'Servicios básicos',
		education_capital: 'Educación', education_flow: 'Educación',
		// Economía e infraestructura
		economic_activity: 'Actividad',
		eudr: 'Comercio (EUDR)',
		powerline_density: 'Infraestructura',
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
	function groupBySubtheme(list: AnalysisConfig[]): Array<{ label: string; items: AnalysisConfig[] }> {
		const order: string[] = [];
		const buckets: Record<string, AnalysisConfig[]> = {};
		for (const a of list) {
			const label = SUBGROUPS[a.id] ?? 'Otros';
			if (!(label in buckets)) { buckets[label] = []; order.push(label); }
			buckets[label].push(a);
		}
		return order.map(label => ({ label, items: buckets[label] }));
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
				<div class="group-label">↔ Comparables entre territorios</div>
				{#each comparableBuckets as bucket}
					<div class="subgroup-label">{bucket.label}</div>
					{#each bucket.items as analysis}
						{@const coverage = getCoverage(analysis)}
						<button
							class="analysis-item"
							class:available={analysis.status === 'available' && coverage === 'available'}
							class:coming-soon={analysis.status === 'coming_soon' || coverage === 'pending'}
							disabled={analysis.status === 'coming_soon' || coverage === 'pending'}
							onclick={() => onSelectAnalysis(analysis)}
						>
							<div class="item-title">{i18n.t(analysis.titleKey)}</div>
							<div class="item-desc">{i18n.t(analysis.descKey)}</div>
							{#if analysis.rigorBadge}
								<span class="item-rigor"
									class:rigor-physical={analysis.rigorBadge === 'physical'}
									class:rigor-modeled={analysis.rigorBadge === 'modeled'}
									class:rigor-census={analysis.rigorBadge === 'census'}>
									{analysis.rigorBadge === 'physical' ? '🛰 Medición satelital'
										: analysis.rigorBadge === 'modeled' ? '📐 Aptitud modelada'
										: '🏛 Indicador censal'}
								</span>
							{/if}
							{#if analysis.status === 'coming_soon'}
								<span class="item-badge">{i18n.t('analysis.status.comingSoon')}</span>
							{:else if coverage === 'pending'}
								<span class="item-badge">⏳ {i18n.t('analysis.coverage.pending')}</span>
							{/if}
						</button>
					{/each}
				{/each}
			{/if}

			{#if localGroup.length > 0}
				<div class="group-label local">
					{activeTerritory?.flag ?? ''} Solo {activeTerritory?.label ?? 'este territorio'}
				</div>
				{#each localBuckets as bucket}
					<div class="subgroup-label">{bucket.label}</div>
					{#each bucket.items as analysis}
						{@const coverage = getCoverage(analysis)}
						<button
							class="analysis-item"
							class:available={analysis.status === 'available' && coverage === 'available'}
							class:coming-soon={analysis.status === 'coming_soon' || coverage === 'pending'}
							disabled={analysis.status === 'coming_soon' || coverage === 'pending'}
							onclick={() => onSelectAnalysis(analysis)}
						>
							<div class="item-title">{i18n.t(analysis.titleKey)}</div>
							<div class="item-desc">{i18n.t(analysis.descKey)}</div>
							{#if analysis.rigorBadge}
								<span class="item-rigor"
									class:rigor-physical={analysis.rigorBadge === 'physical'}
									class:rigor-modeled={analysis.rigorBadge === 'modeled'}
									class:rigor-census={analysis.rigorBadge === 'census'}>
									{analysis.rigorBadge === 'physical' ? '🛰 Medición satelital'
										: analysis.rigorBadge === 'modeled' ? '📐 Aptitud modelada'
										: '🏛 Indicador censal'}
								</span>
							{/if}
							{#if analysis.status === 'coming_soon'}
								<span class="item-badge">{i18n.t('analysis.status.comingSoon')}</span>
							{:else if coverage === 'pending'}
								<span class="item-badge">⏳ {i18n.t('analysis.coverage.pending')}</span>
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
