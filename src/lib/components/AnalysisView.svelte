<script lang="ts">
	import { LENS_CONFIG, HEX_LAYER_REGISTRY, type AnalysisConfig } from '$lib/config';
	import type { LensStore } from '$lib/stores/lens.svelte';
	import type { MapStore } from '$lib/stores/map.svelte';
	import type { HexStore } from '$lib/stores/hex.svelte';
	import type { TerritoryStore } from '$lib/stores/territory.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { lp } from '$lib/utils/locale-path';
	import OvertureAnalysis from './analyses/OvertureAnalysis.svelte';
	import CatastroAnalysis from './analyses/CatastroAnalysis.svelte';

	let {
		lensStore,
		mapStore,
		hexStore,
		territoryStore,
		onBack,
		onRemoveRadio,
		onSelectFloodDpto,
		onSelectFloodCatastroDpto,
		onSelectCatastroDpto,
		onSelectScoresCatastroDpto,
		onSelectRadioAnalysisDpto,
	}: {
		lensStore: LensStore;
		mapStore: MapStore;
		hexStore: HexStore;
		territoryStore: TerritoryStore;
		onBack: () => void;
		onRemoveRadio: (redcode: string) => void;
		onSelectFloodDpto: (dpto: string, parquetKey: string, centroid: [number, number]) => void;
		onSelectFloodCatastroDpto?: (dpto: string, parquetKey: string, centroid: [number, number]) => void;
		onSelectCatastroDpto?: (centroid: [number, number] | null, deptCode?: string | null) => void;
		onSelectScoresCatastroDpto?: (dpto: string, parquetKey: string, centroid: [number, number]) => void;
		onSelectRadioAnalysisDpto?: (dpto: string, analysisId: string, centroid: [number, number]) => void;
	} = $props();

	const analysis = $derived(lensStore.activeAnalysis);
	const lens = $derived(lensStore.activeLens);
	const cfg = $derived(lens ? LENS_CONFIG[lens] : null);

	// Plantations overlay is only meaningful for forestry_aptitude with a department
	// selected (loads per-dept). Reset the flag otherwise so it never lingers on the map.
	const showPlantControl = $derived(analysis?.id === 'forestry_aptitude' && !!hexStore.selectedDpto);
	$effect(() => {
		if (showPlantControl) {
			mapStore.plantationsDept = hexStore.selectedDpto;
		} else {
			mapStore.plantationsDept = null;
			if (mapStore.plantationsVisible) mapStore.plantationsVisible = false;
		}
	});

</script>

{#if analysis && cfg}
	<div class="analysis-view">
		<div class="view-header">
			<div class="view-title-group">
				<div class="view-title">{i18n.t(analysis.titleKey)}</div>
				<div class="view-lens" style:color={cfg.color}>
					{cfg.label[i18n.locale as 'es' | 'en' | 'gn' | 'pt']}
				</div>
			</div>
		</div>

		{#if analysis.id === 'eudr'}
			<p class="mb-2 text-[10px] text-white/40 leading-relaxed">{i18n.t('eudr.layer_hint')}</p>
			<a href={lp('/eudr/check', i18n.locale)} class="block mb-3 px-4 py-3 rounded-lg border border-yellow-400/70 bg-yellow-400/15 text-[13px] font-semibold text-yellow-100 hover:bg-yellow-400/25 hover:border-yellow-400 hover:text-white transition-colors leading-snug shadow-[0_0_18px_rgba(250,204,21,0.18)]">
				{i18n.t('eudr.check.cta_from_layer')}
			</a>
		{/if}

		{#if analysis.status === 'coming_soon'}
			<div class="coming-soon-card">
				<div class="coming-soon-badge">{i18n.t('analysis.status.comingSoon')}</div>
				<p class="coming-soon-text">{i18n.t(analysis.descKey)}</p>
				<p class="coming-soon-body">{i18n.t('analysis.comingSoon.body')}</p>
			</div>
		{:else if analysis.spatialUnit === 'catastro'}
			<CatastroAnalysis {lensStore} {mapStore} {onRemoveRadio} {onSelectCatastroDpto} />
		{:else if HEX_LAYER_REGISTRY[analysis.id]}
			<OvertureAnalysis {analysis} {hexStore} {territoryStore} onSelectDpto={onSelectFloodDpto} />
			{#if showPlantControl}
				<div class="plant-control">
					<label class="plant-toggle">
						<input type="checkbox" bind:checked={mapStore.plantationsVisible} />
						<span>{i18n.t('forestry.overlay.toggle')}</span>
					</label>
					{#if mapStore.plantationsVisible}
						<div class="plant-legend">
							<span><i class="sw" style:background="#22c55e"></i>Pinos</span>
							<span><i class="sw" style:background="#38bdf8"></i>Eucaliptos</span>
							<span><i class="sw" style:background="#f59e0b"></i>Nativas</span>
							<span><i class="sw" style:background="#a78bfa"></i>Sauces/Álamos</span>
							<span><i class="sw" style:background="#9ca3af"></i>Otras</span>
						</div>
					{/if}
					<div class="plant-note">{i18n.t('forestry.overlay.note')}</div>
				</div>
			{/if}
		{:else}
			<p class="text-text-dim text-[10px]">{i18n.t(analysis.descKey)}</p>
		{/if}
	</div>
{/if}

<style>
	.analysis-view {
		font-size: 11px;
	}
	.back-btn {
		font-size: 10px;
		color: #d4d4d4;
		background: none;
		border: none;
		cursor: pointer;
		padding: 2px 0;
		text-align: left;
		transition: color 0.15s;
		margin-bottom: 6px;
	}
	.back-btn:hover { color: #e2e8f0; }
	.view-header {
		display: flex;
		align-items: center;
		gap: 8px;
		margin-bottom: 10px;
	}
	.view-icon {
		font-size: 20px;
		line-height: 1;
	}
	.view-title-group {
		flex: 1;
	}
	.view-title {
		font-size: 12px;
		font-weight: 600;
		color: #e2e8f0;
	}
	.view-lens {
		font-size: 9px;
		font-weight: 500;
	}
	.coming-soon-card {
		background: rgba(100,116,139,0.1);
		border: 1px solid rgba(100,116,139,0.2);
		border-radius: 8px;
		padding: 12px;
	}
	.coming-soon-badge {
		display: inline-block;
		font-size: 9px;
		font-weight: 600;
		color: #d4d4d4;
		background: rgba(100,116,139,0.2);
		padding: 2px 8px;
		border-radius: 9999px;
		margin-bottom: 8px;
	}
	.coming-soon-text {
		font-size: 10px;
		color: #cbd5e1;
		margin: 0 0 6px 0;
	}
	.coming-soon-body {
		font-size: 9px;
		color: #a3a3a3;
		margin: 0;
		line-height: 1.4;
	}
	.plant-control {
		margin-top: 10px; padding: 8px 10px; border-radius: 6px;
		background: rgba(245,158,11,0.08); border: 1px solid rgba(245,158,11,0.25);
	}
	.plant-toggle { display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 11px; font-weight: 600; color: #e2e8f0; }
	.plant-toggle input { cursor: pointer; }
	.plant-legend { display: flex; flex-wrap: wrap; gap: 4px 10px; margin-top: 6px; font-size: 9px; color: #cbd5e1; }
	.plant-legend span { display: inline-flex; align-items: center; gap: 4px; }
	.plant-legend .sw { width: 10px; height: 10px; border-radius: 2px; display: inline-block; flex-shrink: 0; }
	.plant-note { margin-top: 6px; font-size: 9px; color: #94a3b8; line-height: 1.4; }
	.eudr-link { padding: 4px 0; }
	.eudr-btn { display: block; text-align: center; padding: 8px 12px; background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 4px; color: #d4d4d4; font-size: 11px; text-decoration: none; transition: all 0.15s; }
	.eudr-btn:hover { background: rgba(255,255,255,0.1); color: #fff; }
</style>
