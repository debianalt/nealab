<script lang="ts">
	import { onDestroy } from 'svelte';
	import { PETAL_VARS, loadRadioPopulation } from '$lib/utils/petal';
	import { i18n } from '$lib/stores/i18n.svelte';
	import HistogramPanel from './HistogramPanel.svelte';
	import { territoryRef } from '$lib/utils/territoryRef';

	let { territory, onBrushSelect, selected = [] }: {
		territory: string;
		onBrushSelect?: (redcodes: string[]) => void;
		// Tracts selected on the map: marked on the distribution, with their percentile.
		selected?: Array<{ redcode: string; color: string }>;
	} = $props();

	const TERR_LABEL: Record<string, string> = {
		misiones: 'Misiones', corrientes: 'Corrientes', chaco: 'Chaco', formosa: 'Formosa',
		parana_br: 'Paraná (BR)', santa_catarina_br: 'Santa Catarina (BR)', rio_grande_sul_br: 'Rio Grande do Sul (BR)',
	};

	let radioData = $state<Map<string, Record<string, any>> | null>(null);
	let activeVars = $state(PETAL_VARS as typeof PETAL_VARS);
	let loadError = $state(false);
	let selectedVar = $state(PETAL_VARS[0].col);

	// Plain var (NOT $state): tracks which territory is loaded so the effect
	// neither loops nor re-queries. Set before the await. Mirrors `prevLens`.
	let loadedFor: string | null = null;

	$effect(() => {
		const terr = territory; // reactive dep — reload on territory switch
		if (terr === loadedFor) return;
		loadedFor = terr;
		onBrushSelect?.([]); // clear stale highlight from previous territory
		radioData = null;
		loadError = false;
		loadRadioPopulation(terr)
			.then(({ data, vars }) => {
				if (loadedFor !== terr) return;
				radioData = data;
				activeVars = vars;
			})
			.catch(() => { if (loadedFor === terr) loadError = true; });
	});

	// Defensive: when vars shrink (MIS 6 → COR 5) keep a valid selection.
	const effVar = $derived(
		activeVars.some((v) => v.col === selectedVar) ? selectedVar : activeVars[0].col
	);

	// Where each selected tract falls among all the province's tracts, for the
	// chosen variable: value and share of tracts with a lower value.
	const positions = $derived.by(() => {
		if (!radioData) return [];
		const vals: number[] = [];
		for (const row of radioData.values()) {
			const v = Number(row[effVar]);
			if (isFinite(v)) vals.push(v);
		}
		return selected.flatMap((s) => {
			const v = Number(radioData!.get(s.redcode)?.[effVar]);
			if (!isFinite(v) || vals.length === 0) return [];
			const below = vals.reduce((n, x) => n + (x < v ? 1 : 0), 0);
			return [{ ...s, value: v, pct: Math.round((100 * below) / vals.length) }];
		});
	});
	const fmtPct = (v: number) => v.toLocaleString(i18n.locale, { maximumFractionDigits: 1 });

	// Clear the map highlight when the panel goes away (lens opened, switch to
	// Itapúa, radios selected, etc.) so no ghost overlay remains.
	onDestroy(() => onBrushSelect?.([]));
</script>

<div class="chart-root">
	<div class="rc-header">
		<span class="rc-title">{i18n.t('side.radioCensus.title')} — {TERR_LABEL[territory] ?? territory}</span>
	</div>
	<p class="rc-subtitle">{i18n.t('side.radioCensus.subtitle')}</p>

	{#if loadError}
		<p class="rc-msg">{i18n.t('side.radioCensus.error')}</p>
	{:else if radioData === null}
		<p class="rc-msg">{i18n.t('side.radioCensus.loading')}</p>
	{:else if radioData.size === 0}
		<p class="rc-msg">{i18n.t('side.radioCensus.empty')}</p>
	{:else}
		<label class="rc-select-row">
			<span class="rc-select-label">{i18n.t('side.radioCensus.variable')}</span>
			<select class="rc-select" bind:value={selectedVar}>
				{#each activeVars as v}
					<option value={v.col}>{i18n.t(v.labelKey)}</option>
				{/each}
			</select>
		</label>

		<HistogramPanel data={radioData} variable={effVar} xLabel="%" {onBrushSelect}
			markers={positions.map((p) => ({ value: p.value, color: p.color }))} />
		{#each positions as p, i}
			<p class="rc-pos">
				<span class="rc-dot" style:background={p.color}></span>
				{i18n.t(positions.length > 1 ? 'side.radioCensus.positionN' : 'side.radioCensus.position')
					.replace('{n}', String(i + 1))
					.replace('{value}', fmtPct(p.value))
					.replace('{pct}', String(p.pct))
					.replace('{ref}', territoryRef(territory, i18n.locale, 'gen'))}
			</p>
		{/each}
	{/if}
</div>

<style>
	.chart-root { font-size: 11px; line-height: 1.3; }
	.rc-header {
		display: flex; justify-content: space-between; align-items: center;
		margin-bottom: 2px;
	}
	.rc-title { font-size: 10px; font-weight: 600; color: #e2e8f0; }
	.rc-subtitle {
		font-size: 9px; color: rgba(255,255,255,0.45);
		margin: 0 0 8px;
	}
	.rc-msg {
		font-size: 10px; color: #94a3b8;
		padding: 12px 0; text-align: center;
	}
	.rc-select-row {
		display: flex; align-items: center; gap: 6px;
		margin-bottom: 6px;
	}
	.rc-select-label {
		font-size: 9px; font-weight: 600;
		color: #a3a3a3; text-transform: uppercase;
	}
	.rc-select {
		flex: 1;
		font-size: 10px; padding: 2px 6px; border-radius: 4px;
		background: #1e293b; border: 1px solid #334155;
		color: #e2e8f0; cursor: pointer; font-family: inherit;
	}
	.rc-select option { background: #1e293b; color: #e2e8f0; }
	.rc-select:focus { outline: none; border-color: #64748b; }
	.rc-pos {
		display: flex; align-items: baseline; gap: 6px;
		font-size: 10px; color: #cbd5e1;
		margin: 4px 0 0;
	}
	.rc-dot {
		flex: none; width: 8px; height: 8px; border-radius: 50%;
		transform: translateY(1px);
	}
</style>
