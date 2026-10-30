<script lang="ts">
	import type { HexStore } from '$lib/stores/hex.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { unitLabel } from '$lib/utils/units';
	import { deptFillClasses, FILL_NA } from '$lib/utils/deptFill';

	let { hexStore }: { hexStore: HexStore } = $props();

	const PALETTE = ['#1565c0', '#7e57c2', '#4db6ac', '#66bb6a', '#c0ca33', '#ffb74d', '#e65100', '#78909c'];

	const layer = $derived(hexStore.activeLayer);

	// Province view: the departments painted with the ranking reading (deptFill).
	const fill = $derived(hexStore.deptFill);
	const fillBy = $derived(fill ? i18n.t(fill.territory.endsWith('_br') ? 'legend.deptFill.byMun' : fill.territory.endsWith('_py') ? 'legend.deptFill.byDist' : 'legend.deptFill.byDept') : '');
	const fillRows = $derived.by(() => {
		if (!fill) return [];
		const f = (x: number) => (fill.signed && x > 0 ? '+' : x < 0 ? '−' : '') + Math.abs(x).toLocaleString(i18n.locale, { minimumFractionDigits: fill.decimals, maximumFractionDigits: fill.decimals });
		return deptFillClasses(fill.signed).map(c => ({
			color: c.color, opacity: c.opacity,
			label: c.lo < 0 && c.hi > 0 ? `≈ 0 (± ${f(c.hi * fill.max).replace('+', '')})` : `${f(c.lo * fill.max)} – ${f(c.hi * fill.max)}`,
		}));
	});
	const fillHasNull = $derived(!!fill?.values.some(v => v.value == null));
	// Use diverging scale when temporal delta mode is active, otherwise use layer's configured scale
	const effectiveScale = $derived(
		layer?.temporal && hexStore.temporalMode === 'delta' ? 'diverging' : layer?.colorScale
	);
	const isCategorical = $derived(effectiveScale === 'categorical');
	const isSequential = $derived(effectiveScale === 'sequential');
	const isFlood = $derived(effectiveScale === 'flood');
	const isGreen = $derived(effectiveScale === 'green');
	const isWarm = $derived(effectiveScale === 'warm');
	const isNight = $derived(effectiveScale === 'night');
	const isDiverging = $derived(effectiveScale === 'diverging');
	const isGradient = $derived(isSequential || isFlood || isGreen || isWarm || isNight || isDiverging);

	// When the map paints a variable other than the primary (flood ranking tab),
	// the title names it.
	const paintedLabel = $derived.by(() => {
		const pv = hexStore.effectivePrimary;
		if (!layer || !hexStore.colorVariable || pv !== hexStore.colorVariable) return '';
		const v = layer.variables?.find((x: any) => x.col === pv);
		return v ? i18n.t(v.labelKey) : '';
	});
	const title = $derived(layer ? i18n.t(layer.titleKey) + (paintedLabel ? ` · ${paintedLabel}` : '') : '');

	// Discrete classes replace the gradient (flood: JRC occurrence %). Not in delta
	// mode: the change ramp is diverging, not a level.
	const classes = $derived(!isDiverging ? hexStore.effectiveColorClasses : undefined);

	// Two-scale layers (PM2.5): provincial P2–P98 ↔ fixed regional range. The ends
	// show the real values of the active domain so the colors carry their numbers.
	const hasScales = $derived(!!layer?.regionalColorDomain && !isDiverging);
	const scaleStats = $derived(layer?.scaleStats?.territories[hexStore.territoryId]);
	const fmt = (v: number) => v.toLocaleString(i18n.locale === 'en' ? 'en' : 'es', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
	const scaleNote = $derived.by(() => {
		if (hexStore.scaleMode === 'regional') return i18n.t('legend.scale.regionalNote');
		const name = scaleStats?.label ?? '';
		return i18n.t(scaleStats ? 'legend.scale.provincialNote' : 'legend.scale.minmaxNote').replace('{t}', name);
	});
	const scaleUnit = $derived.by(() => {
		const v = layer?.variables?.find((x: any) => x.col === layer?.primaryVariable);
		return unitLabel(v?.unit);
	});
	const classUnit = $derived.by(() => {
		const pv = hexStore.effectivePrimary;
		const v = layer?.variables?.find((x: any) => x.col === pv || x.rawCol === pv);
		return unitLabel(v?.unit);
	});

	// Range badge next to the title. The gradient is colored by the layer's primary
	// variable, which is NOT always a 0–100 score: physical-unit layers (precipitation
	// mm, carbon tC/ha, travel min, density hab/km²) were mislabeled "0–100". Show the
	// real unit when the primary variable carries one; fall back to "0–100" for
	// score/percentile variables. In temporal delta mode the badge is hidden (the value
	// is a signed change, not a level).
	const SCORE_UNITS = new Set(['/100', 'percentil', 'pctl', '%']);
	const rangeLabel = $derived.by(() => {
		if (!layer || isDiverging) return '';
		const pv = hexStore.effectivePrimary; // may be a per-territory fallback column
		const v = layer.variables?.find((x: any) => x.col === pv || x.rawCol === pv);
		const u = v?.unit;
		// A fixed color domain narrower than 0–100 (EUDR: 0–20 %) must show its real ends.
		const fd = layer.fixedColorDomain;
		if (fd && (!u || SCORE_UNITS.has(u))) return `${fd[0]}–${fd[1]}${u === '%' ? ' %' : ''}`;
		if (!u || SCORE_UNITS.has(u)) return '0–100';
		return unitLabel(u);
	});

	// Extract unique type labels from visible data (categorical)
	const typeEntries = $derived.by(() => {
		if (!isCategorical) return [];
		const types = new Map<number, string>();
		for (const [, data] of hexStore.visibleData) {
			const t = data.type ?? data.territorial_type;
			if (t && !types.has(t)) {
				const label = data.type_label;
				types.set(t, typeof label === 'number' ? '' : (label as unknown as string) || '');
			}
		}
		return [...types.entries()].sort((a, b) => a[0] - b[0]);
	});

	const gradient = $derived(
		isFlood
			? 'linear-gradient(to right, #3b82f6, #eab308, #dc2626)'
			: isGreen
			? 'linear-gradient(to right, #bbf7d0, #166534, #14532d)'
			: isWarm
			? 'linear-gradient(to right, #78350f, #f59e0b, #fde725)'
			: isNight
			? 'linear-gradient(to right, #0b1026, #f59e0b, #fff7d6)'
			: isDiverging
			? 'linear-gradient(to right, #ef4444, #a3a3a3, #22c55e)'
			: 'linear-gradient(to right, #5b21b6, #21918c, #fde725)'
	);

	// Danger layers (colorScale 'flood', higher = worse) get their delta sign flipped in
	// the choropleth so an INCREASE renders red. In that mode the static layer labels
	// ("Baja/Alta presión…") would read backwards, so the diverging ends must be labeled
	// Peor/Mejor instead. Non-danger temporal layers (carbon, censo: sequential) are not
	// flipped, so their static low/high labels still fit the change ramp.
	const isDangerDelta = $derived(isDiverging && layer?.colorScale === 'flood');
	const lowLabel = $derived(
		isDangerDelta ? i18n.t('temporal.legend.worse')
		: layer?.legendLowKey ? i18n.t(layer.legendLowKey)
		: isFlood ? i18n.t('legend.lowRisk')
		: isDiverging ? i18n.t('temporal.legend.worse')
		: i18n.t('legend.low')
	);
	const highLabel = $derived(
		isDangerDelta ? i18n.t('temporal.legend.better')
		: layer?.legendHighKey ? i18n.t(layer.legendHighKey)
		: isFlood ? i18n.t('legend.highRisk')
		: isDiverging ? i18n.t('temporal.legend.better')
		: i18n.t('legend.high')
	);
</script>

{#if layer && fill}
	<div class="legend">
		<div class="legend-title">{fill.title}</div>
		<div class="legend-sub">{fill.unit ? `${fillBy} · ${fill.unit}` : fillBy}</div>
		<div class="legend-items">
			{#each fillRows as row}
				<div class="legend-item">
					<span class="legend-swatch" style:background={row.color} style:opacity={row.opacity}></span>
					<span class="legend-label">{row.label}</span>
				</div>
			{/each}
			{#if fillHasNull}
				<div class="legend-item">
					<span class="legend-swatch" style:background={FILL_NA.color} style:opacity={FILL_NA.opacity}></span>
					<span class="legend-label">{i18n.t('legend.deptFill.na')}</span>
				</div>
			{/if}
		</div>
	</div>
{:else if layer && (isCategorical && typeEntries.length > 0)}
	<div class="legend">
		<div class="legend-title">{title}</div>
		<div class="legend-items">
			{#each typeEntries as [type, label]}
				<div class="legend-item">
					<span class="legend-swatch" style:background={PALETTE[(type - 1) % PALETTE.length]}></span>
					<span class="legend-label">{label || `Tipo ${type}`}</span>
				</div>
			{/each}
		</div>
		<div class="nodata-row">
			<span class="nodata-swatch"></span>
			<span class="nodata-label">{i18n.t('legend.noData')}</span>
		</div>
	</div>
{:else if layer && classes}
	<div class="legend">
		<div class="legend-title">{title} {#if classUnit && !paintedLabel}<span class="legend-range">{classUnit}</span>{/if}</div>
		<div class="legend-items">
			{#if classes.zero}
				<div class="legend-item">
					<span class="legend-swatch outlined" style:background={classes.zero.color}></span>
					<span class="legend-label">{i18n.t(classes.zero.labelKey)}</span>
				</div>
			{/if}
			{#each classes.steps as step}
				<div class="legend-item">
					<span class="legend-swatch" style:background={step.color}></span>
					<span class="legend-label">{i18n.t(step.labelKey)}</span>
				</div>
			{/each}
		</div>
		<div class="nodata-row">
			<span class="nodata-swatch"></span>
			<span class="nodata-label">{i18n.t('legend.noData')}</span>
		</div>
	</div>
{:else if layer && hasScales}
	<div class="legend">
		<div class="legend-title">{title} {#if scaleUnit}<span class="legend-range">{scaleUnit}</span>{/if}</div>
		<div class="scale-toggle" role="group">
			<button class:active={hexStore.scaleMode === 'provincial'} onclick={() => hexStore.setScaleMode('provincial')}>{i18n.t('legend.scale.provincial')}</button>
			<button class:active={hexStore.scaleMode === 'regional'} onclick={() => hexStore.setScaleMode('regional')}>{i18n.t('legend.scale.regional')}</button>
		</div>
		<div class="gradient-bar" style:background={gradient}></div>
		<div class="gradient-labels">
			<span>{hexStore.colorDomain ? fmt(hexStore.colorDomain[0]) : ''}</span>
			<span>{hexStore.colorDomain ? `${fmt(hexStore.colorDomain[1])} ${scaleUnit}` : ''}</span>
		</div>
		<div class="scale-note">{scaleNote}</div>
		<div class="nodata-row">
			<span class="nodata-swatch"></span>
			<span class="nodata-label">{i18n.t('legend.noData')}</span>
		</div>
	</div>
{:else if layer && isGradient}
	<div class="legend">
		<div class="legend-title">{title} {#if rangeLabel}<span class="legend-range">{rangeLabel}</span>{/if}</div>
		<div class="gradient-bar" style:background={gradient}></div>
		<div class="gradient-labels">
			<span>{lowLabel}</span>
			<span>{highLabel}</span>
		</div>
		<div class="nodata-row">
			<span class="nodata-swatch"></span>
			<span class="nodata-label">{i18n.t('legend.noData')}</span>
		</div>
	</div>
{:else if !layer}
	<!-- Cold-open canvas: GBA buildings coloured by estimated population -->
	<div class="legend">
		<div class="legend-title">{i18n.t('legend.buildingsCanvas')}</div>
		<div class="gradient-bar" style:background={'linear-gradient(to right, #0f1e30, #1e4060, #3080c0, #50a0e0, #70c0ff, #a0dfff, #d0f0ff)'}></div>
		<div class="gradient-labels">
			<span>−</span>
			<span>{i18n.t('legend.buildingVolume')}</span>
			<span>+</span>
		</div>
		<div class="canvas-note">{i18n.t('legend.buildingVolumeNote')}</div>
	</div>
{/if}

<style>
	.legend {
		position: absolute;
		bottom: 4px;
		left: 50%;
		transform: translateX(-50%);
		background: rgba(10, 12, 18, 0.85);
		backdrop-filter: blur(6px);
		border: 1px solid rgba(255,255,255,0.08);
		border-radius: 6px;
		padding: 8px 10px;
		z-index: 10;
		min-width: 140px;
		max-width: 280px;
	}
	.legend-title {
		font-size: 9px;
		font-weight: 600;
		color: rgba(255,255,255,0.7);
		margin-bottom: 6px;
		text-transform: uppercase;
		letter-spacing: 0.04em;
		line-height: 1.3;
	}
	.legend-sub {
		font-size: 10px;
		color: var(--text-secondary, #94a3b8);
		margin: -2px 0 6px;
	}

	.legend-items {
		display: flex;
		flex-direction: column;
		gap: 3px;
	}
	.legend-item {
		display: flex;
		align-items: center;
		gap: 6px;
	}
	.legend-swatch {
		width: 10px;
		height: 10px;
		border-radius: 2px;
		flex-shrink: 0;
	}
	.scale-toggle {
		display: flex;
		gap: 2px;
		margin-bottom: 6px;
	}
	.scale-toggle button {
		flex: 1;
		font-size: 8.5px;
		padding: 2px 6px;
		border-radius: 3px;
		border: 1px solid rgba(255,255,255,0.12);
		background: transparent;
		color: rgba(255,255,255,0.55);
		cursor: pointer;
	}
	.scale-toggle button.active {
		background: rgba(255,255,255,0.12);
		color: #e5e7eb;
	}
	.scale-note {
		font-size: 7.5px;
		color: rgba(255,255,255,0.45);
		margin-top: 4px;
		line-height: 1.35;
	}
	.legend-swatch.outlined {
		box-shadow: inset 0 0 0 1px rgba(255,255,255,0.3);
	}
	.legend-label {
		font-size: 9px;
		color: #d4d4d4;
	}
	.gradient-bar {
		height: 8px;
		border-radius: 3px;
		width: 100%;
	}
	.gradient-labels {
		display: flex;
		justify-content: space-between;
		font-size: 8px;
		color: rgba(255,255,255,0.55);
		margin-top: 3px;
	}
	.legend-range {
		font-size: 8px;
		font-weight: 400;
		color: rgba(255,255,255,0.4);
		letter-spacing: 0;
		text-transform: none;
		margin-left: 4px;
	}
	.nodata-row {
		display: flex;
		align-items: center;
		gap: 5px;
		margin-top: 5px;
		padding-top: 4px;
		border-top: 1px solid rgba(255,255,255,0.06);
	}
	.nodata-swatch {
		width: 10px;
		height: 10px;
		border-radius: 2px;
		background: #6b7280;
		flex-shrink: 0;
	}
	.nodata-label {
		font-size: 8px;
		color: rgba(255,255,255,0.4);
	}
	.canvas-note {
		font-size: 7.5px;
		color: rgba(255,255,255,0.4);
		font-style: italic;
		margin-top: 4px;
		text-align: center;
	}

	@media (max-width: 768px) {
		.legend {
			bottom: 32px;
			left: 8px;
			transform: none;
			max-width: 220px;
			padding: 6px 8px;
		}
	}
</style>
