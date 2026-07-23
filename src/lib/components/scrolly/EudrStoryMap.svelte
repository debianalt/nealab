<script lang="ts">
	/**
	 * EudrStoryMap — sticky media for the EUDR storymap. Wraps EudrMap and drives it
	 * per scroll step (fitBounds + showCells). Guided-only: the map is non-interactive
	 * (no scroll-zoom, pan or click), so the wheel always advances the story and the
	 * narrative fully controls the camera. Per-parcel inspection lives in the
	 * dedicated interactive tool at /eudr/check.
	 */
	import { onMount } from 'svelte';
	import EudrMap from '$lib/components/EudrMap.svelte';
	import { initDuckDB, query } from '$lib/stores/duckdb';
	import { getEudrParquetUrl } from '$lib/config';
	import type { Locale } from '$lib/stores/i18n.svelte';

	export interface StepView {
		province: string | null;
		bounds: [[number, number], [number, number]];
	}

	interface Props {
		activeStep: number;
		views: StepView[];
		locale?: Locale;
	}
	let { activeStep, views, locale = 'es' }: Props = $props();

	let mapRef: any = $state();
	let ready = $state(false);
	let loading = $state(false);
	const cache = new Map<string, { h3index: string; risk: number }[]>();

	const NEA_LIST = "'ar_misiones','ar_corrientes','ar_chaco','ar_formosa'";

	async function loadProvince(prov: string) {
		if (cache.has(prov)) return cache.get(prov)!;
		const url = getEudrParquetUrl('eudr_deforestation');
		const where = prov === 'ALL' ? `province IN (${NEA_LIST})` : `province = '${prov}'`;
		const limit = prov === 'ALL' ? 4000 : 2500;
		const sql = `SELECT h3index, risk_score FROM read_parquet('${url}')
			WHERE ${where} AND risk_score > 3
			ORDER BY risk_score DESC LIMIT ${limit}`;
		const rows = (await query(sql)).toArray();
		const cells = rows.map((r: any) => ({ h3index: String(r.h3index), risk: Number(r.risk_score) }));
		cache.set(prov, cells);
		return cells;
	}

	async function paint(prov: string) {
		try {
			loading = true;
			const cells = await loadProvince(prov);
			if (views[activeStep]?.province === prov) mapRef?.showCells(cells);
		} catch (e) {
			console.warn('[EudrStoryMap] province load failed', e);
		} finally {
			loading = false;
		}
	}

	function applyStep(i: number) {
		const v = views[i];
		if (!v || !mapRef) return;
		mapRef.fitBounds(v.bounds);
		if (v.province) paint(v.province);
		else mapRef.clearPolygon?.();
	}

	$effect(() => {
		activeStep;
		if (ready) applyStep(activeStep);
	});

	onMount(async () => {
		try {
			await initDuckDB();
		} catch (e) {
			console.warn('[EudrStoryMap] duckdb init failed', e);
		}
		ready = true;
		applyStep(activeStep);
		// MapLibre may finish loading after the first paint attempt; re-paint the
		// current step's cells (without re-flying) so the initial hexagons appear.
		const cur = views[activeStep]?.province;
		if (cur) {
			setTimeout(() => views[activeStep]?.province === cur && paint(cur), 1400);
			setTimeout(() => views[activeStep]?.province === cur && paint(cur), 3000);
		}
	});
</script>

<div class="story-map">
	<EudrMap bind:this={mapRef} interactive={false} />
	<div class="map-scrim"></div>

	{#if loading}
		<div class="map-loading">{locale === 'en' ? 'loading data…' : 'cargando datos…'}</div>
	{/if}

	<div class="map-legend">
		<span class="lg-title">{locale === 'en' ? 'risk' : 'riesgo'}</span>
		<span class="lg-ramp"></span>
		<span class="lg-lo">{locale === 'en' ? 'low' : 'bajo'}</span>
		<span class="lg-hi">{locale === 'en' ? 'high' : 'alto'}</span>
	</div>
</div>

<style>
	.story-map {
		position: absolute;
		inset: 0;
		width: 100%;
		height: 100%;
	}
	/* darken the left (where the text track sits) so the map reads as the backdrop
	   and the copy stays legible; right side of the map stays fully visible. */
	.map-scrim {
		position: absolute;
		inset: 0;
		z-index: 2;
		pointer-events: none;
		background: linear-gradient(
			90deg,
			rgba(8, 9, 12, 0.85) 0%,
			rgba(8, 9, 12, 0.55) 26%,
			rgba(8, 9, 12, 0) 54%
		);
	}
	.map-loading {
		position: absolute;
		top: 12px;
		left: 12px;
		z-index: 5;
		pointer-events: none;
		font-family: 'JetBrains Mono', monospace;
		font-size: 11px;
		color: rgba(255, 255, 255, 0.75);
		background: rgba(10, 12, 18, 0.85);
		border: 1px solid #1e293b;
		padding: 4px 8px;
		border-radius: 4px;
	}
	.map-legend {
		position: absolute;
		bottom: 16px;
		left: 12px;
		z-index: 5;
		pointer-events: none;
		display: grid;
		grid-template-columns: auto auto;
		align-items: center;
		gap: 2px 6px;
		font-family: 'JetBrains Mono', monospace;
		font-size: 10px;
		color: rgba(255, 255, 255, 0.7);
		background: rgba(10, 12, 18, 0.82);
		border: 1px solid #1e293b;
		padding: 6px 8px;
		border-radius: 5px;
	}
	.lg-title {
		grid-column: 1 / -1;
		text-transform: uppercase;
		letter-spacing: 0.1em;
		color: rgba(255, 255, 255, 0.5);
	}
	.lg-ramp {
		grid-column: 1 / -1;
		height: 7px;
		width: 120px;
		border-radius: 3px;
		background: linear-gradient(90deg, #fef3c7, #fde047, #f59e0b, #ef4444, #991b1b);
	}
	.lg-lo {
		justify-self: start;
	}
	.lg-hi {
		justify-self: end;
	}
</style>
