<script lang="ts">
	import type { HexStore, HexZone } from '$lib/stores/hex.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { unitLabel } from '$lib/utils/units';
	import { formatDept } from '$lib/utils/format';
	import { territoryRef } from '$lib/utils/territoryRef';
	import {
		downloadCsvFromRows,
		downloadGeoJsonFromPolygon,
		downloadGeoJsonFromHexList,
	} from '$lib/utils/data-export';

	let {
		hexStore,
		onRemoveHexZone,
		onClearHexZones,
	}: {
		hexStore: HexStore;
		onRemoveHexZone: (id: string) => void;
		onClearHexZones: () => void;
	} = $props();

	const zones = $derived(hexStore.hexZones);
	const variables = $derived(hexStore.activeLayer?.variables ?? []);
	// numericVariables matches rawValues indexing (same filter as store)
	const numVars = $derived(hexStore.numericVariables);

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

	function getDominantType(zone: HexZone): string | null {
		const counts = new Map<string, number>();
		for (const h3 of zone.h3indices) {
			const d = hexStore.zoneRow(h3);
			const t = d?.type_label;
			if (typeof t === 'string') counts.set(t, (counts.get(t) ?? 0) + 1);
		}
		if (counts.size === 0) return null;
		let best = '', max = 0;
		for (const [k, v] of counts) { if (v > max) { best = k; max = v; } }
		return best;
	}

	function zoneFilenameBase(zone: HexZone): string {
		const layerId = hexStore.activeLayer?.id ?? 'layer';
		return `spatia_${layerId}_zone_${zone.id}`;
	}

	function downloadZoneHexesCsv(zone: HexZone) {
		const rows = zone.h3indices.map((h3) => {
			const data = hexStore.zoneRow(h3) ?? {};
			return { h3index: h3, zone: zone.id, origin: zone.origin, ...data };
		});
		if (rows.length === 0) return;
		const allCols = new Set<string>(['h3index', 'zone', 'origin']);
		for (const row of rows) for (const k of Object.keys(row)) allCols.add(k);
		downloadCsvFromRows(rows, [...allCols], `${zoneFilenameBase(zone)}_hexes.csv`);
	}

	function downloadZoneHexesGeoJson(zone: HexZone) {
		if (zone.h3indices.length === 0) return;
		// Rows from zoneRow: chart zones are res-9 cells of the analysis data.
		const rows = new Map<string, Record<string, unknown>>();
		for (const h3 of zone.h3indices) rows.set(h3, { zone: zone.id, origin: zone.origin, ...(hexStore.zoneRow(h3) ?? {}) });
		downloadGeoJsonFromHexList(zone.h3indices, rows, `${zoneFilenameBase(zone)}_hexes.geojson`);
	}

	function downloadZonePolygonGeoJson(zone: HexZone) {
		if (!zone.polygon) return;
		const props: Record<string, unknown> = {
			zone: zone.id,
			origin: zone.origin,
			color: zone.color,
			hex_count: zone.stats.hexCount,
			layer: hexStore.activeLayer?.id ?? null,
		};
		// rawValues is indexed by numericVariables (the old loop used the index over
		// ALL variables, shifting values whenever a non-numeric one came first).
		numVars.forEach((v, i) => {
			const x = zone.stats.rawValues[i];
			props[v.col] = Number.isFinite(x) ? x : null;
		});
		downloadGeoJsonFromPolygon(zone.polygon, props, `${zoneFilenameBase(zone)}_polygon.geojson`);
	}

	// «Solicitar informe»: a mail with what was selected (not the H3 codes: they can be
	// thousands; the body asks to attach the CSV). Copy-the-address as fallback.
	const EMAIL = 'nealab@spatia.ar';
	const reportHref = $derived.by(() => {
		const layer = hexStore.activeLayer ? i18n.t(hexStore.activeLayer.titleKey) : '';
		const dept = hexStore.selectedDpto ? formatDept(hexStore.selectedDpto) : '';
		const where = [dept, territoryRef(hexStore.territoryId, i18n.locale)].filter(Boolean).join(' · ');
		const subject = `Spatia — ${i18n.t('zone.report.subject')}: ${layer}${dept ? ' · ' + dept : ''}`;
		const lines = [
			i18n.t('zone.report.intro'),
			'',
			`${i18n.t('zone.report.layer')}: ${layer}`,
			`${i18n.t('zone.report.where')}: ${where}`,
			...zones.map(z => `${i18n.t('zone.title')} ${z.id} · ${z.origin} · ${z.stats.hexCount} hex`),
			'',
			`${i18n.t('zone.report.link')}: ${typeof location !== 'undefined' ? location.href : ''}`,
			'',
			i18n.t('zone.report.attach'),
		];
		return `mailto:${EMAIL}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(lines.join('\n'))}`;
	});
	let copied = $state(false);
	async function copyEmail() {
		try { await navigator.clipboard.writeText(EMAIL); copied = true; setTimeout(() => (copied = false), 2000); } catch { /* clipboard blocked */ }
	}

	function downloadZonesSummaryCsv() {
		if (zones.length === 0) return;
		const numericVars = numVars;
		const rows = zones.map((z) => {
			const row: Record<string, unknown> = {
				zone: z.id,
				origin: z.origin,
				hex_count: z.stats.hexCount,
				layer: hexStore.activeLayer?.id ?? '',
			};
			numericVars.forEach((v, i) => {
				row[v.col] = z.stats.rawValues[i];
				row[`${v.col}_norm`] = z.stats.normalizedValues[i];
			});
			return row;
		});
		const columns = Object.keys(rows[0]);
		downloadCsvFromRows(rows, columns, `spatia_${hexStore.activeLayer?.id ?? 'layer'}_zones_summary.csv`);
	}
</script>

<div class="hzc">
	<div class="hzc-header">
		<span class="hzc-title">{i18n.t('hexZone.title')}</span>
		<button class="hzc-clear-btn" onclick={onClearHexZones}>
			&#10005; {i18n.t('lasso.clearZones')}
		</button>
	</div>

	<!-- Zone chips -->
	<div class="hzc-chips">
		{#each zones as zone}
			<span class="hzc-chip">
				<span class="hzc-dot" style:background={zone.color}></span>
				<span class="hzc-chip-label">{zone.id}</span>
				{#if zone.origin}<span class="hzc-chip-origin">{zone.origin}</span>{/if}
				<button class="hzc-remove" onclick={() => onRemoveHexZone(zone.id)}>&#10005;</button>
			</span>
		{/each}
	</div>

	<!-- Vertical zone blocks (like HexComparison) -->
	{#if numVars.length > 0}
		{#each zones as zone, zi}
			<div class="zone-block">
				<div class="zone-id">
					<span class="hzc-dot" style:background={zone.color}></span>
					{i18n.t('zone.title')} {zone.id}
					{#if zone.origin}<span class="zone-origin">· {zone.origin}</span>{/if}
					<span class="zone-hex-count">{zone.stats.hexCount.toLocaleString(i18n.locale)} hex</span>
				</div>
				{#if getDominantType(zone)}
					<div class="zone-type">{getDominantType(zone)}</div>
				{/if}
				{#each numVars as v, vi}
					{@const rv = zone.stats.rawValues[vi]}
					<div class="cd-row">
						<span class="cd-label">{i18n.t(v.labelKey)}</span>
						<span class="cd-val">{fmtSmart(rv)}{v.unit ? ` ${unitLabel(v.unit)}` : ' /100'}</span>
					</div>
				{/each}
				<div class="zone-dl">
					<button class="hzc-dl-btn" title="Hexágonos CSV" onclick={() => downloadZoneHexesCsv(zone)}>csv</button>
					<button class="hzc-dl-btn" title="Hexágonos GeoJSON" onclick={() => downloadZoneHexesGeoJson(zone)}>geo</button>
					{#if zone.polygon}
						<button class="hzc-dl-btn" title="Polígono GeoJSON" onclick={() => downloadZonePolygonGeoJson(zone)}>poly</button>
					{/if}
				</div>
			</div>
		{/each}
	{/if}

	{#if zones.length > 0}
		<div class="hzc-download-row">
			<button class="hzc-download-btn" onclick={downloadZonesSummaryCsv}>
				↓ Resumen comparativo (CSV)
			</button>
		</div>
		<div class="hzc-report">
			<a class="hzc-report-btn" href={reportHref}>{i18n.t('zone.report.button')}</a>
			<button class="hzc-report-copy" onclick={copyEmail}>{copied ? `${EMAIL} ✓` : i18n.t('zone.report.copy')}</button>
		</div>
	{/if}
</div>

<style>
	.hzc { font-size: 12.5px; }
	.hzc-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 8px;
	}
	.hzc-title {
		font-size: 13px;
		font-weight: 600;
		color: #e2e8f0;
	}
	.hzc-clear-btn {
		font-size: 11.5px;
		padding: 2px 6px;
		border-radius: 4px;
		background: rgba(255,255,255,0.06);
		border: 1px solid #334155;
		color: #d4d4d4;
		cursor: pointer;
		transition: all 0.15s;
	}
	.hzc-clear-btn:hover { border-color: #ef4444; color: #ef4444; }

	.hzc-chips {
		display: flex;
		flex-wrap: wrap;
		gap: 4px;
		margin-bottom: 6px;
	}
	.hzc-chip {
		display: flex;
		align-items: center;
		gap: 4px;
		padding: 2px 6px;
		border-radius: 4px;
		background: rgba(255,255,255,0.06);
		border: 1px solid #334155;
		font-size: 12px;
		color: #e2e8f0;
	}
	.hzc-dot {
		width: 8px;
		height: 8px;
		border-radius: 50%;
		flex-shrink: 0;
	}
	.hzc-chip-label { font-weight: 600; }
	.hzc-chip-origin { color: var(--s-ink-2); max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.zone-origin { font-weight: 400; color: var(--s-ink-2); min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.hzc-report { display: flex; gap: 6px; align-items: center; margin-top: 6px; }
	.hzc-report-btn { flex: 1; text-align: center; padding: 6px 10px; border-radius: 4px; font-size: 12px; text-decoration: none; color: var(--s-ink-1); border: 1px solid var(--s-cat-1); background: rgba(196, 87, 47, 0.14); }
	.hzc-report-btn:hover { background: rgba(196, 87, 47, 0.26); }
	.hzc-report-copy { font: inherit; font-size: 11.5px; padding: 6px 8px; border-radius: 4px; cursor: pointer; color: var(--s-ink-2); background: none; border: 1px solid rgba(255,255,255,0.12); }
	.hzc-report-copy:hover { color: var(--s-ink-1); }
	.hzc-remove {
		background: none;
		border: none;
		color: #a3a3a3;
		cursor: pointer;
		font-size: 11.5px;
		padding: 0 2px;
		line-height: 1;
	}
	.hzc-remove:hover { color: #ef4444; }

	.zone-block { margin: 8px 0; padding: 6px 0; border-top: 1px solid rgba(255,255,255,0.06); }
	.zone-id { display: flex; align-items: center; gap: 5px; font-size: 12px; color: #e2e8f0; font-weight: 600; margin-bottom: 5px; }
	.zone-hex-count { font-size: 11.5px; color: rgba(255,255,255,0.55); font-weight: 400; margin-left: auto; }
	.zone-type { font-size: 12px; color: #e2e8f0; font-weight: 500; background: rgba(255,255,255,0.08); display: inline-block; padding: 1px 6px; border-radius: 3px; margin-bottom: 4px; }
	.cd-row { display: flex; align-items: center; gap: 6px; margin-bottom: 2px; }
	.cd-label { font-size: 11.5px; color: #d4d4d4; flex: 0 0 auto; min-width: 100px; }
	.cd-val { font-size: 12px; font-weight: 600; color: #e2e8f0; text-align: right; margin-left: auto; white-space: nowrap; }
	.zone-dl { display: flex; gap: 3px; margin-top: 4px; }

	.hzc-dl-btn { background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.1); color: #94a3b8; font-size: 11px; padding: 1px 4px; border-radius: 3px; cursor: pointer; font-family: inherit; transition: all 0.15s; }
	.hzc-dl-btn:hover { background: rgba(96,165,250,0.15); border-color: rgba(96,165,250,0.4); color: #60a5fa; }

	.hzc-download-row { margin-top: 6px; }
	.hzc-download-btn { display: block; width: 100%; text-align: center; padding: 6px 10px; background: rgba(59,130,246,0.15); border: 1px solid rgba(59,130,246,0.3); border-radius: 4px; color: #60a5fa; font-size: 11.5px; font-weight: 600; cursor: pointer; font-family: inherit; transition: all 0.15s; }
	.hzc-download-btn:hover { background: rgba(59,130,246,0.25); border-color: rgba(59,130,246,0.5); }
</style>
