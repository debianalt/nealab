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
	import { compareToRef, type RefCompare } from '$lib/utils/zoneStats';
	import { radioUnit, RADIO_SUPPORT, hasRadios } from '$lib/stores/radioUnit.svelte';

	let {
		hexStore,
		onRemoveHexZone,
		onClearHexZones,
		onRenamed,
	}: {
		hexStore: HexStore;
		onRemoveHexZone: (id: string) => void;
		onClearHexZones: () => void;
		/** A name changed: the map labels repaint. */
		onRenamed?: () => void;
	} = $props();

	const zones = $derived(hexStore.hexZones);
	const variables = $derived(hexStore.activeLayer?.variables ?? []);
	// numericVariables matches rawValues indexing (same filter as store)
	const numVars = $derived(hexStore.numericVariables);
	// The open department aggregated with the zones' rule (hexStore.zoneDeptRef).
	const deptRef = $derived(hexStore.zoneDeptRef);
	const deptName = $derived.by(() => {
		const d = hexStore.selectedDpto ?? hexStore.deptFilterName;
		return d ? formatDept(d) : territoryRef(hexStore.territoryId, i18n.locale);
	});

	function fmtNum(v: number, digits = 1): string {
		return v.toLocaleString(i18n.locale, { maximumFractionDigits: digits, minimumFractionDigits: 0 });
	}
	function cmpOf(vi: number, zone: HexZone): RefCompare | null {
		const r = deptRef;
		if (!r) return null;
		return compareToRef(numVars[vi], zone.stats.rawValues[vi], r.rawValues[vi], zone.stats.totals[vi], r.totals[vi]);
	}
	function fmtCmp(c: RefCompare, unit: string | undefined): string {
		const sign = (x: number) => (x > 0 ? '+' : x < 0 ? '−' : '±');
		switch (c.kind) {
			case 'pts': return `${sign(c.value)}${fmtNum(Math.abs(c.value))} ${i18n.t('hexZone.ref.pts')}`;
			case 'ratio': return `×${fmtNum(c.value, c.value < 10 ? 2 : 1)}`;
			case 'diff': return `${sign(c.value)}${fmtSmart(Math.abs(c.value))}${unit ? ` ${unitLabel(unit)}` : ''}`;
			case 'share': return `${fmtNum(c.value, c.value < 1 ? 2 : 1)} % ${i18n.t('hexZone.ref.ofDept')}`;
		}
	}

	// Small zones, by what the layer measures (7-nov-2026): a census cell repeats its radio's
	// rate, so what counts is how many radios the zone touches; a satellite cell is measured on
	// its own, so what counts is how much one border cell (the centre-in-lasso rule) moves the mean.
	const censusLayer = $derived(!!hexStore.activeLayer && hexStore.activeLayer.id in RADIO_SUPPORT && hasRadios(hexStore.territoryPrefix));
	$effect(() => { if (censusLayer && zones.length) void radioUnit.ensure(hexStore.territoryPrefix); });
	function radiosIn(zone: HexZone): number | null {
		const t = radioUnit.loaded.get(hexStore.territoryPrefix);
		if (!t) return null;
		const ids = new Set<string>();
		for (const h of zone.h3indices) { const r = t.cellToRadio.get(h); if (r) ids.add(r); }
		return ids.size;
	}
	const personsEstimated = $derived(numVars.some(v => v.aggregation === 'sum' && v.unit === 'personas'));
	function zoneNotes(zone: HexZone): { text: string; warn: boolean }[] {
		const out: { text: string; warn: boolean }[] = [];
		// Opened from a link whose cells no longer match (data or grid updated since).
		if (zone.sharedN !== undefined && zone.sharedN !== zone.h3indices.length) {
			out.push({ text: i18n.t('hexZone.sharedN').replace('{a}', String(zone.sharedN)).replace('{b}', String(zone.h3indices.length)), warn: true });
		}
		if (censusLayer) {
			const n = radiosIn(zone);
			if (n !== null && n > 0) {
				const key = n < 3 ? (n === 1 ? 'hexZone.radiosFew1' : 'hexZone.radiosFew') : 'hexZone.radios';
				out.push({ text: i18n.t(key).replace('{n}', String(n)), warn: n < 3 });
			}
		} else if (zone.stats.hexCount < 10) {
			out.push({ text: i18n.t('hexZone.smallSat').replace('{n}', String(zone.stats.hexCount)).replace('{p}', String(Math.round(100 / zone.stats.hexCount))), warn: true });
		}
		if (personsEstimated) out.push({ text: i18n.t('hexZone.personsEst'), warn: false });
		return out;
	}

	function fmtSmart(v: unknown): string {
		if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
		if (v === 0) return '0';
		const abs = Math.abs(v);
		if (abs < 0.01) return v.toExponential(1);
		// The locale's decimal mark, like the department line below each value.
		const d = abs < 10 ? 2 : abs < 100 ? 1 : 0;
		return v.toLocaleString(i18n.locale, { minimumFractionDigits: d, maximumFractionDigits: d });
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

	// Naming a zone (8-nov-2026): ✎ opens a field in place of the title; Enter or leaving it
	// saves, Esc cancels. The letter stays the zone's short identity.
	let editing: string | null = $state(null);
	let draft = $state('');
	function startRename(z: HexZone) { editing = z.id; draft = z.name ?? ''; }
	function commitRename() {
		if (editing === null) return;
		const id = editing;
		editing = null;
		hexStore.renameHexZone(id, draft);
		onRenamed?.();
	}
	function renameKey(e: KeyboardEvent) {
		if (e.key === 'Enter') { e.preventDefault(); commitRename(); }
		else if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); editing = null; }
	}
	function focusOnMount(el: HTMLInputElement) { el.focus(); el.select(); }

	// «Pedir un diagnóstico a medida» with the zones (origin + size, not H3 codes).

	function downloadZonesSummaryCsv() {
		if (zones.length === 0) return;
		const numericVars = numVars;
		const rows = zones.filter((z) => z.layerId === hexStore.activeLayer?.id).map((z) => {
			const row: Record<string, unknown> = {
				zone: z.id,
				zone_name: z.name ?? '',
				origin: z.origin,
				hex_count: z.stats.hexCount,
				layer: hexStore.activeLayer?.id ?? '',
			};
			numericVars.forEach((v, i) => {
				row[v.col] = z.stats.rawValues[i];
				row[`${v.col}_norm`] = z.stats.normalizedValues[i];
				if (v.aggregation === 'sum') row[`${v.col}_total`] = z.stats.totals[i];
				// The department the zone is read against, with the panel's comparison.
				const r = deptRef;
				row[`${v.col}_dept`] = r ? (v.aggregation === 'sum' ? r.totals[i] : r.rawValues[i]) : null;
				const c = cmpOf(i, z);
				row[`${v.col}_${c?.kind === 'pts' ? 'diff_pts' : c?.kind === 'diff' ? 'diff' : c?.kind === 'share' ? 'share_dept_pct' : 'ratio'}`] = c ? c.value : null;
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

	{#if hexStore.activeLayer}
		<p class="hzc-note">{i18n.t('hexZone.valuesOf')} <b>{i18n.t(hexStore.activeLayer.titleKey)}</b>. {i18n.t('hexZone.follow')}</p>
		{#if deptRef}<p class="hzc-note">{i18n.t('hexZone.refNote').replace('{d}', deptName)}</p>{/if}
	{/if}

	<!-- Zone chips -->
	<div class="hzc-chips">
		{#each zones as zone}
			<span class="hzc-chip">
				<span class="hzc-dot" style:background={zone.color}></span>
				<span class="hzc-chip-label">{zone.id}</span>
				{#if zone.name}<span class="hzc-chip-name" title={zone.name}>{zone.name}</span>
				{:else if zone.origin}<span class="hzc-chip-origin">{zone.origin}</span>{/if}
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
					{#if editing === zone.id}
						<span class="zone-letter">{zone.id}</span>
						<input class="zone-name-input" type="text" maxlength="40" bind:value={draft}
							placeholder={i18n.t('zone.namePlaceholder')} aria-label={i18n.t('zone.rename')}
							onkeydown={renameKey} onblur={commitRename} use:focusOnMount />
					{:else}
						{#if zone.name}<span class="zone-letter">{zone.id}</span> <span class="zone-name" title={zone.name}>{zone.name}</span>
						{:else}{i18n.t('zone.title')} {zone.id}{/if}
						<button class="zone-rename" onclick={() => startRename(zone)} title={i18n.t('zone.rename')} aria-label={i18n.t('zone.rename')}>✎</button>
						{#if zone.origin}<span class="zone-origin">· {zone.origin}</span>{/if}
					{/if}
					<span class="zone-hex-count">{zone.stats.hexCount.toLocaleString(i18n.locale)} hex</span>
				</div>
				{#if zone.layerId !== hexStore.activeLayer?.id}
					<div class="zone-type">{i18n.t('hexZone.recomputing')}</div>
				{:else if zone.stats.cellsWithData === 0}
					<div class="zone-type">{i18n.t('hexZone.noData')}</div>
				{:else}
					{#if getDominantType(zone)}
						<div class="zone-type">{getDominantType(zone)}</div>
					{/if}
					{#each numVars as v, vi}
						{@const isSum = v.aggregation === 'sum'}
						{@const rv = isSum ? zone.stats.totals[vi] : zone.stats.rawValues[vi]}
						{@const cmp = cmpOf(vi, zone)}
						{@const dv = deptRef ? (isSum ? deptRef.totals[vi] : deptRef.rawValues[vi]) : NaN}
						<div class="cd-row">
							<span class="cd-label">{i18n.t(v.labelKey)}{#if isSum}<span class="cd-total">{' · '}{i18n.t('hexZone.total')}</span>{/if}</span>
							<span class="cd-val">{isSum ? Math.round(rv).toLocaleString(i18n.locale) : fmtSmart(rv)}{v.unit ? ` ${unitLabel(v.unit)}` : ' /100'}</span>
						</div>
						{#if Number.isFinite(dv)}
							<div class="cd-ref" data-zone-ref={v.col}>
								{deptName}: {isSum ? Math.round(dv).toLocaleString(i18n.locale) : fmtSmart(dv)}{v.unit ? ` ${unitLabel(v.unit)}` : ''}{#if cmp}{' · '}<b>{fmtCmp(cmp, v.unit)}</b>{/if}
							</div>
						{/if}
					{/each}
					{#each zoneNotes(zone) as note}
						<p class="zone-note" class:warn={note.warn}>{note.text}</p>
					{/each}
				{/if}
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

	{#if zones.some(z => !z.polygon)}
		<p class="hzc-note">{i18n.t('hexZone.chartNotInLink')}</p>
	{/if}
	{#if zones.length > 0}
		<div class="hzc-download-row">
			<button class="hzc-download-btn" onclick={downloadZonesSummaryCsv}>
				↓ Resumen comparativo (CSV)
			</button>
		</div>
		<!-- No «Pedir un diagnóstico» here: the layer panel below has the site's one, and it
		     carries these zones (OvertureAnalysis layerRequestCtx). Two showed one under the
		     other in every layer with zones (8-nov-2026). -->
	{/if}
</div>

<style>
	.hzc-note { font-size: 12px; line-height: 1.4; color: var(--c-ink-3); margin: 2px 0 8px; }
	.cd-total { color: var(--c-ink-3); font-weight: 400; }
	.hzc-note b { color: var(--c-ink-2, inherit); font-weight: 600; }
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
		color: var(--c-ink-1);
	}
	.hzc-clear-btn {
		font-size: 11.5px;
		padding: 2px 6px;
		border-radius: 4px;
		background: rgba(var(--c-fg-rgb), 0.06);
		border: 1px solid var(--c-line-2);
		color: var(--c-ink-1n);
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
		background: rgba(var(--c-fg-rgb), 0.06);
		border: 1px solid var(--c-line-2);
		font-size: 12px;
		color: var(--c-ink-1);
	}
	.hzc-dot {
		width: 8px;
		height: 8px;
		border-radius: 50%;
		flex-shrink: 0;
	}
	.hzc-chip-label { font-weight: 600; }
	.hzc-chip-name { color: var(--c-ink-1); max-width: 160px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.zone-letter { font-weight: 700; }
	.zone-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.zone-rename {
		background: none; border: none; padding: 0 2px; cursor: pointer; font-size: 12px;
		color: var(--c-ink-3); line-height: 1;
	}
	.zone-rename:hover { color: var(--c-accent); }
	.zone-name-input {
		flex: 1; min-width: 0; font: inherit; font-weight: 500; color: var(--c-ink-1);
		background: var(--c-bg); border: 1px solid var(--c-accent); border-radius: 3px; padding: 1px 5px;
	}
	.hzc-chip-origin { color: var(--s-ink-2); max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.zone-origin { font-weight: 400; color: var(--s-ink-2); min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	.hzc-remove {
		background: none;
		border: none;
		color: var(--c-ink-2n);
		cursor: pointer;
		font-size: 11.5px;
		padding: 0 2px;
		line-height: 1;
	}
	.hzc-remove:hover { color: #ef4444; }

	.zone-block { margin: 8px 0; padding: 6px 0; border-top: 1px solid rgba(var(--c-fg-rgb), 0.06); }
	.zone-id { display: flex; align-items: center; gap: 5px; font-size: 12px; color: var(--c-ink-1); font-weight: 600; margin-bottom: 5px; }
	.zone-hex-count { font-size: 11.5px; color: rgba(var(--c-fg-rgb), 0.55); font-weight: 400; margin-left: auto; white-space: nowrap; flex-shrink: 0; }
	.zone-type { font-size: 12px; color: var(--c-ink-1); font-weight: 500; background: rgba(var(--c-fg-rgb), 0.08); display: inline-block; padding: 1px 6px; border-radius: 3px; margin-bottom: 4px; }
	.cd-row { display: flex; align-items: center; gap: 6px; margin-bottom: 2px; }
	.cd-label { font-size: 11.5px; color: var(--c-ink-1n); flex: 1 1 auto; min-width: 0; line-height: 1.3; }
	.cd-val { font-size: 12px; font-weight: 600; color: var(--c-ink-1); text-align: right; margin-left: auto; white-space: nowrap; flex: 0 0 auto; }
	.zone-dl { display: flex; gap: 3px; margin-top: 4px; }
	.cd-ref { font-size: 11px; color: var(--c-ink-3); text-align: right; margin: -1px 0 4px; }
	.cd-ref b { color: var(--c-ink-2, inherit); font-weight: 600; }
	.zone-note { font-size: 11.5px; line-height: 1.35; color: var(--c-ink-3); margin: 4px 0 0; }
	.zone-note.warn { color: var(--c-ink-1n); border-left: 2px solid rgba(var(--c-fg-rgb), 0.3); padding-left: 6px; }

	.hzc-dl-btn { background: rgba(var(--c-fg-rgb), 0.04); border: 1px solid rgba(var(--c-fg-rgb), 0.1); color: var(--c-ink-3); font-size: 11px; padding: 1px 4px; border-radius: 3px; cursor: pointer; font-family: inherit; transition: all 0.15s; }
	.hzc-dl-btn:hover { background: rgba(var(--c-accent-rgb), 0.15); border-color: rgba(var(--c-accent-rgb), 0.4); color: var(--c-accent); }

	.hzc-download-row { margin-top: 6px; }
	.hzc-download-btn { display: block; width: 100%; text-align: center; padding: 6px 10px; background: rgba(var(--c-accent-rgb), 0.15); border: 1px solid rgba(var(--c-accent-rgb), 0.3); border-radius: 4px; color: var(--c-accent); font-size: 11.5px; font-weight: 600; cursor: pointer; font-family: inherit; transition: all 0.15s; }
	.hzc-download-btn:hover { background: rgba(var(--c-accent-rgb), 0.25); border-color: rgba(var(--c-accent-rgb), 0.5); }
</style>
