<script lang="ts">
	import { i18n } from '$lib/stores/i18n.svelte';
	import EudrMap from '$lib/components/EudrMap.svelte';
	import { initDuckDB, query } from '$lib/stores/duckdb';
	import { getEudrHiresUrl } from '$lib/config';
	import { latLngToCell, polygonToCells } from 'h3-js';

	const EUDR_RES = 9;
	const DATA_VINTAGE = '2024-12-31';
	const HEX_AREA_HA = 10.5; // res-9 hexagon ≈ 0.105 km² = 10.5 ha
	const MAX_POLY_CELLS = 9500; // ~100k ha cap for interactive analysis

	let mapComponent: EudrMap;

	// Input state
	let latInput = $state('');
	let lonInput = $state('');
	let loading = $state(false);
	let error = $state('');

	// EUDR disclaimer gate — resets on every visit (intentional: EUDR has legal weight)
	let eudrDisclaimerAccepted = $state(false);

	// Result state
	interface EudrResult {
		id: string;
		lat: number;
		lon: number;
		h3_cell: string;
		province: string;
		forest_cover_2020_pct: number | null;
		forest_cover_current_pct: number | null;
		loss_post_2020_pct: number | null;
		fire_post_2020_pct: number | null;
		risk_score: number | null;
		risk_level: string;
		deforestation_post_2020: boolean;
		eudr_assessment: string;
		data_vintage?: string;
		data_sources?: string[];
	}

	let result: EudrResult | null = $state(null);

	// Polygon mode
	interface PolygonResult {
		cells_requested: number;
		cells_in_coverage: number;
		coverage_pct: number;
		area_ha: number;
		deforested_cells: number;
		deforested_pct: number;
		mean_risk: number;
		max_risk: number;
		mean_loss_pos: number;
		provinces: string[];
	}
	let polygonResult: PolygonResult | null = $state(null);
	let polygonError = $state('');
	let polygonLoading = $state(false);
	let polygonName = $state('');
	let drawing = $state(false);

	// Warm-only risk ramp — no green/blue (matches platform aesthetic on EUDR)
	function getRiskColor(level: string): string {
		switch (level) {
			case 'critical': return '#991b1b';
			case 'high': return '#ef4444';
			case 'medium': return '#f59e0b';
			case 'low': return '#fde047';
			default: return '#9ca3af';
		}
	}

	function getAssessmentColor(assessment: string): string {
		switch (assessment) {
			case 'DEFOREST_DETECTED': return '#ef4444';
			case 'HIGH_RISK': return '#f59e0b';
			case 'MEDIUM_RISK': return '#eab308';
			case 'LOW_RISK': return '#fde047';
			default: return '#9ca3af';
		}
	}

	function getAssessmentLabel(assessment: string): string {
		if (assessment === 'DEFOREST_DETECTED') return i18n.t('eudr.check.deforest_detected');
		const labels: Record<string, Record<string, string>> = {
			HIGH_RISK: { es: 'RIESGO ALTO', en: 'HIGH RISK' },
			MEDIUM_RISK: { es: 'RIESGO MEDIO', en: 'MEDIUM RISK' },
			LOW_RISK: { es: 'RIESGO BAJO', en: 'LOW RISK' },
			OUTSIDE_COVERAGE: { es: 'FUERA DE COBERTURA', en: 'OUTSIDE COVERAGE' },
		};
		return labels[assessment]?.[i18n.locale] || assessment;
	}

	function riskLevel(score: number | null): string {
		if (score === null) return 'unknown';
		if (score >= 75) return 'critical';
		if (score >= 50) return 'high';
		if (score >= 25) return 'medium';
		return 'low';
	}

	function assessment(deforested: boolean, score: number | null): string {
		if (deforested) return 'DEFOREST_DETECTED';
		if (score !== null && score >= 50) return 'HIGH_RISK';
		if (score !== null && score >= 25) return 'MEDIUM_RISK';
		return 'LOW_RISK';
	}

	async function checkCoordinates(lat: number, lon: number) {
		loading = true;
		error = '';
		result = null;
		polygonResult = null;
		mapComponent?.clearPolygon();

		const cell = latLngToCell(lat, lon, EUDR_RES);
		mapComponent?.showCell(cell);

		try {
			await initDuckDB();
			// h3 cells are alphanumeric — safe to interpolate. Range-read prunes to one row group.
			const sql = `SELECT * FROM read_parquet('${getEudrHiresUrl()}') WHERE h3index = '${cell}' LIMIT 1`;
			const table = await query(sql);
			const rows = table.toArray();

			if (rows.length === 0) {
				result = {
					id: 'manual', lat, lon, h3_cell: cell, province: '',
					forest_cover_2020_pct: null, forest_cover_current_pct: null,
					loss_post_2020_pct: null, fire_post_2020_pct: null,
					risk_score: null, risk_level: 'outside_coverage',
					deforestation_post_2020: false, eudr_assessment: 'OUTSIDE_COVERAGE',
					data_vintage: DATA_VINTAGE,
				};
				return;
			}

			const r: any = rows[0];
			const score = r.risk_score === null ? null : Number(r.risk_score);
			const deforested = Number(r.deforestation_post_2020) > 0;
			result = {
				id: 'manual', lat, lon, h3_cell: cell,
				province: String(r.province ?? ''),
				forest_cover_2020_pct: r.forest_cover_2020 === null ? null : Number(r.forest_cover_2020),
				forest_cover_current_pct: r.forest_cover_current === null ? null : Number(r.forest_cover_current),
				loss_post_2020_pct: r.loss_post_2020_pct === null ? null : Number(r.loss_post_2020_pct),
				fire_post_2020_pct: r.fire_post_2020_pct === null ? null : Number(r.fire_post_2020_pct),
				risk_score: score,
				risk_level: riskLevel(score),
				deforestation_post_2020: deforested,
				eudr_assessment: assessment(deforested, score),
				data_vintage: DATA_VINTAGE,
			};
		} catch (e: any) {
			error = e?.message || 'Error checking coordinates';
		} finally {
			loading = false;
		}
	}

	// Extract the first polygon's GeoJSON coordinates ([[ [lng,lat], ... ]]) from
	// a GeoJSON Feature / FeatureCollection / Geometry. Returns null if none.
	function extractPolygonCoords(geojson: any): number[][][] | null {
		let geom = geojson;
		if (geom?.type === 'FeatureCollection') geom = geom.features?.[0]?.geometry;
		else if (geom?.type === 'Feature') geom = geom.geometry;
		if (!geom) return null;
		if (geom.type === 'Polygon') return geom.coordinates;
		if (geom.type === 'MultiPolygon') return geom.coordinates?.[0] ?? null; // first polygon only (v1)
		return null;
	}

	async function handleGeoJsonUpload(event: Event) {
		const input = event.target as HTMLInputElement;
		const file = input.files?.[0];
		if (!file) return;
		polygonError = '';
		if (file.size > 1_000_000) {
			polygonError = i18n.t('eudr.check.poly_too_big_file');
			return;
		}
		try {
			const text = await file.text();
			const gj = JSON.parse(text);
			const coords = extractPolygonCoords(gj);
			if (!coords) {
				polygonError = i18n.t('eudr.check.poly_invalid');
				return;
			}
			polygonName = file.name;
			await checkPolygon(coords);
		} catch {
			polygonError = i18n.t('eudr.check.poly_invalid');
		} finally {
			input.value = ''; // allow re-uploading the same file
		}
	}

	async function checkPolygon(rings: number[][][]) {
		polygonLoading = true;
		polygonError = '';
		polygonResult = null;
		result = null; // clear point result when switching to polygon mode

		try {
			// isGeoJson=true → coords are [lng,lat] loops
			const cells: string[] = polygonToCells(rings, EUDR_RES, true);
			if (cells.length === 0) {
				polygonError = i18n.t('eudr.check.poly_no_cells');
				return;
			}
			if (cells.length > MAX_POLY_CELLS) {
				polygonError = i18n.t('eudr.check.poly_too_big_area');
				return;
			}

			await initDuckDB();
			const inList = cells.map((c) => `'${c}'`).join(',');
			const sql = `SELECT h3index, province, risk_score, loss_post_2020_pct, deforestation_post_2020
				FROM read_parquet('${getEudrHiresUrl()}') WHERE h3index IN (${inList})`;
			const rows: any[] = (await query(sql)).toArray();

			const inCov = rows.length;
			const deforested = rows.filter((r) => Number(r.deforestation_post_2020) > 0);
			const risks = rows.map((r) => Number(r.risk_score));
			const lossPos = deforested.map((r) => Number(r.loss_post_2020_pct));
			const provinces = [...new Set(rows.map((r) => String(r.province)).filter(Boolean))];

			polygonResult = {
				cells_requested: cells.length,
				cells_in_coverage: inCov,
				coverage_pct: cells.length ? (inCov / cells.length) * 100 : 0,
				area_ha: inCov * HEX_AREA_HA,
				deforested_cells: deforested.length,
				deforested_pct: inCov ? (deforested.length / inCov) * 100 : 0,
				mean_risk: inCov ? risks.reduce((a, b) => a + b, 0) / inCov : 0,
				max_risk: inCov ? Math.max(...risks) : 0,
				mean_loss_pos: lossPos.length ? lossPos.reduce((a, b) => a + b, 0) / lossPos.length : 0,
				provinces,
			};

			mapComponent?.showPolygon(rings);
			mapComponent?.showCells(rows.map((r) => ({ h3index: String(r.h3index), risk: Number(r.risk_score) })));
		} catch (e: any) {
			polygonError = e?.message || 'Error procesando el polígono';
		} finally {
			polygonLoading = false;
		}
	}

	function handleSubmit() {
		const lat = parseFloat(latInput);
		const lon = parseFloat(lonInput);

		if (isNaN(lat) || isNaN(lon)) {
			error = i18n.t('eudr.check.error_invalid');
			return;
		}

		if (lat < -35 || lat > -21 || lon < -70 || lon > -53) {
			error = i18n.t('eudr.check.error_bounds');
			return;
		}

		mapComponent?.setMarker(lat, lon);
		mapComponent?.flyTo(lat, lon, 9);
		checkCoordinates(lat, lon);
	}

	function handleMapClick(lat: number, lon: number, h3index: string) {
		latInput = lat.toFixed(6);
		lonInput = lon.toFixed(6);
		checkCoordinates(lat, lon);
	}

	function handlePaste() {
		// Support pasting "lat, lon" format
		setTimeout(() => {
			const val = latInput.trim();
			if (val.includes(',')) {
				const parts = val.split(',').map(s => s.trim());
				if (parts.length === 2 && !isNaN(parseFloat(parts[0])) && !isNaN(parseFloat(parts[1]))) {
					latInput = parts[0];
					lonInput = parts[1];
				}
			}
		}, 50);
	}

	function tryExample() {
		latInput = '-27.36';
		lonInput = '-55.90';
		handleSubmit();
	}

	function clearAll() {
		latInput = '';
		lonInput = '';
		error = '';
		result = null;
		polygonResult = null;
		polygonError = '';
		polygonName = '';
		mapComponent?.clearMarker();
		mapComponent?.clearCell();
		mapComponent?.clearPolygon();
		if (drawing) mapComponent?.cancelDraw();
	}

	function fmt(v: number | null, decimals = 1): string {
		if (v === null || v === undefined) return '--';
		return v.toFixed(decimals);
	}
</script>

<svelte:head>
	<title>EUDR Check &mdash; nealab</title>
	<meta name="description" content={i18n.t('eudr.check.empty_desc')} />
	<meta property="og:title" content="EUDR Check — nealab" />
	<meta property="og:description" content={i18n.t('eudr.check.empty_desc')} />
	<meta property="og:image" content="https://spatia.ar/og-image.png" />
	<meta property="og:url" content="https://spatia.ar/eudr/check" />
	<meta property="og:type" content="website" />
</svelte:head>

{#if !eudrDisclaimerAccepted}
<div class="eudr-gate">
	<div class="eudr-gate-card">
		<div class="eudr-gate-kicker">EUDR Check · nealab</div>
		<h1 class="eudr-gate-title">Aviso importante antes de usar esta herramienta</h1>
		<ul class="eudr-gate-points">
			<li>
				<span class="eudr-gate-label">Qué hace esta herramienta.</span>
				Análisis satelital de pérdida forestal post-2020 (Hansen GFC v1.12 + MODIS de área quemada)
				sobre hexágonos de ~0,1 km² (H3 res-9). Cobertura: provincias del NEA argentino,
				departamentos paraguayos y estados del sur de Brasil. Sirve para screening de riesgo
				EUDR, due-diligence preliminar y soporte técnico de informes.
			</li>
			<li>
				<span class="eudr-gate-label">Alcance regulatorio.</span>
				El resultado es un análisis técnico, no una certificación formal bajo el Reglamento
				(UE) 2023/1115. La certificación regulatoria requiere además geometría parcelaria
				oficial, trazabilidad documental y due-diligence profesional independiente.
			</li>
			<li>
				<span class="eudr-gate-label">Resolución del dato.</span>
				El dato satelital subyacente está a 100 m. El resultado refleja el hexágono evaluado,
				no la parcela exacta — usalo como señal espacial robusta, no como medición catastral.
			</li>
			<li>
				<span class="eudr-gate-label">Responsabilidad.</span>
				Al continuar, aceptás que nealab, su autor, CONICET y UNaM no asumen responsabilidad
				por decisiones comerciales o regulatorias basadas exclusivamente en este análisis.
			</li>
		</ul>
		<button class="eudr-gate-btn" onclick={() => eudrDisclaimerAccepted = true}>
			Entendido — continuar con el análisis
		</button>
		<a class="eudr-gate-link" href="/terminos">Ver términos y condiciones completos →</a>
	</div>
</div>
{:else}
<div class="relative h-[calc(100vh-72px)]" style="min-height: 500px;">
	<!-- Map full-bleed -->
	<div class="absolute inset-0">
		<EudrMap bind:this={mapComponent} onCellClick={handleMapClick}
			onPolygonDrawn={(rings) => checkPolygon(rings)}
			onDrawModeChange={(active) => drawing = active} />
	</div>
	<div class="absolute bottom-3 left-3 bg-black/70 backdrop-blur-sm px-3 py-1.5 rounded text-[10px] text-white/50 z-10 pointer-events-none">
		{i18n.t('eudr.check.click_map')}
	</div>

	<!-- Floating panel: input + results -->
	<div class="absolute top-3 right-3 w-[380px] max-w-[calc(100vw-1.5rem)] max-h-[calc(100vh-104px)] flex flex-col gap-3 overflow-y-auto z-20">
			<!-- Coordinate Input -->
			<div class="border border-border rounded-lg p-4 bg-black/75 backdrop-blur-md">
				<h3 class="text-sm font-bold text-white mb-3">{i18n.t('eudr.check.input_title')}</h3>
				<div class="grid grid-cols-2 gap-2 mb-3">
					<div>
						<label class="text-[10px] text-white/40 uppercase">Lat</label>
						<input type="text" inputmode="decimal" bind:value={latInput} onpaste={handlePaste}
							placeholder="-27.5"
							class="w-full bg-white/5 border border-border rounded px-3 py-2 text-[13px] text-white placeholder-white/20 focus:outline-none focus:border-yellow-400/60" />
					</div>
					<div>
						<label class="text-[10px] text-white/40 uppercase">Lon</label>
						<input type="text" inputmode="decimal" bind:value={lonInput}
							placeholder="-60.5"
							class="w-full bg-white/5 border border-border rounded px-3 py-2 text-[13px] text-white placeholder-white/20 focus:outline-none focus:border-yellow-400/60" />
					</div>
				</div>
				<button onclick={handleSubmit} disabled={loading}
					class="w-full py-2 bg-yellow-400 text-black font-bold text-[13px] rounded-lg hover:bg-yellow-400/85 transition-colors disabled:opacity-50 cursor-pointer disabled:cursor-not-allowed flex items-center justify-center gap-2">
					{#if loading}
						<svg class="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none">
							<circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="3" stroke-linecap="round" class="opacity-25"></circle>
							<path d="M4 12a8 8 0 018-8" stroke="currentColor" stroke-width="3" stroke-linecap="round"></path>
						</svg>
					{/if}
					{loading ? i18n.t('eudr.check.checking') : i18n.t('eudr.check.check_btn')}
				</button>
				<div class="mt-1 flex items-center justify-between text-[11px]">
					<button onclick={tryExample} class="text-white/30 hover:text-white/70 transition-colors py-1 bg-transparent border-0 cursor-pointer">
						{i18n.t('eudr.check.try_example')} →
					</button>
					<button onclick={clearAll} class="text-white/30 hover:text-yellow-400 transition-colors py-1 bg-transparent border-0 cursor-pointer">
						{i18n.t('eudr.check.clear_all')}
					</button>
				</div>

				{#if error}
					<p class="mt-2 text-[12px] text-red-400">{error}</p>
				{/if}

				<!-- Polygon upload -->
				<div class="mt-3 pt-3 border-t border-border">
					<div class="text-[10px] text-white/40 uppercase mb-2">{i18n.t('eudr.check.poly_title')}</div>
					{#if drawing}
						<div class="px-3 py-2 rounded bg-yellow-400/10 border border-yellow-400/30 text-[11px] text-white/80 leading-relaxed mb-2">
							{i18n.t('eudr.check.poly_draw_hint')}
							<button onclick={() => mapComponent?.cancelLasso()} class="mt-1 block text-yellow-400 hover:text-white underline cursor-pointer bg-transparent border-0 p-0 text-[11px]">
								{i18n.t('eudr.check.poly_draw_cancel')}
							</button>
						</div>
					{:else}
						<button onclick={() => mapComponent?.setLassoMode(true)}
							class="w-full mb-2 py-2 border border-white/20 rounded-lg text-[12px] text-white/60 hover:border-white/40 hover:text-white transition-colors cursor-pointer bg-transparent">
							{i18n.t('eudr.check.poly_draw')}
						</button>
					{/if}
					<label class="block w-full text-center py-2 border border-dashed border-white/20 rounded-lg text-[12px] text-white/60 hover:border-white/40 hover:text-white transition-colors cursor-pointer">
						{polygonLoading ? i18n.t('eudr.check.checking') : i18n.t('eudr.check.poly_upload')}
						<input type="file" accept=".geojson,.json,application/geo+json,application/json"
							onchange={handleGeoJsonUpload} disabled={polygonLoading} class="hidden" />
					</label>
					{#if polygonName && polygonResult}
						<p class="mt-1 text-[10px] text-white/30 truncate">{polygonName}</p>
					{/if}
					{#if polygonError}
						<p class="mt-2 text-[12px] text-red-400">{polygonError}</p>
					{/if}
				</div>
			</div>

			<!-- Loading skeleton -->
			{#if loading && !result}
				<div class="border border-border rounded-lg p-4 bg-black/75 backdrop-blur-md flex-1 space-y-3">
					<div class="h-4 w-24 bg-white/5 rounded animate-pulse"></div>
					<div class="h-8 w-full bg-white/5 rounded animate-pulse"></div>
					<div class="grid grid-cols-2 gap-3">
						<div class="h-16 bg-white/5 rounded animate-pulse"></div>
						<div class="h-16 bg-white/5 rounded animate-pulse"></div>
						<div class="h-16 bg-white/5 rounded animate-pulse"></div>
						<div class="h-16 bg-white/5 rounded animate-pulse"></div>
					</div>
					<div class="space-y-2 mt-2">
						<div class="h-3 w-full bg-white/5 rounded animate-pulse"></div>
						<div class="h-3 w-3/4 bg-white/5 rounded animate-pulse"></div>
					</div>
				</div>
			{/if}

			<!-- Empty state -->
			{#if !result && !polygonResult && !loading && !polygonLoading}
				<div class="border border-border/50 rounded-lg p-6 bg-black/70 backdrop-blur-md flex-1 flex flex-col items-center justify-center text-center gap-3">
					<svg class="w-10 h-10 text-white/20" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
						<path stroke-linecap="round" stroke-linejoin="round" d="M15 10.5a3 3 0 11-6 0 3 3 0 016 0z" />
						<path stroke-linecap="round" stroke-linejoin="round" d="M19.5 10.5c0 7.142-7.5 11.25-7.5 11.25S4.5 17.642 4.5 10.5a7.5 7.5 0 1115 0z" />
					</svg>
					<h3 class="text-sm font-bold text-white/50">{i18n.t('eudr.check.empty_title')}</h3>
					<p class="text-[12px] text-white/30 max-w-[260px]">{i18n.t('eudr.check.empty_desc')}</p>
					<button onclick={tryExample}
						class="mt-2 px-4 py-1.5 border border-white/20 text-white/60 text-[12px] font-semibold rounded-lg hover:border-white/40 hover:text-white transition-colors cursor-pointer">
						{i18n.t('eudr.check.try_example')}
					</button>
				</div>
			{/if}

			<!-- Results -->
			{#if result}
				<div class="border border-border rounded-lg p-4 bg-black/75 backdrop-blur-md flex-1">
					<!-- Assessment Badge -->
					<div class="flex items-center justify-between mb-4">
						<h3 class="text-sm font-bold text-white">{i18n.t('eudr.check.result_title')}</h3>
						<span class="text-[11px] font-bold uppercase tracking-wider px-3 py-1 rounded-full"
							style="background: {getAssessmentColor(result.eudr_assessment)}20; color: {getAssessmentColor(result.eudr_assessment)};">
							{getAssessmentLabel(result.eudr_assessment)}
						</span>
					</div>

					<!-- Aggregation-unit honesty note -->
					{#if result.eudr_assessment !== 'OUTSIDE_COVERAGE'}
						<div class="mb-4 px-3 py-2 rounded bg-amber-500/10 border border-amber-500/20 text-[10px] text-amber-200/70 leading-relaxed">
							{i18n.t('eudr.check.area_note')}
						</div>
					{/if}

					<!-- Risk Score Gauge -->
					{#if result.risk_score !== null}
						<div class="mb-4">
							<div class="flex items-center justify-between text-[11px] text-white/50 mb-1">
								<span>{i18n.t('eudr.check.risk_score')}</span>
								<span class="text-lg font-bold" style="color: {getRiskColor(result.risk_level)};">
									{fmt(result.risk_score, 0)}
								</span>
							</div>
							<div class="w-full h-2 bg-white/5 rounded-full overflow-hidden">
								<div class="h-full rounded-full transition-all"
									style="width: {result.risk_score}%; background: {getRiskColor(result.risk_level)};"></div>
							</div>
							<div class="flex justify-between text-[9px] text-white/30 mt-0.5">
								<span>0</span><span>25</span><span>50</span><span>75</span><span>100</span>
							</div>
						</div>
					{/if}

					<!-- Metrics Grid -->
					<div class="grid grid-cols-2 gap-3 mb-4">
						<div class="bg-white/[0.03] rounded p-3">
							<div class="text-[10px] text-white/40 uppercase mb-1">{i18n.t('eudr.check.forest_2020')}</div>
							<div class="text-lg font-bold text-white">{fmt(result.forest_cover_2020_pct)}%</div>
						</div>
						<div class="bg-white/[0.03] rounded p-3">
							<div class="text-[10px] text-white/40 uppercase mb-1">{i18n.t('eudr.check.forest_current')}</div>
							<div class="text-lg font-bold text-white">{fmt(result.forest_cover_current_pct)}%</div>
						</div>
						<div class="bg-white/[0.03] rounded p-3">
							<div class="text-[10px] text-white/40 uppercase mb-1">{i18n.t('eudr.check.loss_post_2020')}</div>
							<div class="text-lg font-bold" style="color: {(result.loss_post_2020_pct ?? 0) > 0 ? '#ef4444' : '#9ca3af'};">
								{fmt(result.loss_post_2020_pct)}%
							</div>
						</div>
						<div class="bg-white/[0.03] rounded p-3">
							<div class="text-[10px] text-white/40 uppercase mb-1">{i18n.t('eudr.check.fire_post_2020')}</div>
							<div class="text-lg font-bold" style="color: {(result.fire_post_2020_pct ?? 0) > 0 ? '#f59e0b' : '#9ca3af'};">
								{fmt(result.fire_post_2020_pct)}%
							</div>
						</div>
					</div>

					<!-- Details -->
					<div class="space-y-2 text-[11px] text-white/50">
						<div class="flex justify-between">
							<span>H3 cell</span>
							<span class="font-mono text-white/70">{result.h3_cell}</span>
						</div>
						<div class="flex justify-between">
							<span>{i18n.t('eudr.check.province')}</span>
							<span class="text-white/70">{result.province || '--'}</span>
						</div>
						<div class="flex justify-between">
							<span>{i18n.t('eudr.check.coordinates')}</span>
							<span class="font-mono text-white/70">{result.lat.toFixed(4)}, {result.lon.toFixed(4)}</span>
						</div>
					</div>

					<!-- Data vintage -->
					{#if result.data_vintage}
						<div class="mt-3 flex justify-between text-[11px] text-white/50">
							<span>{i18n.t('eudr.check.vintage')}</span>
							<span class="text-white/70">Hansen GFC · {result.data_vintage}</span>
						</div>
					{/if}

					<!-- Disclaimer -->
					<div class="mt-4 pt-3 border-t border-border text-[10px] text-white/25 leading-relaxed">
						{i18n.t('eudr.disclaimer_short')}
					</div>
					<a href="/metodologia/eudr" class="mt-2 inline-block text-[11px] text-yellow-400 hover:text-white underline transition-colors">
						{i18n.t('eudr.check.methodology_link')}
					</a>
				</div>
			{/if}

			<!-- Polygon results -->
			{#if polygonResult}
				<div class="border border-border rounded-lg p-4 bg-black/75 backdrop-blur-md flex-1">
					<div class="flex items-center justify-between mb-4">
						<h3 class="text-sm font-bold text-white">{i18n.t('eudr.check.poly_result_title')}</h3>
						<span class="text-[11px] font-bold uppercase tracking-wider px-3 py-1 rounded-full"
							style="background: {getRiskColor(polygonResult.deforested_pct > 0 ? 'high' : 'low')}20; color: {getRiskColor(polygonResult.deforested_pct > 0 ? 'high' : 'low')};">
							{polygonResult.deforested_pct > 0 ? i18n.t('eudr.check.deforest_detected') : getAssessmentLabel('LOW_RISK')}
						</span>
					</div>

					{#if polygonResult.coverage_pct < 99.5}
						<div class="mb-3 px-3 py-2 rounded bg-amber-500/10 border border-amber-500/20 text-[10px] text-amber-200/80 leading-relaxed">
							{i18n.t('eudr.check.poly_coverage_warn').replace('{pct}', polygonResult.coverage_pct.toFixed(0))}
						</div>
					{/if}

					<div class="grid grid-cols-2 gap-3 mb-4">
						<div class="bg-white/[0.03] rounded p-3">
							<div class="text-[10px] text-white/40 uppercase mb-1">{i18n.t('eudr.check.poly_area')}</div>
							<div class="text-lg font-bold text-white">{polygonResult.area_ha.toLocaleString(undefined, { maximumFractionDigits: 0 })} ha</div>
						</div>
						<div class="bg-white/[0.03] rounded p-3">
							<div class="text-[10px] text-white/40 uppercase mb-1">{i18n.t('eudr.check.poly_deforested')}</div>
							<div class="text-lg font-bold" style="color: {polygonResult.deforested_pct > 0 ? '#ef4444' : '#9ca3af'};">
								{polygonResult.deforested_pct.toFixed(1)}%
							</div>
						</div>
						<div class="bg-white/[0.03] rounded p-3">
							<div class="text-[10px] text-white/40 uppercase mb-1">{i18n.t('eudr.check.poly_max_risk')}</div>
							<div class="text-lg font-bold text-white">{polygonResult.max_risk.toFixed(0)}</div>
						</div>
						<div class="bg-white/[0.03] rounded p-3">
							<div class="text-[10px] text-white/40 uppercase mb-1">{i18n.t('eudr.check.poly_mean_risk')}</div>
							<div class="text-lg font-bold text-white">{polygonResult.mean_risk.toFixed(1)}</div>
						</div>
					</div>

					<div class="space-y-2 text-[11px] text-white/50">
						<div class="flex justify-between">
							<span>{i18n.t('eudr.check.poly_cells')}</span>
							<span class="text-white/70">{polygonResult.cells_in_coverage.toLocaleString()} / {polygonResult.cells_requested.toLocaleString()}</span>
						</div>
						<div class="flex justify-between">
							<span>{i18n.t('eudr.check.poly_deforested_cells')}</span>
							<span class="text-white/70">{polygonResult.deforested_cells.toLocaleString()}</span>
						</div>
						<div class="flex justify-between">
							<span>{i18n.t('eudr.check.province')}</span>
							<span class="text-white/70">{polygonResult.provinces.join(', ') || '--'}</span>
						</div>
					</div>

					<div class="mt-4 pt-3 border-t border-border text-[10px] text-white/25 leading-relaxed">
						{i18n.t('eudr.disclaimer_short')}
					</div>
					<a href="/metodologia/eudr" class="mt-2 inline-block text-[11px] text-yellow-400 hover:text-white underline transition-colors">
						{i18n.t('eudr.check.methodology_link')}
					</a>
				</div>
			{/if}
	</div>
</div>
{/if}

<style>
	.eudr-gate {
		position: fixed;
		inset: 0;
		z-index: 50;
		display: flex;
		align-items: flex-start;
		justify-content: center;
		overflow-y: auto;
		padding: 32px 24px 64px;
		background: var(--color-bg);
	}

	.eudr-gate-card {
		max-width: 560px;
		width: 100%;
		max-height: calc(100dvh - 64px);
		overflow-y: auto;
		box-sizing: border-box;
		border: 1px solid rgba(255, 255, 255, 0.2);
		background: rgba(255, 255, 255, 0.02);
		padding: 36px 32px 32px;
		font-family: 'JetBrains Mono', ui-monospace, SFMono-Regular, Menlo, monospace;
	}

	.eudr-gate-kicker {
		font-size: 10px;
		color: rgba(255, 255, 255, 0.35);
		text-transform: uppercase;
		letter-spacing: 0.12em;
		margin-bottom: 14px;
	}

	.eudr-gate-title {
		font-size: 18px;
		font-weight: 700;
		color: #ffffff;
		margin: 0 0 20px;
		line-height: 1.3;
	}

	.eudr-gate-points {
		list-style: none;
		padding: 0;
		margin: 0 0 28px;
		display: flex;
		flex-direction: column;
		gap: 12px;
	}

	.eudr-gate-points li {
		font-size: 12px;
		color: rgba(255, 255, 255, 0.7);
		line-height: 1.55;
		padding-left: 14px;
		position: relative;
	}

	.eudr-gate-points li::before {
		content: '—';
		position: absolute;
		left: 0;
		color: rgba(255, 255, 255, 0.25);
	}

	.eudr-gate-label {
		color: #ffffff;
		font-weight: 700;
	}

	.eudr-gate-btn {
		display: block;
		width: 100%;
		background: #ffffff;
		color: #000000;
		border: none;
		padding: 12px 20px;
		font-family: inherit;
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.04em;
		cursor: pointer;
		text-align: center;
		transition: opacity 0.15s;
		margin-bottom: 12px;
	}

	.eudr-gate-btn:hover { opacity: 0.85; }

	.eudr-gate-link {
		display: block;
		text-align: center;
		font-size: 11px;
		color: rgba(255, 255, 255, 0.4);
		text-decoration: underline;
		text-decoration-thickness: 1px;
		text-underline-offset: 3px;
		transition: color 0.15s;
	}

	.eudr-gate-link:hover { color: rgba(255, 255, 255, 0.7); }
</style>
