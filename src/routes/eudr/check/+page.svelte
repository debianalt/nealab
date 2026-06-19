<script lang="ts">
	import { i18n } from '$lib/stores/i18n.svelte';
	import EudrMap from '$lib/components/EudrMap.svelte';
	import { initDuckDB, query } from '$lib/stores/duckdb';
	import { getEudrHiresUrl, getEudrPlantationUrl } from '$lib/config';
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
		loss_by_year?: Record<number, number>; // {2021: 0.3, 2022: 1.2, ...} %
		plantation_pct?: number | null;      // MapBiomas class 9, current (~2023) — null = sin dato (PY/BR)
		native_forest_pct?: number | null;   // MapBiomas native forest, current
		plantation_2020_pct?: number | null; // MapBiomas class 9 at the 2020 EUDR cutoff
		native_2020_pct?: number | null;     // MapBiomas native forest at the 2020 cutoff
	}

	// Plantation context for a hex: distinguishes Hansen "loss" over a forestry
	// plantation (harvest cycle, NOT deforestation) from loss over native forest.
	// The 2020 baseline (MapBiomas at the EUDR cutoff) is the defensible determinant:
	//  - plantation in 2020 + loss        → harvest of a pre-cutoff plantation = compliant
	//  - native in 2020 + loss + now plant → native→plantation conversion post-2020 = NON-compliant
	//  - native in 2020 + loss            → native-forest deforestation
	// Falls back to current-state when the 2020 baseline is absent (e.g. PY/BR = no data).
	const PLANTATION_THRESHOLD = 40; // % of hex that must be plantation to count as forestry
	type PlantCtx = { kind: 'nodata' | 'managed_confirmed' | 'conversion' | 'native_loss' | 'managed' | 'plantation' | 'native'; plantation: number };
	function plantationContext(
		loss: number | null,
		plantNow: number | null | undefined,
		plant2020: number | null | undefined = null,
		native2020: number | null | undefined = null,
	): PlantCtx {
		const T = PLANTATION_THRESHOLD;
		const hasLoss = (loss ?? 0) > 0;
		const pn = plantNow ?? null;
		const show = pn ?? plant2020 ?? 0;
		if (pn === null && (plant2020 === null || plant2020 === undefined)) return { kind: 'nodata', plantation: 0 };
		// Defensible path: 2020 baseline present
		if (plant2020 !== null && plant2020 !== undefined) {
			if (plant2020 >= T) return { kind: hasLoss ? 'managed_confirmed' : 'plantation', plantation: show };
			if (native2020 != null && native2020 >= T && hasLoss)
				return { kind: (pn ?? 0) >= T ? 'conversion' : 'native_loss', plantation: show };
		}
		// Current-state fallback
		if (hasLoss && (pn ?? 0) >= T) return { kind: 'managed', plantation: show };
		if ((pn ?? 0) >= T) return { kind: 'plantation', plantation: show };
		return { kind: 'native', plantation: show };
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
		loss_by_year: Record<number, number>;
		plantation_data_cells: number;   // cells with MapBiomas plantation coverage (AR)
		plantation_cells: number;        // cells that are forestry plantation (≥ threshold, current)
		deforested_harvest: number;      // deforested cells = plantation harvest cycle → compliant
		deforested_conversion: number;   // deforested cells = native→plantation post-2020 → NON-compliant
		deforested_native: number;       // deforested cells = native-forest loss → possible deforestation
	}
	let polygonResult: PolygonResult | null = $state(null);
	let polygonError = $state('');
	let polygonLoading = $state(false);
	let polygonName = $state('');
	let drawing = $state(false);

	// Batch CSV state
	let batchLoading = $state(false);
	let batchError = $state('');
	let batchResult: { processed: number; outside: number; csvUrl: string } | null = $state(null);

	// Last analyzed polygon (rings) — used to hash the input for the report
	let lastPolygonRings: number[][][] | null = null;

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

	function yearBarColor(v: number): string {
		if (v <= 0) return '#3f3f46';
		if (v < 1) return '#fde047';
		if (v < 5) return '#f59e0b';
		if (v < 15) return '#f97316';
		return '#ef4444';
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
			// LEFT JOIN the plantation layer (AR-only) so loss over a forestry plantation
			// isn't mislabelled as native deforestation; plantation_pct is null where absent.
			const sql = `SELECT e.*, p.plantation_pct, p.native_forest_pct, p.plantation_2020_pct, p.native_2020_pct
				FROM read_parquet('${getEudrHiresUrl()}') e
				LEFT JOIN read_parquet('${getEudrPlantationUrl()}') p USING (h3index)
				WHERE e.h3index = '${cell}' LIMIT 1`;
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
			const byYear: Record<number, number> = {};
			for (const y of [2021, 2022, 2023, 2024]) {
				const v = r[`loss_${y}_pct`];
				byYear[y] = v == null ? 0 : Number(v);
			}
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
				loss_by_year: byYear,
				plantation_pct: r.plantation_pct == null ? null : Number(r.plantation_pct),
				native_forest_pct: r.native_forest_pct == null ? null : Number(r.native_forest_pct),
				plantation_2020_pct: r.plantation_2020_pct == null ? null : Number(r.plantation_2020_pct),
				native_2020_pct: r.native_2020_pct == null ? null : Number(r.native_2020_pct),
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
		lastPolygonRings = rings;

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
			const sql = `SELECT e.h3index, e.province, e.risk_score, e.loss_post_2020_pct, e.deforestation_post_2020,
				e.loss_2021_pct, e.loss_2022_pct, e.loss_2023_pct, e.loss_2024_pct,
				p.plantation_pct, p.plantation_2020_pct, p.native_2020_pct
				FROM read_parquet('${getEudrHiresUrl()}') e
				LEFT JOIN read_parquet('${getEudrPlantationUrl()}') p USING (h3index)
				WHERE e.h3index IN (${inList})`;
			const rows: any[] = (await query(sql)).toArray();

			const num = (v: any) => (v == null ? null : Number(v));
			const inCov = rows.length;
			const deforested = rows.filter((r) => Number(r.deforestation_post_2020) > 0);
			const PT = PLANTATION_THRESHOLD;
			const plantationDataCells = rows.filter((r) => r.plantation_pct != null).length;
			const plantationCells = rows.filter((r) => Number(r.plantation_pct) >= PT).length;
			// Classify each deforested cell via the 2020-baseline plantation context.
			const defKinds = deforested.map((r) => plantationContext(
				num(r.loss_post_2020_pct), num(r.plantation_pct), num(r.plantation_2020_pct), num(r.native_2020_pct)).kind);
			const defHarvest = defKinds.filter((k) => k === 'managed_confirmed' || k === 'managed').length;
			const defConversion = defKinds.filter((k) => k === 'conversion').length;
			const defNative = defKinds.filter((k) => k === 'native_loss' || k === 'native').length;
			const risks = rows.map((r) => Number(r.risk_score));
			const lossPos = deforested.map((r) => Number(r.loss_post_2020_pct));
			const provinces = [...new Set(rows.map((r) => String(r.province)).filter(Boolean))];
			const lossByYear: Record<number, number> = {};
			for (const y of [2021, 2022, 2023, 2024]) {
				const sum = rows.reduce((s, r) => s + (Number(r[`loss_${y}_pct`]) || 0), 0);
				lossByYear[y] = inCov ? sum / inCov : 0;
			}

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
				loss_by_year: lossByYear,
				plantation_data_cells: plantationDataCells,
				plantation_cells: plantationCells,
				deforested_harvest: defHarvest,
				deforested_conversion: defConversion,
				deforested_native: defNative,
			};

			mapComponent?.showPolygon(rings);
			mapComponent?.showCells(rows.map((r) => ({ h3index: String(r.h3index), risk: Number(r.risk_score) })));
		} catch (e: any) {
			polygonError = e?.message || 'Error procesando el polígono';
		} finally {
			polygonLoading = false;
		}
	}

	async function handleBatchUpload(event: Event) {
		const input = event.target as HTMLInputElement;
		const file = input.files?.[0];
		if (!file) return;
		batchError = '';
		batchResult = null;
		if (file.size > 5_000_000) {
			batchError = i18n.t('eudr.check.batch_err_size');
			input.value = '';
			return;
		}
		batchLoading = true;
		try {
			const text = await file.text();
			const lines = text.split(/\r?\n/).filter((l) => l.trim());
			if (lines.length < 2) { batchError = i18n.t('eudr.check.batch_err_empty'); return; }
			const headers = lines[0].toLowerCase().split(/[,;]/).map((h) => h.trim());
			const idCol = headers.findIndex((h) => h === 'id' || h === 'name' || h === 'plot');
			const latCol = headers.findIndex((h) => h === 'lat' || h === 'latitude' || h === 'latitud');
			const lonCol = headers.findIndex((h) => h === 'lon' || h === 'lng' || h === 'longitude' || h === 'longitud');
			if (latCol < 0 || lonCol < 0) { batchError = i18n.t('eudr.check.batch_err_cols'); return; }

			const rows: { id: string; lat: number; lon: number; cell: string }[] = [];
			for (let i = 1; i < lines.length; i++) {
				const cols = lines[i].split(/[,;]/);
				const lat = parseFloat(cols[latCol]);
				const lon = parseFloat(cols[lonCol]);
				if (!isFinite(lat) || !isFinite(lon)) continue;
				const id = idCol >= 0 ? cols[idCol].trim().replace(/^"|"$/g, '') : `row_${i}`;
				rows.push({ id, lat, lon, cell: latLngToCell(lat, lon, EUDR_RES) });
			}
			if (rows.length === 0) { batchError = i18n.t('eudr.check.batch_err_empty'); return; }
			if (rows.length > 10000) { batchError = i18n.t('eudr.check.batch_err_too_many'); return; }

			await initDuckDB();
			const uniqueCells = [...new Set(rows.map((r) => r.cell))];
			const inList = uniqueCells.map((c) => `'${c}'`).join(',');
			const sql = `SELECT e.h3index, e.province, e.forest_cover_2020, e.forest_cover_current,
				e.loss_post_2020_pct, e.fire_post_2020_pct, e.risk_score, e.deforestation_post_2020,
				e.loss_2021_pct, e.loss_2022_pct, e.loss_2023_pct, e.loss_2024_pct, p.plantation_pct, p.plantation_2020_pct, p.native_2020_pct
				FROM read_parquet('${getEudrHiresUrl()}') e
				LEFT JOIN read_parquet('${getEudrPlantationUrl()}') p USING (h3index)
				WHERE e.h3index IN (${inList})`;
			const data = (await query(sql)).toArray();
			const byCell = new Map<string, any>();
			for (const d of data) byCell.set(String(d.h3index), d);

			const outHeaders = [
				'id', 'lat', 'lon', 'h3_cell', 'in_coverage', 'province',
				'forest_cover_2020', 'forest_cover_current',
				'loss_post_2020_pct', 'fire_post_2020_pct', 'risk_score',
				'deforestation_post_2020', 'eudr_assessment',
				'loss_2021_pct', 'loss_2022_pct', 'loss_2023_pct', 'loss_2024_pct',
				'plantation_pct', 'loss_context',
			];
			const csv = [outHeaders.join(',')];
			let outside = 0;
			const esc = (v: any) => {
				const s = v == null ? '' : String(v);
				return s.includes(',') || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
			};
			for (const r of rows) {
				const d = byCell.get(r.cell);
				if (!d) {
					outside++;
					csv.push([r.id, r.lat, r.lon, r.cell, 'no', '', '', '', '', '', '', '', 'OUTSIDE_COVERAGE', '', '', '', '', '', ''].map(esc).join(','));
					continue;
				}
				const score = d.risk_score === null ? null : Number(d.risk_score);
				const def = Number(d.deforestation_post_2020) > 0;
				const plant = d.plantation_pct == null ? null : Number(d.plantation_pct);
				let lossCtx = '';
				if (def) {
					const k = plantationContext(
						d.loss_post_2020_pct == null ? null : Number(d.loss_post_2020_pct), plant,
						d.plantation_2020_pct == null ? null : Number(d.plantation_2020_pct),
						d.native_2020_pct == null ? null : Number(d.native_2020_pct)).kind;
					lossCtx = k === 'managed_confirmed' || k === 'managed' ? 'plantation_harvest'
						: k === 'conversion' ? 'native_to_plantation_conversion'
						: k === 'nodata' ? 'no_plantation_data'
						: 'native_deforestation';
				}
				csv.push([
					r.id, r.lat, r.lon, r.cell, 'yes',
					d.province ?? '', d.forest_cover_2020 ?? '', d.forest_cover_current ?? '',
					d.loss_post_2020_pct ?? '', d.fire_post_2020_pct ?? '', score ?? '',
					def ? 1 : 0, assessment(def, score),
					d.loss_2021_pct ?? '', d.loss_2022_pct ?? '', d.loss_2023_pct ?? '', d.loss_2024_pct ?? '',
					plant ?? '', lossCtx,
				].map(esc).join(','));
			}
			const blob = new Blob([csv.join('\n')], { type: 'text/csv;charset=utf-8' });
			if (batchResult?.csvUrl) URL.revokeObjectURL(batchResult.csvUrl);
			batchResult = { processed: rows.length, outside, csvUrl: URL.createObjectURL(blob) };
		} catch (e: any) {
			batchError = e?.message || 'Error procesando CSV';
		} finally {
			batchLoading = false;
			input.value = '';
		}
	}

	// Report request flow (Alternativa A: lead-gen + entrega humana del informe
	// firmado). No PDF self-service público con branding CONICET hasta que el
	// STAN esté aprobado. El formulario arma un mailto a nealab@spatia.ar con
	// los datos del análisis; el usuario lo envía con su cliente de mail y, en
	// paralelo, se descarga el polígono en GeoJSON para que pueda adjuntarlo.
	let reportModalOpen = $state(false);
	let reqName = $state('');
	let reqEmail = $state('');
	let reqCompany = $state('');
	let reqPurpose = $state('');
	let reqHash = $state('');

	async function openReportRequest() {
		if (!polygonResult || !lastPolygonRings) return;
		const ringsText = JSON.stringify(lastPolygonRings);
		const hashBuf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(ringsText));
		reqHash = Array.from(new Uint8Array(hashBuf)).map((b) => b.toString(16).padStart(2, '0')).join('');
		reportModalOpen = true;
	}

	function closeReportModal() {
		reportModalOpen = false;
	}

	function submitReportRequest() {
		if (!polygonResult || !lastPolygonRings) return;
		if (!reqEmail.trim() || !reqName.trim()) {
			polygonError = i18n.t('eudr.check.req_err_fields');
			return;
		}
		const r = polygonResult;
		const yr = (y: number) => (r.loss_by_year[y] || 0).toFixed(3);
		const body = [
			'Hola,',
			'',
			'Solicito un informe técnico firmado sobre el siguiente análisis EUDR.',
			'',
			'-- SOLICITANTE --',
			`Nombre: ${reqName}`,
			`Email: ${reqEmail}`,
			reqCompany ? `Organización: ${reqCompany}` : '',
			reqPurpose ? `Propósito del informe: ${reqPurpose}` : '',
			'',
			'-- POLÍGONO ANALIZADO --',
			`Identificador: ${polygonName || '(dibujado en el mapa)'}`,
			`Hash SHA-256 (geometría): ${reqHash}`,
			`Provincias/unidades: ${r.provinces.map(prettyProvince).join(', ') || '—'}`,
			`Área en cobertura: ${r.area_ha.toLocaleString(undefined, { maximumFractionDigits: 0 })} ha`,
			`Cobertura del polígono: ${r.coverage_pct.toFixed(1)}%`,
			`Celdas evaluadas: ${r.cells_in_coverage.toLocaleString()} de ${r.cells_requested.toLocaleString()}`,
			'',
			'-- RESULTADOS --',
			`Pérdida forestal post-2020: ${r.deforested_pct.toFixed(2)}% del polígono`,
			`Riesgo máximo (0-100): ${r.max_risk.toFixed(0)}`,
			`Riesgo medio (0-100): ${r.mean_risk.toFixed(1)}`,
			`Celdas con pérdida post-2020: ${r.deforested_cells.toLocaleString()}`,
			`Pérdida 2021/22/23/24: ${yr(2021)}% / ${yr(2022)}% / ${yr(2023)}% / ${yr(2024)}%`,
			r.plantation_data_cells > 0
				? `Plantación vs nativo (celdas con pérdida, baseline MapBiomas 2020): ${r.deforested_harvest} cosecha de plantación previa a 2020 (conforme) · ${r.deforested_conversion} conversión nativo→plantación post-2020 (NO conforme) · ${r.deforested_native} pérdida de bosque nativo`
				: 'Plantación vs nativo: SIN dato de plantación para esta zona (Paraguay/Brasil — MapBiomas no clasifica silvicultura). La pérdida no pudo distinguirse entre cosecha de plantación y deforestación de bosque nativo; si hay forestación, posible falso positivo (verificar en terreno o catastro).',
			'',
			'-- METODOLOGÍA --',
			'Hansen GFC v1.12 + MODIS MCD64A1, cutoff 31/12/2020, H3 res-9 (~0,1 km²) sobre dato 100 m.',
			'Score 0-100 = 70% pérdida post-2020 + 20% fuego post-2020 + 10% pérdida previa.',
			'Plantación vs bosque nativo: MapBiomas Argentina Col.1 (clase 9 = silvicultura).',
			'',
			'(Adjunto el GeoJSON del polígono.)',
			'',
			'Gracias.',
		].filter(Boolean).join('\n');
		const subject = `[Solicitud informe EUDR] ${polygonName || 'polígono'} — ${reqHash.slice(0, 8)}`;
		const mailto = `mailto:nealab@spatia.ar?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;

		// Trigger GeoJSON download so the user can attach it to the email
		const gj = {
			type: 'Feature',
			properties: { name: polygonName || 'polygon', hash: reqHash, requested_by: reqEmail },
			geometry: { type: 'Polygon', coordinates: lastPolygonRings },
		};
		const blob = new Blob([JSON.stringify(gj)], { type: 'application/geo+json' });
		const blobUrl = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = blobUrl;
		a.download = `eudr_request_${reqHash.slice(0, 8)}.geojson`;
		document.body.appendChild(a);
		a.click();
		document.body.removeChild(a);
		URL.revokeObjectURL(blobUrl);

		// Open the mail client with the prefilled body
		window.location.href = mailto;
		closeReportModal();
	}

	function handleSubmit() {
		const lat = parseFloat(latInput);
		const lon = parseFloat(lonInput);

		if (isNaN(lat) || isNaN(lon)) {
			error = i18n.t('eudr.check.error_invalid');
			return;
		}

		if (lat < -35 || lat > -19 || lon < -70 || lon > -47) {
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
		if (drawing) mapComponent?.cancelLasso();
	}

	function fmt(v: number | null, decimals = 1): string {
		if (v === null || v === undefined) return '--';
		return v.toFixed(decimals);
	}

	// Stored province codes are ascii/lowercase (ar_misiones, py_itapua, br_parana).
	// Map to display names with accents + country tag; fall back to title-case.
	const PROVINCE_NAMES: Record<string, string> = {
		ar_salta: 'Salta (AR)', ar_jujuy: 'Jujuy (AR)', ar_tucuman: 'Tucumán (AR)',
		ar_catamarca: 'Catamarca (AR)', ar_santiago_del_estero: 'Santiago del Estero (AR)',
		ar_chaco: 'Chaco (AR)', ar_formosa: 'Formosa (AR)', ar_corrientes: 'Corrientes (AR)',
		ar_misiones: 'Misiones (AR)', ar_entre_rios: 'Entre Ríos (AR)',
		py_concepcion: 'Concepción (PY)', py_san_pedro: 'San Pedro (PY)', py_cordillera: 'Cordillera (PY)',
		py_guaira: 'Guairá (PY)', py_caaguazu: 'Caaguazú (PY)', py_caazapa: 'Caazapá (PY)',
		py_itapua: 'Itapúa (PY)', py_misiones: 'Misiones (PY)', py_paraguari: 'Paraguarí (PY)',
		py_alto_parana: 'Alto Paraná (PY)', py_central: 'Central (PY)', py_neembucu: 'Ñeembucú (PY)',
		py_amambay: 'Amambay (PY)', py_canindeyu: 'Canindeyú (PY)', py_presidente_hayes: 'Presidente Hayes (PY)',
		py_boqueron: 'Boquerón (PY)', py_alto_paraguay: 'Alto Paraguay (PY)', py_asuncion: 'Asunción (PY)',
		br_parana: 'Paraná (BR)', br_santa_catarina: 'Santa Catarina (BR)', br_rio_grande_do_sul: 'Rio Grande do Sul (BR)',
	};

	function prettyProvince(code: string): string {
		if (!code) return '';
		const key = code.toLowerCase();
		if (PROVINCE_NAMES[key]) return PROVINCE_NAMES[key];
		const m = key.match(/^(ar|py|br)_(.+)$/);
		const body = (m ? m[2] : key).replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
		return m ? `${body} (${m[1].toUpperCase()})` : body;
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
				sobre hexágonos de ~0,1 km² (H3 res-9). Cobertura: provincias del norte y litoral argentino
				(NEA, NOA y Entre Ríos), departamentos paraguayos y estados del sur de Brasil. Sirve para
				screening de riesgo EUDR, due-diligence preliminar y soporte técnico de informes.
				En Argentina (Misiones y Corrientes) distingue además la pérdida sobre <b>plantación forestal</b>
				(ciclo de cosecha, conforme) de la <b>deforestación de bosque nativo</b>, cruzando con MapBiomas
				y un baseline al corte 2020.
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
				<div class="mt-2 grid grid-cols-2 gap-2 text-[11px]">
					<button onclick={tryExample}
						class="py-1.5 border border-white/15 rounded text-white/60 hover:border-white/35 hover:text-white transition-colors cursor-pointer bg-transparent">
						{i18n.t('eudr.check.try_example')} →
					</button>
					<button onclick={clearAll}
						class="py-1.5 border border-white/15 rounded text-white/60 hover:border-yellow-400/60 hover:text-yellow-400 transition-colors cursor-pointer bg-transparent">
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

				<!-- Batch CSV (id, lat, lon) for operational screening -->
				<div class="mt-3 pt-3 border-t border-border">
					<div class="text-[10px] text-white/40 uppercase mb-2">{i18n.t('eudr.check.batch_title')}</div>
					<label class="block w-full text-center py-2 border border-dashed border-white/20 rounded-lg text-[12px] text-white/60 hover:border-white/40 hover:text-white transition-colors cursor-pointer">
						{batchLoading ? i18n.t('eudr.check.checking') : i18n.t('eudr.check.batch_upload')}
						<input type="file" accept=".csv,text/csv" onchange={handleBatchUpload} disabled={batchLoading} class="hidden" />
					</label>
					<p class="mt-1 text-[10px] text-white/30 leading-relaxed">{i18n.t('eudr.check.batch_format')}</p>
					{#if batchError}
						<p class="mt-2 text-[12px] text-red-400">{batchError}</p>
					{/if}
					{#if batchResult}
						<a href={batchResult.csvUrl} download={`eudr_batch_${Date.now()}.csv`}
							class="mt-2 block text-center py-2 rounded border border-yellow-400/50 bg-yellow-400/15 text-[12px] font-semibold text-yellow-100 hover:bg-yellow-400/25 hover:text-white transition-colors">
							↓ {batchResult.processed.toLocaleString()} {i18n.t('eudr.check.batch_download')}
							{#if batchResult.outside > 0} <span class="font-normal text-white/50">({batchResult.outside} {i18n.t('eudr.check.batch_outside')})</span>{/if}
						</a>
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

					<!-- Plantation context: keep Hansen plantation-harvest "loss" from reading as native deforestation -->
					{#if result.eudr_assessment !== 'OUTSIDE_COVERAGE'}
						{@const ctx = plantationContext(result.loss_post_2020_pct, result.plantation_pct, result.plantation_2020_pct, result.native_2020_pct)}
						{#if ctx.kind === 'managed_confirmed'}
							<div class="mb-4 px-3 py-2 rounded bg-emerald-500/15 border border-emerald-500/35 text-[10px] text-emerald-200/90 leading-relaxed">
								🌲 {i18n.t('eudr.check.plant_managed_confirmed').replace('{pct}', fmt(ctx.plantation, 0))}
							</div>
						{:else if ctx.kind === 'conversion'}
							<div class="mb-4 px-3 py-2 rounded bg-red-500/10 border border-red-500/30 text-[10px] text-red-200/90 leading-relaxed">
								{i18n.t('eudr.check.plant_conversion')}
							</div>
						{:else if ctx.kind === 'native_loss'}
							<div class="mb-4 px-3 py-2 rounded bg-amber-500/10 border border-amber-500/25 text-[10px] text-amber-200/80 leading-relaxed">
								{i18n.t('eudr.check.plant_native_loss')}
							</div>
						{:else if ctx.kind === 'managed'}
							<div class="mb-4 px-3 py-2 rounded bg-emerald-500/10 border border-emerald-500/25 text-[10px] text-emerald-200/80 leading-relaxed">
								🌲 {i18n.t('eudr.check.plant_managed').replace('{pct}', fmt(ctx.plantation, 0))}
							</div>
						{:else if ctx.kind === 'plantation'}
							<div class="mb-4 px-3 py-2 rounded bg-emerald-500/[0.06] border border-emerald-500/15 text-[10px] text-emerald-200/60 leading-relaxed">
								🌲 {i18n.t('eudr.check.plant_zone').replace('{pct}', fmt(ctx.plantation, 0))}
							</div>
						{:else if ctx.kind === 'nodata' && (result.loss_post_2020_pct ?? 0) > 0}
							<div class="mb-4 px-3 py-2 rounded bg-amber-500/10 border border-amber-500/30 text-[10px] text-amber-200/85 leading-relaxed">
								{i18n.t('eudr.check.plant_nodata_loss')}
							</div>
						{:else if ctx.kind === 'nodata'}
							<div class="mb-4 px-3 py-2 rounded bg-white/[0.03] border border-white/10 text-[10px] text-white/35 leading-relaxed">
								{i18n.t('eudr.check.plant_nodata')}
							</div>
						{/if}
					{/if}

					<!-- Loss by year (post-cutoff temporal curve) -->
					{#if result.loss_by_year && (result.loss_post_2020_pct ?? 0) > 0}
						<div class="mb-4">
							<div class="text-[10px] text-white/40 uppercase mb-2">{i18n.t('eudr.check.loss_by_year')}</div>
							<svg viewBox="0 0 200 64" class="w-full" preserveAspectRatio="none">
								{#each [2021, 2022, 2023, 2024] as y, i}
									{@const v = result.loss_by_year?.[y] || 0}
									{@const maxV = Math.max(result.loss_by_year?.[2021] || 0, result.loss_by_year?.[2022] || 0, result.loss_by_year?.[2023] || 0, result.loss_by_year?.[2024] || 0, 0.01)}
									{@const h = (v / maxV) * 38}
									<rect x={i * 50 + 8} y={50 - h} width="34" height={h} fill={yearBarColor(v)} rx="2"/>
									<text x={i * 50 + 25} y="61" text-anchor="middle" font-size="9" fill="#94a3b8" font-family="JetBrains Mono">{y}</text>
									{#if v > 0.01}
										<text x={i * 50 + 25} y={48 - h} text-anchor="middle" font-size="8" fill={yearBarColor(v)} font-family="JetBrains Mono">{v.toFixed(2)}%</text>
									{/if}
								{/each}
							</svg>
						</div>
					{/if}

					<!-- Details -->
					<div class="space-y-2 text-[11px] text-white/50">
						<div class="flex justify-between">
							<span>H3 cell</span>
							<span class="font-mono text-white/70">{result.h3_cell}</span>
						</div>
						<div class="flex justify-between">
							<span>{i18n.t('eudr.check.province')}</span>
							<span class="text-white/70">{result.province ? prettyProvince(result.province) : '--'}</span>
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

					<!-- Plantation vs native split of the deforested cells (forestry false-positive guard) -->
					{#if polygonResult.deforested_cells > 0 && polygonResult.plantation_data_cells > 0}
						<div class="mb-4 px-3 py-2 rounded bg-emerald-500/10 border border-emerald-500/25 text-[10px] text-emerald-200/80 leading-relaxed">
							🌲 {i18n.t('eudr.check.poly_plant_split').replace('{harvest}', String(polygonResult.deforested_harvest)).replace('{conversion}', String(polygonResult.deforested_conversion)).replace('{native}', String(polygonResult.deforested_native))}
						</div>
					{:else if polygonResult.plantation_data_cells === 0 && polygonResult.deforested_cells > 0}
						<div class="mb-4 px-3 py-2 rounded bg-amber-500/10 border border-amber-500/30 text-[10px] text-amber-200/85 leading-relaxed">
							{i18n.t('eudr.check.plant_nodata_loss')}
						</div>
					{:else if polygonResult.plantation_data_cells === 0}
						<div class="mb-4 px-3 py-2 rounded bg-white/[0.03] border border-white/10 text-[10px] text-white/35 leading-relaxed">
							{i18n.t('eudr.check.plant_nodata')}
						</div>
					{/if}

					<!-- Loss by year (post-cutoff EUDR temporal curve) -->
					{#if polygonResult.deforested_cells > 0}
						<div class="mb-4">
							<div class="text-[10px] text-white/40 uppercase mb-2">{i18n.t('eudr.check.loss_by_year')}</div>
							<svg viewBox="0 0 200 64" class="w-full" preserveAspectRatio="none">
								{#each [2021, 2022, 2023, 2024] as y, i}
									{@const v = polygonResult.loss_by_year[y] || 0}
									{@const maxV = Math.max(polygonResult.loss_by_year[2021] || 0, polygonResult.loss_by_year[2022] || 0, polygonResult.loss_by_year[2023] || 0, polygonResult.loss_by_year[2024] || 0, 0.01)}
									{@const h = (v / maxV) * 38}
									<rect x={i * 50 + 8} y={50 - h} width="34" height={h} fill={yearBarColor(v)} rx="2"/>
									<text x={i * 50 + 25} y="61" text-anchor="middle" font-size="9" fill="#94a3b8" font-family="JetBrains Mono">{y}</text>
									{#if v > 0.01}
										<text x={i * 50 + 25} y={48 - h} text-anchor="middle" font-size="8" fill={yearBarColor(v)} font-family="JetBrains Mono">{v.toFixed(2)}%</text>
									{/if}
								{/each}
							</svg>
						</div>
					{/if}

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
							<span class="text-white/70">{polygonResult.provinces.map(prettyProvince).join(', ') || '--'}</span>
						</div>
					</div>

					<button onclick={openReportRequest}
						class="mt-4 w-full py-2 rounded border border-yellow-400/60 bg-yellow-400/15 text-[12px] font-semibold text-yellow-100 hover:bg-yellow-400/25 hover:border-yellow-400 hover:text-white transition-colors cursor-pointer">
						{i18n.t('eudr.check.poly_report')}
					</button>

					<div class="mt-3 pt-3 border-t border-border text-[10px] text-white/25 leading-relaxed">
						{i18n.t('eudr.disclaimer_short')}
					</div>
					<a href="/metodologia/eudr" class="mt-2 inline-block text-[11px] text-yellow-400 hover:text-white underline transition-colors">
						{i18n.t('eudr.check.methodology_link')}
					</a>
				</div>
			{/if}
	</div>

	<!-- Report-request modal (lead-gen — STAN-CONICET emite el informe humano) -->
	{#if reportModalOpen}
		<div class="fixed inset-0 z-50 bg-black/85 backdrop-blur-sm flex items-center justify-center p-4" onclick={closeReportModal} role="presentation">
			<div class="border border-yellow-400/40 rounded-lg p-6 max-w-md w-full max-h-[calc(100vh-32px)] overflow-y-auto" style="background: var(--color-bg);" onclick={(e: MouseEvent) => e.stopPropagation()} role="dialog">
				<h3 class="text-base font-bold text-white mb-2">{i18n.t('eudr.check.req_title')}</h3>
				<p class="text-[11px] text-white/60 mb-4 leading-relaxed">{i18n.t('eudr.check.req_intro')}</p>
				<div class="space-y-2">
					<input type="text" placeholder={i18n.t('eudr.check.req_name')} bind:value={reqName}
						class="w-full bg-white/5 border border-border rounded px-3 py-2 text-[13px] text-white placeholder-white/30 focus:outline-none focus:border-yellow-400/60" />
					<input type="email" placeholder={i18n.t('eudr.check.req_email')} bind:value={reqEmail}
						class="w-full bg-white/5 border border-border rounded px-3 py-2 text-[13px] text-white placeholder-white/30 focus:outline-none focus:border-yellow-400/60" />
					<input type="text" placeholder={i18n.t('eudr.check.req_company')} bind:value={reqCompany}
						class="w-full bg-white/5 border border-border rounded px-3 py-2 text-[13px] text-white placeholder-white/30 focus:outline-none focus:border-yellow-400/60" />
					<textarea placeholder={i18n.t('eudr.check.req_purpose')} bind:value={reqPurpose} rows="3"
						class="w-full bg-white/5 border border-border rounded px-3 py-2 text-[13px] text-white placeholder-white/30 focus:outline-none focus:border-yellow-400/60 resize-none"></textarea>
				</div>
				<p class="mt-3 text-[10px] text-white/40 leading-relaxed">{i18n.t('eudr.check.req_note')}</p>
				<div class="mt-4 flex gap-2">
					<button onclick={closeReportModal}
						class="flex-1 py-2 border border-white/20 rounded text-[12px] text-white/60 hover:border-white/40 hover:text-white transition-colors cursor-pointer bg-transparent">
						{i18n.t('eudr.check.req_cancel')}
					</button>
					<button onclick={submitReportRequest}
						class="flex-1 py-2 rounded bg-yellow-400 text-black font-bold text-[12px] hover:bg-yellow-400/85 transition-colors cursor-pointer">
						{i18n.t('eudr.check.req_send')}
					</button>
				</div>
			</div>
		</div>
	{/if}
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
