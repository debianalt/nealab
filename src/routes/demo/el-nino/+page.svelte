<script lang="ts">
	// Demo «El Niño: dónde mirar primero» — El Soberbio, crecida del Uruguay por altura del hidrómetro.
	// Unlisted page with its own map: it does not touch Map.svelte or the analysis registry.
	// Data are precomputed outside this repo and served from R2 under data/demo/el_nino/:
	//   agua.png            one band, value = gauge height (x4) at which water reaches each 20 m pixel, 0 = never
	//   edificios.pmtiles   every building of the municipality: s impact height (-1 never), a area, h height,
	//                       p persons and v dwellings (census radio totals spread by area over probable dwellings),
	//                       c condition by size (0 small annex, 1 probable dwelling, 2 probable non-residential),
	//                       r in a RENABAP neighbourhood, z census radio
	//   edificios_pts.json  the same buildings as flat arrays, for the lasso
	//   radios.geojson      the 36 census radios with census profile, housing type, priority and scenario totals
	//   puntos.geojson      schools and health centres from the provincial emergency viewer
	//   tabla_altura.json   totals per 0.5 m step, scenarios, annual peaks, thresholds, provincial averages, raster
	import { onMount } from 'svelte';
	import 'maplibre-gl/dist/maplibre-gl.css';
	import { page } from '$app/state';
	import { BASEMAP, getTilesUrl } from '$lib/config';
	import { pointInPolygon } from '$lib/utils/geometry';

	const CDN = 'https://cdn.spatia.ar/data/demo/el_nino';
	const TOWN_WIDE = { center: [-54.2025, -27.2995] as [number, number], zoom: 14.9, pitch: 55, bearing: -20 };
	const TOWN_NARROW = { center: [-54.1995, -27.3005] as [number, number], zoom: 14.1, pitch: 50, bearing: -20 };
	const MUNI = { center: [-54.12, -27.15] as [number, number], zoom: 10.4, pitch: 0, bearing: 0 };
	// phones get a wider town view so the river stays in frame
	const town = () => (typeof window !== 'undefined' && window.innerWidth < 760 ? TOWN_NARROW : TOWN_WIDE);

	const PRIO = [
		{ id: 'vigilar ya', label: 'Vigilar ya', color: '#ef4444' },
		{ id: 'vigilar', label: 'Vigilar', color: '#f59e0b' },
		{ id: 'atento', label: 'Atento', color: '#fef08a' },
		{ id: 'sin prioridad', label: 'Sin prioridad', color: '#64748b' }
	];
	const TIPOS = [
		{ id: 'precaria', label: 'Construcción precaria', color: '#f472b6', def: 'piso o cielorraso de baja calidad por encima del promedio' },
		{ id: 'saneamiento', label: 'Saneamiento que se contamina', color: '#a78bfa', def: 'pozo sin cámara y agua sin cañería por encima del promedio: con la crecida el pozo desborda' },
		{ id: 'tenencia', label: 'Tenencia insegura', color: '#fb923c', def: 'hogares ni propietarios ni inquilinos por encima del promedio, o mayoría de viviendas en barrio popular' },
		{ id: 'dependientes', label: 'Hogares que no se evacúan solos', color: '#34d399', def: 'niños de 0 a 5 y mayores de 65 por encima del promedio' },
		{ id: 'consolidada', label: 'Consolidada', color: '#94a3b8', def: 'ninguno de los rasgos anteriores' }
	];
	// census layers as raw counts per radio: share in the radio x its persons or households.
	// prov = Misiones share (tabla.provincia key, or a number: 0-5 and 65+ from the 2022 census, all radios 54*)
	const CENSO = [
		{ id: 'mayores', share: 's65', base: 'c_personas', unit: 'personas', prov: 0.0827 as number | string, label: 'Personas de 65 años y más' },
		{ id: 'ninos', share: 's05', base: 'c_personas', unit: 'niños', prov: 0.0982 as number | string, label: 'Niños de 0 a 5 años' },
		{ id: 'c_nbi', share: 'c_nbi', base: 'c_hogares', unit: 'hogares', prov: 'nbi' as number | string, label: 'Hogares con NBI' },
		{ id: 'c_piso', share: 'c_piso', base: 'c_hogares', unit: 'hogares', prov: 'piso' as number | string, label: 'Hogares con piso de baja calidad' },
		{ id: 'c_cielo', share: 'c_cielo', base: 'c_hogares', unit: 'hogares', prov: 'cielo' as number | string, label: 'Hogares sin cielorraso' },
		{ id: 'c_pozo', share: 'c_pozo', base: 'c_hogares', unit: 'hogares', prov: 'pozo' as number | string, label: 'Hogares con pozo sin cámara' },
		{ id: 'c_canieria', share: 'c_canieria', base: 'c_hogares', unit: 'hogares', prov: 'canieria' as number | string, label: 'Hogares sin agua por cañería' },
		{ id: 'c_hacin', share: 'c_hacin', base: 'c_hogares', unit: 'hogares', prov: 'hacin' as number | string, label: 'Hogares con hacinamiento' },
		{ id: 'c_tenencia', share: 'c_tenencia', base: 'c_hogares', unit: 'hogares', prov: 'tenencia' as number | string, label: 'Hogares ni propietarios ni inquilinos' }
	];
	type CVar = (typeof CENSO)[number];
	const shareOf = (c: CVar, p: any) => +p[c.share] || 0;
	const countOf = (c: CVar, p: any) => shareOf(c, p) * (+p[c.base] || 0);
	const provOf = (c: CVar) => (typeof c.prov === 'number' ? c.prov : (tabla?.provincia[c.prov] ?? 1));
	// sequential plasma scale: reads well on the dark basemap and does not mix with the water blues
	const PLASMA = ['#3b1f6b', '#7b2f8c', '#c13e75', '#f0624b', '#fca636'];
	// round to half the order of magnitude: 23 -> 25, 38 -> 40, 140 -> 150
	const niceNum = (x: number) => {
		if (x <= 0) return 0;
		const step = Math.max(10 ** Math.floor(Math.log10(x)) / 2, 1);
		return Math.max(Math.round(x / step) * step, step);
	};
	// class breaks: quintiles of the 36 radios rounded to clean numbers
	function breaksFor(c: CVar): number[] {
		const v = (radios?.features ?? []).map((f: any) => countOf(c, f.properties)).sort((a: number, b: number) => a - b);
		if (!v.length) return [];
		const q = [0.2, 0.4, 0.6, 0.8].map((t) => niceNum(v[Math.floor(t * (v.length - 1))]));
		return [...new Set(q)].filter((x) => x > 0).sort((a, b) => a - b);
	}
	// continuous water-depth scale: colour interpolated between stops that the legend shows evenly spaced
	const WSTOPS: [number, number[]][] = [[0, [224, 247, 255, 175]], [0.5, [158, 230, 255, 190]], [1.5, [76, 195, 255, 205]],
		[3, [31, 143, 224, 215]], [6, [20, 83, 168, 225]], [10, [11, 42, 107, 235]]];
	const LUT_STEP = 0.05;
	const WLUT = (() => {
		const n = Math.round(10 / LUT_STEP) + 1, out = new Uint8ClampedArray(n * 4);
		for (let i = 0; i < n; i++) {
			const d = i * LUT_STEP;
			let k = 0;
			while (k < WSTOPS.length - 2 && d > WSTOPS[k + 1][0]) k++;
			const [a, ca] = WSTOPS[k], [b, cb] = WSTOPS[k + 1];
			const t = Math.min(Math.max((d - a) / (b - a), 0), 1);
			for (let j = 0; j < 4; j++) out[i * 4 + j] = ca[j] + (cb[j] - ca[j]) * t;
		}
		return out;
	})();
	const WLUT_N = WLUT.length / 4;
	const WGRAD = `linear-gradient(to right, ${WSTOPS.map((s, i) => `rgb(${s[1].slice(0, 3).join(',')}) ${(i / (WSTOPS.length - 1)) * 100}%`).join(', ')})`;
	const BDEPTH: [number, string][] = [[0, '#fdba74'], [1, '#f97316'], [2.5, '#dc2626'], [5, '#991b1b']];
	const DRY = '#4b5563';
	const COND = ['Anexo chico (menos de 25 m²)', 'Vivienda probable (25 a 400 m²)', 'No residencial probable (más de 400 m²)'];
	const SCEN = [
		{ id: '2026', label: 'Jul. 2026' },
		{ id: '2023', label: 'Nov. 2023' },
		{ id: '2014', label: 'Jun. 2014' },
		{ id: '1983', label: '1983', note: 'récord' }
	];
	// official levels at the El Soberbio gauge (cited by the municipality and Protección Civil, 2025-2026)
	const COTAS = [{ h: 11, label: 'alerta', side: 'left' }, { h: 13, label: 'evacuación', side: 'right' }];
	const ISO = '#a78bfa';
	const DIMS: [string, string][] = [
		['dim_material', 'vivienda precaria o hacinamiento'],
		['dim_recuperacion', 'hogares con NBI'],
		['dim_dependientes', 'niños y mayores que no se evacúan solos'],
		['dim_aislamiento', 'lejos del hospital, población indígena o barrio popular']
	];

	interface Paso {
		altura: number; viviendas: number; personas: number; ninos: number; mayores: number; hog_nbi: number;
		viv_renabap: number; escuelas: number; escuelas_secas: number; salud: number; comunidades: number; microzonas: number;
		aisladas_viv: number; aisladas_pers: number;
	}
	interface Tabla {
		escenarios: Record<string, { fecha: string; altura_m: number; fuente?: string }>;
		cortes: { nombre: string; altura: number }[];
		calibracion_delta: number;
		s_ref: number;
		pasos: Paso[];
		picos: { anio: number; altura: number }[];
		umbrales: { altura: number; microzona: string; id: string; prioridad: string }[];
		provincia: Record<string, number>;
		raster: { ancho: number; alto: number; res_m: number; esquinas: [number, number][] };
	}
	interface Lasso { viv: number; pers: number; ninos: number; mayores: number; ren: number; esc: number }

	let container: HTMLDivElement;
	let map: any;
	let tabla = $state<Tabla | null>(null);
	let stage = $state(19);
	let selected = $state<Record<string, any> | null>(null);
	let error = $state('');
	let capa = $state('agua');
	let lassoOn = $state(false);
	let lassoPts = $state<[number, number][]>([]);
	let lassoPoly = $state<[number, number][] | null>(null);
	let radios: any = null;
	let radioById = new Map<string, any>();
	// where water first reaches each listed radio: mean position of its first-reached buildings
	let starPos = new Map<string, [number, number]>();
	let puntos: any = null;
	let caminos: any = null;
	// buildings as flat arrays for the lasso
	let B = { x: [] as number[], y: [] as number[], s: [] as (number | null)[], v: [] as number[], p: [] as number[],
		r: [] as number[], z: [] as number[], i: [] as number[], radios: [] as string[] };
	// water raster
	let vals: Uint8Array | null = null;
	let wet: Int32Array | null = null;
	let wctx: CanvasRenderingContext2D | null = null;
	let wraw: CanvasRenderingContext2D | null = null;
	let wimg: ImageData | null = null;

	const pasoAt = (h: number) => {
		if (!tabla) return null;
		let best = tabla.pasos[0];
		for (const p of tabla.pasos) if (p.altura <= h + 1e-6) best = p;
		return best;
	};
	const pasoActual = $derived(tabla ? pasoAt(stage) : null);
	const pasoMenos = $derived(tabla ? pasoAt(stage - 1) : null);
	const pasoMas = $derived(tabla ? pasoAt(stage + 1) : null);
	const minStage = $derived(tabla ? tabla.pasos[0].altura : 2.5);
	const maxStage = $derived(tabla ? tabla.pasos[tabla.pasos.length - 1].altura : 28);
	const alcanzadas = $derived(tabla ? tabla.umbrales.filter((u) => u.altura <= stage + 1e-6) : []);
	const proximas = $derived(tabla ? tabla.umbrales.filter((u) => u.altura > stage + 1e-6).slice(0, 2) : []);
	// map view: water (default), priority, housing type, or one census variable
	const tab = $derived(capa === 'agua' || capa === 'prioridad' || capa === 'tipo' ? capa : 'censo');
	// numbered stars: the number is the position in the full threshold list, so it does not change with the slider
	const numOf = $derived(new Map((tabla?.umbrales ?? []).map((u, i) => [u.id, i + 1])));
	let starHover = $state('');
	const aniosSobre = $derived(tabla ? tabla.picos.filter((p) => p.altura >= stage - 1e-6).length : 0);
	const censoSel = $derived(CENSO.find((c) => c.id === capa));
	const esRuta = (n: string) => /ruta|RP|RN/i.test(n);
	const cortados = $derived(tabla ? (tabla.cortes ?? []).filter((c) => c.altura <= stage + 1e-6) : []);
	const rutasCortadas = $derived(cortados.filter((c) => esRuta(c.nombre)));
	const callesCortadas = $derived(cortados.filter((c) => !esRuta(c.nombre)));
	const proximoCorte = $derived(tabla ? (tabla.cortes ?? []).find((c) => c.altura > stage + 1e-6) : undefined);
	const censoKeys = $derived.by(() => {
		if (!censoSel || !tabla) return [];
		const b = breaksFor(censoSel), cols = PLASMA.slice(PLASMA.length - (b.length + 1));
		return cols.map((color, i) => ({ color,
			label: i === 0 ? `menos de ${fmt(b[0])}` : i === b.length ? `${fmt(b[i - 1])} o más` : `${fmt(b[i - 1])} a ${fmt(b[i])}` }));
	});
	const lassoRes = $derived.by(() => {
		if (!lassoPoly || !tabla) return null;
		const alturas = [stage, ...SCEN.map((s) => tabla!.escenarios[s.id].altura_m)];
		const out: Lasso[] = alturas.map(() => ({ viv: 0, pers: 0, ninos: 0, mayores: 0, ren: 0, esc: 0 }));
		const total = { viv: 0, pers: 0, n: 0 };
		const bb = bbox(lassoPoly);
		for (let i = 0; i < B.x.length; i++) {
			const x = B.x[i], y = B.y[i];
			if (x < bb[0] || x > bb[2] || y < bb[1] || y > bb[3] || !pointInPolygon([x, y], lassoPoly)) continue;
			total.viv += B.v[i]; total.pers += B.p[i]; total.n++;
			const s = B.s[i];
			if (s == null) continue;
			const rp = radioById.get(B.radios[B.z[i]])?.properties ?? { s05: 0, s65: 0 };
			alturas.forEach((h, k) => {
				if (s <= h + 1e-6) {
					const o = out[k];
					o.viv += B.v[i]; o.pers += B.p[i]; o.ninos += B.p[i] * rp.s05; o.mayores += B.p[i] * rp.s65;
					if (B.r[i]) o.ren += B.v[i];
				}
			});
		}
		for (const f of puntos?.features ?? []) {
			const pi = f.properties.primer_impacto;
			if (f.properties.tipo !== 'escuela' || pi == null) continue;
			if (!pointInPolygon(f.geometry.coordinates, lassoPoly)) continue;
			alturas.forEach((h, k) => { if (pi <= h + 1e-6) out[k].esc += 1; });
		}
		return { out, total };
	});

	const STAR = Array.from({ length: 10 }, (_, k) => {
		const r = k % 2 ? 6.6 : 11.6, a = -Math.PI / 2 + (k * Math.PI) / 5;
		return [12 + r * Math.cos(a), 12.8 + r * Math.sin(a)];
	});
	const STAR_PATH = 'M' + STAR.map(([x, y]) => `${x.toFixed(2)},${y.toFixed(2)}`).join('L') + 'Z';
	const starText = (c: string) => (c === '#ef4444' || c === '#64748b' ? '#fff' : '#0a0a0a');
	function starImage(n: number, color: string) {
		const S = 80, k = S / 24, cv = document.createElement('canvas');
		cv.width = cv.height = S;
		const g = cv.getContext('2d')!;
		g.beginPath();
		STAR.forEach(([x, y], i) => (i ? g.lineTo(x * k, y * k) : g.moveTo(x * k, y * k)));
		g.closePath();
		g.fillStyle = color; g.fill();
		g.lineWidth = 3; g.strokeStyle = '#0a0a0a'; g.stroke();
		g.fillStyle = starText(color);
		g.font = `800 ${n > 9 ? 22 : 27}px sans-serif`;
		g.textAlign = 'center'; g.textBaseline = 'middle';
		g.fillText(String(n), 12 * k, 13.9 * k);
		return g.getImageData(0, 0, S, S);
	}
	function starData() {
		const feats = alcanzadas.flatMap((u) => {
			const pos = starPos.get(u.id);
			return pos ? [{ type: 'Feature', geometry: { type: 'Point', coordinates: pos },
				properties: { id: u.id, n: numOf.get(u.id) ?? 0, nombre: u.microzona.startsWith('cerca de') ? '' : u.microzona.split(',')[0].trim() } }] : [];
		});
		return { type: 'FeatureCollection', features: feats };
	}

	const fmt = (n: number) => Math.round(n).toLocaleString('es-AR');
	const approx = (n: number) => (n < 20 ? fmt(n) : '≈ ' + fmt(Math.round(n / 10) * 10));
	const m = (x: number) => x.toLocaleString('es-AR', { minimumFractionDigits: 1, maximumFractionDigits: 2 }) + ' m';
	const dec = (x: number) => x.toLocaleString('es-AR', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
	const pct = (x: number) => (x * 100).toLocaleString('es-AR', { maximumFractionDigits: 0 }) + ' %';
	const colorOf = (p: string) => PRIO.find((x) => x.id === p)?.color ?? '#64748b';
	const tipoOf = (t: string) => TIPOS.find((x) => x.id === t);
	const fecha = (iso: string) => { const [y, mo, d] = iso.split('-'); return `${+d}/${+mo}/${y}`; };
	const num = (v: any) => (v === null || v === undefined || v === 'null' || +v < 0 ? null : +v);

	function bbox(poly: [number, number][]) {
		let a = Infinity, b = Infinity, c = -Infinity, d = -Infinity;
		for (const [x, y] of poly) { a = Math.min(a, x); b = Math.min(b, y); c = Math.max(c, x); d = Math.max(d, y); }
		return [a, b, c, d];
	}

	// Slider moves in 0.5 m steps; a scenario button keeps its exact height (19.2 m) and
	// the totals come from the highest precomputed step at or below it.
	function setStage(v: number, exact = false) {
		stage = exact ? v : Math.round(v * 2) / 2;
	}

	// ── water: recolour the canvas by depth = stage − height at which water arrives ──────
	function paintWater() {
		if (!vals || !wet || !wctx || !wraw || !wimg) return;
		const d = wimg.data;
		d.fill(0);
		const st4 = stage * 4 + 1e-6;
		for (let k = 0; k < wet.length; k++) {
			const i = wet[k], v = vals[i];
			if (v > st4) continue;
			const li = Math.min(Math.round((stage - v / 4) / LUT_STEP), WLUT_N - 1) * 4;
			const o = i * 4;
			d[o] = WLUT[li]; d[o + 1] = WLUT[li + 1]; d[o + 2] = WLUT[li + 2]; d[o + 3] = WLUT[li + 3];
		}
		// paint on a scratch canvas, then draw it blurred by ~1 px so 20 m pixels do not show as blocks
		wraw.putImageData(wimg, 0, 0);
		wctx.clearRect(0, 0, wimg.width, wimg.height);
		wctx.filter = 'blur(1px)';
		wctx.drawImage(wraw.canvas, 0, 0);
		wctx.filter = 'none';
		const src = map?.getSource('agua');
		if (src) { src.play(); map.triggerRepaint(); setTimeout(() => src.pause(), 60); }
	}

	function depthAt(lng: number, lat: number): { depth: number; desde: number } | null {
		if (!vals || !tabla) return null;
		const [[w, n], [e], [, s]] = [tabla.raster.esquinas[0], tabla.raster.esquinas[1], tabla.raster.esquinas[2]];
		const { ancho, alto } = tabla.raster;
		const c = Math.floor(((lng - w) / (e - w)) * ancho), r = Math.floor(((n - lat) / (n - s)) * alto);
		if (c < 0 || r < 0 || c >= ancho || r >= alto) return null;
		const v = vals[r * ancho + c];
		if (!v || v / 4 > stage + 1e-6) return null;
		return { depth: stage - v / 4, desde: v / 4 };
	}

	// colour of a radio under the current layer (used for radios and, through `z`, for buildings)
	function radioColor(p: any): string {
		if (capa === 'prioridad' || capa === 'agua') return colorOf(p.prioridad);
		if (capa === 'tipo') return tipoOf(p.tipo_vivienda)?.color ?? '#94a3b8';
		const c = CENSO.find((x) => x.id === capa)!;
		const b = breaksFor(c), cols = PLASMA.slice(PLASMA.length - (b.length + 1)), n = countOf(c, p);
		let k = 0;
		while (k < b.length && n >= b[k]) k++;
		return cols[k];
	}
	function colorPairs(): string[] {
		return (radios?.features ?? []).flatMap((f: any) => [f.properties.id, radioColor(f.properties)]);
	}
	function buildingColor(): any {
		const st = stage + 1e-6;
		const flooded = ['all', ['>=', ['get', 's'], 0], ['<=', ['get', 's'], st]];
		const depthRamp = ['interpolate', ['linear'], ['-', st, ['get', 's']], ...BDEPTH.flat()];
		const isolated = ['all', ['>=', ['get', 'i'], 0], ['<=', ['get', 'i'], st]];
		if (capa === 'agua') return ['case', flooded, depthRamp, isolated, ISO, DRY];
		return ['match', ['get', 'z'], ...colorPairs(), '#64748b'];
	}

	function applyLayers() {
		if (!map || !map.getLayer('radios-fill') || !radios) return;
		const st = stage + 1e-6;
		const reached = ['<=', ['coalesce', ['get', 'primer_impacto'], 99], st];
		const col = ['match', ['get', 'id'], ...colorPairs(), '#64748b'];
		const water = capa === 'agua';
		map.setPaintProperty('radios-fill', 'fill-color', col);
		map.setPaintProperty('radios-fill', 'fill-opacity', water ? 0 : 0.16);
		map.setPaintProperty('radios-line', 'line-color', '#e2e8f0');
		map.setPaintProperty('radios-line', 'line-opacity', water ? ['case', reached, 0.6, 0] : 0.28);
		map.setPaintProperty('radios-line', 'line-width', water ? 1.3 : 0.8);
		map.setPaintProperty('radios-line', 'line-dasharray', water ? [2, 2] : [1, 0]);
		map.setPaintProperty('eb-3d', 'fill-extrusion-color', buildingColor());
		if (map.getLayer('aisl-pts')) {
			map.setFilter('aisl-pts', ['all', ['<=', ['get', 'i'], st], ['any', ['<', ['get', 's'], 0], ['>', ['get', 's'], st]]]);
			map.setLayoutProperty('aisl-pts', 'visibility', water ? 'visible' : 'none');
		}
		if (map.getLayer('cortes')) {
			map.setFilter('cortes', ['<=', ['get', 's'], st]);
			map.setFilter('cortes-casing', ['<=', ['get', 's'], st]);
		}
		// schools and health centres only when they end up under water
		map.setFilter('puntos', ['<=', ['coalesce', ['get', 'primer_impacto'], 99], st]);
	}

	$effect(() => { stage; capa; applyLayers(); });
	$effect(() => { stage; paintWater(); });
	$effect(() => {
		const data = starData();
		const src = map?.getSource('stars');
		if (src) src.setData(data);
	});
	$effect(() => {
		const h = starHover;
		if (map?.getLayer('stars')) map.setLayoutProperty('stars', 'icon-size', ['case', ['==', ['get', 'id'], h], 1.2, 0.85]);
	});
	$effect(() => {
		const id = selected?.id ?? starHover ?? '';
		if (map?.getLayer('radio-sel')) map.setFilter('radio-sel', ['==', ['get', 'id'], id]);
	});
	$effect(() => {
		const pts = lassoPoly ?? lassoPts;
		const src = map?.getSource('lasso');
		if (!src) return;
		const ring = pts.length ? [...pts, ...(lassoPoly ? [pts[0]] : [])] : [];
		src.setData({ type: 'FeatureCollection', features: ring.length > 1 ? [
			{ type: 'Feature', properties: {}, geometry: lassoPoly ? { type: 'Polygon', coordinates: [ring] } : { type: 'LineString', coordinates: ring } }
		] : [] });
	});

	function startLasso() {
		lassoOn = true; lassoPts = []; lassoPoly = null; selected = null;
		map?.doubleClickZoom.disable();
		if (map) map.getCanvas().style.cursor = 'crosshair';
	}
	function endLasso(close: boolean) {
		if (close && lassoPts.length >= 3) lassoPoly = [...lassoPts];
		lassoOn = false; lassoPts = [];
		if (map) { map.getCanvas().style.cursor = ''; setTimeout(() => map.doubleClickZoom.enable(), 300); }
	}
	function clearLasso() { lassoPoly = null; lassoPts = []; }

	function flyToRadio(id: string) {
		const f = radioById.get(id);
		if (!f || !map) return;
		const xs: number[] = [], ys: number[] = [];
		const walk = (c: any) => (typeof c[0] === 'number' ? (xs.push(c[0]), ys.push(c[1])) : c.forEach(walk));
		walk(f.geometry.coordinates);
		map.fitBounds([[Math.min(...xs), Math.min(...ys)], [Math.max(...xs), Math.max(...ys)]], { padding: 80, maxZoom: 16, pitch: 50 });
		selected = f.properties;
	}
	function view(v: typeof TOWN_WIDE) { map?.flyTo({ ...v, duration: 1200 }); }

	function tooltipHtml(e: any): string | null {
		const feats = map.queryRenderedFeatures(e.point, { layers: ['puntos', 'eb-3d', 'radios-fill'] });
		const lines: string[] = [];
		const pt = feats.find((f: any) => f.layer.id === 'puntos');
		const b = feats.find((f: any) => f.layer.id === 'eb-3d');
		let z = feats.find((f: any) => f.layer.id === 'radios-fill')?.properties;
		if (pt) {
			const s = num(pt.properties.primer_impacto);
			lines.push(`<b>${pt.properties.nombre}</b>`,
				s == null ? 'Fuera del alcance de los escenarios' : `El agua llega con ${m(s)}` + (s <= stage + 1e-6 ? ` · <span class="r">bajo ${m(stage - s)} de agua</span>` : ' · seca a esta altura'));
		} else if (b) {
			const p = b.properties, s = num(p.s);
			z = radioById.get(p.z)?.properties ?? z;
			lines.push(`<b>${COND[+p.c]}</b>${+p.r ? ' · barrio popular' : ''}`);
			lines.push(`${fmt(+p.a)} m² · ${dec(+p.h)} m de alto · ${+p.c === 1 ? `<b>≈ ${dec(+p.p)} personas</b> (estimado por tamaño)` : 'sin personas asignadas'}`);
			if (+p.c === 1 && z && +z.c_hogares > 0)
				lines.push(`<span class="dim">En su radio hay 1 hogar cada ${dec(+z.viv_probables / +z.c_hogares)} construcciones de este tamaño: muchas son galpones o anexos</span>`);
			lines.push(s == null ? 'El agua no llega en ningún escenario' : `El agua llega con ${m(s)} en el hidrómetro`
				+ (s <= stage + 1e-6 ? ` · <span class="r">con el río a ${m(stage)}: ≈ ${m(stage - s)} de agua</span>` : ''));
		} else {
			const w = depthAt(e.lngLat.lng, e.lngLat.lat);
			if (w) lines.push(w.desde <= (tabla?.s_ref ?? 2.5) + 0.3
				? `<b>Cauce del río</b><br>≈ ${m(stage - (tabla?.s_ref ?? 2.1))} por encima de su nivel habitual`
				: `<b>Agua: ≈ ${m(w.depth)} sobre el terreno</b><br>Aquí el agua llega desde ${m(w.desde)} en el hidrómetro`);
		}
		if (z) {
			const t = tipoOf(z.tipo_vivienda);
			lines.push(`${z.nombre} <span class="dim">· radio censal ${z.id}</span>`,
				`<span class="dim">${PRIO.find((x) => x.id === z.prioridad)?.label ?? ''} · ${t?.label ?? ''} · NBI ${pct(+z.c_nbi)} (Misiones ${pct(tabla?.provincia.nbi ?? 0)})</span>`);
			if (censoSel) lines.push(`${censoSel.label}: <b>${fmt(countOf(censoSel, z))}</b> <span class="dim">(${pct(shareOf(censoSel, z))})</span>`);
		}
		return lines.length ? lines.join('<br>') : null;
	}

	onMount(() => {
		const base = (import.meta.env.DEV && page.url.searchParams.get('src')) || CDN;
		let cancelled = false;
		(async () => {
			const [{ default: maplibregl }, { Protocol }] = await Promise.all([import('maplibre-gl'), import('pmtiles')]);
			if (cancelled) return;
			maplibregl.addProtocol('pmtiles', new Protocol().tile);
			let t: Tabla;
			try {
				const get = async (f: string) => {
					const r = await fetch(`${base}/${f}`);
					if (!r.ok) throw new Error(`${f}: ${r.status}`);
					return r.json();
				};
				let pts: any;
				[t, pts, radios, puntos, caminos] = await Promise.all([
					get('tabla_altura.json'), get('edificios_pts.json'), get('radios.geojson'), get('puntos.geojson'), get('caminos.geojson')
				]);
				B = pts;
				for (const f of radios.features) radioById.set(f.properties.id, f);
				const zi = new Map(B.radios.map((id: string, i: number) => [id, i]));
				for (const u of t.umbrales) {
					const k = zi.get(u.id);
					let sx = 0, sy = 0, n = 0;
					for (let i = 0; i < B.x.length; i++) {
						const s = B.s[i];
						if (B.z[i] === k && s != null && s <= u.altura + 1e-6) { sx += B.x[i]; sy += B.y[i]; n++; }
					}
					if (n) starPos.set(u.id, [sx / n, sy / n]);
				}
				const img = new Image();
				img.crossOrigin = 'anonymous';
				img.src = `${base}/agua.png`;
				await img.decode();
				const { ancho, alto } = t.raster;
				const tmp = document.createElement('canvas');
				tmp.width = ancho; tmp.height = alto;
				const tctx = tmp.getContext('2d', { willReadFrequently: true })!;
				tctx.drawImage(img, 0, 0);
				const px = tctx.getImageData(0, 0, ancho, alto).data;
				vals = new Uint8Array(ancho * alto);
				let n = 0;
				for (let i = 0; i < vals.length; i++) { vals[i] = px[i * 4]; if (vals[i]) n++; }
				wet = new Int32Array(n);
				for (let i = 0, k = 0; i < vals.length; i++) if (vals[i]) wet[k++] = i;
			} catch (e) {
				error = 'No se pudieron cargar los datos de la demo.';
				return;
			}
			tabla = t;
			setStage(t.escenarios['2023'].altura_m, true);

			const wc = document.createElement('canvas');
			wc.width = t.raster.ancho; wc.height = t.raster.alto;
			wctx = wc.getContext('2d')!;
			const raw = document.createElement('canvas');
			raw.width = wc.width; raw.height = wc.height;
			wraw = raw.getContext('2d')!;
			wimg = wraw.createImageData(wc.width, wc.height);

			map = new maplibregl.Map({ container, style: BASEMAP, ...town(), maxPitch: 70, attributionControl: { compact: true } });
			map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'top-left');
			if (import.meta.env.DEV) (window as any).__demo = map;   // for the local Playwright check
			map.on('load', () => {
				const firstSymbol = map.getStyle().layers.find((l: any) => l.type === 'symbol')?.id;
				map.addSource('radios', { type: 'geojson', data: radios });
				map.addLayer({ id: 'radios-fill', type: 'fill', source: 'radios', paint: { 'fill-color': '#64748b', 'fill-opacity': 0 } }, firstSymbol);
				map.addSource('agua', { type: 'canvas', canvas: wc, coordinates: t.raster.esquinas, animate: false });
				map.addLayer({ id: 'agua', type: 'raster', source: 'agua',
					paint: { 'raster-resampling': 'linear', 'raster-opacity': 0.9, 'raster-fade-duration': 0 } }, firstSymbol);
				map.addLayer({ id: 'radios-line', type: 'line', source: 'radios', paint: { 'line-color': '#e2e8f0', 'line-width': 1, 'line-opacity': 0 } });
				map.addLayer({ id: 'radio-sel', type: 'line', source: 'radios', filter: ['==', ['get', 'id'], ''],
					paint: { 'line-color': '#22d3ee', 'line-width': 3 } });
				// context: the site's building tiles outside the municipality only
				// at low zoom (municipality view) the 3D tiles are not loaded: buildings that can be isolated as points
				const aisl = { type: 'FeatureCollection', features: (B.i ?? []).flatMap((iv: number, k: number) => iv >= 0
					? [{ type: 'Feature', geometry: { type: 'Point', coordinates: [B.x[k], B.y[k]] }, properties: { i: iv, s: B.s[k] ?? -1 } }] : []) };
				map.addSource('aisl', { type: 'geojson', data: aisl });
				map.addLayer({ id: 'aisl-pts', type: 'circle', source: 'aisl', maxzoom: 12.5, filter: ['<=', ['get', 'i'], 0],
					paint: { 'circle-color': ISO, 'circle-opacity': 0.6,
						'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 0.9, 12, 2.2] } });
				// roads the model cuts at the chosen height ("posible corte"): only those are drawn
				map.addSource('caminos', { type: 'geojson', data: caminos });
				map.addLayer({ id: 'cortes-casing', type: 'line', source: 'caminos', filter: ['<=', ['get', 's'], 0],
					layout: { 'line-cap': 'round' }, paint: { 'line-color': '#0b0d12', 'line-width': 6 } });
				map.addLayer({ id: 'cortes', type: 'line', source: 'caminos', filter: ['<=', ['get', 's'], 0],
					layout: { 'line-cap': 'round' }, paint: { 'line-color': '#ef4444', 'line-width': 3 } });
				map.addSource('buildings', { type: 'vector', url: getTilesUrl('buildings') });
				map.addLayer({ id: 'ctx-3d', type: 'fill-extrusion', source: 'buildings', 'source-layer': 'buildings', minzoom: 12,
					filter: ['!', ['in', ['get', 'redcode'], ['literal', B.radios]]],
					paint: {
						'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 3], 3], 60],
						'fill-extrusion-color': '#2b313b', 'fill-extrusion-opacity': 0.8
					} });
				map.addSource('eb', { type: 'vector', url: `pmtiles://${CDN}/edificios_v5.pmtiles` });
				map.addLayer({ id: 'eb-3d', type: 'fill-extrusion', source: 'eb', 'source-layer': 'b', minzoom: 12,
					paint: { 'fill-extrusion-height': ['get', 'h'], 'fill-extrusion-color': DRY, 'fill-extrusion-opacity': 0.95 } });
				map.addSource('puntos', { type: 'geojson', data: puntos });
				map.addLayer({ id: 'puntos', type: 'circle', source: 'puntos', minzoom: 11,
					paint: {
						'circle-radius': ['interpolate', ['linear'], ['zoom'], 11, 3.5, 16, 7],
						'circle-color': '#ef4444', 'circle-stroke-color': '#ffffff', 'circle-stroke-width': 1.5,
						'circle-pitch-alignment': 'map'
					} });
				// numbered stars for "Dónde llega primero"
				t.umbrales.forEach((u, i) => {
					if (!map.hasImage(`star-${i + 1}`)) map.addImage(`star-${i + 1}`, starImage(i + 1, colorOf(u.prioridad)), { pixelRatio: 2 });
				});
				map.addSource('stars', { type: 'geojson', data: starData() });
				map.addLayer({ id: 'stars', type: 'symbol', source: 'stars',
					layout: {
						'icon-image': ['concat', 'star-', ['to-string', ['get', 'n']]],
						'icon-allow-overlap': true, 'icon-ignore-placement': true, 'icon-size': 0.85,
						'symbol-sort-key': ['-', 0, ['get', 'n']]
					} });
				// barrio name next to each star, only when zoomed in; labels that would collide are dropped
				map.addLayer({ id: 'star-labels', type: 'symbol', source: 'stars', minzoom: 13.5,
					layout: {
						'text-field': ['get', 'nombre'], 'text-font': ['Montserrat Medium'], 'text-size': 11.5,
						'text-anchor': 'left', 'text-offset': [1.15, 0], 'text-max-width': 12,
						'symbol-sort-key': ['get', 'n']
					},
					paint: { 'text-color': '#f1f5f9', 'text-halo-color': 'rgba(11, 13, 18, 0.92)', 'text-halo-width': 1.6 } });
				map.addSource('lasso', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
				map.addLayer({ id: 'lasso-fill', type: 'fill', source: 'lasso', filter: ['==', '$type', 'Polygon'],
					paint: { 'fill-color': '#22d3ee', 'fill-opacity': 0.12 } });
				map.addLayer({ id: 'lasso-line', type: 'line', source: 'lasso',
					paint: { 'line-color': '#22d3ee', 'line-width': 2.5, 'line-dasharray': [3, 1.5] } });

				const tip = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 10, maxWidth: '340px', className: 'demo-tip' });
				map.on('mousemove', (e: any) => {
					if (lassoOn) { tip.remove(); return; }
					const html = tooltipHtml(e);
					if (html) { tip.setLngLat(e.lngLat).setHTML(html).addTo(map); map.getCanvas().style.cursor = 'pointer'; }
					else { tip.remove(); map.getCanvas().style.cursor = ''; }
				});
				map.on('mouseout', () => tip.remove());
				// click on a building or on empty ground: highlight its census radio and open the radio card
				map.on('click', (e: any) => {
					if (lassoOn) { lassoPts = [...lassoPts, [e.lngLat.lng, e.lngLat.lat]]; return; }
					const b = map.queryRenderedFeatures(e.point, { layers: ['eb-3d'] })[0];
					const id = b?.properties.z ?? map.queryRenderedFeatures(e.point, { layers: ['radios-fill'] })[0]?.properties.id;
					if (id && radioById.has(id)) selected = radioById.get(id).properties;
				});
				map.on('dblclick', (e: any) => { if (lassoOn) { e.preventDefault(); endLasso(true); } });
				applyLayers();
				paintWater();
			});
		})();
		const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape' && lassoOn) endLasso(false); };
		window.addEventListener('keydown', onKey);
		return () => { cancelled = true; window.removeEventListener('keydown', onKey); map?.remove(); };
	});
</script>

<svelte:head>
	<title>El Niño en El Soberbio · Spatia</title>
	<meta name="robots" content="noindex, nofollow" />
</svelte:head>

<div class="demo">
	<div class="mapwrap">
		<div class="map" bind:this={container}></div>
		<div class="tools">
			<button onclick={() => view(town())}>Pueblo 3D</button>
			<button onclick={() => view(MUNI)}>Municipio</button>
			{#if lassoOn}
				<button class="on" onclick={() => endLasso(true)}>Cerrar área · {lassoPts.length}</button>
			{:else}
				<button onclick={startLasso}>Medir un área</button>
			{/if}
		</div>
		{#if lassoOn}
			<div class="hint">Clic para marcar los vértices · doble clic para cerrar · Esc para cancelar</div>
		{/if}
	</div>

	<aside class="panel">
		<header>
			<p class="kicker">Río Uruguay · El Soberbio · El Niño 2026-27</p>
			<h1>Si el río llega a…</h1>
		</header>

		{#if error}
			<p class="warn">{error}</p>
		{:else if tabla && pasoActual}
			<section class="control">
				<div class="stage"><span class="big">{m(stage)}</span><span class="dim">en la escala de Prefectura del puerto de El Soberbio</span></div>
				<div class="slider">
					<input type="range" min={minStage} max={maxStage} step="0.5" value={stage}
						oninput={(e) => setStage(+(e.currentTarget as HTMLInputElement).value)} aria-label="Altura del río" />
					{#each SCEN as s}
						{@const f = (tabla.escenarios[s.id].altura_m - minStage) / (maxStage - minStage)}
						<span class="tick" style:left={`calc(${f * 100}% + ${8 - f * 16}px)`}></span>
					{/each}
					{#each COTAS as c}
						{@const f = (c.h - minStage) / (maxStage - minStage)}
						<span class="cota {c.side}" style:left={`calc(${f * 100}% + ${8 - f * 16}px)`}>{c.label} {c.h}</span>
					{/each}
				</div>
				<div class="scen">
					{#each SCEN as s}
						{@const e = tabla.escenarios[s.id]}
						<button class:on={Math.abs(stage - e.altura_m) < 0.01} onclick={() => setStage(e.altura_m, true)} title={`${fecha(e.fecha)} · ${e.fuente ?? ''}`}>
							<b>{s.label}</b><span>{m(e.altura_m)}{s.note ? ` · ${s.note}` : ''}</span>
						</button>
					{/each}
				</div>
			</section>

			<section class="impact">
				{#if pasoActual.viviendas < 1}
					<p class="lead">A esta altura el agua no llega a viviendas.</p>
				{:else}
					<p class="lead"><b>{fmt(pasoActual.viviendas)} viviendas</b> y <b>{approx(pasoActual.personas)} personas</b> quedarían bajo el agua, entre ellas {approx(pasoActual.ninos)} niños de 0 a 5 años y {approx(pasoActual.mayores)} mayores de 65.</p>
				{/if}
				{#if pasoActual.aisladas_viv > 0}
					<p class="iso">Además, <b>{fmt(pasoActual.aisladas_viv)} viviendas</b> quedarían aisladas: secas, pero sin camino al hospital de El Soberbio. <button class="lnk" onclick={() => { capa = 'agua'; view(MUNI); }}>Ver en el mapa</button></p>
				{/if}
				<p class="roads">
					{#if cortados.length}<span class="rl"></span>Posible corte de {[...rutasCortadas.map((c) => `${c.nombre.replace(' (RP2)', '')} (${m(c.altura)})`), ...(callesCortadas.length ? [`${callesCortadas.length} ${callesCortadas.length === 1 ? 'calle' : 'calles'} del pueblo`] : [])].join(' · ')}.
					{:else if proximoCorte}Primer corte de camino: {proximoCorte.nombre}, con {m(proximoCorte.altura)}.{/if}
				</p>
				<p class="sub">
					{fmt(pasoActual.viv_renabap)} en barrios populares · {pasoActual.escuelas} {pasoActual.escuelas === 1 ? 'escuela' : 'escuelas'} bajo el agua, {pasoActual.escuelas_secas} secas para refugio
					{#if pasoMenos && pasoMas}· con ±1 m de error del terreno, entre {fmt(pasoMenos.viviendas)} y {fmt(pasoMas.viviendas)} viviendas{/if}
				</p>
			</section>

			{#if lassoRes}
				<section class="card">
					<button class="close" onclick={clearLasso} aria-label="Borrar área">×</button>
					<h2>Área medida</h2>
					<p class="sub">{fmt(lassoRes.total.n)} construcciones · {fmt(lassoRes.total.viv)} viviendas · {approx(lassoRes.total.pers)} personas</p>
					<table>
						<thead><tr><th>Bajo el agua</th><th>{m(stage)}</th>{#each SCEN as s}<th>{s.id}</th>{/each}</tr></thead>
						<tbody>
							{#each [['viv', 'Viviendas'], ['pers', 'Personas'], ['ninos', 'Niños 0-5'], ['mayores', 'Mayores 65+'], ['ren', 'Barrio popular'], ['esc', 'Escuelas']] as [k, l]}
								<tr><td>{l}</td>{#each lassoRes.out as r, i}<td class:cur={i === 0}>{k === 'pers' || k === 'ninos' || k === 'mayores' ? approx(r[k as keyof Lasso]) : fmt(r[k as keyof Lasso])}</td>{/each}</tr>
							{/each}
						</tbody>
					</table>
				</section>
			{/if}

			<section>
				<h2>Dónde llega primero</h2>
				<ul class="umbrales">
					{#each alcanzadas as u}
						<li><button onclick={() => flyToRadio(u.id)} onmouseenter={() => (starHover = u.id)} onmouseleave={() => (starHover = '')}>
							<svg class="star" viewBox="0 0 24 24" aria-hidden="true"><path d={STAR_PATH} fill={colorOf(u.prioridad)} stroke="#0a0a0a" stroke-width="1.1" /><text x="12" y="13.9" text-anchor="middle" dominant-baseline="middle" font-size={(numOf.get(u.id) ?? 0) > 9 ? 7.6 : 9.2} font-weight="800" fill={starText(colorOf(u.prioridad))}>{numOf.get(u.id)}</text></svg>
							<span class="nm">{u.microzona}</span><span class="h">{m(u.altura)}</span>
						</button></li>
					{/each}
					{#each proximas as u}
						<li class="next"><button onclick={() => flyToRadio(u.id)}>
							<span class="dot-next"></span><span class="nm">{u.microzona}</span><span class="h">{m(u.altura)}</span>
						</button></li>
					{/each}
				</ul>
				{#if alcanzadas.length === 0}<p class="sub">Todavía no llega a ningún barrio.</p>{/if}
				<p class="note">Altura desde la que el agua entra a las viviendas de cada radio censal: propuesta de umbral de aviso, no oficial.</p>
			</section>

			<section>
				<h2>Ver en el mapa</h2>
				<div class="tabs" role="tablist">
					{#each [['agua', 'Agua'], ['prioridad', 'Prioridad'], ['tipo', 'Vivienda'], ['censo', 'Censo']] as [k, l]}
						<button role="tab" aria-selected={tab === k} class:on={tab === k} onclick={() => (capa = k === 'censo' ? (tab === 'censo' ? capa : 'mayores') : k)}>{l}</button>
					{/each}
				</div>
				{#if tab === 'censo'}
					<select bind:value={capa} aria-label="Variable del censo">
						{#each CENSO as c}<option value={c.id}>{c.label}</option>{/each}
					</select>
				{/if}
				<div class="legend">
					{#if tab === 'agua'}
						<div class="ramp"><span>Agua</span><i class="bar" style:background={WGRAD}></i><span class="dim">0–10 m</span></div>
						<div class="ramp"><span>Casas</span><i class="bar" style:background={`linear-gradient(to right, ${BDEPTH.map(([, c]) => c).join(', ')})`}></i><span class="dim">0–5 m</span></div>
						<div class="keys"><span><i style:background={ISO}></i>Aislada: seca, sin camino al hospital</span><span><i class="road"></i>Camino posiblemente cortado</span></div>
						<p class="note">Gris: secas. Línea punteada: radios censales a los que ya llega el agua.</p>
					{:else if tab === 'prioridad'}
						<div class="keys">{#each PRIO as p}<span><i style:background={p.color}></i>{p.label}</span>{/each}</div>
						<p class="note">Combina cuán pronto llega el agua con cuántas privaciones tiene el radio.</p>
					{:else if tab === 'tipo'}
						<div class="keys">{#each TIPOS as p}<span title={p.def}><i style:background={p.color}></i>{p.label}</span>{/each}</div>
						<p class="note">Tipo de vulnerabilidad de la vivienda por radio censal (censo 2022, contra el promedio de Misiones).</p>
					{:else if censoSel}
						<div class="keys">{#each censoKeys as r}<span><i style:background={r.color}></i>{r.label} {censoSel.unit}</span>{/each}</div>
						<p class="note">Cantidad por radio censal (censo 2022). Los radios rurales grandes suman más porque tienen más gente: la ficha da también el porcentaje.</p>
					{/if}
					<p class="note radio-def">Cada zona es un radio censal del INDEC (en el pueblo, unas 200 a 300 viviendas) y lleva el nombre de los barrios que contiene.</p>
				</div>
			</section>

			{#if selected}
				{@const s0 = num(selected.primer_impacto)}
				{@const tv = tipoOf(selected.tipo_vivienda)}
				<section class="card">
					<button class="close" onclick={() => (selected = null)} aria-label="Cerrar">×</button>
					<h2>{selected.nombre}</h2>
					<p class="sub">Radio censal {selected.id} · {selected.tipo === 'rural' ? 'rural' : 'urbano'} · {fmt(+selected.c_personas)} personas · {fmt(+selected.c_hogares)} hogares</p>
					<p class="tags">
						<span><i style:background={colorOf(selected.prioridad)}></i>{PRIO.find((p) => p.id === selected?.prioridad)?.label}</span>
						<span><i style:background={tv?.color}></i>{tv?.label}</span>
					</p>
					<p>{#if s0 != null}El agua entra a sus viviendas con {m(s0)}; el río superó esa altura {selected.anios_alcanzada} {+selected.anios_alcanzada === 1 ? 'año' : 'años'} desde 1980.{:else}El agua no llega a sus viviendas en los escenarios.{/if}</p>
					{#if s0 != null}
						<table>
							<thead><tr><th>Bajo el agua</th>{#each SCEN as s}<th>{s.id}</th>{/each}</tr></thead>
							<tbody>
								{#each [['viviendas', 'Viviendas'], ['personas', 'Personas'], ['ninos', 'Niños 0-5'], ['mayores', 'Mayores 65+'], ['viv_renabap', 'Barrio popular']] as [k, l]}
									<tr><td>{l}</td>{#each SCEN as s}<td>{fmt(+selected[`esc${s.id}_${k}`])}</td>{/each}</tr>
								{/each}
							</tbody>
						</table>
						{@const refs = SCEN.filter((s) => +selected?.[`esc${s.id}_viviendas`] > 0)}
						{#if refs.length}
							<p class="ref"><b>Refugio seco más cercano por camino</b>
								{#each refs as s}<br /><span class="dim">{s.label}:</span> {selected[`esc${s.id}_refugio`] && selected[`esc${s.id}_refugio`] !== 'null' ? `${selected[`esc${s.id}_refugio`]}, a ${dec(+selected[`esc${s.id}_refugio_km`])} km` : 'ninguna escuela seca accesible'}{/each}
							</p>
							<p class="note">Capacidad de los refugios: dato a pedir a Defensa Civil.</p>
						{/if}
					{/if}
					<div class="bars">
						{#each CENSO as c}
							{@const z = shareOf(c, selected)}
							{@const pr = provOf(c)}
							<!-- fixed scale: the provincial average sits at the middle, the end of the track is twice it -->
							<div class="bar">
								<span class="bl">{c.label.replace('Hogares ', '').replace('Personas de ', '').replace(/^con /, '')}</span>
								<span class="track"><span class="z" class:hi={z > pr} style:width={`${Math.min(z / (2 * pr), 1) * 100}%`}></span><span class="pv"></span></span>
								<span class="bv"><b>{fmt(countOf(c, selected))}</b> · {pct(z)}</span>
							</div>
						{/each}
					</div>
					<p class="note">Censo 2022: cantidad en el radio y porcentaje. La marca blanca es el promedio de Misiones; la barra llena, el doble.</p>
				</section>
			{/if}

			<details>
				<summary>El río desde 1980</summary>
				<svg class="peaks" viewBox="0 0 300 64" preserveAspectRatio="none" aria-label="Picos anuales del hidrómetro">
					{#each tabla.picos as p, i}
						{@const w = 300 / tabla.picos.length}
						{@const h = (p.altura / 30) * 56}
						<rect x={i * w + 0.5} y={60 - h} width={Math.max(w - 1, 1)} height={h} fill={p.altura >= stage - 1e-6 ? '#38bdf8' : '#334155'}><title>{p.anio}: {m(p.altura)}</title></rect>
					{/each}
					<line x1="0" x2="300" y1={60 - (stage / 30) * 56} y2={60 - (stage / 30) * 56} stroke="#f8fafc" stroke-width="1" stroke-dasharray="3 2" />
				</svg>
				<p class="sub">Lectura diaria de Prefectura: el río superó {m(stage)} en <b>{aniosSobre}</b> de {tabla.picos.length} años con registro.</p>
			</details>

			<details>
				<summary>Cómo leer esta demo</summary>
				<div class="fine">
					<p><b>Escenarios, no pronóstico.</b> Muestran qué pasaría si el río llega a esa altura. Sólo crecida del Uruguay: no incluye lluvia ni arroyos.</p>
					<p><b>Calibrado con siete crecidas documentadas (2014-2026):</b> el terreno satelital se ajustó {dec(tabla.calibracion_delta ?? 0)} m para que coincidan las alturas a las que se evacuó (por ejemplo, Maracaná y Chivilcoy desde 13 m). Las marcas de crecida de la provincia lo vuelven exacto.</p>
					<p><b>Escala:</b> la de Prefectura en el puerto de El Soberbio, la misma que usa Defensa Civil. Los escenarios son el pico de cada crecida; la historia usa la lectura diaria de Prefectura (serie del INA), que puede quedar hasta 2 m por debajo del pico.</p>
					<p><b>Caminos y aislamiento:</b> red de OpenStreetMap; un camino se corta cuando el agua cubre algún tramo. El terreno no ve terraplenes: los accesos a puentes se ajustaron con los cierres documentados del Yabotí (16,1 m) y del arroyo El Soberbio. Se dice «posible corte».</p>
					<p><b>Personas por casa:</b> la población censal del radio repartida por superficie entre las viviendas probables (25 a 400 m²).</p>
					<p><b>Fuentes:</b> INA, FABDEM, Censo 2022 (INDEC), RENABAP, edificios Google y Microsoft, OpenStreetMap, Mapa para la Gestión de la Emergencia.</p>
				</div>
			</details>

			<footer>CONICET / FHyCS-UNaM · Spatia</footer>
		{:else}
			<p class="dim">Cargando…</p>
		{/if}
	</aside>
</div>

<style>
	.demo { position: fixed; inset: 0; display: flex; background: #0b0d12; color: #e5e7eb; font-size: 13.5px;
		font-family: system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif; }
	.mapwrap { flex: 1; min-width: 0; position: relative; }
	.map { position: absolute; inset: 0; }
	.tools { position: absolute; top: 12px; left: 52px; display: flex; gap: 4px; z-index: 2; }
	.tools button { padding: 5px 10px; border-radius: 999px; background: rgba(11, 13, 18, 0.78); border: 1px solid rgba(148, 163, 184, 0.25);
		color: #cbd5e1; cursor: pointer; font: inherit; font-size: 12px; backdrop-filter: blur(6px); }
	.tools button:hover { color: #fff; border-color: rgba(148, 163, 184, 0.5); }
	.tools button.on { border-color: #22d3ee; color: #22d3ee; }
	.hint { position: absolute; bottom: 36px; left: 50%; transform: translateX(-50%); padding: 6px 14px; border-radius: 999px;
		background: rgba(11, 13, 18, 0.9); border: 1px solid #22d3ee; font-size: 12px; z-index: 2; }

	.panel { width: 380px; max-width: 100%; overflow-y: auto; padding: 22px 22px 28px; background: #0f1218;
		border-left: 1px solid #1c2230; display: flex; flex-direction: column; gap: 20px; }
	.kicker { margin: 0; color: #7dd3fc; font-size: 11px; letter-spacing: 0.06em; text-transform: uppercase; }
	h1 { margin: 6px 0 0; font-size: 22px; font-weight: 600; letter-spacing: -0.01em; color: #fff; }
	h2 { margin: 0 0 8px; font-size: 11px; font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; color: #94a3b8; }
	b { color: #fff; font-weight: 600; }
	.dim, .sub { color: #94a3b8; }
	.sub { margin: 6px 0 0; font-size: 12.5px; line-height: 1.5; }
	.note { margin: 8px 0 0; color: #7c8798; font-size: 11.5px; line-height: 1.45; }

	.control { display: flex; flex-direction: column; gap: 10px; }
	.stage { display: flex; align-items: baseline; gap: 10px; }
	.big { white-space: nowrap; font-size: 40px; font-weight: 600; letter-spacing: -0.02em; color: #38bdf8; font-variant-numeric: tabular-nums; }
	.slider { position: relative; }
	.slider input[type='range'] { width: 100%; accent-color: #38bdf8; margin: 0; }
	.tick { position: absolute; top: -3px; width: 2px; height: 8px; margin-left: -1px; background: #64748b; border-radius: 1px; pointer-events: none; }
	.scen { display: grid; grid-template-columns: repeat(4, 1fr); gap: 5px; }
	.scen button { display: flex; flex-direction: column; align-items: flex-start; gap: 1px; padding: 7px 9px; border-radius: 10px;
		background: transparent; border: 1px solid #263042; color: #e5e7eb; cursor: pointer; text-align: left; font: inherit; }
	.scen button b { font-size: 12.5px; }
	.scen button span { color: #94a3b8; font-size: 11px; font-variant-numeric: tabular-nums; }
	.scen button.on { border-color: #38bdf8; background: rgba(56, 189, 248, 0.1); }

	.impact .lead { margin: 0; font-size: 17px; line-height: 1.45; color: #cbd5e1; }
	.impact .lead b { color: #fdba74; }
	.impact .iso { margin: 8px 0 0; font-size: 14px; line-height: 1.45; color: #cbd5e1; }
	.impact .iso b { color: #c4b5fd; }
	.impact .roads { margin: 6px 0 0; font-size: 12.5px; line-height: 1.45; color: #cbd5e1; }
	.impact .roads:empty { display: none; }
	.rl, i.road { display: inline-block; width: 14px; height: 3px; border-radius: 2px; background: #ef4444; vertical-align: middle; margin-right: 6px; }
	.slider { padding-bottom: 14px; }
	.cota { position: absolute; bottom: 0; transform: translateX(-50%); font-size: 10px; color: #fca5a5; white-space: nowrap; pointer-events: none; }
	.cota.left { transform: translateX(calc(-100% + 1px)); padding-right: 3px; }
	.cota.right { transform: none; padding-left: 3px; }
	.cota::before { content: ''; position: absolute; top: -8px; width: 1px; height: 14px; background: #fca5a5; }
	.cota.left::before { right: 0; }
	.cota.right::before { left: 0; }
	.lnk { background: none; border: 0; padding: 0; color: #c4b5fd; text-decoration: underline; cursor: pointer; font: inherit; }
	.ref { font-size: 12.5px; }

	.umbrales { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; }
	.umbrales button { display: flex; gap: 10px; align-items: center; width: 100%; padding: 5px 4px; background: none; border: 0;
		border-radius: 8px; color: #e5e7eb; cursor: pointer; text-align: left; font: inherit; font-size: 13px; }
	.umbrales button:hover { background: rgba(148, 163, 184, 0.08); }
	.umbrales .nm { flex: 1; }
	.umbrales .h { color: #94a3b8; font-size: 12px; font-variant-numeric: tabular-nums; }
	.umbrales .next button { color: #7c8798; }
	.umbrales .star { width: 24px; height: 24px; flex: none; }
	.dot-next { width: 24px; flex: none; display: flex; justify-content: center; }
	.dot-next::before { content: ''; width: 7px; height: 7px; border-radius: 50%; border: 1.5px solid #64748b; }

	.tabs { display: flex; padding: 3px; gap: 2px; border-radius: 10px; background: #161b24; }
	.tabs button { flex: 1; padding: 6px 4px; border: 0; border-radius: 8px; background: none; color: #94a3b8; cursor: pointer; font: inherit; font-size: 12.5px; }
	.tabs button.on { background: #263042; color: #fff; }
	select { width: 100%; margin-top: 8px; padding: 6px 8px; border-radius: 8px; background: #161b24; color: #e5e7eb; border: 1px solid #263042; font: inherit; font-size: 12.5px; }
	.legend { margin-top: 10px; }
	.ramp { display: grid; grid-template-columns: 44px 1fr 46px; gap: 8px; align-items: center; font-size: 11.5px; color: #cbd5e1; margin-bottom: 6px; }
	.ramp .bar { height: 8px; border-radius: 4px; width: auto; }
	.keys { display: flex; flex-wrap: wrap; gap: 6px 12px; font-size: 12px; color: #cbd5e1; }
	.keys span, .tags span { display: inline-flex; align-items: center; gap: 6px; }
	i { display: inline-block; width: 10px; height: 10px; border-radius: 3px; flex: none; font-style: normal; }

	.card { position: relative; padding: 14px 14px 12px; border: 1px solid #263042; border-radius: 12px; background: #131822; }
	.card h2 { text-transform: none; letter-spacing: 0; font-size: 15px; color: #fff; padding-right: 20px; }
	.card p { margin: 6px 0; line-height: 1.45; }
	.tags { display: flex; flex-wrap: wrap; gap: 6px 14px; font-size: 12.5px; }
	.card table { width: 100%; border-collapse: collapse; margin: 8px 0 10px; font-variant-numeric: tabular-nums; font-size: 12.5px; }
	.card th, .card td { padding: 3px 4px; text-align: right; }
	.card th { color: #7c8798; font-weight: 500; }
	.card th:first-child, .card td:first-child { text-align: left; color: #94a3b8; }
	.card td.cur { color: #38bdf8; font-weight: 600; }
	.close { position: absolute; top: 6px; right: 10px; background: none; border: 0; color: #7c8798; font-size: 18px; cursor: pointer; }
	.bars { display: flex; flex-direction: column; gap: 5px; margin-top: 6px; }
	.bar { display: grid; grid-template-columns: 128px 1fr 74px; gap: 8px; align-items: center; font-size: 11.5px; }
	.bl { color: #cbd5e1; }
	.bv { text-align: right; font-variant-numeric: tabular-nums; color: #cbd5e1; }
	.track { position: relative; height: 6px; background: #1c2230; border-radius: 3px; }
	.track .z { position: absolute; left: 0; top: 0; bottom: 0; background: #60a5fa; border-radius: 3px; }
	.track .z.hi { background: #f59e0b; }
	.track .pv { position: absolute; left: 50%; top: -3px; bottom: -3px; width: 2px; background: #e5e7eb; }

	details { border-top: 1px solid #1c2230; padding-top: 12px; }
	summary { cursor: pointer; list-style: none; font-size: 11px; font-weight: 600; letter-spacing: 0.06em; text-transform: uppercase; color: #94a3b8; }
	summary::-webkit-details-marker { display: none; }
	summary::before { content: '+'; display: inline-block; width: 14px; color: #64748b; }
	details[open] summary::before { content: '−'; }
	details > :not(summary) { margin-top: 10px; }
	.peaks { width: 100%; height: 64px; display: block; }
	.fine p { margin: 0 0 6px; font-size: 12px; line-height: 1.5; color: #94a3b8; }
	footer { color: #64748b; font-size: 11.5px; }
	.warn { color: #fca5a5; }

	:global(.demo-tip .maplibregl-popup-content) { background: rgba(15, 18, 24, 0.96); color: #e5e7eb; border: 1px solid #263042;
		border-radius: 10px; padding: 9px 11px; font: 12.5px/1.45 system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif;
		box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4); }
	:global(.demo-tip .maplibregl-popup-tip) { display: none; }
	:global(.demo-tip b) { color: #fff; font-weight: 600; }
	:global(.demo-tip .dim) { color: #94a3b8; }
	:global(.demo-tip .r) { color: #fca5a5; }

	@media (max-width: 760px) {
		.demo { flex-direction: column; }
		.mapwrap { flex: none; height: 52vh; }
		.panel { width: 100%; border-left: 0; border-top: 1px solid #1c2230; padding: 16px 16px 24px; flex: 1; gap: 16px; }
		.big { font-size: 34px; }
		.impact .lead { font-size: 15.5px; }
		.bar { grid-template-columns: 104px 1fr 70px; }
		.tools { left: 48px; }
	}
</style>
