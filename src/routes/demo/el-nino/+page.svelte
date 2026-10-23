<script lang="ts">
	// Demo «El Niño: dónde mirar primero» — El Soberbio, crecida del Uruguay por altura del hidrómetro.
	// Unlisted page with its own map: it does not touch Map.svelte or the analysis registry.
	// Data are precomputed outside this repo and served from R2 under data/demo/el_nino/:
	//   agua.png        one band, value = gauge height (x4) at which water reaches each 20 m pixel, 0 = never
	//   edificios.geojson  buildings near the water (3D + lasso); persons per building are only ever summed
	//   microzonas.geojson census radios / riverside strips with priority, census profile, housing type
	//   puntos.geojson  schools and health centres from the provincial emergency viewer
	//   tabla_altura.json  totals per 0.5 m step, scenarios, thresholds, provincial averages, raster corners
	import { onMount } from 'svelte';
	import 'maplibre-gl/dist/maplibre-gl.css';
	import { page } from '$app/state';
	import { BASEMAP, getTilesUrl } from '$lib/config';
	import { pointInPolygon } from '$lib/utils/geometry';

	const CDN = 'https://cdn.spatia.ar/data/demo/el_nino';
	const GAUGE: [number, number] = [-54.199, -27.298];
	const TOWN_WIDE = { center: [-54.2035, -27.2965] as [number, number], zoom: 14.9, pitch: 55, bearing: -20 };
	const TOWN_NARROW = { center: [-54.1995, -27.2975] as [number, number], zoom: 14.1, pitch: 50, bearing: -20 };
	// phones get a wider town view so the river stays in frame
	const town = () => (typeof window !== 'undefined' && window.innerWidth < 760 ? TOWN_NARROW : TOWN_WIDE);
	const MUNI = { center: [-54.12, -27.22] as [number, number], zoom: 11.2, pitch: 0, bearing: 0 };

	const PRIO = [
		{ id: 'vigilar ya', label: 'Vigilar ya', color: '#ef4444' },
		{ id: 'vigilar', label: 'Vigilar', color: '#f97316' },
		{ id: 'atento', label: 'Atento', color: '#facc15' },
		{ id: 'sin prioridad', label: 'Sin prioridad', color: '#64748b' }
	];
	const TIPOS = [
		{ id: 'precaria', label: 'Construcción precaria', color: '#f472b6', def: 'piso o cielorraso de baja calidad por encima del promedio' },
		{ id: 'saneamiento', label: 'Saneamiento que se contamina', color: '#a78bfa', def: 'pozo sin cámara y agua sin cañería por encima del promedio: con la crecida el pozo desborda' },
		{ id: 'tenencia', label: 'Tenencia insegura', color: '#fb923c', def: 'hogares ni propietarios ni inquilinos por encima del promedio, o mayoría de viviendas en barrio popular' },
		{ id: 'dependientes', label: 'Hogares que no se evacúan solos', color: '#34d399', def: 'niños de 0 a 5 y mayores de 65 por encima del promedio' },
		{ id: 'consolidada', label: 'Consolidada', color: '#94a3b8', def: 'ninguno de los rasgos anteriores' }
	];
	// census layers: zone share vs. provincial share (key in tabla.provincia)
	const CENSO = [
		{ id: 'c_nbi', prov: 'nbi', label: 'Hogares con NBI' },
		{ id: 'c_piso', prov: 'piso', label: 'Piso de baja calidad' },
		{ id: 'c_cielo', prov: 'cielo', label: 'Sin cielorraso' },
		{ id: 'c_pozo', prov: 'pozo', label: 'Desagüe a pozo sin cámara' },
		{ id: 'c_canieria', prov: 'canieria', label: 'Sin agua por cañería' },
		{ id: 'c_hacin', prov: 'hacin', label: 'Hacinamiento' },
		{ id: 'c_tenencia', prov: 'tenencia', label: 'Ni propietarios ni inquilinos' },
		{ id: 'c_dep', prov: 'dep', label: 'Niños 0-5 y mayores 65+' }
	];
	const RATIO = [
		{ lim: 0, color: '#1d4ed8', label: 'menos de ¾ del promedio' },
		{ lim: 0.75, color: '#93c5fd', label: '¾ a 1 vez' },
		{ lim: 1, color: '#fde68a', label: '1 a 1¼ veces' },
		{ lim: 1.25, color: '#f97316', label: '1¼ a 1¾ veces' },
		{ lim: 1.75, color: '#dc2626', label: 'más de 1¾ veces' }
	];
	const DEPTH = [
		{ max: 0.5, rgba: [186, 230, 253, 190], label: 'hasta 0,5 m' },
		{ max: 1.5, rgba: [125, 211, 252, 200], label: '0,5 a 1,5 m' },
		{ max: 3, rgba: [56, 189, 248, 210], label: '1,5 a 3 m' },
		{ max: 6, rgba: [2, 132, 199, 215], label: '3 a 6 m' },
		{ max: 1e9, rgba: [7, 89, 133, 220], label: 'más de 6 m' }
	];
	const SCEN = [
		{ id: '2015', label: 'Como 2015' },
		{ id: '2023', label: 'Como 2023' },
		{ id: '1983', label: 'Como 1983', note: 'récord' }
	];
	const DIMS: [string, string][] = [
		['dim_material', 'vivienda precaria o hacinamiento'],
		['dim_recuperacion', 'hogares con NBI'],
		['dim_dependientes', 'niños y mayores que no se evacúan solos'],
		['dim_aislamiento', 'lejos del hospital, población indígena o barrio popular']
	];

	interface Paso {
		altura: number; viviendas: number; personas: number; ninos: number; mayores: number;
		hog_nbi: number; viv_renabap: number; escuelas: number; salud: number; comunidades: number; microzonas: number;
	}
	interface Tabla {
		escenarios: Record<string, { fecha: string; altura_m: number }>;
		s_ref: number;
		pasos: Paso[];
		umbrales: { altura: number; microzona: string; prioridad: string }[];
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
	let capa = $state('prioridad');
	let lassoOn = $state(false);
	let lassoPts = $state<[number, number][]>([]);
	let lassoPoly = $state<[number, number][] | null>(null);
	let zonas: any = null;
	let puntos: any = null;
	// buildings as flat arrays for the lasso
	let B = { x: new Float64Array(0), y: new Float64Array(0), s: new Float32Array(0), v: new Int32Array(0),
		p: new Int32Array(0), r: new Uint8Array(0), z: [] as string[] };
	let zshare = new Map<string, { s05: number; s65: number }>();
	// water raster
	let vals: Uint8Array | null = null;
	let wet: Int32Array | null = null;
	let wctx: CanvasRenderingContext2D | null = null;
	let wraw: CanvasRenderingContext2D | null = null;
	let wimg: ImageData | null = null;

	const pasoActual = $derived.by(() => {
		if (!tabla) return null;
		let best = tabla.pasos[0];
		for (const p of tabla.pasos) if (p.altura <= stage + 1e-6) best = p;
		return best;
	});
	const minStage = $derived(tabla ? tabla.pasos[0].altura : 2.5);
	const maxStage = $derived(tabla ? tabla.pasos[tabla.pasos.length - 1].altura : 28);
	const alcanzadas = $derived(tabla ? tabla.umbrales.filter((u) => u.altura <= stage + 1e-6) : []);
	const proximas = $derived(tabla ? tabla.umbrales.filter((u) => u.altura > stage + 1e-6).slice(0, 3) : []);
	const censoSel = $derived(CENSO.find((c) => c.id === capa));
	const lassoRes = $derived.by(() => {
		if (!lassoPoly || !tabla) return null;
		const alturas = [stage, ...SCEN.map((s) => tabla!.escenarios[s.id].altura_m)];
		const out: Lasso[] = alturas.map(() => ({ viv: 0, pers: 0, ninos: 0, mayores: 0, ren: 0, esc: 0 }));
		const bb = bbox(lassoPoly);
		for (let i = 0; i < B.x.length; i++) {
			if (!(B.s[i] < 99)) continue;
			const x = B.x[i], y = B.y[i];
			if (x < bb[0] || x > bb[2] || y < bb[1] || y > bb[3] || !pointInPolygon([x, y], lassoPoly)) continue;
			const zs = zshare.get(B.z[i]) ?? { s05: 0, s65: 0 };
			alturas.forEach((h, k) => {
				if (B.s[i] <= h + 1e-6) {
					const o = out[k];
					o.viv += B.v[i]; o.pers += B.p[i]; o.ninos += B.p[i] * zs.s05; o.mayores += B.p[i] * zs.s65;
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
		return out;
	});

	const fmt = (n: number) => Math.round(n).toLocaleString('es-AR');
	const approx = (n: number) => (n < 20 ? fmt(n) : '≈ ' + fmt(Math.round(n / 10) * 10));
	const m = (x: number) => x.toLocaleString('es-AR', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + ' m';
	const pct = (x: number) => (x * 100).toLocaleString('es-AR', { maximumFractionDigits: 0 }) + ' %';
	const colorOf = (p: string) => PRIO.find((x) => x.id === p)?.color ?? '#64748b';
	const tipoOf = (t: string) => TIPOS.find((x) => x.id === t);
	const fecha = (iso: string) => { const [y, mo, d] = iso.split('-'); return `${+d}/${+mo}/${y}`; };
	const num = (v: any) => (v === null || v === undefined || v === 'null' ? null : +v);

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
		if (!vals || !wet || !wctx || !wimg) return;
		const d = wimg.data;
		d.fill(0);
		const st4 = stage * 4 + 1e-6;
		for (let k = 0; k < wet.length; k++) {
			const i = wet[k], v = vals[i];
			if (v > st4) continue;
			const depth = stage - v / 4;
			let c = DEPTH[DEPTH.length - 1].rgba;
			for (const b of DEPTH) if (depth <= b.max) { c = b.rgba; break; }
			const o = i * 4;
			d[o] = c[0]; d[o + 1] = c[1]; d[o + 2] = c[2]; d[o + 3] = c[3];
		}
		// paint on a scratch canvas, then draw it blurred by ~1 px so 20 m pixels do not show as blocks
		wraw!.putImageData(wimg, 0, 0);
		wctx.clearRect(0, 0, wimg.width, wimg.height);
		wctx.filter = 'blur(1px)';
		wctx.drawImage(wraw!.canvas, 0, 0);
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

	function zoneColorExpr(): any {
		if (capa === 'prioridad') return ['match', ['get', 'prioridad'], ...PRIO.flatMap((p) => [p.id, p.color]), '#64748b'];
		if (capa === 'tipo') return ['match', ['get', 'tipo_vivienda'], ...TIPOS.flatMap((p) => [p.id, p.color]), '#94a3b8'];
		const c = CENSO.find((x) => x.id === capa)!;
		const prov = tabla?.provincia[c.prov] ?? 1;
		return ['step', ['/', ['get', c.id], prov], RATIO[0].color, ...RATIO.slice(1).flatMap((r) => [r.lim, r.color])];
	}

	function applyStage() {
		if (!map || !map.getLayer('zonas-fill')) return;
		const st = stage + 1e-6;
		const reached = ['<=', ['coalesce', ['get', 'primer_impacto'], 99], st];
		map.setPaintProperty('zonas-fill', 'fill-opacity', ['case', reached, 0.26, 0.06]);
		map.setPaintProperty('zonas-line', 'line-width', ['case', reached, 2.4, 0.8]);
		map.setPaintProperty('exp-3d', 'fill-extrusion-color', [
			'case', ['<=', ['coalesce', ['get', 's'], 99], st],
			['interpolate', ['linear'], ['-', st, ['coalesce', ['get', 's'], 99]], 0, '#fdba74', 1, '#f97316', 2.5, '#dc2626', 5, '#7f1d1d'],
			'#e2e8f0'
		]);
		map.setPaintProperty('puntos', 'circle-color',
			['case', ['<=', ['coalesce', ['get', 'primer_impacto'], 99], st], '#ef4444', '#f8fafc']);
		paintWater();
	}

	$effect(() => { stage; applyStage(); });
	$effect(() => {
		capa;
		if (map?.getLayer('zonas-fill')) {
			const e = zoneColorExpr();
			map.setPaintProperty('zonas-fill', 'fill-color', e);
			map.setPaintProperty('zonas-line', 'line-color', e);
		}
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

	function flyTo(nombre: string) {
		const f = zonas?.features.find((x: any) => x.properties.nombre === nombre);
		if (!f || !map) return;
		const xs: number[] = [], ys: number[] = [];
		const walk = (c: any) => (typeof c[0] === 'number' ? (xs.push(c[0]), ys.push(c[1])) : c.forEach(walk));
		walk(f.geometry.coordinates);
		map.fitBounds([[Math.min(...xs), Math.min(...ys)], [Math.max(...xs), Math.max(...ys)]],
			{ padding: 80, maxZoom: 16, pitch: 50 });
		selected = f.properties;
	}
	function view(v: typeof TOWN_WIDE) { map?.flyTo({ ...v, duration: 1200 }); }

	function tooltipHtml(e: any): string | null {
		const feats = map.queryRenderedFeatures(e.point, { layers: ['puntos', 'exp-3d', 'zonas-fill'] });
		const lines: string[] = [];
		const pt = feats.find((f: any) => f.layer.id === 'puntos');
		const b = feats.find((f: any) => f.layer.id === 'exp-3d');
		const z = feats.find((f: any) => f.layer.id === 'zonas-fill');
		if (pt) {
			const s = num(pt.properties.primer_impacto);
			lines.push(`<b>${pt.properties.nombre}</b>`,
				s == null ? 'Fuera del alcance de los escenarios' : `El agua llega con ${m(s)}` + (s <= stage + 1e-6 ? ` · <span class="r">bajo ${m(stage - s)} de agua</span>` : ''));
		} else if (b) {
			const p = b.properties, s = num(p.s);
			lines.push(`<b>Construcción de ${fmt(+p.a)} m²</b> · ${+p.v > 0 ? 'vivienda estimada' : 'sin vivienda estimada (galpón u otra)'}${+p.r ? ' · barrio popular' : ''}`);
			lines.push(s == null ? 'El agua no llega en ningún escenario' : `El agua llega con ${m(s)} en el hidrómetro`
				+ (s <= stage + 1e-6 ? ` · <span class="r">con el río a ${m(stage)}: ≈ ${m(stage - s)} de agua</span>` : ''));
		} else {
			const w = depthAt(e.lngLat.lng, e.lngLat.lat);
			if (w) lines.push(w.desde <= (tabla?.s_ref ?? 2.5) + 0.3
				? `<b>Cauce del río</b><br>≈ ${m(stage - (tabla?.s_ref ?? 2.1))} por encima de su nivel habitual`
				: `<b>Agua: ≈ ${m(w.depth)} sobre el terreno</b><br>Aquí el agua llega desde ${m(w.desde)} en el hidrómetro`);
		}
		if (z) {
			const p = z.properties, s = num(p.primer_impacto), t = tipoOf(p.tipo_vivienda);
			lines.push(`<span class="dim">${p.nombre} · ${PRIO.find((x) => x.id === p.prioridad)?.label ?? ''} · ${t?.label ?? ''}</span>`);
			if (!pt && !b && s != null) lines.push(`<span class="dim">El agua llega a sus viviendas con ${m(s)}; el río superó esa altura ${p.anios_alcanzada} ${+p.anios_alcanzada === 1 ? 'año' : 'años'} desde 1980</span>`);
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
			let t: Tabla, edif: any;
			try {
				const get = async (f: string) => {
					const r = await fetch(`${base}/${f}`);
					if (!r.ok) throw new Error(`${f}: ${r.status}`);
					return r.json();
				};
				[t, edif, zonas, puntos] = await Promise.all([
					get('tabla_altura.json'), get('edificios.geojson'), get('microzonas.geojson'), get('puntos.geojson')
				]);
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
			const F = edif.features;
			B = { x: new Float64Array(F.length), y: new Float64Array(F.length), s: new Float32Array(F.length),
				v: new Int32Array(F.length), p: new Int32Array(F.length), r: new Uint8Array(F.length), z: new Array(F.length) };
			F.forEach((f: any, i: number) => {
				const p = f.properties;
				B.x[i] = p.x; B.y[i] = p.y; B.s[i] = p.s ?? 99; B.v[i] = p.v; B.p[i] = p.p; B.r[i] = p.r; B.z[i] = p.z;
			});
			for (const f of zonas.features) zshare.set(f.properties.id, { s05: f.properties.s05, s65: f.properties.s65 });
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
				map.addSource('zonas', { type: 'geojson', data: zonas });
				map.addLayer({ id: 'zonas-fill', type: 'fill', source: 'zonas',
					paint: { 'fill-color': zoneColorExpr(), 'fill-opacity': 0.2 } }, firstSymbol);
				map.addSource('agua', { type: 'canvas', canvas: wc, coordinates: t.raster.esquinas, animate: false });
				map.addLayer({ id: 'agua', type: 'raster', source: 'agua',
					paint: { 'raster-resampling': 'linear', 'raster-opacity': 0.9, 'raster-fade-duration': 0 } }, firstSymbol);
				map.addLayer({ id: 'zonas-line', type: 'line', source: 'zonas',
					paint: { 'line-color': zoneColorExpr(), 'line-width': 1 } });
				map.addSource('buildings', { type: 'vector', url: getTilesUrl('buildings') });
				map.addLayer({ id: 'ctx-3d', type: 'fill-extrusion', source: 'buildings', 'source-layer': 'buildings', minzoom: 12,
					paint: {
						'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 3], 3], 60],
						'fill-extrusion-color': '#475569', 'fill-extrusion-opacity': 0.75
					} });
				map.addSource('edificios', { type: 'geojson', data: edif });
				map.addLayer({ id: 'exp-3d', type: 'fill-extrusion', source: 'edificios', minzoom: 11,
					paint: {
						'fill-extrusion-height': ['+', ['get', 'h'], 0.4],
						'fill-extrusion-color': '#e2e8f0', 'fill-extrusion-opacity': 0.95
					} });
				map.addSource('puntos', { type: 'geojson', data: puntos });
				map.addLayer({ id: 'puntos', type: 'circle', source: 'puntos', minzoom: 11,
					paint: {
						'circle-radius': ['interpolate', ['linear'], ['zoom'], 11, 3, 16, 7],
						'circle-color': '#f8fafc', 'circle-stroke-color': '#0a0a0a', 'circle-stroke-width': 1.5,
						'circle-pitch-alignment': 'map'
					} });
				map.addSource('lasso', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
				map.addLayer({ id: 'lasso-fill', type: 'fill', source: 'lasso', filter: ['==', '$type', 'Polygon'],
					paint: { 'fill-color': '#22d3ee', 'fill-opacity': 0.12 } });
				map.addLayer({ id: 'lasso-line', type: 'line', source: 'lasso',
					paint: { 'line-color': '#22d3ee', 'line-width': 2.5, 'line-dasharray': [3, 1.5] } });
				new maplibregl.Marker({ color: '#22d3ee', scale: 0.75 }).setLngLat(GAUGE)
					.setPopup(new maplibregl.Popup({ offset: 18 }).setText('Hidrómetro de El Soberbio (INA / Prefectura)'))
					.addTo(map);

				const tip = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 10, maxWidth: '320px', className: 'demo-tip' });
				map.on('mousemove', (e: any) => {
					if (lassoOn) { tip.remove(); return; }
					const html = tooltipHtml(e);
					if (html) { tip.setLngLat(e.lngLat).setHTML(html).addTo(map); map.getCanvas().style.cursor = 'pointer'; }
					else { tip.remove(); map.getCanvas().style.cursor = ''; }
				});
				map.on('mouseout', () => tip.remove());
				map.on('click', (e: any) => {
					if (lassoOn) { lassoPts = [...lassoPts, [e.lngLat.lng, e.lngLat.lat]]; return; }
					const z = map.queryRenderedFeatures(e.point, { layers: ['zonas-fill'] })[0];
					if (z) selected = z.properties;
				});
				map.on('dblclick', (e: any) => { if (lassoOn) { e.preventDefault(); endLasso(true); } });
				applyStage();
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
			<button onclick={() => view(town())}>El pueblo en 3D</button>
			<button onclick={() => view(MUNI)}>Todo el municipio</button>
			{#if lassoOn}
				<button class="on" onclick={() => endLasso(true)}>Cerrar área ({lassoPts.length} puntos)</button>
			{:else}
				<button onclick={startLasso}>Seleccionar un área</button>
			{/if}
		</div>
		{#if lassoOn}
			<div class="hint">Hacé clic para marcar los vértices; doble clic para cerrar; Esc para cancelar.</div>
		{/if}
	</div>

	<aside class="panel">
		<header>
			<p class="kicker">El Niño 2026-2027 · Escenarios de impacto social</p>
			<h1>El Soberbio: si el Uruguay llega a…</h1>
		</header>

		{#if error}
			<p class="warn">{error}</p>
		{:else if tabla && pasoActual}
			<div class="stage">
				<span class="big">{m(stage)}</span>
				<span class="dim">en el hidrómetro de El Soberbio</span>
			</div>
			<input type="range" min={minStage} max={maxStage} step="0.5" value={stage}
				oninput={(e) => setStage(+(e.currentTarget as HTMLInputElement).value)} aria-label="Altura del río" />
			<div class="scen">
				{#each SCEN as s}
					{@const e = tabla.escenarios[s.id]}
					<button class:on={Math.abs(stage - e.altura_m) < 0.01} onclick={() => setStage(e.altura_m, true)} title={`Pico del ${fecha(e.fecha)}`}>
						<b>{s.label}</b><span>{m(e.altura_m)}{s.note ? ` · ${s.note}` : ''}</span>
					</button>
				{/each}
			</div>

			<div class="grid">
				<div><b>{fmt(pasoActual.viviendas)}</b><span>viviendas bajo el agua</span></div>
				<div><b>{approx(pasoActual.personas)}</b><span>personas</span></div>
				<div><b>{approx(pasoActual.ninos)}</b><span>niños de 0 a 5 años</span></div>
				<div><b>{approx(pasoActual.mayores)}</b><span>mayores de 65</span></div>
				<div><b>{fmt(pasoActual.viv_renabap)}</b><span>viviendas en barrios populares</span></div>
				<div><b>{fmt(pasoActual.escuelas)}</b><span>escuelas bajo el agua</span></div>
			</div>

			{#if lassoRes}
				<section class="ficha">
					<button class="close" onclick={clearLasso} aria-label="Borrar área">×</button>
					<h2>Área seleccionada</h2>
					<table>
						<thead><tr><th></th><th>{m(stage)}</th>{#each SCEN as s}<th>{s.id}</th>{/each}</tr></thead>
						<tbody>
							{#each [['viv', 'Viviendas'], ['pers', 'Personas'], ['ninos', 'Niños 0-5'], ['mayores', 'Mayores 65+'], ['ren', 'En barrio popular'], ['esc', 'Escuelas']] as [k, l]}
								<tr><td>{l}</td>{#each lassoRes as r, i}<td class:cur={i === 0}>{k === 'pers' || k === 'ninos' || k === 'mayores' ? approx(r[k as keyof Lasso]) : fmt(r[k as keyof Lasso])}</td>{/each}</tr>
							{/each}
						</tbody>
					</table>
					<p class="dim">Edificios a menos de 500 m del agua del escenario 1983. Personas: reparto del censo por edificio, sólo sumado.</p>
				</section>
			{/if}

			<section>
				<h2>Qué se pinta en las zonas</h2>
				<select bind:value={capa} aria-label="Capa de las zonas">
					<option value="prioridad">Prioridad (agua × vulnerabilidad)</option>
					<option value="tipo">Tipo de vulnerabilidad de la vivienda</option>
					<optgroup label="Censo 2022, contra el promedio de Misiones">
						{#each CENSO as c}<option value={c.id}>{c.label}</option>{/each}
					</optgroup>
				</select>
				<div class="legend">
					{#if capa === 'prioridad'}
						{#each PRIO as p}<span><i style:background={p.color}></i>{p.label}</span>{/each}
					{:else if capa === 'tipo'}
						{#each TIPOS as p}<span title={p.def}><i style:background={p.color}></i>{p.label}</span>{/each}
					{:else if censoSel}
						<span class="dim">Misiones: {pct(tabla.provincia[censoSel.prov])} de los {censoSel.id === 'c_dep' ? 'habitantes' : 'hogares'}</span>
						{#each RATIO as r}<span><i style:background={r.color}></i>{r.label}</span>{/each}
					{/if}
				</div>
				{#if capa === 'tipo'}<p class="note">Tipología por zona censal: el censo no permite clasificar cada casa.</p>{/if}
			</section>

			{#if selected}
				{@const s0 = num(selected.primer_impacto)}
				{@const tv = tipoOf(selected.tipo_vivienda)}
				<section class="ficha">
					<button class="close" onclick={() => (selected = null)} aria-label="Cerrar">×</button>
					<h2>{selected.nombre}</h2>
					<p><i style:background={colorOf(selected.prioridad)}></i> <b>{PRIO.find((p) => p.id === selected?.prioridad)?.label}</b>
						{#if s0 != null}· el agua llega a sus viviendas con {m(s0)}; el río superó esa altura {selected.anios_alcanzada} {+selected.anios_alcanzada === 1 ? 'año' : 'años'} desde 1980{/if}</p>
					<p><i style:background={tv?.color}></i> <b>{tv?.label}</b>
						{#if selected.rasgos && selected.rasgos !== 'consolidada' && String(selected.rasgos).includes(',')}
							<span class="dim">· también: {String(selected.rasgos).split(',').filter((r) => r !== selected?.tipo_vivienda).map((r) => tipoOf(r)?.label.toLowerCase()).join(', ')}</span>
						{/if}</p>
					<table>
						<thead><tr><th></th>{#each SCEN as s}<th>{s.id}</th>{/each}</tr></thead>
						<tbody>
							{#each [['viviendas', 'Viviendas'], ['personas', 'Personas'], ['ninos', 'Niños 0-5'], ['mayores', 'Mayores 65+'], ['viv_renabap', 'En barrio popular']] as [k, l]}
								<tr><td>{l}</td>{#each SCEN as s}<td>{fmt(+selected[`esc${s.id}_${k}`])}</td>{/each}</tr>
							{/each}
						</tbody>
					</table>
					<h2>Censo 2022 · {fmt(+selected.c_hogares)} hogares</h2>
					<div class="bars">
						{#each CENSO as c}
							{@const z = +selected[c.id]}
							{@const pr = tabla.provincia[c.prov]}
							<!-- fixed scale: the provincial average sits at the middle, the end of the track is twice it -->
							<div class="bar">
								<span class="bl">{c.label}</span>
								<span class="track"><span class="z" class:hi={z > pr} style:width={`${Math.min(z / (2 * pr), 1) * 100}%`}></span><span class="pv" style:left="50%"></span></span>
								<span class="bv">{pct(z)}</span>
							</div>
						{/each}
					</div>
					<p class="dim">La marca blanca es el promedio de Misiones; la barra llena, el doble. Vulnerable en {selected.privaciones} de 4 dimensiones{#if +selected.privaciones > 0}: {DIMS.filter(([k]) => +selected?.[k] === 1).map(([, l]) => l).join('; ')}{/if}.</p>
				</section>
			{/if}

			<section>
				<h2>Dónde llega primero</h2>
				{#if alcanzadas.length === 0}<p class="dim">Con esta altura el agua no llega a viviendas.</p>{/if}
				<ul class="umbrales">
					{#each alcanzadas as u}
						<li><button onclick={() => flyTo(u.microzona)}><i style:background={colorOf(u.prioridad)}></i><span class="h">{m(u.altura)}</span><span>{u.microzona}</span></button></li>
					{/each}
					{#each proximas as u}
						<li class="next"><button onclick={() => flyTo(u.microzona)}><i style:background={colorOf(u.prioridad)}></i><span class="h">{m(u.altura)}</span><span>{u.microzona}</span></button></li>
					{/each}
				</ul>
				<p class="note">Altura a partir de la cual el agua llega a las viviendas de cada zona: propuesta técnica de umbral de aviso, no umbral oficial.</p>
			</section>

			<section class="legend">
				{#each DEPTH as d}<span><i style:background={`rgba(${d.rgba.slice(0, 3).join(',')},0.95)`}></i>{d.label}</span>{/each}
				<span><i style:background="#f97316"></i>Construcción bajo el agua</span>
				<span><i style:background="#e2e8f0"></i>Construcción seca</span>
				<span><i class="dot"></i>Escuela o centro de salud</span>
			</section>

			<footer>
				<p><b>Escenarios, no pronóstico.</b> Muestran qué pasaría si el río llega a esa altura. Crecida del Uruguay únicamente: no incluye lluvia ni arroyos.</p>
				<p><b>Sin calibrar con marcas de crecida:</b> sirve para comparar zonas entre sí; las cifras son estimaciones. Con las marcas de crecida y los registros de evacuados de la provincia se ajusta a lo que ya pasó.</p>
				<p class="src">Fuentes: hidrómetro INA (serie 1980-2026), FABDEM, Censo 2022 (INDEC), RENABAP, edificios Google y Microsoft, inventario del Mapa para la Gestión de la Emergencia. Población por edificio: reparto modelado, publicado sólo agregado.</p>
				<p class="credit">CONICET / FHyCS-UNaM · Spatia</p>
			</footer>
		{:else}
			<p class="dim">Cargando…</p>
		{/if}
	</aside>
</div>

<style>
	.demo { position: fixed; inset: 0; display: flex; background: #0a0a0a; color: #fff; font-size: 13px; }
	.mapwrap { flex: 1; min-width: 0; position: relative; }
	.map { position: absolute; inset: 0; }
	.tools { position: absolute; top: 10px; left: 50px; display: flex; flex-wrap: wrap; gap: 6px; z-index: 2; }
	.tools button { padding: 6px 10px; border-radius: 8px; background: rgba(10, 12, 18, 0.88); border: 1px solid #334155;
		color: #fff; cursor: pointer; font-size: 12px; }
	.tools button.on { border-color: #22d3ee; color: #22d3ee; }
	.hint { position: absolute; bottom: 36px; left: 50%; transform: translateX(-50%); padding: 6px 12px; border-radius: 8px;
		background: rgba(10, 12, 18, 0.9); border: 1px solid #22d3ee; font-size: 12px; z-index: 2; }
	.panel { width: 400px; max-width: 100%; overflow-y: auto; padding: 18px 18px 24px; background: rgba(10, 12, 18, 0.96);
		border-left: 1px solid #1e293b; display: flex; flex-direction: column; gap: 14px; }
	.kicker { margin: 0; color: #22d3ee; font-size: 11px; letter-spacing: 0.04em; text-transform: uppercase; }
	h1 { margin: 4px 0 0; font-size: 19px; line-height: 1.25; }
	h2 { margin: 0 0 6px; font-size: 13px; color: #d4d4d4; }
	.stage { display: flex; align-items: baseline; gap: 8px; }
	.big { white-space: nowrap; font-size: 34px; font-weight: 700; color: #38bdf8; font-variant-numeric: tabular-nums; }
	.dim { color: #a3a3a3; }
	input[type='range'] { width: 100%; accent-color: #38bdf8; }
	select { width: 100%; padding: 6px 8px; border-radius: 8px; background: #111827; color: #fff; border: 1px solid #334155; font-size: 12.5px; }
	.scen { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; }
	.scen button { display: flex; flex-direction: column; align-items: flex-start; gap: 2px; padding: 7px 8px; border-radius: 8px;
		background: rgba(255, 255, 255, 0.06); border: 1px solid #334155; color: #fff; cursor: pointer; text-align: left; }
	.scen button span { color: #a3a3a3; font-size: 11px; }
	.scen button.on { border-color: #38bdf8; background: rgba(56, 189, 248, 0.15); }
	.grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; }
	.grid div { display: flex; flex-direction: column; padding: 8px 10px; border-radius: 8px; background: rgba(255, 255, 255, 0.04); }
	.grid b { font-size: 20px; font-variant-numeric: tabular-nums; }
	.grid span { color: #a3a3a3; font-size: 11.5px; }
	.umbrales { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 2px; }
	.umbrales button { display: flex; gap: 8px; align-items: center; width: 100%; padding: 4px 2px; background: none; border: 0;
		color: #fff; cursor: pointer; text-align: left; font-size: 12.5px; }
	.umbrales button:hover { background: rgba(255, 255, 255, 0.05); }
	.umbrales .next button { color: #a3a3a3; }
	.umbrales .h { width: 48px; flex: none; font-variant-numeric: tabular-nums; }
	i { display: inline-block; width: 10px; height: 10px; border-radius: 2px; flex: none; }
	i.dot { border-radius: 50%; background: #f8fafc; border: 1px solid #0a0a0a; }
	.note { margin: 6px 0 0; color: #a3a3a3; font-size: 11px; }
	.ficha { position: relative; padding: 10px 12px; border: 1px solid #334155; border-radius: 8px; background: rgba(255, 255, 255, 0.03); }
	.ficha p { margin: 4px 0; }
	.ficha table { width: 100%; border-collapse: collapse; margin: 6px 0 10px; font-variant-numeric: tabular-nums; }
	.ficha th, .ficha td { padding: 2px 4px; text-align: right; }
	.ficha th:first-child, .ficha td:first-child { text-align: left; color: #a3a3a3; }
	.ficha td.cur { color: #38bdf8; font-weight: 700; }
	.close { position: absolute; top: 4px; right: 8px; background: none; border: 0; color: #a3a3a3; font-size: 18px; cursor: pointer; }
	.bars { display: flex; flex-direction: column; gap: 4px; margin-bottom: 6px; }
	.bar { display: grid; grid-template-columns: 150px 1fr 40px; gap: 6px; align-items: center; font-size: 11.5px; }
	.bl { color: #d4d4d4; }
	.bv { text-align: right; font-variant-numeric: tabular-nums; }
	.track { position: relative; height: 8px; background: rgba(255, 255, 255, 0.06); border-radius: 4px; }
	.track .z { position: absolute; left: 0; top: 0; bottom: 0; background: #60a5fa; border-radius: 4px; }
	.track .z.hi { background: #f97316; }
	.track .pv { position: absolute; top: -2px; bottom: -2px; width: 2px; background: #fff; }
	.legend { display: flex; flex-wrap: wrap; gap: 6px 12px; color: #d4d4d4; font-size: 11.5px; margin-top: 6px; }
	.legend span { display: flex; align-items: center; gap: 5px; }
	footer { display: flex; flex-direction: column; gap: 6px; color: #a3a3a3; font-size: 11px; line-height: 1.4; }
	footer p { margin: 0; }
	footer b { color: #d4d4d4; }
	.credit { color: #d4d4d4; }
	.warn { color: #fca5a5; }
	:global(.demo-tip .maplibregl-popup-content) { background: rgba(10, 12, 18, 0.95); color: #fff; border: 1px solid #334155;
		border-radius: 8px; padding: 8px 10px; font-size: 12px; line-height: 1.45; }
	:global(.demo-tip .maplibregl-popup-tip) { display: none; }
	:global(.demo-tip .dim) { color: #a3a3a3; }
	:global(.demo-tip .r) { color: #fca5a5; }
	@media (max-width: 760px) {
		.demo { flex-direction: column; }
		.mapwrap { flex: none; height: 52vh; }
		.panel { width: 100%; border-left: 0; border-top: 1px solid #1e293b; padding: 14px 16px 24px; flex: 1; }
		.bar { grid-template-columns: 120px 1fr 36px; }
	}
</style>
