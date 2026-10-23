<script lang="ts">
	// Demo «El Niño: dónde mirar primero» — El Soberbio, crecida del Uruguay por altura del hidrómetro.
	// Unlisted page with its own map: it does not touch Map.svelte or the analysis registry.
	// Data are precomputed outside this repo and served from R2 under data/demo/el_nino/.
	import { onMount } from 'svelte';
	import 'maplibre-gl/dist/maplibre-gl.css';
	import { page } from '$app/state';
	import { BASEMAP, getTilesUrl } from '$lib/config';

	const CDN = 'https://cdn.spatia.ar/data/demo/el_nino';
	const GAUGE: [number, number] = [-54.199, -27.298];

	const PRIO = [
		{ id: 'vigilar ya', label: 'Vigilar ya', color: '#ef4444' },
		{ id: 'vigilar', label: 'Vigilar', color: '#f97316' },
		{ id: 'atento', label: 'Atento', color: '#facc15' },
		{ id: 'sin prioridad', label: 'Sin prioridad', color: '#64748b' }
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
	}

	let container: HTMLDivElement;
	let map: any;
	let tabla = $state<Tabla | null>(null);
	let stage = $state(19);
	let selected = $state<Record<string, any> | null>(null);
	let error = $state('');
	let zonas: any = null;

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

	const fmt = (n: number) => Math.round(n).toLocaleString('es-AR');
	const approx = (n: number) => (n < 20 ? fmt(n) : '≈ ' + fmt(Math.round(n / 10) * 10));
	const m = (x: number) => x.toLocaleString('es-AR', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + ' m';
	const colorOf = (p: string) => PRIO.find((x) => x.id === p)?.color ?? '#64748b';
	const fecha = (iso: string) => {
		const [y, mo, d] = iso.split('-');
		return `${+d}/${+mo}/${y}`;
	};

	// Slider moves in 0.5 m steps; a scenario button keeps its exact height (19.2 m) and
	// the numbers come from the highest precomputed step at or below it.
	function setStage(v: number, exact = false) {
		stage = exact ? v : Math.round(v * 2) / 2;
	}

	function applyStage() {
		if (!map || !map.getLayer('agua')) return;
		map.setFilter('agua', ['<=', ['get', 'altura'], stage + 1e-6]);
		const reached = ['all', ['!=', ['get', 'primer_impacto'], null], ['<=', ['get', 'primer_impacto'], stage + 1e-6]];
		map.setPaintProperty('zonas-fill', 'fill-opacity', ['case', reached, 0.45, 0.1]);
		map.setPaintProperty('zonas-line', 'line-width', ['case', reached, 2.2, 0.8]);
		map.setPaintProperty('puntos', 'circle-color', [
			'case', ['all', ['!=', ['get', 'primer_impacto'], null], ['<=', ['get', 'primer_impacto'], stage + 1e-6]],
			'#ef4444', '#e5e7eb'
		]);
	}

	$effect(() => {
		stage;
		applyStage();
	});

	function flyTo(nombre: string) {
		const f = zonas?.features.find((x: any) => x.properties.nombre === nombre);
		if (!f || !map) return;
		const xs: number[] = [], ys: number[] = [];
		const walk = (c: any) => (typeof c[0] === 'number' ? (xs.push(c[0]), ys.push(c[1])) : c.forEach(walk));
		walk(f.geometry.coordinates);
		map.fitBounds([[Math.min(...xs), Math.min(...ys)], [Math.max(...xs), Math.max(...ys)]], { padding: 80, maxZoom: 15.5 });
		selected = f.properties;
	}

	onMount(() => {
		const base = (import.meta.env.DEV && page.url.searchParams.get('src')) || CDN;
		let cancelled = false;
		(async () => {
			const [{ default: maplibregl }, { Protocol }] = await Promise.all([import('maplibre-gl'), import('pmtiles')]);
			if (cancelled) return;
			const protocol = new Protocol();
			maplibregl.addProtocol('pmtiles', protocol.tile);
			let t: Tabla, manchas: any, pts: any;
			try {
				const get = async (f: string) => {
					const r = await fetch(`${base}/${f}`);
					if (!r.ok) throw new Error(`${f}: ${r.status}`);
					return r.json();
				};
				[t, manchas, zonas, pts] = await Promise.all([
					get('tabla_altura.json'), get('manchas.geojson'), get('microzonas.geojson'), get('puntos.geojson')
				]);
			} catch (e) {
				error = 'No se pudieron cargar los datos de la demo.';
				return;
			}
			tabla = t;
			setStage(t.escenarios['2023'].altura_m, true);

			map = new maplibregl.Map({
				container,
				style: BASEMAP,
				center: [-54.2, -27.29],
				zoom: 12.6,
				attributionControl: { compact: true }
			});
			map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-left');
			map.on('load', () => {
				const firstSymbol = map.getStyle().layers.find((l: any) => l.type === 'symbol')?.id;
				map.addSource('buildings', { type: 'vector', url: getTilesUrl('buildings') });
				map.addLayer({
					id: 'edificios', type: 'fill', source: 'buildings', 'source-layer': 'buildings', minzoom: 13,
					paint: { 'fill-color': '#94a3b8', 'fill-opacity': 0.35 }
				}, firstSymbol);
				map.addSource('zonas', { type: 'geojson', data: zonas });
				map.addLayer({
					id: 'zonas-fill', type: 'fill', source: 'zonas',
					paint: {
						'fill-color': ['match', ['get', 'prioridad'], ...PRIO.flatMap((p) => [p.id, p.color]), '#64748b'],
						'fill-opacity': 0.2
					}
				}, firstSymbol);
				map.addSource('manchas', { type: 'geojson', data: manchas });
				map.addLayer({
					id: 'agua', type: 'fill', source: 'manchas',
					paint: { 'fill-color': '#38bdf8', 'fill-opacity': 0.55 }
				}, firstSymbol);
				map.addLayer({
					id: 'zonas-line', type: 'line', source: 'zonas',
					paint: {
						'line-color': ['match', ['get', 'prioridad'], ...PRIO.flatMap((p) => [p.id, p.color]), '#64748b'],
						'line-width': 1
					}
				});
				map.addSource('puntos', { type: 'geojson', data: pts });
				map.addLayer({
					id: 'puntos', type: 'circle', source: 'puntos', minzoom: 11,
					paint: {
						'circle-radius': ['interpolate', ['linear'], ['zoom'], 11, 3, 15, 6],
						'circle-color': '#e5e7eb', 'circle-stroke-color': '#0a0a0a', 'circle-stroke-width': 1
					}
				});
				new maplibregl.Marker({ color: '#22d3ee', scale: 0.7 })
					.setLngLat(GAUGE)
					.setPopup(new maplibregl.Popup({ offset: 18 }).setText('Hidrómetro de El Soberbio (INA / Prefectura)'))
					.addTo(map);

				map.on('click', 'zonas-fill', (e: any) => { selected = e.features?.[0]?.properties ?? null; });
				map.on('mouseenter', 'zonas-fill', () => (map.getCanvas().style.cursor = 'pointer'));
				map.on('mouseleave', 'zonas-fill', () => (map.getCanvas().style.cursor = ''));
				const tip = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 8 });
				map.on('mouseenter', 'puntos', (e: any) => {
					const p = e.features[0].properties;
					const s = p.primer_impacto && p.primer_impacto !== 'null' ? `el agua llega con ${m(+p.primer_impacto)}` : 'fuera del alcance de los escenarios';
					tip.setLngLat(e.lngLat).setHTML(`<b>${p.nombre}</b><br>${s}`).addTo(map);
				});
				map.on('mouseleave', 'puntos', () => tip.remove());
				applyStage();
			});
		})();
		return () => {
			cancelled = true;
			map?.remove();
		};
	});
</script>

<svelte:head>
	<title>El Niño en El Soberbio · Spatia</title>
	<meta name="robots" content="noindex, nofollow" />
</svelte:head>

<div class="demo">
	<div class="map" bind:this={container}></div>

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
					<button class:on={Math.abs(stage - e.altura_m) < 0.01}
						onclick={() => setStage(e.altura_m, true)} title={`Pico del ${fecha(e.fecha)}`}>
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

			<section>
				<h2>Dónde llega primero</h2>
				{#if alcanzadas.length === 0}
					<p class="dim">Con esta altura el agua no llega a viviendas.</p>
				{/if}
				<ul class="umbrales">
					{#each alcanzadas as u}
						<li><button onclick={() => flyTo(u.microzona)}>
							<i style:background={colorOf(u.prioridad)}></i><span class="h">{m(u.altura)}</span><span>{u.microzona}</span>
						</button></li>
					{/each}
					{#each proximas as u}
						<li class="next"><button onclick={() => flyTo(u.microzona)}>
							<i style:background={colorOf(u.prioridad)}></i><span class="h">{m(u.altura)}</span><span>{u.microzona}</span>
						</button></li>
					{/each}
				</ul>
				<p class="note">Altura a partir de la cual el agua llega a las viviendas de cada zona: propuesta técnica de umbral de aviso, no umbral oficial.</p>
			</section>

			{#if selected}
				<section class="ficha">
					<button class="close" onclick={() => (selected = null)} aria-label="Cerrar">×</button>
					<h2>{selected.nombre}</h2>
					<p><i style:background={colorOf(selected.prioridad)}></i> <b>{PRIO.find((p) => p.id === selected?.prioridad)?.label}</b>
						{#if selected.primer_impacto && selected.primer_impacto !== 'null'}
							· el agua llega con {m(+selected.primer_impacto)}; el río superó esa altura {selected.anios_alcanzada} {+selected.anios_alcanzada === 1 ? 'año' : 'años'} desde 1980
						{/if}
					</p>
					<table>
						<thead><tr><th></th>{#each SCEN as s}<th>{s.label.replace('Como ', '')}</th>{/each}</tr></thead>
						<tbody>
							{#each [['viviendas', 'Viviendas'], ['personas', 'Personas'], ['ninos', 'Niños 0-5'], ['mayores', 'Mayores 65+'], ['viv_renabap', 'En barrio popular']] as [k, l]}
								<tr><td>{l}</td>{#each SCEN as s}<td>{fmt(+selected[`esc${s.id}_${k}`])}</td>{/each}</tr>
							{/each}
						</tbody>
					</table>
					<p class="dim">Más vulnerable que el promedio de Misiones en {selected.privaciones} de 4 dimensiones{#if +selected.privaciones > 0}:
						{DIMS.filter(([k]) => +selected?.[k] === 1).map(([, l]) => l).join('; ')}{/if}.</p>
				</section>
			{/if}

			<section class="legend">
				{#each PRIO as p}<span><i style:background={p.color}></i>{p.label}</span>{/each}
				<span><i style:background="#38bdf8"></i>Agua</span>
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
	.map { flex: 1; min-width: 0; }
	.panel { width: 390px; max-width: 100%; overflow-y: auto; padding: 18px 18px 24px; background: rgba(10, 12, 18, 0.96);
		border-left: 1px solid #1e293b; display: flex; flex-direction: column; gap: 14px; }
	.kicker { margin: 0; color: #22d3ee; font-size: 11px; letter-spacing: 0.04em; text-transform: uppercase; }
	h1 { margin: 4px 0 0; font-size: 19px; line-height: 1.25; }
	h2 { margin: 0 0 6px; font-size: 13px; color: #d4d4d4; }
	.stage { display: flex; align-items: baseline; gap: 8px; }
	.big { white-space: nowrap; font-size: 34px; font-weight: 700; color: #38bdf8; font-variant-numeric: tabular-nums; }
	.dim { color: #a3a3a3; }
	input[type='range'] { width: 100%; accent-color: #38bdf8; }
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
	i.dot { border-radius: 50%; background: #e5e7eb; border: 1px solid #0a0a0a; }
	.note { margin: 6px 0 0; color: #a3a3a3; font-size: 11px; }
	.ficha { position: relative; padding: 10px 12px; border: 1px solid #334155; border-radius: 8px; background: rgba(255, 255, 255, 0.03); }
	.ficha p { margin: 4px 0; }
	.ficha table { width: 100%; border-collapse: collapse; margin: 6px 0; font-variant-numeric: tabular-nums; }
	.ficha th, .ficha td { padding: 2px 4px; text-align: right; }
	.ficha th:first-child, .ficha td:first-child { text-align: left; color: #a3a3a3; }
	.close { position: absolute; top: 4px; right: 8px; background: none; border: 0; color: #a3a3a3; font-size: 18px; cursor: pointer; }
	.legend { display: flex; flex-wrap: wrap; gap: 6px 12px; color: #d4d4d4; font-size: 11.5px; }
	.legend span { display: flex; align-items: center; gap: 5px; }
	footer { display: flex; flex-direction: column; gap: 6px; color: #a3a3a3; font-size: 11px; line-height: 1.4; }
	footer p { margin: 0; }
	footer b { color: #d4d4d4; }
	.credit { color: #d4d4d4; }
	.warn { color: #fca5a5; }
	@media (max-width: 760px) {
		.demo { flex-direction: column; }
		.map { flex: none; height: 48vh; }
		.panel { width: 100%; border-left: 0; border-top: 1px solid #1e293b; padding: 14px 16px 24px; flex: 1; }
	}
</style>
