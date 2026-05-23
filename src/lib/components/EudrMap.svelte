<script lang="ts">
	import { onMount } from 'svelte';
	import maplibregl from 'maplibre-gl';
	import 'maplibre-gl/dist/maplibre-gl.css';

	interface Props {
		onCellClick?: (lat: number, lon: number, h3index: string) => void;
		onPolygonDrawn?: (rings: number[][][]) => void;
		onDrawModeChange?: (active: boolean) => void;
	}

	let { onCellClick, onPolygonDrawn, onDrawModeChange }: Props = $props();

	let mapContainer: HTMLDivElement;
	let map: maplibregl.Map;
	let marker: maplibregl.Marker | null = null;

	// Hand-rolled polygon draw state
	let drawMode = false;
	let drawPts: [number, number][] = [];

	function setDrawState(active: boolean) {
		drawMode = active;
		onDrawModeChange?.(active);
	}

	function updateDrawSource() {
		const src = map?.getSource('eudr-draw') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;
		const features: any[] = drawPts.map((pt) => ({
			type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: pt },
		}));
		if (drawPts.length >= 2) {
			const line = drawPts.length >= 3 ? [...drawPts, drawPts[0]] : drawPts;
			features.push({ type: 'Feature', properties: {}, geometry: { type: 'LineString', coordinates: line } });
		}
		src.setData({ type: 'FeatureCollection', features });
	}

	export function startDraw() {
		clearMarker();
		clearPolygon();
		drawPts = [];
		setDrawState(true);
		map?.doubleClickZoom.disable();
		updateDrawSource();
	}

	export function cancelDraw() {
		drawPts = [];
		setDrawState(false);
		map?.doubleClickZoom.enable();
		updateDrawSource();
	}

	function finishDraw() {
		if (drawPts.length >= 3) {
			const ring = [...drawPts, drawPts[0]];
			onPolygonDrawn?.([ring]);
		}
		setDrawState(false);
		map?.doubleClickZoom.enable();
		drawPts = [];
		updateDrawSource();
	}

	const MAP_CENTER: [number, number] = [-62.5, -26.5];
	const MAP_ZOOM = 5.5;
	const BASEMAP = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';

	// Risk score color ramp
	const RISK_COLORS: [number, string][] = [
		[0, '#22c55e'],    // green — low risk
		[25, '#84cc16'],   // lime
		[50, '#f59e0b'],   // amber — medium
		[75, '#ef4444'],   // red — high
		[100, '#991b1b'],  // dark red — critical
	];

	export function flyTo(lat: number, lon: number, zoom = 10) {
		map?.flyTo({ center: [lon, lat], zoom, duration: 1500 });
	}

	export function setMarker(lat: number, lon: number) {
		marker?.remove();
		marker = new maplibregl.Marker({ color: '#60a5fa' })
			.setLngLat([lon, lat])
			.addTo(map);
	}

	export function clearMarker() {
		marker?.remove();
		marker = null;
	}

	// Polygon mode: draw the input polygon outline + a risk-colored heatmap of
	// the res-9 cells it covers, and fit the view to the polygon.
	export function showPolygon(rings: number[][][]) {
		if (!map || !map.getSource('eudr-polygon')) return;
		const src = map.getSource('eudr-polygon') as maplibregl.GeoJSONSource;
		src.setData({ type: 'Feature', properties: {}, geometry: { type: 'Polygon', coordinates: rings } });
		// Fit bounds
		const bounds = new maplibregl.LngLatBounds();
		for (const ring of rings) for (const [lng, lat] of ring) bounds.extend([lng, lat]);
		if (!bounds.isEmpty()) map.fitBounds(bounds, { padding: 60, duration: 1200, maxZoom: 12 });
	}

	export function showCells(cells: { h3index: string; risk: number }[]) {
		if (!map || !map.getSource('eudr-cells')) return;
		import('h3-js').then(({ cellToBoundary }) => {
			const features = cells.map((c) => {
				const b = cellToBoundary(c.h3index);
				const ring = b.map(([lat, lng]) => [lng, lat]);
				ring.push(ring[0]);
				return {
					type: 'Feature' as const,
					properties: { risk: c.risk ?? 0 },
					geometry: { type: 'Polygon' as const, coordinates: [ring] },
				};
			});
			const src = map.getSource('eudr-cells') as maplibregl.GeoJSONSource;
			src.setData({ type: 'FeatureCollection', features });
		});
	}

	export function clearPolygon() {
		const empty = { type: 'FeatureCollection' as const, features: [] };
		(map?.getSource('eudr-cells') as maplibregl.GeoJSONSource)?.setData(empty);
		(map?.getSource('eudr-polygon') as maplibregl.GeoJSONSource)?.setData(empty as any);
	}

	// Draw the evaluated H3 res-9 hexagon so the user sees the ~0.1 km²
	// aggregation unit instead of inferring point-level precision from the marker.
	export function showCell(h3index: string) {
		if (!map || !map.getSource('eudr-cell')) return;
		import('h3-js').then(({ cellToBoundary }) => {
			const boundary = cellToBoundary(h3index); // [[lat, lng], ...]
			const ring = boundary.map(([lat, lng]) => [lng, lat]);
			ring.push(ring[0]);
			const source = map.getSource('eudr-cell') as maplibregl.GeoJSONSource;
			source.setData({
				type: 'Feature',
				properties: {},
				geometry: { type: 'Polygon', coordinates: [ring] },
			});
		});
	}

	onMount(() => {
		map = new maplibregl.Map({
			container: mapContainer,
			style: BASEMAP,
			center: MAP_CENTER,
			zoom: MAP_ZOOM,
			minZoom: 4,
			maxZoom: 14,
			attributionControl: false,
		});

		map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');

		map.on('load', () => {
			// Evaluated H3 cell (filled in by showCell)
			map.addSource('eudr-cell', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] },
			});

			// Polygon-mode sources: covered cells (heatmap) + input polygon outline
			map.addSource('eudr-cells', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] },
			});
			map.addSource('eudr-polygon', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] },
			});
			map.addSource('eudr-draw', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] },
			});

			// Province boundaries
			map.addSource('eudr-provinces', {
				type: 'geojson',
				data: '/data/eudr_provinces_boundary.json',
			});

			map.addLayer({
				id: 'eudr-provinces-fill',
				type: 'fill',
				source: 'eudr-provinces',
				paint: {
					'fill-color': '#ffffff',
					'fill-opacity': 0.05,
				},
			});

			map.addLayer({
				id: 'eudr-provinces-line',
				type: 'line',
				source: 'eudr-provinces',
				paint: {
					'line-color': '#ec4899',
					'line-width': 1.2,
					'line-opacity': 0.7,
				},
			});

			// Area of interest: NEA + cross-border (PY/BR), yellow — matches main app
			map.addSource('eudr-focus', {
				type: 'geojson',
				data: '/data/eudr_focus_boundary.json',
			});
			map.addLayer({
				id: 'eudr-focus-line',
				type: 'line',
				source: 'eudr-focus',
				paint: {
					'line-color': '#facc15',
					'line-width': 2.2,
					'line-opacity': 0.95,
				},
			});

			// Evaluated hexagon — drawn on top of provinces
			map.addLayer({
				id: 'eudr-cell-fill',
				type: 'fill',
				source: 'eudr-cell',
				paint: {
					'fill-color': '#60a5fa',
					'fill-opacity': 0.18,
				},
			});

			map.addLayer({
				id: 'eudr-cell-line',
				type: 'line',
				source: 'eudr-cell',
				paint: {
					'line-color': '#60a5fa',
					'line-width': 2,
					'line-opacity': 0.9,
				},
			});

			// Polygon-mode: risk-colored cells (heatmap)
			map.addLayer({
				id: 'eudr-cells-fill',
				type: 'fill',
				source: 'eudr-cells',
				paint: {
					'fill-color': [
						'interpolate', ['linear'], ['get', 'risk'],
						0, '#22c55e', 25, '#84cc16', 50, '#f59e0b', 75, '#ef4444', 100, '#991b1b',
					],
					'fill-opacity': 0.55,
				},
			});

			// Polygon-mode: input polygon outline
			map.addLayer({
				id: 'eudr-polygon-line',
				type: 'line',
				source: 'eudr-polygon',
				paint: {
					'line-color': '#ffffff',
					'line-width': 2,
					'line-dasharray': [2, 1],
					'line-opacity': 0.9,
				},
			});

			// Draw-in-progress: line + vertices
			map.addLayer({
				id: 'eudr-draw-line',
				type: 'line',
				source: 'eudr-draw',
				filter: ['==', '$type', 'LineString'],
				paint: { 'line-color': '#60a5fa', 'line-width': 2, 'line-dasharray': [2, 1] },
			});
			map.addLayer({
				id: 'eudr-draw-pts',
				type: 'circle',
				source: 'eudr-draw',
				filter: ['==', '$type', 'Point'],
				paint: {
					'circle-radius': 4,
					'circle-color': '#60a5fa',
					'circle-stroke-color': '#ffffff',
					'circle-stroke-width': 1.5,
				},
			});

			// Province labels
			const provinces = [
				{ name: 'Jujuy', coords: [-65.7, -23.3] },
				{ name: 'Salta', coords: [-65.0, -24.5] },
				{ name: 'Tucuman', coords: [-65.5, -27.0] },
				{ name: 'Catamarca', coords: [-66.8, -28.0] },
				{ name: 'Sgo. del Estero', coords: [-63.5, -28.0] },
				{ name: 'Formosa', coords: [-59.5, -25.0] },
				{ name: 'Chaco', coords: [-60.5, -26.5] },
				{ name: 'Corrientes', coords: [-58.0, -29.0] },
				{ name: 'Misiones', coords: [-54.8, -27.0] },
				{ name: 'Entre Rios', coords: [-59.5, -32.0] },
			];

			for (const p of provinces) {
				const el = document.createElement('div');
				el.className = 'eudr-label';
				el.textContent = p.name;
				new maplibregl.Marker({ element: el })
					.setLngLat(p.coords as [number, number])
					.addTo(map);
			}
		});

		// Click handler
		map.on('click', (e) => {
			const lat = e.lngLat.lat;
			const lon = e.lngLat.lng;

			// Draw mode: accumulate vertices instead of running a point check
			if (drawMode) {
				drawPts.push([lon, lat]);
				updateDrawSource();
				return;
			}

			setMarker(lat, lon);

			// Convert to H3 (done in parent via callback)
			if (onCellClick) {
				// Lazy-load h3-js
				import('h3-js').then(({ latLngToCell }) => {
					const h3index = latLngToCell(lat, lon, 9);
					showCell(h3index);
					onCellClick(lat, lon, h3index);
				});
			}
		});

		map.on('dblclick', (e) => {
			if (drawMode) {
				e.preventDefault();
				finishDraw();
			}
		});

		return () => map?.remove();
	});
</script>

<div bind:this={mapContainer} class="w-full h-full rounded-lg overflow-hidden"></div>

<style>
	:global(.eudr-label) {
		color: rgba(255, 255, 255, 0.6);
		font-size: 11px;
		font-family: 'JetBrains Mono', monospace;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.1em;
		text-shadow: 0 1px 4px rgba(0, 0, 0, 0.9);
		pointer-events: none;
	}
</style>
