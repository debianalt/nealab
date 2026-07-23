<script lang="ts">
	import { onMount } from 'svelte';
	import maplibregl from 'maplibre-gl';
	import 'maplibre-gl/dist/maplibre-gl.css';

	interface Props {
		onCellClick?: (lat: number, lon: number, h3index: string) => void;
		onPolygonDrawn?: (rings: number[][][]) => void;
		onDrawModeChange?: (active: boolean) => void;
		/** When false, all gesture handlers and zoom controls are disabled and the
		 *  camera moves only programmatically (flyTo/showCells). Used by the guided
		 *  storymap so the wheel scrolls the page instead of zooming the map. */
		interactive?: boolean;
	}

	let { onCellClick, onPolygonDrawn, onDrawModeChange, interactive = true }: Props = $props();

	let mapContainer: HTMLDivElement;
	let map: maplibregl.Map;
	let marker: maplibregl.Marker | null = null;

	// Lasso state — press+drag to outline a region, release to close.
	// Same UX pattern as the main app's lasso (much friendlier than click-vertex).
	let lassoActive = false;
	let lassoDrawing = false;
	let lassoPoints: [number, number][] = [];

	function updateLassoSource() {
		if (!map) return;
		const src = map.getSource('eudr-lasso') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;
		if (lassoPoints.length === 0) {
			src.setData({ type: 'FeatureCollection', features: [] });
			return;
		}
		const coords = lassoPoints.length >= 3
			? [[...lassoPoints, lassoPoints[0]]]
			: [lassoPoints];
		src.setData({ type: 'Feature', geometry: { type: 'Polygon', coordinates: coords }, properties: {} });
	}

	export function setLassoMode(active: boolean) {
		if (active) { clearMarker(); clearPolygon(); }
		lassoActive = active;
		lassoDrawing = false;
		lassoPoints = [];
		updateLassoSource();
		if (map) {
			map.getCanvas().style.cursor = active ? 'crosshair' : '';
			if (active) map.dragPan.disable(); else map.dragPan.enable();
		}
		onDrawModeChange?.(active);
	}

	export function cancelLasso() { setLassoMode(false); }

	const MAP_CENTER: [number, number] = [-56, -26];
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

	// Frame a bounding box so the whole extent stays visible. Generous top/bottom
	// padding keeps the full N-S extent in view (a fixed-zoom flyTo clipped the
	// southern edge); left padding clears the storymap's text column on wide
	// screens so the province sits fully in the open right area.
	export function fitBounds(bounds: [[number, number], [number, number]]) {
		if (!map) return;
		const w = map.getContainer().clientWidth;
		let left = 30;
		let right = 30;
		if (w >= 820) {
			const trackLeft = Math.max(0, (w - 1200) / 2);
			left = trackLeft + 500; // clear the ~500px text card
			right = 48;
		}
		// Bottom padding >> top so the frame sits higher: it both zooms out for a
		// margin and biases the province upward, keeping the southern edge clear of
		// the viewport bottom (the previous framing clipped it).
		map.fitBounds(bounds as maplibregl.LngLatBoundsLike, {
			padding: { top: 56, bottom: 150, left, right },
			duration: 1500,
			maxZoom: 8.5,
		});
	}

	export function setMarker(lat: number, lon: number) {
		marker?.remove();
		marker = new maplibregl.Marker({ color: '#ec4899' })
			.setLngLat([lon, lat])
			.addTo(map);
	}

	export function clearMarker() {
		marker?.remove();
		marker = null;
	}

	export function clearCell() {
		(map?.getSource('eudr-cell') as maplibregl.GeoJSONSource)?.setData({ type: 'FeatureCollection', features: [] });
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
			interactive,
		});

		if (interactive) {
			map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
		}

		map.on('load', () => {
			// Hide the basemap's own admin/boundary lines so they don't compete
			// with our GADM pink (AR) / yellow (focus) outlines — single source of truth.
			for (const lyr of map.getStyle().layers ?? []) {
				if (/admin|boundary/i.test(lyr.id) && (lyr.type === 'line' || lyr.type === 'fill')) {
					try { map.setLayoutProperty(lyr.id, 'visibility', 'none'); } catch {}
				}
			}

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
			map.addSource('eudr-lasso', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] },
			});

			// (Pink AR provinces removed — only the yellow area-of-interest is shown
			// here for clarity; users came to this tool to focus on the relevant zone.)

			// Area of interest: NEA + cross-border (PY/BR), yellow — matches main app.
			// Layer added LATER, after heatmap & admin-2, so yellow stays on top.
			map.addSource('eudr-focus', {
				type: 'geojson',
				data: '/data/eudr_focus_boundary.json',
			});
			// Admin-2 (depts/distritos/municipios) — thin white line, drawn ABOVE the
			// hexagon heatmap so users can orient inside the choropleth.
			map.addSource('eudr-admin2', {
				type: 'geojson',
				data: '/data/eudr_admin2_boundary.json',
			});

			// Evaluated hexagon — drawn on top of provinces
			map.addLayer({
				id: 'eudr-cell-fill',
				type: 'fill',
				source: 'eudr-cell',
				paint: {
					'fill-color': '#facc15',
					'fill-opacity': 0.22,
				},
			});

			map.addLayer({
				id: 'eudr-cell-line',
				type: 'line',
				source: 'eudr-cell',
				paint: {
					'line-color': '#facc15',
					'line-width': 2.2,
					'line-opacity': 0.95,
				},
			});

			// Polygon-mode: risk-colored cells (heatmap, warm ramp only)
			map.addLayer({
				id: 'eudr-cells-fill',
				type: 'fill',
				source: 'eudr-cells',
				paint: {
					'fill-color': [
						'interpolate', ['linear'], ['get', 'risk'],
						0, '#fef3c7', 25, '#fde047', 50, '#f59e0b', 75, '#ef4444', 100, '#991b1b',
					],
					'fill-opacity': 0.55,
				},
			});

			// Admin-2 lines (thin, white, subordinate to admin-1)
			map.addLayer({
				id: 'eudr-admin2-line',
				type: 'line',
				source: 'eudr-admin2',
				paint: {
					'line-color': '#ffffff',
					'line-width': 0.6,
					'line-opacity': 0.32,
				},
			});

			// Yellow admin-1 focus — drawn AFTER admin-2 so it stays on top for orientation
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

			// Lasso outline-in-progress: fill + dashed line
			map.addLayer({
				id: 'eudr-lasso-fill',
				type: 'fill',
				source: 'eudr-lasso',
				paint: { 'fill-color': '#facc15', 'fill-opacity': 0.15 },
			});
			map.addLayer({
				id: 'eudr-lasso-line',
				type: 'line',
				source: 'eudr-lasso',
				paint: { 'line-color': '#facc15', 'line-width': 2, 'line-dasharray': [3, 2], 'line-opacity': 0.95 },
			});

			// (Province labels removed — the basemap already shows place labels and
			// the yellow area-of-interest outline is enough orientation.)
		});

		// Click handler — runs only when NOT in lasso mode (lasso swallows the gesture)
		map.on('click', (e) => {
			if (!interactive || lassoActive) return;
			const lat = e.lngLat.lat;
			const lon = e.lngLat.lng;
			setMarker(lat, lon);
			if (onCellClick) {
				import('h3-js').then(({ latLngToCell }) => {
					const h3index = latLngToCell(lat, lon, 9);
					showCell(h3index);
					onCellClick(lat, lon, h3index);
				});
			}
		});

		// Lasso interaction: press-and-drag to outline, release to finish
		map.on('mousedown', (e) => {
			if (!lassoActive || e.originalEvent.button !== 0) return;
			e.preventDefault();
			lassoDrawing = true;
			lassoPoints = [[e.lngLat.lng, e.lngLat.lat]];
			updateLassoSource();
		});
		map.on('mousemove', (e) => {
			if (!lassoDrawing) return;
			lassoPoints.push([e.lngLat.lng, e.lngLat.lat]);
			updateLassoSource();
		});
		map.on('mouseup', () => {
			if (!lassoDrawing) return;
			lassoDrawing = false;
			if (lassoPoints.length >= 3) {
				const ring = [...lassoPoints, lassoPoints[0]];
				onPolygonDrawn?.([ring]);
			}
			setLassoMode(false);
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
