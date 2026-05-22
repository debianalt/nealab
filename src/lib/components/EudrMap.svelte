<script lang="ts">
	import { onMount } from 'svelte';
	import maplibregl from 'maplibre-gl';
	import 'maplibre-gl/dist/maplibre-gl.css';

	interface Props {
		onCellClick?: (lat: number, lon: number, h3index: string) => void;
	}

	let { onCellClick }: Props = $props();

	let mapContainer: HTMLDivElement;
	let map: maplibregl.Map;
	let marker: maplibregl.Marker | null = null;

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

	// Draw the evaluated H3 res-7 hexagon so the user sees the ~5 km²
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
					'line-color': '#ffffff',
					'line-width': 1.5,
					'line-opacity': 0.4,
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
