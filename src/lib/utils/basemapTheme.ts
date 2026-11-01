// Basemap per theme without setStyle. Carto Positron (claro) and Dark Matter (oscuro)
// have the same 95 layer ids with identical type, source, filter, zoom range and
// layout (diffed 1-nov-2026; only the paint, the sprite and the slot of
// `waterway_label` differ). So a theme change copies the other style's paint onto the
// mounted layers and swaps the sprite: Spatia's ~40 own layers, their data, filters
// and selection survive untouched.
//
// Claro is the atlas sheet of the El Soberbio demo (demo/el-nino atlasStyle): paper,
// no land-cover blotches, POIs, house numbers or aeroways.
import type * as maplibregl from '$lib/maplibre';
import { BASEMAP_STYLE } from '$lib/config';
import { MAP_THEME, type MapTema } from '$lib/utils/sello';

type Style = { sprite?: unknown; layers: Array<{ id: string; type: string; paint?: Record<string, unknown>; layout?: Record<string, unknown> }> };

const cache = new Map<MapTema, Promise<Style>>();
export function basemapStyle(t: MapTema): Promise<Style> {
	let p = cache.get(t);
	if (!p) {
		p = fetch(BASEMAP_STYLE[t]).then((r) => {
			if (!r.ok) throw new Error(`basemap ${t}: HTTP ${r.status}`);
			return r.json();
		});
		p.catch(() => cache.delete(t));
		cache.set(t, p);
	}
	return p;
}

// Hidden in claro, shown again (as the style says) in oscuro. Not `landuse_residential`:
// the faint urban tint is useful context on paper.
const ATLAS_HIDE = /^(landcover|park_|landuse$|poi_|housenumber|aeroway)/;

/** The first symbol layer sits in different slots: `waterway_label` comes before the
 *  roads in Positron and after them in Dark Matter. Put it with the other labels so the
 *  layer order (and every `beforeId` computed from it) is the same in both themes. */
export function normaliseBasemapOrder(map: maplibregl.Map) {
	const layers = map.getStyle().layers ?? [];
	const firstOther = layers.find((l) => l.type === 'symbol' && l.id !== 'waterway_label');
	if (firstOther && map.getLayer('waterway_label')) map.moveLayer('waterway_label', firstOther.id);
}

/**
 * Repaint the basemap's own layers for a theme. `target` = that theme's style JSON
 * (basemapStyle); null when the map was created with it, so only the retouches apply.
 * Never touches visibility outside ATLAS_HIDE (Map.svelte hides the admin lines and,
 * in the cadastre, the flat buildings) nor any layer that is not the basemap's.
 */
export function applyBasemapTheme(map: maplibregl.Map, t: MapTema, target: Style | null) {
	const T = MAP_THEME[t];
	const current = new Map((map.getStyle().layers ?? []).map((l: any) => [l.id, l]));
	const layers = target?.layers ?? [...current.values()].filter((l: any) => l.source === 'protomaps' || l.type === 'background');
	for (const tl of layers) {
		const cl: any = current.get(tl.id);
		if (!cl) continue;
		if (target) {
			const want = tl.paint ?? {};
			const have = cl.paint ?? {};
			for (const k of new Set([...Object.keys(have), ...Object.keys(want)])) {
				if (JSON.stringify(have[k]) === JSON.stringify(want[k])) continue;
				try { map.setPaintProperty(tl.id, k as any, want[k] as any); } catch { /* unsupported in this layer */ }
			}
		}
		if (ATLAS_HIDE.test(tl.id)) {
			const vis = t === 'claro' ? 'none' : ((tl.layout?.visibility as string) ?? 'visible');
			map.setLayoutProperty(tl.id, 'visibility', vis as any);
		}
		if (tl.type === 'symbol' && cl.layout?.['text-field']) {
			try {
				map.setPaintProperty(tl.id, 'text-halo-color', T.halo);
				map.setPaintProperty(tl.id, 'text-halo-width', T.haloWidth);
				if (T.placeText && tl.id.startsWith('place_')) map.setPaintProperty(tl.id, 'text-color', T.placeText);
			} catch { /* layer may not support it */ }
		}
	}
	if (T.paper && map.getLayer('background')) map.setPaintProperty('background', 'background-color', T.paper);
	if (T.water && map.getLayer('water')) map.setPaintProperty('water', 'fill-color', T.water);
	if (T.waterway && map.getLayer('waterway')) map.setPaintProperty('waterway', 'line-color', T.waterway);
	if (target?.sprite && typeof target.sprite === 'string' && map.getStyle().sprite !== target.sprite) {
		map.setSprite(target.sprite);
	}
}

/** Id of the basemap layer right above `water`: the hillshade goes there in claro,
 *  under roads, labels and data (the atlas convention: relief is ground, not a veil). */
export function layerAfterWater(map: maplibregl.Map): string | undefined {
	const layers = map.getStyle().layers ?? [];
	const i = layers.findIndex((l) => l.id === 'water');
	if (i < 0) return undefined;
	return layers.slice(i + 1).find((l) => l.id !== 'hillshade')?.id;
}
