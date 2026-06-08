// Sentinel-2 cloudless tile proxy (EOX). Mirrors functions/api/terrain to dodge CORS,
// cache on Cloudflare, and keep the client pointed at our own origin.
// MapLibre requests XYZ as {z}/{x}/{y}; EOX WMTS GoogleMapsCompatible is {z}/{TileRow}/{TileCol}
// = {z}/{y}/{x}, so x and y are swapped when building the upstream URL.
const TILE_PATH_RE = /^(\d+)\/(\d+)\/(\d+)\.jpg$/;

export const onRequestGet: PagesFunction = async ({ params }) => {
	const path = Array.isArray(params.path) ? params.path.join('/') : (params.path ?? '');

	const m = TILE_PATH_RE.exec(path);
	if (!m) {
		return new Response('Invalid tile path', { status: 400 });
	}
	const [, z, x, y] = m;

	const tileUrl = `https://tiles.maps.eox.at/wmts/1.0.0/s2cloudless-2024_3857/default/GoogleMapsCompatible/${z}/${y}/${x}.jpg`;

	const tileResponse = await fetch(tileUrl, {
		cf: { cacheTtl: 604800, cacheEverything: true }
	});

	if (!tileResponse.ok) {
		return new Response('Tile not found', { status: tileResponse.status });
	}

	return new Response(tileResponse.body, {
		headers: {
			'Content-Type': 'image/jpeg',
			'Access-Control-Allow-Origin': '*',
			'Cache-Control': 'public, max-age=86400, s-maxage=604800'
		}
	});
};
