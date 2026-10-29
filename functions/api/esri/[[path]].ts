// Esri World Imagery tile proxy. Unlike functions/api/satellite (EOX), the point is
// the key, not CORS: ESRI_API_KEY (Pages secret; .dev.vars locally) is added here so
// it never reaches the browser bundle. Esri's referrer restriction did not stop
// direct requests from foreign origins in testing, so a key in the page could be
// copied and spend the free quota.
// No edge caching (cf cacheEverything off): Esri terms do not allow storing tiles.
// Browser caching only. Same {z}/{y}/{x} order as Esri's tile endpoint.
const TILE_PATH_RE = /^(\d+)\/(\d+)\/(\d+)$/;
// Spatia's region (NEA, Paraguay, PR/SC/RS) — same box as the map source bounds.
// Tiles outside it are refused so the proxy cannot spend the quota on the world.
const REGION = { west: -63, south: -34, east: -48, north: -19 };
const ALLOWED_HOST_RE = /^(spatia\.ar|www\.spatia\.ar|[a-z0-9-]+\.neahub\.pages\.dev|neahub\.pages\.dev|localhost|127\.0\.0\.1)$/;

interface Env { ESRI_API_KEY?: string }

function tileInRegion(z: number, x: number, y: number): boolean {
	const n = 2 ** z;
	const lng = (t: number) => (t / n) * 360 - 180;
	const lat = (t: number) => (Math.atan(Math.sinh(Math.PI * (1 - (2 * t) / n))) * 180) / Math.PI;
	return lng(x + 1) > REGION.west && lng(x) < REGION.east && lat(y + 1) < REGION.north && lat(y) > REGION.south;
}

function allowedOrigin(request: Request): boolean {
	const src = request.headers.get('Origin') ?? request.headers.get('Referer');
	if (!src) return false;
	try { return ALLOWED_HOST_RE.test(new URL(src).hostname); } catch { return false; }
}

export const onRequestGet: PagesFunction<Env> = async ({ params, request, env }) => {
	const path = Array.isArray(params.path) ? params.path.join('/') : (params.path ?? '');
	const m = TILE_PATH_RE.exec(path);
	if (!m) return new Response('Invalid tile path', { status: 400 });
	if (!env.ESRI_API_KEY) return new Response('Not configured', { status: 503 });
	if (!allowedOrigin(request)) return new Response('Forbidden', { status: 403 });

	const [, z, y, x] = m;
	if (!tileInRegion(+z, +x, +y)) return new Response('Outside region', { status: 404 });
	const upstream = await fetch(
		`https://ibasemaps-api.arcgis.com/arcgis/rest/services/World_Imagery/MapServer/tile/${z}/${y}/${x}?token=${env.ESRI_API_KEY}`,
		{ headers: { Referer: 'https://spatia.ar/' } }
	);
	// Quota spent / key expired / no tile → non-200; the client then shows EOX underneath.
	if (!upstream.ok || !(upstream.headers.get('Content-Type') ?? '').startsWith('image/')) {
		return new Response('Tile unavailable', { status: upstream.ok ? 502 : upstream.status });
	}
	return new Response(upstream.body, {
		headers: {
			'Content-Type': upstream.headers.get('Content-Type') ?? 'image/jpeg',
			'Cache-Control': 'private, max-age=86400'
		}
	});
};
