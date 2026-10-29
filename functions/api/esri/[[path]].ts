// Esri World Imagery tile proxy. Unlike functions/api/satellite (EOX), the point is
// the key, not CORS: ESRI_API_KEY (Pages secret; .dev.vars locally) is added here so
// it never reaches the browser bundle. Esri's referrer restriction did not stop
// direct requests from foreign origins in testing, so a key in the page could be
// copied and spend the free quota.
// No edge caching (cf cacheEverything off): Esri terms do not allow storing tiles.
// Browser caching only. Same {z}/{y}/{x} order as Esri's tile endpoint.
const TILE_PATH_RE = /^(\d+)\/(\d+)\/(\d+)$/;
const ALLOWED_HOST_RE = /^(spatia\.ar|www\.spatia\.ar|[a-z0-9-]+\.neahub\.pages\.dev|neahub\.pages\.dev|localhost|127\.0\.0\.1)$/;

interface Env { ESRI_API_KEY?: string }

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
