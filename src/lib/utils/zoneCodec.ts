// Lasso zones in the URL (7-nov-2026): a shared link reproduces the zones instead of asking
// whoever opens it to draw them again (and pick another border cell).
//
// Format of the `z` parameter: <letter>.<cells>.<outline> per zone, all joined with «.», e.g.
// z=A.25.<outline>.C.61.<outline>. The letter keeps «zona A» of a post; <cells> is how many
// hexagons the zone had when shared, so the page can say if today's data takes another count.
// The outline is the Google polyline algorithm (deltas, zigzag, 5-bit chunks, 5 decimals ≈ 1 m)
// over the base64url alphabet: the standard one uses ? @ [ \ ] ^ ` { | } ~, which a URL escapes
// as %XX. «.» is the only separator because URLSearchParams escapes even «~».
// Zones saved from a chart are left out: they are a rule over the data (a Moran quadrant, a
// box in the scatter), not a place; frozen as a list of cells they would outlive the rule.

const ALPHA = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
const IDX = new Map([...ALPHA].map((c, i) => [c, i]));
const SCALE = 1e5;

export type Ring = [number, number][];

function encodeValue(v: number, out: string[]) {
	let z = v < 0 ? ~(v << 1) : v << 1;
	while (z >= 0x20) { out.push(ALPHA[0x20 | (z & 0x1f)]); z >>>= 5; }
	out.push(ALPHA[z]);
}

/** [lng, lat] points → URL-safe string (5 decimals). */
export function encodePolyline(pts: Ring): string {
	const out: string[] = [];
	let px = 0, py = 0;
	for (const [lng, lat] of pts) {
		const x = Math.round(lng * SCALE), y = Math.round(lat * SCALE);
		encodeValue(x - px, out); encodeValue(y - py, out);
		px = x; py = y;
	}
	return out.join('');
}

/** Inverse of encodePolyline; null on any character outside the alphabet. */
export function decodePolyline(s: string): Ring | null {
	const vals: number[] = [];
	let shift = 0, acc = 0;
	for (const c of s) {
		const b = IDX.get(c);
		if (b === undefined) return null;
		acc |= (b & 0x1f) << shift;
		if (b & 0x20) { shift += 5; continue; }
		vals.push(acc & 1 ? ~(acc >>> 1) : acc >>> 1);
		shift = 0; acc = 0;
	}
	if (shift !== 0 || vals.length % 2) return null;
	const pts: Ring = [];
	let x = 0, y = 0;
	for (let i = 0; i < vals.length; i += 2) {
		x += vals[i]; y += vals[i + 1];
		pts.push([x / SCALE, y / SCALE]);
	}
	return pts;
}

/** Points rounded to the 5 decimals the URL keeps (what the page that opens the link sees). */
export function roundRing(pts: Ring): Ring {
	return pts.map(([x, y]) => [Math.round(x * SCALE) / SCALE, Math.round(y * SCALE) / SCALE]);
}

function perpDist(p: [number, number], a: [number, number], b: [number, number]): number {
	const dx = b[0] - a[0], dy = b[1] - a[1];
	const len2 = dx * dx + dy * dy;
	if (len2 === 0) return Math.hypot(p[0] - a[0], p[1] - a[1]);
	const t = Math.max(0, Math.min(1, ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / len2));
	return Math.hypot(p[0] - (a[0] + t * dx), p[1] - (a[1] + t * dy));
}

/** Douglas–Peucker in degrees (planar: a lasso spans a few km). Keeps first and last points. */
export function simplifyRing(pts: Ring, tol: number): Ring {
	if (pts.length <= 3 || tol <= 0) return pts.slice();
	const keep = new Uint8Array(pts.length);
	keep[0] = keep[pts.length - 1] = 1;
	const stack: [number, number][] = [[0, pts.length - 1]];
	while (stack.length) {
		const [i, j] = stack.pop()!;
		let maxD = 0, idx = -1;
		for (let k = i + 1; k < j; k++) {
			const d = perpDist(pts[k], pts[i], pts[j]);
			if (d > maxD) { maxD = d; idx = k; }
		}
		if (idx >= 0 && maxD > tol) { keep[idx] = 1; stack.push([i, idx], [idx, j]); }
	}
	return pts.filter((_, k) => keep[k]);
}

/**
 * The shortest outline that still picks exactly the zone's cells: simplify with a tolerance
 * that halves until the cells found inside the rounded outline are the same set (a simplified
 * edge can move a border cell's centre in or out). Falls back to the full outline, rounded.
 */
export function urlOutline(polygon: Ring, cells: string[], find: (ring: Ring) => string[]): string {
	const want = new Set(cells);
	const same = (ring: Ring) => {
		const got = find(ring);
		return got.length === want.size && got.every(h => want.has(h));
	};
	for (let tol = 2e-4; tol >= 1e-6; tol /= 2) {
		const ring = roundRing(simplifyRing(polygon, tol));
		if (ring.length >= 4 && same(ring)) return encodePolyline(ring);
	}
	return encodePolyline(roundRing(polygon));
}

export interface UrlZone { id: string; n: number; ring: Ring; outline: string }

export function encodeZones(zones: { id: string; n: number; outline: string }[]): string {
	return zones.map(z => `${z.id}.${z.n}.${z.outline}`).join('.');
}

/** Tolerant: a malformed zone is skipped, not the whole parameter. */
export function decodeZones(s: string | null): UrlZone[] {
	if (!s) return [];
	const parts = s.split('.');
	const out: UrlZone[] = [];
	for (let i = 0; i + 2 < parts.length; i += 3) {
		const [id, n, outline] = [parts[i], parts[i + 1], parts[i + 2]];
		if (!/^[A-Z]$/.test(id) || !/^\d{1,6}$/.test(n)) continue;
		const ring = decodePolyline(outline);
		if (!ring || ring.length < 4 || out.some(z => z.id === id)) continue;
		out.push({ id, n: Number(n), ring, outline });
	}
	return out;
}
