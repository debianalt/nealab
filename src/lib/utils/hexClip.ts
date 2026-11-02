// Hexagons cut at the official department boundary (decided with the author on
// 1-nov-2026). The line on the map is the official polygon (INDEC / DGEEC / IBGE);
// each cell belongs to its department by the areal crosswalk of census radios (or
// the largest-area district in PY/BR), so the department's cells poke out of the
// line or stop short of it by up to one cell, and the coarser cells of the aggregated
// view by 1–3 km. Drawn clipped, nothing crosses the line; a cell keeps its own
// value (it is the cell's, satellite or dasymetric, not the department's).
//
// Only cells the boundary passes through are cut: a grid index of the boundary
// segments finds them, and the polygon is clipped against the (convex) hexagon with
// Sutherland–Hodgman, ring by ring (holes included). Planar lng/lat is fine at
// department scale.

type Pt = [number, number];
type Ring = Pt[];

export interface HexClip {
	polys: { rings: Ring[]; bbox: [number, number, number, number] }[];
	east: number;                  // easternmost x (end of the point-in-polygon ray)
	grid: Map<string, number[]>;   // bucket → segment ids
	segs: [Pt, Pt][];
}

const K = 0.01;   // grid bucket, degrees (~1 km): a res-9 cell spans one or two
const bucket = (x: number, y: number) => `${Math.floor(x / K)},${Math.floor(y / K)}`;

const open = (r: Ring): Ring => (r.length > 1 && r[0][0] === r[r.length - 1][0] && r[0][1] === r[r.length - 1][1] ? r.slice(0, -1) : r);
const close = (r: Ring): Ring => [...r, r[0]];

function bboxOf(r: Ring): [number, number, number, number] {
	let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
	for (const [x, y] of r) { if (x < w) w = x; if (x > e) e = x; if (y < s) s = y; if (y > n) n = y; }
	return [w, s, e, n];
}

/** Index of a department polygon (GeoJSON Feature or geometry), or null without one. */
export function makeHexClip(feature: any): HexClip | null {
	const g = feature?.geometry ?? feature;
	if (!g) return null;
	const coords: Pt[][][] = g.type === 'Polygon' ? [g.coordinates] : g.type === 'MultiPolygon' ? g.coordinates : [];
	if (!coords.length) return null;
	const clip: HexClip = { polys: [], east: -Infinity, grid: new Map(), segs: [] };
	for (const poly of coords) {
		const rings = poly.map((r) => open(r as Ring)).filter((r) => r.length >= 3);
		if (!rings.length) continue;
		clip.polys.push({ rings, bbox: bboxOf(rings[0]) });
		clip.east = Math.max(clip.east, bboxOf(rings[0])[2]);
		for (const r of rings) {
			for (let i = 0; i < r.length; i++) {
				const a = r[i], b = r[(i + 1) % r.length];
				const id = clip.segs.push([a, b]) - 1;
				const [x0, x1] = a[0] < b[0] ? [a[0], b[0]] : [b[0], a[0]];
				const [y0, y1] = a[1] < b[1] ? [a[1], b[1]] : [b[1], a[1]];
				for (let gx = Math.floor(x0 / K); gx <= Math.floor(x1 / K); gx++)
					for (let gy = Math.floor(y0 / K); gy <= Math.floor(y1 / K); gy++) {
						const k = `${gx},${gy}`;
						const list = clip.grid.get(k);
						if (list) list.push(id); else clip.grid.set(k, [id]);
					}
			}
		}
	}
	return clip.polys.length ? clip : null;
}

function touchesBoundary(clip: HexClip, bb: [number, number, number, number]): boolean {
	const [w, s, e, n] = bb;
	for (let gx = Math.floor(w / K); gx <= Math.floor(e / K); gx++)
		for (let gy = Math.floor(s / K); gy <= Math.floor(n / K); gy++) {
			for (const id of clip.grid.get(`${gx},${gy}`) ?? []) {
				const [a, b] = clip.segs[id];
				if (Math.max(a[0], b[0]) >= w && Math.min(a[0], b[0]) <= e && Math.max(a[1], b[1]) >= s && Math.min(a[1], b[1]) <= n) return true;
			}
		}
	return false;
}

/** Even-odd point-in-polygon with a ray to the east, reading only the index row the
 *  point sits in (holes and multipolygons count right with even-odd). */
function insideByIndex(clip: HexClip, [x, y]: Pt): boolean {
	const gy = Math.floor(y / K), seen = new Set<number>();
	let inside = false;
	for (let gx = Math.floor(x / K); gx <= Math.floor(clip.east / K); gx++) {
		for (const id of clip.grid.get(`${gx},${gy}`) ?? []) {
			if (seen.has(id)) continue;
			seen.add(id);
			const [[xi, yi], [xj, yj]] = clip.segs[id];
			if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) inside = !inside;
		}
	}
	return inside;
}

/** Sutherland–Hodgman: `subject` (any simple ring) clipped by a convex ring. */
function clipByConvex(subject: Ring, convex: Ring): Ring {
	let area = 0;
	for (let i = 0; i < convex.length; i++) {
		const [x1, y1] = convex[i], [x2, y2] = convex[(i + 1) % convex.length];
		area += x1 * y2 - x2 * y1;
	}
	const sign = area >= 0 ? 1 : -1;
	let out = subject;
	for (let i = 0; i < convex.length && out.length; i++) {
		const a = convex[i], b = convex[(i + 1) % convex.length];
		const inside = (p: Pt) => sign * ((b[0] - a[0]) * (p[1] - a[1]) - (b[1] - a[1]) * (p[0] - a[0])) >= 0;
		const cross = (p: Pt, q: Pt): Pt => {
			const dx = q[0] - p[0], dy = q[1] - p[1], ex = b[0] - a[0], ey = b[1] - a[1];
			const t = (ex * (p[1] - a[1]) - ey * (p[0] - a[0])) / (ey * dx - ex * dy);
			return [p[0] + t * dx, p[1] + t * dy];
		};
		const input = out;
		out = [];
		for (let j = 0; j < input.length; j++) {
			const p = input[j], q = input[(j + 1) % input.length];
			const pin = inside(p), qin = inside(q);
			if (pin) out.push(p);
			if (pin !== qin) out.push(cross(p, q));
		}
	}
	return out;
}

/** GeoJSON geometry of a cell (`boundary` = its [lng, lat] ring) cut at the department
 *  boundary: the cell itself when the boundary does not cross it, null when nothing of
 *  it lies inside. */
export function clipCell(clip: HexClip | null, boundary: number[][]): any | null {
	if (!clip) return { type: 'Polygon', coordinates: [boundary] };
	const hex = open(boundary as Ring);
	const bb = bboxOf(hex);
	if (!touchesBoundary(clip, bb)) {
		// Wholly inside or wholly outside (PY/BR cells were assigned with another
		// polygon source than the one drawn): one point decides.
		const c: Pt = [(bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2];
		return insideByIndex(clip, c) ? { type: 'Polygon', coordinates: [boundary] } : null;
	}
	const parts: Ring[][] = [];
	for (const p of clip.polys) {
		const [w, s, e, n] = p.bbox;
		if (w > bb[2] || e < bb[0] || s > bb[3] || n < bb[1]) continue;
		const outer = clipByConvex(p.rings[0], hex);
		if (outer.length < 3) continue;
		const holes = p.rings.slice(1).map((h) => clipByConvex(h, hex)).filter((h) => h.length >= 3);
		parts.push([close(outer), ...holes.map(close)]);
	}
	if (!parts.length) return null;
	return parts.length === 1 ? { type: 'Polygon', coordinates: parts[0] } : { type: 'MultiPolygon', coordinates: parts };
}
