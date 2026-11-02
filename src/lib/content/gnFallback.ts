// Guaraní: long texts (servicios, términos, metodología) are served in Spanish.
// Decided with the author on 2-nov-2026: nobody can check a Guaraní translation,
// and the stale ones said false things. Short labels stay translated in i18n.
// Links to the text routes keep the /gn/ prefix so the reader stays in the Guaraní
// site; the map at / is not locale-prefixed (locale-path.ts).
export function gnFromEs<T>(es: T): T {
	const map = (v: unknown): unknown =>
		typeof v === 'string'
			? v.replace(/href="\/(?=(servicios|metodologia|terminos|eudr))/g, 'href="/gn/')
			: Array.isArray(v)
				? v.map(map)
				: v && typeof v === 'object'
					? Object.fromEntries(Object.entries(v as Record<string, unknown>).map(([k, x]) => [k, map(x)]))
					: v;
	return map(es) as T;
}
