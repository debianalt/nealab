import { listMethodologyIds } from '$lib/content/methodology';

export const prerender = true;

const ORIGIN = 'https://spatia.ar';

// /terminos is noindex and /calibracion is an internal QA grid — neither belongs here.
const STATIC_PATHS = ['/', '/servicios', '/eudr/check', '/metodologia'];

// No lastmod/changefreq/priority: Google ignores the latter two, and a lastmod it
// learns not to trust is worse than none. The methodology ids come from the same
// source as entries() in metodologia/[id]/+page.ts, so the two cannot drift.
export function GET() {
	const paths = [...STATIC_PATHS, ...listMethodologyIds().map((id) => `/metodologia/${id}`)];

	const body = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${paths.map((path) => `\t<url>\n\t\t<loc>${ORIGIN}${path}</loc>\n\t</url>`).join('\n')}
</urlset>
`;

	return new Response(body, {
		headers: { 'Content-Type': 'application/xml' },
	});
}
