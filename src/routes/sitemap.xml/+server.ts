import { listMethodologyIds } from '$lib/content/methodology';
import { LOCALES, lp } from '$lib/utils/locale-path';

export const prerender = true;

const ORIGIN = 'https://spatia.ar';

// The map. Not locale-prefixed (it carries locale in localStorage), so it has no
// alternates. /terminos is noindex and /calibracion is an internal QA grid — neither
// belongs here.
const MAP_PATH = '/';

// One entry per path; each expands to its 4 locale variants below.
const LOCALIZED_PATHS = ['/servicios', '/eudr/informe', '/eudr/check', '/metodologia'];

// No lastmod/changefreq/priority: Google ignores the latter two, and a lastmod it
// learns not to trust is worse than none. The methodology ids come from the same
// source as entries() in metodologia/[id]/+page.ts, so the two cannot drift.
export function GET() {
	const basePaths = [
		...LOCALIZED_PATHS,
		...listMethodologyIds().map((id) => `/metodologia/${id}`),
	];

	const urls: string[] = [url(`${ORIGIN}${MAP_PATH}`, [])];

	for (const basePath of basePaths) {
		// Every variant must list all of them, itself included, using the same URL the
		// others point at — that reciprocity is what Google requires to honour hreflang.
		const alternates = LOCALES.map((locale) => ({
			hreflang: locale,
			href: `${ORIGIN}${lp(basePath, locale)}`,
		}));
		alternates.push({ hreflang: 'x-default', href: `${ORIGIN}${basePath}` });

		for (const locale of LOCALES) {
			urls.push(url(`${ORIGIN}${lp(basePath, locale)}`, alternates));
		}
	}

	const body = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">
${urls.join('\n')}
</urlset>
`;

	return new Response(body, {
		headers: { 'Content-Type': 'application/xml' },
	});
}

function url(loc: string, alternates: Array<{ hreflang: string; href: string }>): string {
	const links = alternates
		.map((a) => `\n\t\t<xhtml:link rel="alternate" hreflang="${a.hreflang}" href="${a.href}"/>`)
		.join('');
	return `\t<url>\n\t\t<loc>${loc}</loc>${links}\n\t</url>`;
}
