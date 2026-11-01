// mailto for «Pedir un diagnóstico a medida»: what the person was looking at plus
// four questions that turn «me interesa» into a request that can be scoped.
// No H3 codes for zones (they can be thousands): the body asks for the GeoJSON.
import type { RequestDiagnosticText } from '$lib/content/requestDiagnostic';

export const REQUEST_EMAIL = 'nealab@spatia.ar';

export interface RequestZone { id: string; origin: string; hexCount: number }

export interface RequestContext {
	layer?: string;
	place?: string;
	zones?: RequestZone[];
	h3?: string;
	href?: string;
}

export function buildRequestMailto(t: RequestDiagnosticText, ctx: RequestContext = {}, locale = 'es'): string {
	const head = [ctx.layer, ctx.place].filter(Boolean).join(' · ');
	const subject = `Spatia — ${t.subject}${head ? ': ' + head : ''}`;
	const seen: string[] = [];
	if (ctx.layer) seen.push(`- ${t.layer}: ${ctx.layer}`);
	if (ctx.place) seen.push(`- ${t.place}: ${ctx.place}`);
	for (const z of ctx.zones ?? []) {
		seen.push(`- ${t.zone} ${z.id}${z.origin ? ' · ' + z.origin : ''} · ${z.hexCount.toLocaleString(locale)} hex`);
	}
	if (ctx.h3) seen.push(`- ${t.hexagon}: ${ctx.h3}`);
	if (ctx.href) seen.push(`- ${t.link}: ${ctx.href}`);
	const lines = [
		t.intro,
		...(seen.length ? ['', t.looking, ...seen] : []),
		'',
		t.askTitle,
		`- ${t.askPurpose}`,
		`- ${t.askArea}`,
		`- ${t.askWhen}`,
		`- ${t.askWho}`,
	];
	return `mailto:${REQUEST_EMAIL}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(lines.join('\n'))}`;
}
