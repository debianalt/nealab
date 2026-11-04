// «Pedir un diagnóstico a medida»: the one contact call of the site (layer panel,
// zones, welcome). Spec and rationale:
// OneDrive/conicet(1)/07_spatia_vitrina_y_oficio/06_diagnostico_a_medida.md.
// Kept out of i18n.svelte.ts on purpose: a self-contained block, like servicios.ts.
import type { Locale } from '$lib/stores/i18n.svelte';

export interface RequestDiagnosticText {
	button: string;
	copy: string;
	copied: string;
	whatTitle: string;
	what: string;
	servicesLink: string;
	welcomeLabel: string;
	subject: string;
	intro: string;
	looking: string;
	layer: string;
	place: string;
	zone: string;
	hexagon: string;
	link: string;
	askTitle: string;
	askPurpose: string;
	askArea: string;
	askWhen: string;
	askWho: string;
}

const ES: RequestDiagnosticText = {
	button: 'Pedir un diagnóstico a medida',
	copy: 'copiar email',
	copied: 'copiado',
	whatTitle: '¿Qué incluye?',
	what: 'Lo que ves en Spatia es libre: todo se descarga en CSV y GeoJSON. A medida trabajamos el área exacta de tu proyecto, las variables que necesites (también las que no están en la plataforma), series temporales, trabajo de campo e interpretación técnica. Alcance, plazo y costo se acuerdan según el caso, y lo que trabajamos para vos es confidencial: no se publica en Spatia.',
	servicesLink: 'Ver servicios →',
	welcomeLabel: '¿Necesitás un diagnóstico de un lugar en particular?',
	subject: 'diagnóstico a medida',
	intro: 'Hola, me interesa un diagnóstico a medida.',
	looking: 'Lo que estaba mirando en Spatia:',
	layer: 'Capa',
	place: 'Lugar',
	zone: 'Zona',
	hexagon: 'Hexágono',
	link: 'Enlace',
	askTitle: 'Para orientar la respuesta (completá lo que sepas):',
	askPurpose: 'Para qué lo necesitás (estudio de impacto, proyecto, plan municipal, investigación…):',
	askArea: 'Área exacta (podés adjuntar un polígono o el GeoJSON de las zonas):',
	askWhen: 'Plazo:',
	askWho: 'Institución:',
};

export const REQUEST_DIAGNOSTIC: Record<Locale, RequestDiagnosticText> = {
	es: ES,
	en: {
		button: 'Request a tailored assessment',
		copy: 'copy email',
		copied: 'copied',
		whatTitle: 'What does it include?',
		what: 'Everything you see on Spatia is free: it all downloads as CSV and GeoJSON. A tailored assessment covers the exact area of your project, the variables you need (including ones not on the platform), time series, fieldwork and expert interpretation. Scope, timing and cost are agreed case by case, and the work we do for you is confidential: it is not published on Spatia.',
		servicesLink: 'See services →',
		welcomeLabel: 'Need an assessment of a specific place?',
		subject: 'tailored assessment',
		intro: 'Hello, I am interested in a tailored assessment.',
		looking: 'What I was looking at on Spatia:',
		layer: 'Layer',
		place: 'Place',
		zone: 'Zone',
		hexagon: 'Hexagon',
		link: 'Link',
		askTitle: 'To help us answer (fill in what you know):',
		askPurpose: 'What it is for (impact study, project, municipal plan, research…):',
		askArea: 'Exact area (you may attach a polygon or the zones GeoJSON):',
		askWhen: 'Timeline:',
		askWho: 'Organisation:',
	},
	pt: {
		button: 'Pedir um diagnóstico sob medida',
		copy: 'copiar email',
		copied: 'copiado',
		whatTitle: 'O que inclui?',
		what: 'O que você vê no Spatia é livre: tudo pode ser baixado em CSV e GeoJSON. Sob medida trabalhamos a área exata do seu projeto, as variáveis de que você precisa (inclusive as que não estão na plataforma), séries temporais, trabalho de campo e interpretação técnica. Escopo, prazo e custo são combinados caso a caso, e o trabalho que fazemos para você é confidencial: não é publicado no Spatia.',
		servicesLink: 'Ver serviços →',
		welcomeLabel: 'Precisa de um diagnóstico de um lugar específico?',
		subject: 'diagnóstico sob medida',
		intro: 'Olá, tenho interesse em um diagnóstico sob medida.',
		looking: 'O que eu estava vendo no Spatia:',
		layer: 'Camada',
		place: 'Local',
		zone: 'Zona',
		hexagon: 'Hexágono',
		link: 'Link',
		askTitle: 'Para orientar a resposta (preencha o que souber):',
		askPurpose: 'Para que precisa (estudo de impacto, projeto, plano municipal, pesquisa…):',
		askArea: 'Área exata (pode anexar um polígono ou o GeoJSON das zonas):',
		askWhen: 'Prazo:',
		askWho: 'Instituição:',
	},
	gn: ES, // gn: pendiente
};
