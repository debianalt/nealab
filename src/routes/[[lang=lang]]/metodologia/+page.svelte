<script lang="ts">
	import { browser } from '$app/environment';
	import { i18n } from '$lib/stores/i18n.svelte';
	import LangSwitcher from '$lib/components/LangSwitcher.svelte';
	import Seo from '$lib/components/Seo.svelte';
	import { lp } from '$lib/utils/locale-path';

	let { data } = $props();

	// page title in the route's language (gn keeps the Spanish loanword, as in page.metodologia.kicker)
	const SEO = {
		es: ['Metodología e indicadores · Spatia', 'Metodología de los indicadores geoespaciales de Spatia — fuentes, variables y métodos de cada análisis.'],
		gn: ['Metodología e indicadores · Spatia', 'Metodología de los indicadores geoespaciales de Spatia — fuentes, variables y métodos de cada análisis.'],
		en: ['Methodology and indicators · Spatia', 'Methodology of the Spatia geospatial indicators: sources, variables and methods of each analysis.'],
		pt: ['Metodologia e indicadores · Spatia', 'Metodologia dos indicadores geoespaciais do Spatia: fontes, variáveis e métodos de cada análise.']
	} as const;
	const seo = $derived(SEO[(i18n.locale as keyof typeof SEO)] ?? SEO.es);

	// Stamps the printout, so it must be the reader's date — computed client-side only,
	// since prerendering would otherwise freeze the build date into the HTML.
	const today = browser
		? new Date().toLocaleDateString('es-AR', {
				year: 'numeric',
				month: 'long',
				day: 'numeric',
			})
		: '';

	function handlePrint() {
		if (typeof window !== 'undefined') window.print();
	}
</script>

<Seo
	title={seo[0]}
	description={seo[1]}
/>

<div class="page">
	<div class="print-brand">
		<strong>Spatia</strong> · análisis geoespacial abierto · spatia.ar
	</div>

	<header class="hdr">
		<div class="hdr-actions no-print">
			<a class="back-link" href="/">{i18n.t('nav.backToMap')}</a>
			<div class="hdr-right">
				<LangSwitcher />
				<button class="print-btn" onclick={handlePrint} type="button">
					{i18n.t('nav.printSave')}
				</button>
			</div>
		</div>
		<h1 class="title">{i18n.t('page.metodologia.title')}</h1>
		<div class="kicker">{i18n.t('page.metodologia.kicker')}</div>
		<p class="desc">
			{i18n.t('page.metodologia.desc')}
		</p>
	</header>

	<section class="lens-section" id="limites">
		<h2 class="bounds-title">{i18n.t('page.metodologia.boundsTitle')}</h2>
		<p class="desc">{i18n.t('page.metodologia.boundsText')}</p>
	</section>

	{#each data.byLens as group}
		<section class="lens-section">
			<h2 class="lens-title" style="color: {group.color}">{group.label}</h2>
			<ul class="analysis-list">
				{#each group.analyses as analysis}
					<li class="analysis-item">
						<a href={lp(`/metodologia/${analysis.id}`, i18n.locale)} class="analysis-link">
							<span class="analysis-name">{i18n.t(analysis.titleKey)}</span>
							{#if analysis.descKey}
								<span class="analysis-desc">{i18n.t(analysis.descKey)}</span>
							{/if}
							<span class="analysis-arrow">{i18n.t('page.metodologia.arrowLink')}</span>
						</a>
					</li>
				{/each}
			</ul>
		</section>
	{/each}

	<footer class="footer">
		<p>
			{i18n.t('page.citeLabel')} Gómez, R. E. (2026). Spatia: A Reproducible Platform for Subnational Geospatial
			Analysis. Zenodo.
			<a href="https://doi.org/10.5281/zenodo.19483040">https://doi.org/10.5281/zenodo.19483040</a>
		</p>
		<p class="affil">CONICET · FHyCS-UNaM · Google Earth Engine Partner Tier</p>
		<p class="print-only generated">{i18n.t('page.generated').replace('{date}', today).replace('{url}', 'spatia.ar/metodologia')}</p>
	</footer>
</div>

<style>
	.page {
		max-width: 720px;
		margin: 0 auto;
		padding: 32px 24px 64px;
		color: var(--c-ink-1);
		font-family: var(--font-sans);
		font-size: 14px;
		line-height: 1.6;
	}
	.print-brand { display: none; }
	.print-only { display: none; }
	.hdr { margin-bottom: 32px; }
	.hdr-actions {
		display: flex;
		justify-content: space-between;
		align-items: center;
		gap: 12px;
		margin-bottom: 16px;
	}
	.hdr-right {
		display: flex;
		align-items: center;
		gap: 8px;
	}
	.back-link {
		display: inline-block;
		color: var(--c-ink-3);
		font-size: 12px;
		text-decoration: none;
	}
	.back-link:hover { color: var(--c-ink-1); text-decoration: underline; }
	.print-btn {
		background: rgba(var(--c-fg-rgb), 0.06);
		border: 1px solid var(--c-line-2);
		border-radius: 6px;
		color: var(--c-ink-3);
		font-size: 12px;
		font-weight: 600;
		padding: 6px 12px;
		cursor: pointer;
		font-family: inherit;
		transition: all 0.15s;
	}
	.print-btn:hover {
		background: rgba(var(--c-fg-rgb), 0.1);
		border-color: var(--c-line-3);
		color: var(--c-ink-1);
	}
	.title {
		font-size: 28px;
		font-weight: 700;
		color: var(--c-t-f8fafc);
		margin: 0 0 4px;
		line-height: 1.2;
	}
	.kicker {
		font-size: 11px;
		color: var(--c-ink-4);
		text-transform: uppercase;
		letter-spacing: 0.05em;
		margin-bottom: 12px;
	}
	.desc {
		color: var(--c-ink-2);
		font-size: 13px;
		margin: 0;
	}
	.lens-section { margin: 28px 0; }
	.lens-title {
		font-size: 11px;
		font-weight: 700;
		text-transform: uppercase;
		letter-spacing: 0.08em;
		margin: 0 0 12px;
		padding-bottom: 6px;
		border-bottom: 1px solid var(--c-line);
	}
	.analysis-list {
		list-style: none;
		padding: 0;
		margin: 0;
		display: flex;
		flex-direction: column;
		gap: 4px;
	}
	.analysis-item { display: block; }
	.analysis-link {
		display: flex;
		flex-direction: column;
		gap: 2px;
		padding: 10px 12px;
		background: rgba(var(--c-fg-rgb), 0.03);
		border: 1px solid rgba(var(--c-fg-rgb), 0.06);
		border-radius: 6px;
		text-decoration: none;
		transition: all 0.15s;
	}
	.analysis-link:hover {
		background: rgba(var(--c-fg-rgb), 0.07);
		border-color: rgba(var(--c-fg-rgb), 0.15);
	}
	.analysis-name {
		font-size: 13px;
		font-weight: 600;
		color: var(--c-ink-1);
	}
	.analysis-desc {
		font-size: 11px;
		color: var(--c-ink-4);
		line-height: 1.4;
	}
	.analysis-arrow {
		font-size: 10px;
		color: var(--c-line-3);
		margin-top: 2px;
		transition: color 0.15s;
	}
	.analysis-link:hover .analysis-arrow { color: var(--c-ink-3); }
	.footer {
		margin-top: 48px;
		padding-top: 20px;
		border-top: 1px solid var(--c-line);
		font-size: 12px;
		color: var(--c-ink-4);
	}
	.footer p { margin: 4px 0; }
	.footer a { color: var(--c-ink-3); text-decoration: underline; }
	.affil { font-style: italic; }

	:global(html), :global(body) { overflow: auto !important; background: var(--c-bg); }

	/* ═════ Print ═════ */
	@media print {
		:global(html), :global(body) {
			background: var(--c-ink-0) !important;
			color: #1a1a1a !important;
		}
		@page { size: A4; margin: 18mm 16mm 22mm 16mm; }
		.no-print { display: none !important; }
		.print-only { display: block !important; }
		.print-brand {
			display: block !important;
			font-size: 9pt;
			color: var(--c-ink-4n);
			letter-spacing: 0.02em;
			padding-bottom: 6pt;
			margin-bottom: 14pt;
			border-bottom: 0.5pt solid var(--c-ink-2);
		}
		.print-brand strong { color: var(--c-panel-2); font-weight: 700; }
		.page {
			max-width: none;
			margin: 0;
			padding: 0;
			color: #1a1a1a;
			font-size: 10pt;
			line-height: 1.45;
		}
		.hdr { margin-bottom: 18pt; }
		.title { color: var(--c-panel-2); font-size: 22pt; line-height: 1.15; margin-bottom: 4pt; }
		.kicker { color: var(--c-ink-4); font-size: 8pt; margin-bottom: 8pt; }
		.desc { color: var(--c-line-2); font-size: 10pt; }
		.lens-section { margin: 14pt 0; page-break-inside: avoid; }
		.lens-title { font-size: 9pt; border-bottom: 0.5pt solid var(--c-ink-2); padding-bottom: 3pt; margin-bottom: 8pt; }
		.analysis-link {
			background: #f8fafc;
			border: 0.25pt solid var(--c-ink-1);
			border-radius: 3pt;
		}
		.analysis-name { color: var(--c-panel-2); font-size: 10pt; }
		.analysis-desc { color: var(--c-line-3); font-size: 9pt; }
		.analysis-arrow { display: none; }
		.footer {
			border-top: 0.5pt solid var(--c-ink-2);
			color: var(--c-line-3);
			font-size: 8pt;
			margin-top: 28pt;
			padding-top: 8pt;
		}
		.footer a { color: var(--c-line-2); }
		.footer .generated { margin-top: 6pt; font-style: italic; color: var(--c-ink-4); }
	}
	.bounds-title { font-size: 12px; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; color: var(--c-ink-2); margin: 0 0 8px; }
</style>
