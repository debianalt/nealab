<script lang="ts">
	import { onMount } from 'svelte';
	import Seo from '$lib/components/Seo.svelte';
	import ScrollySection from '$lib/components/scrolly/ScrollySection.svelte';
	import EudrStoryMap from '$lib/components/scrolly/EudrStoryMap.svelte';
	import PlantationSplitChart from '$lib/components/scrolly/PlantationSplitChart.svelte';
	import EudrLossExtentChart from '$lib/components/scrolly/EudrLossExtentChart.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { lp } from '$lib/utils/locale-path';
	import { INFORME, pick, CITA_MAPBIOMAS } from '$lib/content/eudr_informe';
	import { getEudrMetaUrl } from '$lib/config';
	import { reveal } from '$lib/actions/reveal';

	let locale = $derived(i18n.locale);
	const t = (block: keyof typeof INFORME) => pick(INFORME[block], locale);
	const tr = (es: string, en: string) => (locale === 'en' ? en : es);

	let mapStep = $state(0);
	let progress = $state(0);

	const views = [
		{ province: 'ALL' as string | null, lat: -26.5, lon: -58.5, zoom: 5.3 },
		{ province: 'ar_misiones', lat: -26.9, lon: -54.5, zoom: 6.8 },
		{ province: 'ar_corrientes', lat: -28.8, lon: -57.8, zoom: 6.4 },
		{ province: 'ar_chaco', lat: -26.4, lon: -60.5, zoom: 6.2 },
		{ province: 'ar_formosa', lat: -24.8, lon: -59.6, zoom: 6.3 }
	];

	const stepDefs = [
		{ key: 'stepOverview', title: { es: 'El territorio', en: 'The territory' } },
		{ key: 'stepMisiones', title: { es: 'Misiones', en: 'Misiones' } },
		{ key: 'stepCorrientes', title: { es: 'Corrientes', en: 'Corrientes' } },
		{ key: 'stepChaco', title: { es: 'Chaco', en: 'Chaco' } },
		{ key: 'stepFormosa', title: { es: 'Formosa', en: 'Formosa' } }
	] as const;

	let steps = $derived(
		stepDefs.map((s) => ({ title: pick(s.title, locale), text: pick(INFORME[s.key], locale) }))
	);

	const sections = $derived([
		{ n: '01', h: tr('El problema', 'The problem'), body: 'problema' as const },
		{ n: '02', h: tr('La línea base MapBiomas', 'The MapBiomas baseline'), body: 'solucion' as const },
		{ n: '03', h: tr('Cómo se calcula', 'How it is computed'), body: 'metodo' as const }
	]);

	let stamp = $state<string | null>(null);
	onMount(() => {
		(async () => {
			try {
				const r = await fetch(getEudrMetaUrl());
				if (r.ok) {
					const m = await r.json();
					stamp = [m.hansen_version && `Hansen ${m.hansen_version}`, m.refreshed ?? m.vintage]
						.filter(Boolean)
						.join(' · ');
				}
			} catch {
				/* stamp stays null */
			}
		})();

		const onScroll = () => {
			const h = document.documentElement;
			const max = h.scrollHeight - h.clientHeight;
			progress = max > 0 ? Math.min(1, h.scrollTop / max) : 0;
		};
		window.addEventListener('scroll', onScroll, { passive: true });
		onScroll();
		return () => window.removeEventListener('scroll', onScroll);
	});
</script>

<Seo
	title={tr(
		'Cosecha o deforestación · pre-diagnóstico EUDR con MapBiomas — nealab',
		'Harvest or deforestation · EUDR pre-diagnosis with MapBiomas — nealab'
	)}
	description={t('subtitle')}
/>

<div class="progress" style={`transform:scaleX(${progress})`}></div>

<article class="informe" lang={locale}>
	<!-- Hero -->
	<header class="hero fullbleed">
		<div class="hero-inner">
			<p class="kicker">{t('kicker')}</p>
			<h1 class="hero-title">{t('title')}</h1>
			<p class="hero-sub">{t('subtitle')}</p>
			<div class="hero-stats">
				<div class="stat">
					<span class="sv">{tr('82,6 %', '82.6%')}</span>
					<span class="sl">{tr('de la pérdida sobre nativo', 'of loss on native')}</span>
				</div>
				<div class="stat">
					<span class="sv">68.084</span>
					<span class="sl">{tr('hexágonos analizados', 'hexagons analysed')}</span>
				</div>
				<div class="stat">
					<span class="sv">4</span>
					<span class="sl">{tr('provincias · NEA', 'provinces · NE')}</span>
				</div>
			</div>
			<div class="hero-meta">
				<span>Reglamento (UE) 2023/1115</span>
				{#if stamp}<span class="dot">·</span><span>{stamp}</span>{/if}
			</div>
			<div class="scroll-hint">{tr('desplazá para explorar', 'scroll to explore')} <span class="arw">↓</span></div>
		</div>
	</header>

	<!-- Lead -->
	<section class="lead" use:reveal>
		<div class="prose lead-prose" lang={locale}>{@html t('intro')}</div>
	</section>

	<!-- Numbered framing sections -->
	{#each sections as s}
		<section class="sec" use:reveal>
			<div class="sec-num">{s.n}</div>
			<div class="sec-body">
				<h2 class="sec-h">{s.h}</h2>
				<div class="prose" lang={locale}>{@html t(s.body)}</div>
			</div>
		</section>
	{/each}

	<!-- Findings header -->
	<div class="findings-head fullbleed" use:reveal>
		<span class="fh-label">{tr('Hallazgos', 'Findings')}</span>
		<h2 class="fh-title">{t('hallazgosLead')}</h2>
		<p class="fh-hint">
			{tr(
				'Cada paso enfoca una provincia y colorea su riesgo de deforestación post-2020 por hexágono. Para inspeccionar una parcela puntual, al final está la herramienta interactiva.',
				'Each step focuses on a province and colours its post-2020 deforestation risk per hexagon. To inspect a specific parcel, the interactive tool is at the end.'
			)}
		</p>
	</div>

	<!-- Scrolly map -->
	<div class="fullbleed">
		<ScrollySection bind:activeStep={mapStep} {steps} textBoxPosition="left" textBoxVariant="dark">
			{#snippet children({ activeStep })}
				<EudrStoryMap {activeStep} {views} {locale} />
			{/snippet}
		</ScrollySection>
	</div>

	<!-- Charts -->
	<section class="charts" use:reveal>
		<div class="prose" lang={locale}>{@html t('chartLead')}</div>
		<div class="chart-card">
			<PlantationSplitChart {locale} />
		</div>
		<div class="prose" lang={locale}>{@html t('chartAfter')}</div>
		<div class="chart-card">
			<EudrLossExtentChart {locale} />
		</div>
	</section>

	<!-- Limitations -->
	<section class="sec" use:reveal>
		<div class="sec-num">04</div>
		<div class="sec-body">
			<h2 class="sec-h">{tr('Limitaciones', 'Limitations')}</h2>
			<div class="prose" lang={locale}>{@html t('limitaciones')}</div>
		</div>
	</section>

	<!-- CTA -->
	<section class="cta fullbleed" use:reveal>
		<div class="cta-inner">
			<h2 class="cta-h">{tr('Cómo usarlo', 'How to use it')}</h2>
			<div class="prose" lang={locale}>{@html t('cta')}</div>
			<a class="cta-btn" href={lp('/eudr/check', locale)}>{t('ctaButton')}</a>
		</div>
	</section>

	<!-- Sources -->
	<footer class="sources">
		<h2 class="src-h">{tr('Fuentes y cita', 'Sources and citation')}</h2>
		<p class="cite">{CITA_MAPBIOMAS} <span class="cc">CC-BY</span></p>
		<ul>
			<li>Hansen Global Forest Change (UMD / Google) · MODIS MCD64A1 · MapBiomas Argentina</li>
			<li>
				{tr('Código', 'Code')}:
				<a href="https://github.com/debianalt/nealab" target="_blank" rel="noopener">github.com/debianalt/nealab</a>
			</li>
		</ul>
		<p class="disclaimer">
			{tr(
				'Señales orientativas, no veredictos de cumplimiento. La verificación formal requiere geometría parcelaria oficial y debida diligencia profesional independiente.',
				'Indicative signals, not compliance verdicts. Formal verification requires official parcel geometry and independent professional due diligence.'
			)}
		</p>
	</footer>
</article>

<style>
	/* app.css sets overflow:hidden on html,body (full-screen map pages). For this
	   scrolling report we must override with !important AND keep a single scroll
	   container: html scrolls, body stays visible, so position:sticky sticks to the
	   viewport. Without this the sticky map un-sticks and scrolls out of view. */
	:global(html) {
		overflow-x: clip !important;
		overflow-y: auto !important;
		height: auto !important;
	}
	:global(body) {
		overflow: visible !important;
		height: auto !important;
	}
	:global(.reveal-init) {
		opacity: 0;
		transform: translateY(20px);
		transition:
			opacity 0.7s cubic-bezier(0.22, 0.61, 0.36, 1),
			transform 0.7s cubic-bezier(0.22, 0.61, 0.36, 1);
	}
	:global(.reveal-in) {
		opacity: 1;
		transform: none;
	}

	.progress {
		position: fixed;
		top: 0;
		left: 0;
		width: 100%;
		height: 3px;
		background: linear-gradient(90deg, #199e70, #9085e9);
		transform-origin: 0 50%;
		z-index: 60;
	}

	.informe {
		font-family: 'Roboto Condensed', system-ui, sans-serif;
		color: #e8e8e6;
		padding-bottom: 5rem;
	}

	/* break out of the layout's max-w-6xl centered column to viewport width */
	.fullbleed {
		width: 100vw;
		margin-left: calc(50% - 50vw);
	}

	/* ---------- Hero ---------- */
	.hero {
		position: relative;
		background:
			radial-gradient(1100px 520px at 12% -8%, rgba(57, 135, 229, 0.16), transparent 60%),
			radial-gradient(900px 500px at 92% 108%, rgba(217, 89, 38, 0.14), transparent 62%),
			#0a0a0a;
		border-bottom: 1px solid #171922;
	}
	.hero::before {
		content: '';
		position: absolute;
		inset: 0;
		background-image: radial-gradient(rgba(255, 255, 255, 0.05) 1px, transparent 1px);
		background-size: 26px 26px;
		mask-image: linear-gradient(to bottom, #000, transparent 85%);
		pointer-events: none;
	}
	.hero-inner {
		position: relative;
		max-width: 62rem;
		margin: 0 auto;
		padding: clamp(4rem, 15vh, 10rem) 1.5rem 4.5rem;
	}
	.kicker {
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.72rem;
		text-transform: uppercase;
		letter-spacing: 0.22em;
		color: #22c39a;
		margin: 0 0 1.4rem;
	}
	.hero-title {
		font-weight: 700;
		font-size: clamp(2.6rem, 7vw, 4.8rem);
		line-height: 1;
		letter-spacing: -0.02em;
		margin: 0 0 1.4rem;
		color: #fff;
		text-wrap: balance;
	}
	.hero-sub {
		font-size: clamp(1.15rem, 2.3vw, 1.45rem);
		line-height: 1.58;
		color: #d4d3ca;
		max-width: 56rem;
		margin: 0 0 2.5rem;
		text-align: justify;
		text-justify: inter-word;
		hyphens: auto;
	}
	.hero-stats {
		display: flex;
		flex-wrap: wrap;
		gap: 2.4rem;
		margin-bottom: 2.2rem;
	}
	.stat {
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
	}
	.stat .sv {
		font-family: 'JetBrains Mono', monospace;
		font-size: clamp(1.6rem, 4vw, 2.3rem);
		font-weight: 700;
		color: #fff;
		line-height: 1;
	}
	.stat:first-child .sv {
		color: #a78bfa;
	}
	.stat .sl {
		font-size: 0.8rem;
		color: #898781;
		max-width: 14ch;
	}
	.hero-meta {
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.74rem;
		color: #6b6a64;
		display: flex;
		gap: 0.5rem;
		flex-wrap: wrap;
	}
	.scroll-hint {
		margin-top: 2.6rem;
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.72rem;
		text-transform: uppercase;
		letter-spacing: 0.16em;
		color: #6b6a64;
	}
	.arw {
		display: inline-block;
		animation: bob 1.8s ease-in-out infinite;
	}
	@keyframes bob {
		0%,
		100% {
			transform: translateY(0);
		}
		50% {
			transform: translateY(4px);
		}
	}

	/* ---------- Prose ---------- */
	.prose {
		max-width: 44rem;
		font-size: 1.1rem;
		line-height: 1.72;
		color: #d4d3ca;
		text-align: justify;
		text-justify: inter-word;
		hyphens: auto;
	}
	.prose :global(p) {
		margin: 0 0 1.15rem;
	}
	.prose :global(p:last-child) {
		margin-bottom: 0;
	}
	.prose :global(strong) {
		color: #fff;
		font-weight: 700;
	}
	.lead {
		max-width: 62rem;
		margin: 4.5rem auto;
		padding: 0 1.5rem;
	}
	.lead-prose {
		max-width: 44rem;
		margin-inline: auto;
		font-size: clamp(1.2rem, 2.4vw, 1.45rem);
		line-height: 1.6;
		color: #e8e8e6;
	}
	/* drop-cap on the lead */
	.lead-prose :global(p:first-child::first-letter) {
		float: left;
		font-family: 'JetBrains Mono', monospace;
		font-size: 3.6rem;
		line-height: 0.82;
		font-weight: 700;
		padding: 0.2rem 0.7rem 0 0;
		color: #199e70;
	}

	/* ---------- Numbered sections ---------- */
	.sec {
		max-width: 62rem;
		margin: 3.5rem auto;
		padding: 0 1.5rem;
		display: grid;
		grid-template-columns: 5rem 1fr;
		gap: 1.5rem;
	}
	.sec-num {
		font-family: 'JetBrains Mono', monospace;
		font-size: 1rem;
		font-weight: 700;
		color: #199e70;
		padding-top: 0.35rem;
		border-top: 2px solid #199e70;
		height: fit-content;
	}
	.sec-h {
		font-family: 'Roboto Condensed', system-ui, sans-serif;
		font-size: clamp(1.4rem, 3vw, 1.9rem);
		font-weight: 700;
		color: #fff;
		margin: 0 0 1.1rem;
		line-height: 1.1;
	}

	/* ---------- Findings header ---------- */
	.findings-head {
		background: linear-gradient(180deg, #0a0a0a, #0d0f16);
		border-top: 1px solid #171922;
		text-align: center;
		padding: 4.5rem 1.5rem 2.5rem;
	}
	.fh-label {
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.74rem;
		text-transform: uppercase;
		letter-spacing: 0.2em;
		color: #a78bfa;
	}
	.fh-title {
		font-size: clamp(1.8rem, 5vw, 3rem);
		font-weight: 700;
		color: #fff;
		margin: 0.8rem 0 0.6rem;
	}
	.fh-hint {
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.8rem;
		color: #898781;
		margin: 0;
	}

	/* ---------- Charts ---------- */
	.charts {
		max-width: 44rem;
		margin: 4rem auto;
		padding: 0 1.5rem;
	}
	.charts .prose {
		max-width: none;
	}
	.chart-card {
		margin: 2rem 0 3rem;
		padding: 1.9rem;
		background: #131417;
		border: 1px solid #24252b;
		border-radius: 12px;
		box-shadow: 0 10px 34px rgba(0, 0, 0, 0.35);
	}

	/* ---------- CTA ---------- */
	.cta {
		background: linear-gradient(180deg, #0d0f16, #0a0a0a);
		border-top: 1px solid #171922;
		border-bottom: 1px solid #171922;
		padding: 4rem 1.5rem;
	}
	.cta-inner {
		max-width: 44rem;
		margin: 0 auto;
	}
	.cta-h {
		font-size: clamp(1.4rem, 3vw, 1.9rem);
		font-weight: 700;
		color: #fff;
		margin: 0 0 1rem;
	}
	.cta-btn {
		display: inline-block;
		margin-top: 1.4rem;
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.9rem;
		color: #0a0a0a;
		background: #22c39a;
		padding: 0.85rem 1.4rem;
		border-radius: 7px;
		text-decoration: none;
		font-weight: 700;
		transition: transform 0.15s ease, background 0.15s ease;
	}
	.cta-btn:hover {
		background: #3fe0bc;
		transform: translateY(-1px);
	}

	/* ---------- Sources ---------- */
	.sources {
		max-width: 44rem;
		margin: 4rem auto 0;
		padding: 0 1.5rem;
		font-size: 0.9rem;
		color: #898781;
	}
	.src-h {
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.82rem;
		text-transform: uppercase;
		letter-spacing: 0.12em;
		color: #22c39a;
		margin: 0 0 1rem;
		padding-bottom: 0.5rem;
		border-bottom: 1px solid #24252b;
	}
	.cite {
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.8rem;
		line-height: 1.55;
		color: #c3c2b7;
		background: rgba(57, 135, 229, 0.06);
		border-left: 2px solid #199e70;
		padding: 0.8rem 1rem;
		border-radius: 0 5px 5px 0;
	}
	.cite .cc {
		color: #22c39a;
		font-weight: 700;
	}
	.sources ul {
		list-style: none;
		padding: 0;
		margin: 1.25rem 0;
	}
	.sources li {
		margin: 0.45rem 0;
		font-size: 0.85rem;
	}
	.sources a {
		color: #22c39a;
		text-decoration: none;
	}
	.sources a:hover {
		text-decoration: underline;
	}
	.disclaimer {
		font-size: 0.82rem;
		line-height: 1.5;
		color: #6b6a64;
	}

	@media (max-width: 640px) {
		.sec {
			grid-template-columns: 1fr;
			gap: 0.5rem;
		}
		.sec-num {
			border-top: none;
			padding-top: 0;
		}
		.prose {
			text-align: left;
			hyphens: none;
		}
	}
</style>
