<script lang="ts">
	/**
	 * FireFilterChart — de toda el area quemada post-2020 de cada provincia, cuanta
	 * cae sobre vegetacion nativa lenosa (la que el score cuenta como riesgo) y cuanta
	 * sobre pastizal y humedal (regimen natural de fuego, descartada).
	 *
	 * Las barras estan en unidades absolutas (% del hexagono quemado) y no en
	 * porcentaje del total de cada provincia: si se normalizara, Misiones -que casi no
	 * se quema- luciria igual que Formosa.
	 *
	 * Paleta validada con scripts/validate_palette.js --mode dark (los 5 chequeos):
	 * nativa lenosa #d9772e / pastizal y humedal #5b7fd4.
	 */
	import type { Locale } from '$lib/stores/i18n.svelte';

	interface Props {
		locale?: Locale;
	}
	let { locale = 'es' }: Props = $props();

	let shown = $state(false);
	function inView(node: HTMLElement) {
		const io = new IntersectionObserver(
			(es) => {
				for (const e of es)
					if (e.isIntersecting) {
						shown = true;
						io.unobserve(node);
					}
			},
			{ threshold: 0.2 }
		);
		io.observe(node);
		return { destroy: () => io.disconnect() };
	}

	// res-7, 68.517 hexagonos (ver A_02). total = fuego crudo, nat = sobre nativa lenosa
	let rows = $derived([
		{ name: 'Misiones', total: 0.5, nat: 0.3 },
		{ name: 'Corrientes', total: 18.34, nat: 1.03, key: true },
		{ name: 'Chaco', total: 7.54, nat: 3.37 },
		{ name: 'Formosa', total: 20.36, nat: 15.54 },
		{
			name: locale === 'en' ? '4 provinces' : '4 provincias',
			total: 13.4,
			nat: 5.34,
			total_row: true
		}
	]);

	const SCALE = 21; // techo del eje, en % de superficie del hexagono

	const T = {
		es: {
			nat: 'sobre nativa leñosa',
			other: 'sobre pastizal y humedal',
			heroNum: '94 %',
			heroLabel:
				'del área quemada de Corrientes ocurre sobre pastizal y humedal. Es régimen natural de fuego, no conversión de bosque, y el índice ya no lo cuenta como riesgo.',
			caption:
				'Área quemada posterior al corte (MODIS MCD64A1, 2021–2025) separada según la cobertura de 2020. El score de riesgo pondera únicamente la fracción sobre vegetación nativa leñosa.',
			tipNat: 'Nativa leñosa',
			tipOther: 'Pastizal/humedal'
		},
		en: {
			nat: 'on native woody cover',
			other: 'on grassland and wetland',
			heroNum: '94%',
			heroLabel:
				'of the area burned in Corrientes falls on grassland and wetland. That is a natural fire regime, not forest conversion, and the index no longer counts it as risk.',
			caption:
				'Post-cutoff burned area (MODIS MCD64A1, 2021–2025) split by 2020 land cover. The risk score weights only the fraction over native woody vegetation.',
			tipNat: 'Native woody',
			tipOther: 'Grassland/wetland'
		}
	} as const;
	const t = $derived((T as any)[locale] ?? T.es);

	const fmt = (v: number) => (locale === 'en' ? v.toFixed(2) : v.toFixed(2).replace('.', ','));

	let hovered = $state<number | null>(null);
	let tip = $state<{ x: number; y: number; i: number } | null>(null);
	function onMove(e: MouseEvent, i: number) {
		hovered = i;
		tip = { x: e.clientX, y: e.clientY, i };
	}
</script>

<figure class="fc" class:shown use:inView>
	<div class="hero">
		<span class="hero-num">{t.heroNum}</span>
		<span class="hero-label">{t.heroLabel}</span>
	</div>

	<div class="legend">
		<span class="key"><span class="sw nat"></span>{t.nat}</span>
		<span class="key"><span class="sw other"></span>{t.other}</span>
	</div>

	<div class="rows" onmouseleave={() => { hovered = null; tip = null; }} role="list">
		{#each rows as r, i}
			<div
				class="row"
				class:total={r.total_row}
				class:keyrow={r.key}
				class:dim={hovered !== null && hovered !== i}
				role="listitem"
				onmousemove={(e) => onMove(e, i)}
			>
				<div class="rl">{r.name}</div>
				<div class="track">
					<div
						class="bar"
						style={`--nat:${(r.nat / SCALE) * 100}%; --oth:${((r.total - r.nat) / SCALE) * 100}%`}
					>
						<div class="seg nat"></div>
						<div class="seg other"></div>
						<span class="val">{fmt(r.nat)} / {fmt(r.total)}</span>
					</div>
				</div>
			</div>
		{/each}
	</div>

	<figcaption>{t.caption}</figcaption>
</figure>

{#if tip}
	<div class="tooltip" style={`left:${tip.x + 14}px; top:${tip.y + 14}px`}>
		<strong>{rows[tip.i].name}</strong>
		<span><i class="d nat"></i>{t.tipNat} {fmt(rows[tip.i].nat)} %</span>
		<span
			><i class="d other"></i>{t.tipOther} {fmt(rows[tip.i].total - rows[tip.i].nat)} %</span
		>
	</div>
{/if}

<style>
	.fc {
		margin: 0;
		font-family: 'Roboto Condensed', system-ui, sans-serif;
		color: #fff;
	}
	.hero {
		display: flex;
		align-items: baseline;
		gap: 1rem;
		margin-bottom: 1.75rem;
		flex-wrap: wrap;
	}
	.hero-num {
		font-family: 'JetBrains Mono', monospace;
		font-size: clamp(2.6rem, 7vw, 4rem);
		font-weight: 700;
		line-height: 1;
		color: #5b7fd4;
		letter-spacing: -0.02em;
	}
	.hero-label {
		max-width: 22rem;
		font-size: 0.95rem;
		line-height: 1.45;
		color: #c3c2b7;
	}
	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 1.4rem;
		font-size: 0.82rem;
		color: #c3c2b7;
		margin-bottom: 1.15rem;
	}
	.key {
		display: inline-flex;
		align-items: center;
		gap: 0.45rem;
	}
	.sw {
		width: 13px;
		height: 13px;
		border-radius: 3px;
	}
	.sw.nat,
	.d.nat {
		background: #d9772e;
	}
	.sw.other,
	.d.other {
		background: #5b7fd4;
	}
	.rows {
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
	}
	.row {
		display: grid;
		grid-template-columns: 108px 1fr;
		align-items: center;
		gap: 0.85rem;
		transition: opacity 0.18s ease;
	}
	.row.dim {
		opacity: 0.38;
	}
	.rl {
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.82rem;
		text-align: right;
		color: #c3c2b7;
	}
	.row.total .rl,
	.row.keyrow .rl {
		color: #fff;
		font-weight: 700;
	}
	.track {
		position: relative;
	}
	.bar {
		position: relative;
		display: flex;
		gap: 2px; /* 2px de superficie entre rellenos (dataviz) */
		height: 26px;
		align-items: stretch;
	}
	.row.total .bar {
		height: 32px;
	}
	.seg {
		width: 0;
		border-radius: 3px;
		transition: width 1s cubic-bezier(0.22, 0.61, 0.36, 1);
	}
	.seg.nat {
		background: #d9772e;
	}
	.seg.other {
		background: #5b7fd4;
	}
	.fc.shown .seg.nat {
		width: var(--nat);
	}
	.fc.shown .seg.other {
		width: var(--oth);
	}
	.val {
		align-self: center;
		padding-left: 0.55rem;
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.72rem;
		color: #c3c2b7;
		white-space: nowrap;
	}
	figcaption {
		margin-top: 1.2rem;
		font-size: 0.78rem;
		line-height: 1.45;
		color: #898781;
	}

	.tooltip {
		position: fixed;
		z-index: 60;
		pointer-events: none;
		background: #1a1a19;
		border: 1px solid #383835;
		border-radius: 6px;
		padding: 0.55rem 0.7rem;
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.74rem;
		color: #fff;
		display: flex;
		flex-direction: column;
		gap: 0.25rem;
		box-shadow: 0 6px 20px rgba(0, 0, 0, 0.5);
	}
	.tooltip strong {
		font-size: 0.8rem;
	}
	.tooltip span {
		display: inline-flex;
		align-items: center;
		gap: 0.4rem;
		color: #c3c2b7;
	}
	.tooltip .d {
		width: 9px;
		height: 9px;
		border-radius: 2px;
	}

	@media (max-width: 560px) {
		.row {
			grid-template-columns: 76px 1fr;
			gap: 0.55rem;
		}
		.rl {
			font-size: 0.72rem;
		}
		.val {
			font-size: 0.66rem;
		}
	}
</style>
