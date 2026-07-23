<script lang="ts">
	/**
	 * PlantationSplitChart — of the post-2020 forest loss in each NEA province, how
	 * much fell on land that was already plantation in 2020 (compatible with harvest)
	 * vs on native vegetation. Figures are the computed res-7 results (see A_02).
	 * Palette validated (dataviz): plantation blue #199e70 / native orange #9085e9.
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

	let rows = $derived([
		{ name: 'Misiones', plant: 19.4 },
		{ name: 'Corrientes', plant: 68.3, key: true },
		{ name: 'Chaco', plant: 0.0 },
		{ name: 'Formosa', plant: 0.0 },
		{ name: locale === 'en' ? '4 provinces' : '4 provincias', plant: 17.4, total: true }
	]);

	const T = {
		es: {
			plant: 'plantación (cosecha)',
			native: 'vegetación nativa',
			heroNum: '82,6 %',
			heroLabel: 'de la pérdida forestal posterior al corte recae sobre vegetación nativa; el resto es cosecha de plantación preexistente.',
			caption:
				'Reparto de la pérdida posterior al corte EUDR (31 dic 2020) según la cobertura de 2020. Grilla H3-7, 68.084 hexágonos.',
			tipPlant: 'Plantación',
			tipNative: 'Nativo'
		},
		en: {
			plant: 'plantation (harvest)',
			native: 'native vegetation',
			heroNum: '82.6%',
			heroLabel: 'of post-cutoff forest loss falls on native vegetation; the rest is harvest of pre-existing plantation.',
			caption:
				'Split of post-cutoff loss (EUDR, 31 Dec 2020) by 2020 land cover. H3-7 grid, 68,084 hexagons.',
			tipPlant: 'Plantation',
			tipNative: 'Native'
		}
	} as const;
	const t = $derived((T as any)[locale] ?? T.es);

	const fmt = (v: number) => (locale === 'en' ? v.toFixed(1) : v.toFixed(1).replace('.', ','));

	let hovered = $state<number | null>(null);
	let tip = $state<{ x: number; y: number; i: number } | null>(null);
	function onMove(e: MouseEvent, i: number) {
		hovered = i;
		tip = { x: e.clientX, y: e.clientY, i };
	}
</script>

<figure class="pc" class:shown use:inView>
	<div class="hero">
		<span class="hero-num">{t.heroNum}</span>
		<span class="hero-label">{t.heroLabel}</span>
	</div>

	<div class="legend">
		<span class="key"><span class="sw plant"></span>{t.plant}</span>
		<span class="key"><span class="sw nat"></span>{t.native}</span>
	</div>

	<div class="rows" onmouseleave={() => { hovered = null; tip = null; }} role="list">
		{#each rows as r, i}
			<div
				class="row"
				class:total={r.total}
				class:keyrow={r.key}
				class:dim={hovered !== null && hovered !== i}
				role="listitem"
				onmousemove={(e) => onMove(e, i)}
			>
				<div class="rl">{r.name}</div>
				<div class="bar" style={`--plant:${r.plant}%; --nat:${100 - r.plant}%`}>
					{#if r.plant >= 4}
						<div class="seg plant"><span>{fmt(r.plant)}</span></div>
					{/if}
					<div class="seg nat"><span>{fmt(100 - r.plant)}</span></div>
					{#if r.plant < 4}
						<span class="zero">plant. ≈ 0</span>
					{/if}
				</div>
			</div>
		{/each}
	</div>

	<figcaption>{t.caption}</figcaption>
</figure>

{#if tip}
	<div class="tooltip" style={`left:${tip.x + 14}px; top:${tip.y + 14}px`}>
		<strong>{rows[tip.i].name}</strong>
		<span><i class="d plant"></i>{t.tipPlant} {fmt(rows[tip.i].plant)}%</span>
		<span><i class="d nat"></i>{t.tipNative} {fmt(100 - rows[tip.i].plant)}%</span>
	</div>
{/if}

<style>
	.pc {
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
		color: #9085e9;
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
	.sw.plant,
	.d.plant {
		background: #199e70;
	}
	.sw.nat,
	.d.nat {
		background: #9085e9;
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
	.bar {
		position: relative;
		display: flex;
		gap: 2px; /* 2px surface gap between fills (dataviz) */
		height: 30px;
	}
	.row.total .bar {
		height: 38px;
	}
	.seg {
		display: flex;
		align-items: center;
		height: 100%;
		width: 0;
		border-radius: 3px;
		overflow: hidden;
		transition: width 1s cubic-bezier(0.22, 0.61, 0.36, 1);
	}
	.seg span {
		padding: 0 0.5rem;
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.74rem;
		font-weight: 600;
		color: rgba(255, 255, 255, 0.95);
		white-space: nowrap;
	}
	.seg.plant {
		background: #199e70;
	}
	.seg.nat {
		background: #9085e9;
		justify-content: flex-start;
	}
	/* animate to real widths once in view */
	.pc.shown .seg.plant {
		width: var(--plant);
	}
	.pc.shown .seg.nat {
		width: var(--nat);
	}
	.zero {
		position: absolute;
		left: 0.55rem;
		top: 50%;
		transform: translateY(-50%);
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.7rem;
		color: rgba(255, 255, 255, 0.55);
		pointer-events: none;
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
	}
</style>
