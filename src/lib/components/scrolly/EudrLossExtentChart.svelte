<script lang="ts">
	/**
	 * EudrLossExtentChart — share of res-7 hexagons per province showing any post-2020
	 * forest loss (deforestation_post_2020 = 1). Single-series magnitude. Figures from
	 * the computed res-7 results (see A_02).
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

	// share of hexagons with post-2020 loss, sorted desc
	const rows = [
		{ name: 'Misiones', v: 78.0 },
		{ name: 'Chaco', v: 47.5 },
		{ name: 'Formosa', v: 47.1 },
		{ name: 'Corrientes', v: 24.8 }
	];
	const max = 80;

	const T = {
		es: {
			title: 'Hexágonos con pérdida forestal posterior al corte',
			unit: '% de los hexágonos de la provincia',
			caption:
				'Presencia de pérdida (al menos un píxel) por hexágono H3-7. En Misiones es frecuente pero de baja intensidad por hexágono.'
		},
		en: {
			title: 'Hexagons with post-cutoff forest loss',
			unit: '% of the province’s hexagons',
			caption:
				'Presence of loss (at least one pixel) per H3-7 hexagon. In Misiones it is frequent but low-intensity per hexagon.'
		}
	} as const;
	const t = $derived((T as any)[locale] ?? T.es);
	const fmt = (v: number) => (locale === 'en' ? v.toFixed(1) : v.toFixed(1).replace('.', ','));

	let hovered = $state<number | null>(null);
</script>

<figure class="lc" class:shown use:inView>
	<figcaption class="title">{t.title}<span class="unit">{t.unit}</span></figcaption>
	<div class="rows" onmouseleave={() => (hovered = null)} role="list">
		{#each rows as r, i}
			<div
				class="row"
				class:dim={hovered !== null && hovered !== i}
				role="listitem"
				onmouseenter={() => (hovered = i)}
			>
				<div class="rl">{r.name}</div>
				<div class="track">
					<div class="fill" style={`--w:${(r.v / max) * 100}%`}></div>
					<span class="val">{fmt(r.v)}%</span>
				</div>
			</div>
		{/each}
	</div>
	<figcaption class="note">{t.caption}</figcaption>
</figure>

<style>
	.lc {
		margin: 0;
		font-family: 'Roboto Condensed', system-ui, sans-serif;
		color: #fff;
	}
	.title {
		font-size: 0.92rem;
		color: #fff;
		margin-bottom: 1.1rem;
		display: flex;
		flex-direction: column;
		gap: 0.15rem;
	}
	.title .unit {
		font-size: 0.74rem;
		color: #898781;
		font-family: 'JetBrains Mono', monospace;
	}
	.rows {
		display: flex;
		flex-direction: column;
		gap: 0.55rem;
	}
	.row {
		display: grid;
		grid-template-columns: 100px 1fr;
		align-items: center;
		gap: 0.85rem;
		transition: opacity 0.18s ease;
	}
	.row.dim {
		opacity: 0.4;
	}
	.rl {
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.82rem;
		text-align: right;
		color: #c3c2b7;
	}
	.track {
		position: relative;
		display: flex;
		align-items: center;
		height: 26px;
	}
	.fill {
		height: 100%;
		width: 0;
		background: #199e70;
		border-radius: 3px;
		transition: width 0.95s cubic-bezier(0.22, 0.61, 0.36, 1);
	}
	.lc.shown .fill {
		width: var(--w);
	}
	.val {
		margin-left: 0.55rem;
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.78rem;
		font-weight: 600;
		color: #fff;
		white-space: nowrap;
	}
	.note {
		margin-top: 1.1rem;
		font-size: 0.78rem;
		line-height: 1.45;
		color: #898781;
	}
	@media (max-width: 560px) {
		.row {
			grid-template-columns: 74px 1fr;
			gap: 0.5rem;
		}
	}
</style>
