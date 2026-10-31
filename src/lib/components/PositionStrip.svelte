<script lang="ts">
	// Where one hexagon's value sits in each comparison area: a P10–P90 band, the
	// P25–P75 box, the median tick, the mean diamond and the hexagon as a dot. One
	// row per area (department today; province and region later), all on one
	// shared axis. Square-root axis for non-negative skewed variables, so a long
	// right tail does not squash the middle half into a few pixels.
	// Drawn in real screen pixels (measured width): same size in the 380 px panel
	// and docked. Colour only on the hexagon dot; everything else is ink.
	import { measure } from '$lib/utils/chartType';
	import { TIERRA, INK_1, INK_2, INK_3, SURFACE, GRID, FONT } from '$lib/utils/sello';
	import type { FichaScope } from '$lib/utils/ficha';

	let {
		scopes,
		value,
		labels,
		fmt,
		unit = '',
		sqrtLabel = '√',
		ariaLabel = '',
	}: {
		scopes: FichaScope[];
		value: number | null;
		labels: string[];          // one per scope, shown left of its row when there are several
		fmt: (v: number) => string;
		unit?: string;
		sqrtLabel?: string;
		ariaLabel?: string;
	} = $props();

	let w = $state(0);
	let hover = $state<number | null>(null);

	const ROW = 22;
	const AXIS = 16;
	const multi = $derived(scopes.length > 1);
	const LABEL_W = $derived(multi ? 64 : 0);
	const PAD = 7;

	const lo0 = $derived(Math.min(...scopes.map(s => Math.min(s.p10, s.mean)), value ?? Infinity));
	const hi0 = $derived(Math.max(...scopes.map(s => Math.max(s.p90, s.mean)), value ?? -Infinity));
	// Skewed and non-negative: long right tail (P90 far above the median compared
	// with how far P10 sits below it).
	const sqrt = $derived(lo0 >= 0 && scopes.some(s => s.p90 > 0 && s.p90 - s.p50 > 3 * (s.p50 - s.p10)));
	const lo = $derived(sqrt ? 0 : lo0);
	const hi = $derived(hi0 > lo ? hi0 : lo + 1);
	const tf = (v: number) => (sqrt ? Math.sqrt(Math.max(0, v)) : v);
	const x = (v: number) => {
		const span = tf(hi) - tf(lo) || 1;
		return LABEL_W + PAD + ((tf(v) - tf(lo)) / span) * Math.max(10, w - LABEL_W - 2 * PAD);
	};
	const H = $derived(scopes.length * ROW + AXIS);
	const ticks = $derived(sqrt ? [lo, (lo + hi) / 4, hi] : [lo, (lo + hi) / 2, hi]);
</script>

<div class="ps" use:measure={(cw) => (w = cw)} role="img" aria-label={ariaLabel}>
	{#if w > 0}
		<!-- inline size: ChartFrame stretches every svg when docked -->
		<svg width={w} height={H} style:width={`${w}px`} style:height={`${H}px`} style:max-height="none" style:font-family={FONT.sans}>
			{#each scopes as s, i}
				{@const cy = i * ROW + ROW / 2}
				<!-- svelte-ignore a11y_no_static_element_interactions -->
				<g onmouseenter={() => (hover = i)} onmouseleave={() => (hover = null)}>
					<rect x={0} y={i * ROW} width={w} height={ROW} fill="transparent" />
					{#if multi}
						<text x={0} y={cy + 4} font-size="11" fill={INK_2}>{labels[i] ?? ''}</text>
					{/if}
					<line x1={LABEL_W + PAD} x2={w - PAD} y1={cy} y2={cy} stroke={GRID} stroke-width="1" />
					<!-- P10–P90 band -->
					<rect x={x(s.p10)} y={cy - 2} width={Math.max(1, x(s.p90) - x(s.p10))} height="4" rx="2" fill={INK_3} opacity="0.45" />
					<!-- P25–P75 box -->
					<rect x={x(s.p25)} y={cy - 6} width={Math.max(2, x(s.p75) - x(s.p25))} height="12" rx="2"
						fill={INK_3} fill-opacity="0.28" stroke={INK_2} stroke-width="1" />
					<!-- median -->
					<line x1={x(s.p50)} x2={x(s.p50)} y1={cy - 8} y2={cy + 8} stroke={INK_1} stroke-width="2" />
					<!-- mean: hollow diamond -->
					<path d={`M${x(s.mean)},${cy - 5} l5,5 l-5,5 l-5,-5 z`} fill={SURFACE} stroke={INK_1} stroke-width="1.5" />
					<!-- this hexagon -->
					{#if value !== null}
						<circle cx={x(value)} cy={cy} r="5" fill={TIERRA} stroke={SURFACE} stroke-width="2" />
					{/if}
				</g>
			{/each}
			<!-- axis -->
			{#each ticks as t, j}
				<text x={j === 0 ? LABEL_W + PAD : j === ticks.length - 1 ? w - PAD : x(t)} y={H - 3}
					text-anchor={j === 0 ? 'start' : j === ticks.length - 1 ? 'end' : 'middle'}
					font-size="11" fill={INK_3} style="font-variant-numeric: tabular-nums">{fmt(t)}{j === ticks.length - 1 && unit ? ` ${unit}` : ''}{#if j === 0 && sqrt}<tspan font-style="italic"> · {sqrtLabel}</tspan>{/if}</text>
			{/each}
		</svg>
		{#if hover !== null}
			{@const s = scopes[hover]}
			<div class="ps-tip" style:top={`${hover * ROW - 4}px`}>
				P10 {fmt(s.p10)} · P25 {fmt(s.p25)} · <b>med. {fmt(s.p50)}</b> · P75 {fmt(s.p75)} · P90 {fmt(s.p90)} · ◇ {fmt(s.mean)}{unit ? ` ${unit}` : ''}
			</div>
		{/if}
	{/if}
</div>

<style>
	.ps { position: relative; width: 100%; margin: 2px 0 0; }
	svg { display: block; overflow: visible; }
	.ps-tip {
		position: absolute; left: 0; right: 0; transform: translateY(-100%);
		font-size: 11px; line-height: 1.4; color: var(--s-ink-1); background: var(--s-surface);
		border: 1px solid rgba(255, 255, 255, 0.18); border-radius: 4px; padding: 3px 6px;
		font-variant-numeric: tabular-nums; pointer-events: none; z-index: 2;
	}
</style>
