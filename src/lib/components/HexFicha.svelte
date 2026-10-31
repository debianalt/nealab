<script lang="ts">
	import { i18n } from '$lib/stores/i18n.svelte';
	import { LENS_CONFIG, TERRITORY_REGISTRY, type LensId } from '$lib/config';
	import { fichaStore } from '$lib/stores/ficha.svelte';
	import { loadFicha, type FichaLayer, type FichaVar } from '$lib/utils/ficha';
	import { formatDept } from '$lib/utils/format';
	import { territoryRef } from '$lib/utils/territoryRef';
	import ChartFrame from './ChartFrame.svelte';
	import PositionStrip from './PositionStrip.svelte';

	let {
		deptUrl = (_id: string, _key: string) => null as string | null,
	}: {
		deptUrl?: (layerId: string, key: string) => string | null;
	} = $props();

	const LENS_ORDER: LensId[] = ['ambiente', 'produccion', 'poblacion', 'economia'];

	let layers = $state<FichaLayer[] | null>(null);
	let loading = $state(false);
	let failed = $state(false);
	let copied = $state(false);
	let docked = $state(false);

	const h3 = $derived(fichaStore.h3);
	const ctx = $derived(fichaStore.ctx);
	const territoryLabel = $derived(ctx ? TERRITORY_REGISTRY[ctx.territoryId]?.label ?? ctx.territoryId : '');

	$effect(() => {
		const cell = h3, c = ctx;
		if (!cell || !c) return;
		loading = true;
		failed = false;
		layers = null;
		loadFicha(cell, c, deptUrl)
			.then((ls) => { if (fichaStore.h3 === cell) { layers = ls; loading = false; } })
			.catch(() => { failed = true; loading = false; });
	});

	const groups = $derived(LENS_ORDER
		.map(id => ({ id, label: LENS_CONFIG[id].label[i18n.locale as 'es'] ?? LENS_CONFIG[id].label.es, color: LENS_CONFIG[id].color,
			layers: (layers ?? []).filter(l => l.lensId === id) }))
		.filter(g => g.layers.length > 0));

	function fmt(v: number | null): string {
		if (v === null || !Number.isFinite(v)) return '—';
		const a = Math.abs(v);
		// Same precision as the hex panel: 0 decimals from 100, 1 below, 2 only under 0.1.
		return v.toLocaleString(i18n.locale, { maximumFractionDigits: a >= 100 ? 0 : a >= 0.1 || a === 0 ? 1 : 2 });
	}
	const pct = (p: number | null) => (p === null ? '—' : `P${Math.round(p)}`);

	// «de cada 100 hexágonos del departamento Capital» / «… de la provincia de Misiones»
	function ofPlace(l: FichaLayer): string {
		if (!ctx) return '';
		if (l.scope === 'territory' || !ctx.deptName) return territoryRef(ctx.territoryId, i18n.locale, 'gen');
		const c = TERRITORY_REGISTRY[ctx.territoryId]?.country ?? 'ar';
		return i18n.t(`ficha.of.${c === 'py' ? 'py' : c === 'br' ? 'br' : 'ar'}`).replace('{name}', formatDept(ctx.deptName));
	}

	// The percentile said in words, from strict counts (floored: never overclaim).
	// Ties are named when they are common (zero-heavy layers).
	function words(v: FichaVar, l: FichaLayer): string {
		const s = v.scopes[0];
		if (!s || v.value === null || !Number.isFinite(s.below)) return '';
		const of = ofPlace(l);
		const below = Math.floor(s.below), above = Math.floor(100 - s.below - s.equal), eq = Math.round(s.equal);
		if (s.equal >= 20) {
			const head = i18n.t('ficha.w.equal').replace('{n}', String(eq)).replace('{of}', of);
			if (below >= 1 && below >= above) return head + i18n.t('ficha.w.andHigher').replace('{n}', String(below));
			if (above >= 1) return head + i18n.t('ficha.w.andLower').replace('{n}', String(above));
			return head + '.';
		}
		return below >= above
			? i18n.t('ficha.w.higher').replace('{n}', String(below)).replace('{of}', of)
			: i18n.t('ficha.w.lower').replace('{n}', String(above)).replace('{of}', of);
	}

	function ctxLine(v: FichaVar): string {
		const s = v.scopes[0];
		if (!s) return '';
		const u = v.unit ? ` ${v.unit}` : '';
		return i18n.t('ficha.ctx').replace('{med}', fmt(s.p50) + u).replace('{mean}', fmt(s.mean) + u);
	}
	const fmtN = (x: number) => fmt(x);

	function valueText(v: FichaVar): string {
		if (v.text) return v.text;
		if (v.nodata) return '';
		return `${fmt(v.value)} ${v.unit}`.trim();
	}
	function emptyText(l: FichaLayer): string {
		return l.census ? i18n.t('ficha.noPop') : i18n.t('ficha.noData');
	}

	async function copyLink() {
		if (!h3) return;
		const url = `${location.origin}${location.pathname}?h3=${h3}`;
		try { await navigator.clipboard.writeText(url); copied = true; setTimeout(() => (copied = false), 1800); } catch { /* clipboard blocked */ }
	}

	function csvRows() {
		const rows: Record<string, unknown>[] = [];
		for (const l of layers ?? []) {
			for (const v of l.vars) {
				rows.push({
					h3index: h3, capa: i18n.t(l.titleKey), variable: i18n.t(v.labelKey),
					valor: v.text ?? (v.value ?? ''), unidad: v.unit,
					percentil_depto: v.pctlDept === null ? '' : v.pctlDept.toFixed(1),
					pct_menor_depto: v.scopes[0] && Number.isFinite(v.scopes[0].below) ? v.scopes[0].below.toFixed(1) : '',
					pct_igual_depto: v.scopes[0] && Number.isFinite(v.scopes[0].equal) ? v.scopes[0].equal.toFixed(1) : '',
					p10_depto: v.scopes[0]?.p10.toFixed(3) ?? '',
					p25_depto: v.scopes[0]?.p25.toFixed(3) ?? '',
					mediana_depto: v.scopes[0]?.p50.toFixed(3) ?? '',
					p75_depto: v.scopes[0]?.p75.toFixed(3) ?? '',
					p90_depto: v.scopes[0]?.p90.toFixed(3) ?? '',
					promedio_depto: v.meanDept === null ? '' : v.meanDept.toFixed(3),
					ambito: l.scope === 'dept' ? 'departamento' : 'territorio',
				});
			}
		}
		return rows;
	}
</script>

{#if h3 && ctx}
	<ChartFrame bind:expanded={docked} title={i18n.t('ficha.title')} csvRows={csvRows} csvFilename={`spatia_ficha_${h3}`} collapsible={false}>
		<div class="fi">
			<div class="fi-head">
				<div class="fi-code">{h3}</div>
				<div class="fi-place">{ctx.deptName ? formatDept(ctx.deptName) + ' · ' : ''}{territoryLabel}</div>
				<div class="fi-actions">
					<button class="fi-btn" onclick={copyLink}>{copied ? i18n.t('ficha.copied') : i18n.t('ficha.copyLink')}</button>
					<button class="fi-btn" onclick={() => fichaStore.close()}>× {i18n.t('ficha.close')}</button>
				</div>
			</div>

			{#if loading}
				<div class="fi-state">{i18n.t('ficha.loading')}</div>
			{:else if failed}
				<div class="fi-state fi-error">{i18n.t('chart.loadError')}</div>
			{:else}
				<p class="fi-legend">{i18n.t('ficha.legend2')}</p>
								<div class="fi-key" aria-hidden="true">
					<span class="fi-key-item"><svg width="34" height="12" style="width:34px;height:12px"><rect x="1" y="4" width="32" height="4" rx="2" fill="var(--s-ink-3)" opacity="0.45" /></svg>{i18n.t('ficha.lg.band')}</span>
					<span class="fi-key-item"><svg width="22" height="12" style="width:22px;height:12px"><rect x="1" y="0.5" width="20" height="11" rx="2" fill="var(--s-ink-3)" fill-opacity="0.28" stroke="var(--s-ink-2)" /></svg>{i18n.t('ficha.lg.box')}</span>
					<span class="fi-key-item"><svg width="6" height="14" style="width:6px;height:14px"><line x1="3" x2="3" y1="0" y2="14" stroke="var(--s-ink-1)" stroke-width="2" /></svg>{i18n.t('ficha.lg.median')}</span>
					<span class="fi-key-item"><svg width="12" height="12" style="width:12px;height:12px"><path d="M6,1 l5,5 l-5,5 l-5,-5 z" fill="var(--s-surface)" stroke="var(--s-ink-1)" stroke-width="1.5" /></svg>{i18n.t('ficha.lg.mean')}</span>
					<span class="fi-key-item"><svg width="12" height="12" style="width:12px;height:12px"><circle cx="6" cy="6" r="5" fill="var(--s-cat-1)" /></svg>{i18n.t('ficha.lg.hex')}</span>
				</div>
				{#each groups as g}
					<div class="fi-lens">
						<div class="fi-lens-title"><span class="fi-dot" style:background={g.color}></span>{g.label}</div>
						{#each g.layers as l}
							{@const p = l.primary}
							<section class="fi-card">
								<div class="fi-layer-title">{i18n.t(l.titleKey)}</div>
								{#if l.error}
									<div class="fi-muted">{i18n.t('chart.loadError')}</div>
								{:else if !l.found || !p || p.nodata}
									<div class="fi-muted">{emptyText(l)}</div>
								{:else if p.text}
									<div class="fi-val"><span class="fi-big fi-big-text">{p.text}</span></div>
								{:else}
									<div class="fi-val">
										<span class="fi-big num">{fmt(p.value)}</span>{#if p.unit}<span class="fi-unit">{p.unit}</span>{/if}
										<span class="fi-ctx tnum">{ctxLine(p)}</span>
									</div>
									{#if l.typeLabel}<div class="fi-type">{l.typeLabel}</div>{/if}
									<div class="fi-words">{words(p, l)}</div>
									{#if p.scopes.length}
										<PositionStrip scopes={p.scopes} value={p.value} fmt={fmtN} unit={p.unit}
											labels={[l.scope === 'dept' && ctx.deptName ? formatDept(ctx.deptName) : territoryLabel]}
											sqrtLabel={i18n.t('ficha.sqrt')}
											ariaLabel={`${i18n.t(l.titleKey)}: ${fmt(p.value)} ${p.unit}. ${ctxLine(p)}. ${words(p, l)}`} />
									{/if}
								{/if}
								{#if l.found && l.vars.length > 1}
									<details class="fi-more">
										<summary>{i18n.t('ficha.allVars').replace('{n}', String(l.vars.length))}</summary>
										<table class="fi-table">
											<thead>
												<tr><th></th><th>{i18n.t('ficha.colValue')}</th><th>{l.scope === 'dept' ? i18n.t('ficha.colPctlDept') : i18n.t('ficha.colPctlTerr')}</th><th>{l.scope === 'dept' ? i18n.t('ficha.colMedianDept') : i18n.t('ficha.colMedianTerr')}</th><th>{l.scope === 'dept' ? i18n.t('ficha.colMeanDept') : i18n.t('ficha.colMeanTerr')}</th></tr>
											</thead>
											<tbody>
												{#each l.vars as v}
													<tr>
														<td class="fi-var">{i18n.t(v.labelKey)}</td>
														<td class="fi-num">{v.nodata ? '—' : valueText(v)}</td>
														<td class="fi-num">{pct(v.pctlDept)}</td>
														<td class="fi-num">{v.text || !v.scopes[0] ? '' : fmt(v.scopes[0].p50)}</td>
														<td class="fi-num">{v.text ? '' : fmt(v.meanDept)}</td>
													</tr>
												{/each}
											</tbody>
										</table>
									</details>
								{/if}
								{#if l.found && l.scope === 'territory'}<div class="fi-note">{i18n.t('ficha.scopeTerr')}</div>{/if}
							</section>
						{/each}
					</div>
				{/each}
				<p class="fi-note">{i18n.t('ficha.pending')}</p>
			{/if}
		</div>
	</ChartFrame>
{/if}

<style>
	/* Sello: text in ink tokens, numbers in serif with tabular figures, colour only on the hexagon dot. */
	.fi { font-size: 12.5px; color: var(--s-ink-1); }
	.fi-head { padding: 2px 2px 8px; border-bottom: 1px solid var(--s-grid); margin-bottom: 6px; }
	.fi-code { font-family: var(--font-mono); font-size: 13px; color: var(--s-ink-1); }
	.fi-place { font-size: 12px; color: var(--s-ink-2); margin-top: 2px; }
	.fi-actions { display: flex; gap: 6px; margin-top: 6px; }
	.fi-btn {
		font-size: 11.5px; font-family: inherit; color: var(--s-ink-1); cursor: pointer;
		background: rgba(255,255,255,0.06); border: 1px solid #334155; border-radius: 4px; padding: 3px 8px;
	}
	.fi-btn:hover { border-color: var(--s-ink-2); }
	.fi-state { font-size: 12px; color: var(--s-ink-3); padding: 12px 2px; font-style: italic; }
	.fi-error { color: var(--s-cat-1); }
	.fi-legend { font-size: 11.5px; color: var(--s-ink-2); line-height: 1.45; margin: 0 2px 4px; }
	.fi-key {
		display: flex; flex-wrap: wrap; align-items: center; gap: 2px 6px; margin: 0 2px 6px;
		font-size: 11px; color: var(--s-ink-3);
	}
	.fi-key-item { display: inline-flex; align-items: center; gap: 4px; white-space: nowrap; margin-right: 6px; }
	.fi-key svg { flex: none; }
	.fi-lens { margin: 10px 0 4px; }
	.fi-lens-title { font-size: 11.5px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; color: var(--s-ink-2); display: flex; align-items: center; gap: 6px; margin-bottom: 2px; }
	.fi-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
	.fi-card { padding: 7px 2px 8px; border-bottom: 1px solid var(--s-grid); }
	.fi-layer-title { font-size: 12px; color: var(--s-ink-2); line-height: 1.35; }
	.fi-val { display: flex; flex-wrap: wrap; align-items: baseline; gap: 0 4px; margin-top: 2px; }
	.fi-big { font-size: 22px; line-height: 1.15; font-weight: 500; color: var(--s-ink-1); }
	.fi-big-text { font-family: var(--font-sans); font-size: 15px; font-weight: 600; }
	.fi-unit { font-size: 12.5px; color: var(--s-ink-2); margin-right: 8px; }
	.fi-ctx { font-size: 11.5px; color: var(--s-ink-3); margin-left: auto; white-space: nowrap; }
	.fi-words { font-size: 12px; font-weight: 400; color: var(--s-ink-1); line-height: 1.4; margin: 2px 0 1px; }
	.fi-muted { color: var(--s-ink-3); font-style: italic; font-size: 11.5px; margin-top: 2px; }
	.fi-type { font-size: 11.5px; color: var(--s-ink-2); margin: 1px 0 0; }
	.fi-more { margin-top: 4px; }
	.fi-more summary { font-size: 11.5px; color: var(--s-ink-3); cursor: pointer; list-style: none; padding: 2px 0; }
	.fi-more summary::-webkit-details-marker { display: none; }
	.fi-more summary::before { content: '▸ '; font-size: 10px; }
	.fi-more[open] summary::before { content: '▾ '; }
	.fi-table { width: 100%; border-collapse: collapse; margin: 2px 0 4px; font-variant-numeric: tabular-nums; }
	.fi-table th { font-size: 11px; font-weight: 600; color: var(--s-ink-3); text-align: right; padding: 2px 3px; }
	.fi-table td { font-size: 11.5px; padding: 2px 3px; border-top: 1px solid var(--s-grid); }
	.fi-var { color: var(--s-ink-2); }
	.fi-num { text-align: right; white-space: nowrap; color: var(--s-ink-1); }
	.fi-note { font-size: 11px; color: var(--s-ink-3); line-height: 1.45; margin: 4px 2px 6px; }
</style>
