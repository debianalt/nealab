<script lang="ts">
	import { i18n } from '$lib/stores/i18n.svelte';
	import { LENS_CONFIG, TERRITORY_REGISTRY, type LensId } from '$lib/config';
	import { fichaStore } from '$lib/stores/ficha.svelte';
	import { loadFicha, type FichaLayer, type FichaVar } from '$lib/utils/ficha';
	import { formatDept } from '$lib/utils/format';
	import ChartFrame from './ChartFrame.svelte';

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
				<p class="fi-legend">{i18n.t('ficha.legend')}</p>
				{#each groups as g}
					<div class="fi-lens">
						<div class="fi-lens-title"><span class="fi-dot" style:background={g.color}></span>{g.label}</div>
						{#each g.layers as l}
							<details class="fi-layer">
								<summary>
									<span class="fi-layer-title">{i18n.t(l.titleKey)}</span>
									{#if l.error}
										<span class="fi-sum fi-muted">{i18n.t('chart.loadError')}</span>
									{:else if !l.found || !l.primary || l.primary.nodata}
										<span class="fi-sum fi-muted">{emptyText(l)}</span>
									{:else}
										<span class="fi-sum">{valueText(l.primary)}</span>
										<span class="fi-pctl" title={i18n.t('ficha.pctlHint')}>{pct(l.primary.pctlDept)}</span>
									{/if}
								</summary>
								{#if l.found && l.vars.length}
									{#if l.typeLabel}<div class="fi-type">{l.typeLabel}</div>{/if}
									<table class="fi-table">
										<thead>
											<tr><th></th><th>{i18n.t('ficha.colValue')}</th><th>{l.scope === 'dept' ? i18n.t('ficha.colPctlDept') : i18n.t('ficha.colPctlTerr')}</th><th>{l.scope === 'dept' ? i18n.t('ficha.colMeanDept') : i18n.t('ficha.colMeanTerr')}</th></tr>
										</thead>
										<tbody>
											{#each l.vars as v}
												<tr>
													<td class="fi-var">{i18n.t(v.labelKey)}</td>
													<td class="fi-num">{v.nodata ? '—' : valueText(v)}</td>
													<td class="fi-num">{pct(v.pctlDept)}</td>
													<td class="fi-num">{v.text ? '' : fmt(v.meanDept)}</td>
												</tr>
											{/each}
										</tbody>
									</table>
									{#if l.scope === 'territory'}<div class="fi-note">{i18n.t('ficha.scopeTerr')}</div>{/if}
								{:else if !l.error}
									<div class="fi-note">{emptyText(l)}</div>
								{/if}
							</details>
						{/each}
					</div>
				{/each}
				<p class="fi-note">{i18n.t('ficha.pending')}</p>
			{/if}
		</div>
	</ChartFrame>
{/if}

<style>
	.fi { font-size: 12.5px; color: #e2e8f0; }
	.fi-head { padding: 2px 2px 8px; border-bottom: 1px solid rgba(255,255,255,0.08); margin-bottom: 6px; }
	.fi-code { font-family: monospace; font-size: 13px; color: #f8fafc; }
	.fi-place { font-size: 12px; color: rgba(255,255,255,0.7); margin-top: 2px; }
	.fi-actions { display: flex; gap: 6px; margin-top: 6px; }
	.fi-btn {
		font-size: 11.5px; font-family: inherit; color: #e2e8f0; cursor: pointer;
		background: rgba(255,255,255,0.06); border: 1px solid #334155; border-radius: 4px; padding: 3px 8px;
	}
	.fi-btn:hover { border-color: #60a5fa; }
	.fi-state { font-size: 12px; color: rgba(255,255,255,0.6); padding: 12px 2px; font-style: italic; }
	.fi-error { color: #f87171; }
	.fi-legend { font-size: 11px; color: rgba(255,255,255,0.55); line-height: 1.45; margin: 0 2px 6px; }
	.fi-lens { margin: 8px 0 4px; }
	.fi-lens-title { font-size: 11.5px; font-weight: 700; letter-spacing: 0.04em; text-transform: uppercase; color: rgba(255,255,255,0.75); display: flex; align-items: center; gap: 6px; margin-bottom: 2px; }
	.fi-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
	.fi-layer { border-bottom: 1px solid rgba(255,255,255,0.06); }
	.fi-layer summary {
		display: flex; flex-wrap: wrap; align-items: baseline; gap: 2px 6px; padding: 5px 2px; cursor: pointer; list-style: none;
	}
	.fi-layer summary::-webkit-details-marker { display: none; }
	.fi-layer summary::before { content: '▸'; font-size: 10px; color: rgba(255,255,255,0.5); transition: transform 0.15s; }
	.fi-layer[open] summary::before { transform: rotate(90deg); }
	.fi-layer-title { flex: 1; min-width: 0; font-size: 12px; color: rgba(255,255,255,0.85); }
	.fi-sum { font-size: 12.5px; font-weight: 600; white-space: nowrap; }
	/* No-data text goes on its own line under the title instead of squeezing it. */
	.fi-muted { color: rgba(255,255,255,0.5); font-weight: 400; font-style: italic; flex-basis: 100%; padding-left: 14px; white-space: normal; font-size: 11.5px; }
	.fi-pctl { font-size: 11px; font-weight: 700; color: #0f172a; background: #94a3b8; border-radius: 3px; padding: 0 4px; }
	.fi-type { font-size: 11.5px; color: #cbd5e1; margin: 2px 2px 4px 14px; }
	.fi-table { width: 100%; border-collapse: collapse; margin: 2px 0 6px; }
	.fi-table th { font-size: 10.5px; font-weight: 600; color: rgba(255,255,255,0.55); text-align: right; padding: 2px 4px; }
	.fi-table td { font-size: 11.5px; padding: 2px 4px; border-top: 1px solid rgba(255,255,255,0.04); }
	.fi-var { color: #cbd5e1; }
	.fi-num { text-align: right; white-space: nowrap; color: #e2e8f0; }
	.fi-note { font-size: 11px; color: rgba(255,255,255,0.5); line-height: 1.45; margin: 4px 2px 6px; }
</style>
