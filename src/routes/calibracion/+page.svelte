<script lang="ts">
	import { onMount } from 'svelte';
	import { ANALYSIS_REGISTRY, HEX_LAYER_REGISTRY, TERRITORY_REGISTRY, LENS_CONFIG, getSatGlobalUrl, type AnalysisConfig, type TerritoryConfig } from '$lib/config';
	import { query, isReady } from '$lib/stores/duckdb';

	// ── Build the set of analyses and territories to show ──────────────────────
	const comparableAnalyses = ANALYSIS_REGISTRY.filter(a => {
		if (!a.comparable) return false;
		const layer = HEX_LAYER_REGISTRY[a.id];
		if (!layer) return false;
		// Skip categorical layers — their "score" column is the underlying mean, still useful
		// Skip only if there's no score-like column
		return true;
	}).filter((a, idx, arr) => arr.findIndex(b => b.id === a.id) === idx); // deduplicate by id

	const territories = Object.values(TERRITORY_REGISTRY).filter(t => t.available);

	// ── Cell state ──────────────────────────────────────────────────────────────
	interface CellData {
		p25: number;
		p50: number;
		p75: number;
		avg: number;
		n: number;
	}
	type CellState = CellData | 'loading' | 'unavailable' | 'error';

	let cells = $state(new Map<string, CellState>());
	let loaded = $state(0);
	let total = $state(0);
	let dbReady = $state(false);

	function cellKey(analysisId: string, territoryId: string): string {
		return `${analysisId}:${territoryId}`;
	}

	function getCoverage(analysis: AnalysisConfig, territory: TerritoryConfig): 'available' | 'pending' | 'unavailable' {
		if (!analysis.coverage) return 'available';
		return (analysis.coverage[territory.id] as any) ?? 'available';
	}

	function getScoreCol(analysisId: string): string {
		const layer = HEX_LAYER_REGISTRY[analysisId];
		const pv = layer?.primaryVariable ?? 'score';
		// For categorical primaryVariable ('type', 'territorial_type') use 'score' instead
		if (pv === 'type' || pv === 'territorial_type') return 'score';
		return pv;
	}

	function cellColor(data: CellData): string {
		const dev = Math.abs(data.p50 - 50);
		if (dev <= 8)  return 'rgba(34,197,94,0.15)';
		if (dev <= 18) return 'rgba(234,179,8,0.15)';
		return 'rgba(239,68,68,0.18)';
	}

	function cellTextColor(data: CellData): string {
		const dev = Math.abs(data.p50 - 50);
		if (dev <= 8)  return '#86efac';
		if (dev <= 18) return '#fde68a';
		return '#fca5a5';
	}

	async function waitForDb(maxMs = 15000): Promise<boolean> {
		const start = Date.now();
		while (!isReady()) {
			if (Date.now() - start > maxMs) return false;
			await new Promise(r => setTimeout(r, 200));
		}
		return true;
	}

	async function loadAll() {
		const ready = await waitForDb();
		if (!ready) return;
		dbReady = true;

		// Count queryable cells
		const pairs: { analysis: AnalysisConfig; territory: TerritoryConfig }[] = [];
		for (const a of comparableAnalyses) {
			for (const t of territories) {
				const cov = getCoverage(a, t);
				const key = cellKey(a.id, t.id);
				if (cov !== 'available') {
					cells.set(key, 'unavailable');
					cells = new Map(cells);
				} else {
					cells.set(key, 'loading');
					cells = new Map(cells);
					pairs.push({ analysis: a, territory: t });
				}
			}
		}
		total = pairs.length;

		// Fire all queries in parallel, update cells as they complete
		await Promise.allSettled(pairs.map(async ({ analysis, territory }) => {
			const key = cellKey(analysis.id, territory.id);
			const col = getScoreCol(analysis.id);
			const url = getSatGlobalUrl(analysis.id, territory.parquetPrefix);
			try {
				const r = await query(
					`SELECT quantile_disc("${col}", 0.25) as p25, quantile_disc("${col}", 0.5) as p50, quantile_disc("${col}", 0.75) as p75, avg("${col}") as avg_score, count(*) as n FROM '${url}' WHERE "${col}" IS NOT NULL AND "${col}" > 0`
				);
				if (r.numRows === 0) {
					cells.set(key, 'error');
				} else {
					const row = r.get(0)!.toJSON() as Record<string, any>;
					const p50 = Number(row.p50);
					if (!Number.isFinite(p50)) { cells.set(key, 'error'); }
					else {
						cells.set(key, {
							p25: Number(row.p25),
							p50,
							p75: Number(row.p75),
							avg: Number(row.avg_score),
							n: Number(row.n),
						});
					}
				}
			} catch {
				cells.set(key, 'error');
			} finally {
				cells = new Map(cells);
				loaded++;
			}
		}));
	}

	onMount(() => { loadAll(); });

	// ── UI helpers ──────────────────────────────────────────────────────────────
	function analysisLabel(analysis: AnalysisConfig): string {
		// Extract short label from titleKey — just use id as fallback
		const id = analysis.id.replace(/_/g, ' ');
		return id;
	}

	function lensColor(analysis: AnalysisConfig): string {
		return LENS_CONFIG[analysis.lensId]?.color ?? '#737373';
	}

	function lensLabel(analysis: AnalysisConfig): string {
		return LENS_CONFIG[analysis.lensId]?.label.es ?? analysis.lensId;
	}
</script>

<svelte:head>
	<title>Calibración · nealab</title>
</svelte:head>

<div class="page">
	<div class="page-header">
		<a class="back-link" href="/">← mapa</a>
		<div>
			<h1 class="page-title">Calibración de capas comparables</h1>
			<p class="page-sub">
				Mediana (P50) por capa × territorio. Verde = bien calibrado (P50 ≈ 50). Rojo = revisar goalposts.
				{#if total > 0}
					<span class="counter">{loaded}/{total} celdas</span>
				{/if}
			</p>
		</div>
	</div>

	<div class="table-wrap">
		<table class="matrix">
			<thead>
				<tr>
					<th class="th-label">Capa</th>
					{#each territories as t}
						<th class="th-territory">{t.flag} {t.shortLabel}</th>
					{/each}
				</tr>
			</thead>
			<tbody>
				{#each comparableAnalyses as analysis}
					{@const layer = HEX_LAYER_REGISTRY[analysis.id]}
					<tr>
						<td class="td-label">
							<span class="analysis-name">{analysis.id.replace(/_/g, ' ')}</span>
							<span class="lens-badge" style:background="{lensColor(analysis)}22" style:color={lensColor(analysis)}>
								{lensLabel(analysis)}
							</span>
						</td>
						{#each territories as t}
							{@const key = cellKey(analysis.id, t.id)}
							{@const state = cells.get(key)}
							<td
								class="td-cell"
								class:cell-unavailable={state === 'unavailable'}
								class:cell-loading={state === 'loading' || state === undefined}
								class:cell-error={state === 'error'}
								style:background={typeof state === 'object' ? cellColor(state) : undefined}
							>
								{#if state === 'unavailable'}
									<span class="cell-dash">—</span>
								{:else if state === 'loading' || state === undefined}
									<span class="cell-spin">·</span>
								{:else if state === 'error'}
									<span class="cell-error-icon">⚠</span>
								{:else}
									<span class="cell-p50" style:color={cellTextColor(state)}>{state.p50.toFixed(0)}</span>
									<div class="cell-bar-wrap">
										<div
											class="cell-bar"
											style:left="{state.p25}%"
											style:width="{state.p75 - state.p25}%"
											style:background={cellTextColor(state)}
										></div>
									</div>
									<span class="cell-avg">avg {state.avg.toFixed(1)}</span>
								{/if}
							</td>
						{/each}
					</tr>
				{/each}
			</tbody>
		</table>
	</div>

	<div class="legend">
		<span class="leg-item leg-green">■ P50 40–60 (bien calibrado)</span>
		<span class="leg-item leg-yellow">■ P50 32–68 (aceptable)</span>
		<span class="leg-item leg-red">■ P50 &lt;32 o &gt;68 (revisar goalposts)</span>
		<span class="leg-item leg-dash">— sin datos en este territorio</span>
	</div>
</div>

<style>
	.page {
		background: #0a0f1a;
		min-height: 100vh;
		color: #e2e8f0;
		font-family: 'JetBrains Mono', ui-monospace, monospace;
		padding: 24px;
		box-sizing: border-box;
	}
	.page-header {
		display: flex;
		align-items: flex-start;
		gap: 16px;
		margin-bottom: 20px;
	}
	.back-link {
		color: #60a5fa;
		text-decoration: none;
		font-size: 11px;
		padding-top: 4px;
		white-space: nowrap;
	}
	.back-link:hover { text-decoration: underline; }
	.page-title {
		font-size: 18px;
		font-weight: 700;
		color: #f1f5f9;
		margin: 0 0 4px;
	}
	.page-sub {
		font-size: 10px;
		color: #94a3b8;
		margin: 0;
		line-height: 1.5;
	}
	.counter {
		margin-left: 12px;
		color: #60a5fa;
		font-weight: 600;
	}

	/* ── Table ── */
	.table-wrap {
		overflow-x: auto;
		border-radius: 8px;
		border: 1px solid rgba(100,116,139,0.2);
	}
	.matrix {
		border-collapse: collapse;
		width: 100%;
		font-size: 9px;
	}
	.matrix thead {
		position: sticky;
		top: 0;
		z-index: 2;
		background: #0f172a;
	}
	.th-label {
		text-align: left;
		padding: 8px 10px;
		color: #94a3b8;
		font-weight: 600;
		min-width: 160px;
		position: sticky;
		left: 0;
		background: #0f172a;
		border-right: 1px solid rgba(100,116,139,0.15);
		z-index: 3;
	}
	.th-territory {
		text-align: center;
		padding: 8px 6px;
		color: #94a3b8;
		font-weight: 600;
		min-width: 72px;
		border-left: 1px solid rgba(100,116,139,0.08);
	}

	/* ── Rows ── */
	.matrix tbody tr:nth-child(even) { background: rgba(255,255,255,0.015); }
	.matrix tbody tr:hover { background: rgba(255,255,255,0.035); }

	.td-label {
		padding: 6px 10px;
		position: sticky;
		left: 0;
		background: inherit;
		border-right: 1px solid rgba(100,116,139,0.15);
		z-index: 1;
	}
	.matrix tbody tr:nth-child(even) .td-label { background: #0d1520; }
	.matrix tbody tr:hover .td-label { background: #111827; }
	.analysis-name {
		display: block;
		font-size: 9px;
		color: #cbd5e1;
		font-weight: 500;
		white-space: nowrap;
	}
	.lens-badge {
		display: inline-block;
		font-size: 7px;
		font-weight: 600;
		padding: 1px 4px;
		border-radius: 3px;
		margin-top: 2px;
		letter-spacing: 0.03em;
	}

	/* ── Cells ── */
	.td-cell {
		text-align: center;
		padding: 4px 4px;
		border-left: 1px solid rgba(100,116,139,0.08);
		border-top: 1px solid rgba(100,116,139,0.05);
		vertical-align: middle;
		min-width: 72px;
		height: 44px;
	}
	.cell-unavailable { background: rgba(15,23,42,0.6) !important; }
	.cell-loading { background: rgba(255,255,255,0.02) !important; }
	.cell-error { background: rgba(100,116,139,0.06) !important; }

	.cell-dash { color: #334155; font-size: 11px; }
	.cell-spin {
		color: #475569;
		font-size: 14px;
		display: inline-block;
		animation: pulse 1s ease-in-out infinite;
	}
	@keyframes pulse {
		0%, 100% { opacity: 0.3; }
		50% { opacity: 1; }
	}
	.cell-error-icon { color: #64748b; font-size: 10px; }

	.cell-p50 {
		display: block;
		font-size: 14px;
		font-weight: 700;
		line-height: 1.1;
	}
	.cell-bar-wrap {
		position: relative;
		height: 3px;
		background: rgba(255,255,255,0.06);
		border-radius: 2px;
		margin: 2px 4px;
		overflow: hidden;
	}
	.cell-bar {
		position: absolute;
		top: 0;
		height: 100%;
		border-radius: 2px;
		opacity: 0.6;
	}
	.cell-avg {
		display: block;
		font-size: 7px;
		color: #64748b;
		margin-top: 1px;
	}

	/* ── Legend ── */
	.legend {
		display: flex;
		gap: 20px;
		margin-top: 16px;
		font-size: 9px;
		flex-wrap: wrap;
	}
	.leg-item { color: #94a3b8; }
	.leg-green { color: #86efac; }
	.leg-yellow { color: #fde68a; }
	.leg-red { color: #fca5a5; }
	.leg-dash { color: #334155; }
</style>
