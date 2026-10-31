<script lang="ts">
	import type { Snippet } from 'svelte';
	import { exportSvgAsPng, exportSvgAsSvg, downloadCsvFromRows } from '$lib/utils/data-export';

	let {
		title,
		children,
		csvRows,
		csvColumns,
		csvFilename = 'spatia_chart',
		collapsible = true,
		defaultCollapsed = false,
		expanded = $bindable(false),
	}: {
		title: string;
		children: Snippet;
		csvRows?: () => Array<Record<string, unknown>>;
		csvColumns?: string[];
		csvFilename?: string;
		collapsible?: boolean;
		defaultCollapsed?: boolean;
		expanded?: boolean;
	} = $props();

	// Collapse hides the body via CSS (not {#if}) so chart state — brush selections,
	// computed layouts — survives toggling, same rationale as the expand portal.
	let collapsed = $state(collapsible && defaultCollapsed);

	function toggleCollapsed() {
		if (!collapsible || expanded) return;
		collapsed = !collapsed;
	}
	let exportMenuOpen = $state(false);
	let host: HTMLDivElement | undefined = $state();

	// Expanded = docked beside the map (split view): the chart takes the right
	// side, the map stays live on the left and pads its camera to that width.
	const DOCK_KEY = 'spatia_dock_w';
	const frameId = Math.random().toString(36).slice(2);
	function defaultDockW(): number {
		if (typeof window === 'undefined') return 600;
		if (window.innerWidth < 768) return window.innerWidth;
		try {
			const saved = Number(localStorage.getItem(DOCK_KEY));
			if (saved > 0) return clampDock(saved);
		} catch {}
		return clampDock(window.innerWidth * 0.5);
	}
	function clampDock(w: number): number {
		if (window.innerWidth < 768) return window.innerWidth;
		return Math.round(Math.max(420, Math.min(w, window.innerWidth - 320, 1100)));
	}
	let dockW = $state(600);

	function announceDock(width: number) {
		window.dispatchEvent(new CustomEvent('spatia:chart-dock', { detail: { width, id: frameId } }));
	}

	function toggleExpanded() {
		exportMenuOpen = false;
		if (expanded) { expanded = false; return; }
		dockW = defaultDockW();
		expanded = true;
	}

	// One docked chart at a time; closing tells the map to drop its padding.
	$effect(() => {
		if (!expanded) return;
		announceDock(dockW);
		const onOther = (e: Event) => {
			const d = (e as CustomEvent<{ width: number; id: string }>).detail;
			if (d?.id !== frameId && d?.width > 0) expanded = false;
		};
		window.addEventListener('spatia:chart-dock', onOther);
		return () => {
			window.removeEventListener('spatia:chart-dock', onOther);
			announceDock(0);
		};
	});

	function startResize(e: MouseEvent) {
		e.preventDefault();
		const onMove = (ev: MouseEvent) => { dockW = clampDock(window.innerWidth - ev.clientX); };
		const onUp = () => {
			window.removeEventListener('mousemove', onMove);
			window.removeEventListener('mouseup', onUp);
			try { localStorage.setItem(DOCK_KEY, String(dockW)); } catch {}
			announceDock(dockW);
		};
		window.addEventListener('mousemove', onMove);
		window.addEventListener('mouseup', onUp);
	}

	// Move DOM node to document.body when expanded, restore on collapse.
	// Critical: keeps Svelte component state (brush selections, computed values)
	// intact — using {#if}{:else} would remount and reset.
	function portal(node: HTMLElement, active: boolean) {
		const placeholder = document.createComment('chart-frame-portal');
		const originalParent = node.parentElement;
		const originalNext = node.nextSibling;

		function update(active: boolean) {
			if (active) {
				if (!placeholder.parentNode && originalParent) {
					originalParent.insertBefore(placeholder, node);
				}
				if (node.parentElement !== document.body) {
					document.body.appendChild(node);
				}
			} else {
				if (placeholder.parentNode) {
					placeholder.parentNode.insertBefore(node, placeholder);
					placeholder.parentNode.removeChild(placeholder);
				} else if (originalParent && node.parentElement !== originalParent) {
					originalParent.insertBefore(node, originalNext);
				}
			}
		}

		update(active);

		return {
			update(newActive: boolean) { update(newActive); },
			destroy() {
				if (placeholder.parentNode) placeholder.parentNode.removeChild(placeholder);
			}
		};
	}

	function findSvg(): SVGElement | null {
		return host?.querySelector('.chart-frame-body svg') ?? null;
	}

	function timestamp(): string {
		const d = new Date();
		const pad = (n: number) => String(n).padStart(2, '0');
		return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
	}

	async function handleExportPng() {
		const svg = findSvg();
		if (!svg) return;
		await exportSvgAsPng(svg, `${csvFilename}_${timestamp()}.png`);
		exportMenuOpen = false;
	}

	function handleExportSvg() {
		const svg = findSvg();
		if (!svg) return;
		exportSvgAsSvg(svg, `${csvFilename}_${timestamp()}.svg`);
		exportMenuOpen = false;
	}

	function handleExportCsv() {
		if (!csvRows) return;
		const rows = csvRows();
		if (rows.length === 0) {
			exportMenuOpen = false;
			return;
		}
		const cols = csvColumns ?? Object.keys(rows[0]);
		downloadCsvFromRows(rows, cols, `${csvFilename}_${timestamp()}.csv`);
		exportMenuOpen = false;
	}

	$effect(() => {
		if (!expanded) return;
		const onKey = (e: KeyboardEvent) => {
			if (e.key === 'Escape') {
				// Esc closes the docked chart only, not the page selection (+page clearAll).
				e.stopPropagation();
				expanded = false;
				exportMenuOpen = false;
			}
		};
		window.addEventListener('keydown', onKey, true);
		return () => window.removeEventListener('keydown', onKey, true);
	});

	$effect(() => {
		if (!exportMenuOpen) return;
		const onClick = (e: MouseEvent) => {
			const target = e.target as Node;
			if (host && !host.querySelector('.chart-frame-export-wrap')?.contains(target)) {
				exportMenuOpen = false;
			}
		};
		// Defer to skip the click that opened the menu
		const id = setTimeout(() => window.addEventListener('click', onClick), 0);
		return () => {
			clearTimeout(id);
			window.removeEventListener('click', onClick);
		};
	});
</script>

<div
	bind:this={host}
	class="chart-frame"
	class:chart-frame-expanded={expanded}
	style:--dock-w="{dockW}px"
	use:portal={expanded}
>
	{#if expanded}
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div class="chart-frame-dock-handle" onmousedown={startResize} title="Arrastrá para cambiar el ancho"></div>
	{/if}
	<div
		class="chart-frame-header"
		class:chart-frame-header-clickable={collapsible && !expanded}
		onclick={toggleCollapsed}
		role="presentation"
	>
		<span class="chart-frame-title">
			{#if collapsible && !expanded}<span class="chart-frame-chevron" class:chart-frame-chevron-open={!collapsed}>▸</span>{/if}
			{title}
		</span>
		<div class="chart-frame-actions" onclick={(e) => e.stopPropagation()} role="presentation">
			<div class="chart-frame-export-wrap">
				<button
					class="chart-frame-btn"
					title="Exportar"
					onclick={() => { exportMenuOpen = !exportMenuOpen; }}
					aria-label="Exportar"
				>↓</button>
				{#if exportMenuOpen}
					<div class="chart-frame-export-menu">
						<button class="chart-frame-export-item" onclick={handleExportPng}>↓ PNG</button>
						<button class="chart-frame-export-item" onclick={handleExportSvg}>↓ SVG</button>
						{#if csvRows}
							<button class="chart-frame-export-item" onclick={handleExportCsv}>↓ CSV</button>
						{/if}
					</div>
				{/if}
			</div>
			<button
				class="chart-frame-btn"
				title={expanded ? 'Cerrar (Esc)' : 'Ver en grande junto al mapa'}
				onclick={toggleExpanded}
				aria-label={expanded ? 'Contraer' : 'Expandir'}
			>{expanded ? '×' : '⤢'}</button>
		</div>
	</div>
	<div class="chart-frame-body" class:chart-frame-body-collapsed={collapsed && !expanded}>
		{@render children()}
	</div>
</div>

<style>
	.chart-frame {
		display: flex;
		flex-direction: column;
		background: rgba(15, 23, 42, 0.5);
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 6px;
		margin: 6px 0;
		overflow: hidden;
	}

	.chart-frame-expanded {
		position: fixed;
		top: 48px;
		right: 0;
		bottom: 0;
		width: var(--dock-w);
		z-index: 1000;
		background: #0a0e1a;
		border: none;
		border-left: 1px solid rgba(255, 255, 255, 0.18);
		border-radius: 0;
		box-shadow: -12px 0 40px rgba(0, 0, 0, 0.55);
		overflow: hidden;
		margin: 0;
	}
	.chart-frame-dock-handle {
		position: absolute;
		left: 0;
		top: 0;
		bottom: 0;
		width: 6px;
		cursor: ew-resize;
		z-index: 2;
	}
	.chart-frame-dock-handle:hover { background: rgba(96, 165, 250, 0.35); }

	.chart-frame-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 5px 8px;
		background: rgba(255, 255, 255, 0.04);
		border-bottom: 1px solid rgba(255, 255, 255, 0.06);
		font-size: 12px;
		font-weight: 600;
		color: rgba(255, 255, 255, 0.75);
		flex-shrink: 0;
	}

	.chart-frame-expanded .chart-frame-header {
		padding: 10px 16px;
		font-size: 14px;
	}

	.chart-frame-title {
		text-transform: uppercase;
		letter-spacing: 0.04em;
		pointer-events: none;
		display: flex;
		align-items: center;
		gap: 5px;
	}

	.chart-frame-header-clickable { cursor: pointer; }
	.chart-frame-header-clickable:hover { background: rgba(255, 255, 255, 0.07); }

	.chart-frame-chevron {
		font-size: 10px;
		color: rgba(255, 255, 255, 0.45);
		transition: transform 0.15s;
	}
	.chart-frame-chevron-open { transform: rotate(90deg); }

	.chart-frame-body-collapsed { display: none; }

	.chart-frame-actions {
		display: flex;
		align-items: center;
		gap: 4px;
		pointer-events: auto;
	}

	.chart-frame-btn {
		background: none;
		border: 1px solid transparent;
		color: rgba(255, 255, 255, 0.55);
		font-size: 13px;
		line-height: 1;
		padding: 3px 7px;
		border-radius: 3px;
		cursor: pointer;
		font-family: inherit;
		transition: all 0.12s;
	}
	.chart-frame-btn:hover {
		color: #fff;
		background: rgba(255, 255, 255, 0.08);
		border-color: rgba(255, 255, 255, 0.18);
	}

	.chart-frame-export-wrap {
		position: relative;
	}

	.chart-frame-export-menu {
		position: absolute;
		top: calc(100% + 4px);
		right: 0;
		background: rgba(10, 14, 26, 0.98);
		border: 1px solid rgba(255, 255, 255, 0.15);
		border-radius: 5px;
		padding: 3px;
		display: flex;
		flex-direction: column;
		gap: 1px;
		z-index: 1001;
		min-width: 90px;
		box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
	}

	.chart-frame-export-item {
		background: none;
		border: none;
		color: rgba(255, 255, 255, 0.85);
		font-family: inherit;
		font-size: 12px;
		text-align: left;
		padding: 5px 10px;
		cursor: pointer;
		border-radius: 3px;
		transition: background 0.1s;
	}
	.chart-frame-export-item:hover { background: rgba(255, 255, 255, 0.08); }

	.chart-frame-body {
		flex: 1;
		overflow: auto;
		min-height: 0;
		padding: 4px 6px 6px;
	}

	.chart-frame-expanded .chart-frame-body {
		padding: 12px 18px 18px;
		font-size: 13px;
	}

	/* Charts have fixed-aspect SVGs by default; scale them up when expanded */
	.chart-frame-expanded .chart-frame-body :global(svg) {
		width: 100%;
		height: auto;
		max-height: calc(100% - 20px);
	}
</style>
