// Type scale and ink for the panel charts, in real screen pixels.
// SVG charts draw on a fixed viewBox that the browser stretches to the panel
// (or to the expanded floating window), so a viewBox font-size would grow and
// shrink with the width. Charts multiply these sizes by `k = viewBoxW / renderedW`
// to keep text at the same pixel size everywhere.
export const CHART_PX = { tick: 11, label: 12 } as const;

// Minimum text contrast on the dark panel (#0f172a-ish): below ~0.5 white
// alpha the text fails legibility on a laptop screen.
export const INK = {
	tick: 'rgba(255,255,255,0.62)',
	label: 'rgba(255,255,255,0.82)',
	hint: 'rgba(255,255,255,0.5)',
} as const;

// viewBox units per screen pixel, from the box the SVG renders in. With
// preserveAspectRatio="meet" the tighter side wins (the expanded window caps the
// height). 1 while the chart is hidden (size 0).
export function svgScale(viewBoxW: number, viewBoxH: number, renderedW: number, renderedH = 0): number {
	if (renderedW <= 0) return 1;
	const px = renderedH > 0 ? Math.min(renderedW / viewBoxW, renderedH / viewBoxH) : renderedW / viewBoxW;
	return 1 / px;
}

// Axis titles can be long ("Edificios en zona baja (HAND < 5 m)"): trim to the
// pixels available with an ellipsis, keeping `suffix` whole at the end.
let measureCtx: CanvasRenderingContext2D | null = null;
export function fitLabel(text: string, maxPx: number, fontPx: number, family = 'system-ui, sans-serif', suffix = ''): string {
	if (typeof document === 'undefined' || maxPx <= 0) return text + suffix;
	measureCtx ??= document.createElement('canvas').getContext('2d');
	if (!measureCtx) return text + suffix;
	measureCtx.font = `${fontPx}px ${family}`;
	const fits = (t: string) => measureCtx!.measureText(t).width <= maxPx;
	if (fits(text + suffix)) return text + suffix;
	let lo = 0, hi = text.length;
	while (lo < hi) {
		const mid = (lo + hi + 1) >> 1;
		if (fits(text.slice(0, mid).trimEnd() + '…' + suffix)) lo = mid; else hi = mid - 1;
	}
	return text.slice(0, lo).trimEnd() + '…' + suffix;
}

// Charts docked beside the map (ChartFrame `bind:expanded`) get one step
// bigger text: they multiply their k by this.
// Svelte action: reports the box a chart renders in. Its own ResizeObserver plus
// a re-read when a chart docks/undocks: bind:clientWidth could keep the panel
// width after ChartFrame moves the node to <body> (text came out ×2.5).
export function measure(node: HTMLElement, onSize: (w: number, h: number) => void) {
	const read = () => onSize(node.clientWidth, node.clientHeight);
	const ro = new ResizeObserver(read);
	ro.observe(node);
	// The observer can miss the move into the docked panel: re-read after layout.
	let timer: ReturnType<typeof setTimeout> | undefined;
	const later = () => {
		requestAnimationFrame(read);
		clearTimeout(timer);
		timer = setTimeout(read, 250);
	};
	window.addEventListener('spatia:chart-dock', later);
	window.addEventListener('resize', later);
	read();
	return {
		destroy() {
			ro.disconnect();
			clearTimeout(timer);
			window.removeEventListener('spatia:chart-dock', later);
			window.removeEventListener('resize', later);
		},
	};
}

export const dockBoost = (docked: boolean): number => (docked ? 1.2 : 1);

// 3×3 bivariate palette (Joshua Stevens), index = class − 1 with
// class = 1 + yTercile·3 + xTercile: rows from low Y to high Y, X grows to the right.
export const BIVAR_COLORS = [
	'#e8e8e8', '#ace4e4', '#5ac8c8',
	'#dfb0d6', '#a5add3', '#5698b9',
	'#be64ac', '#8c62aa', '#3b4994',
] as const;

export function quantile(sorted: number[], q: number): number {
	if (sorted.length === 0) return NaN;
	const pos = (sorted.length - 1) * q;
	const lo = Math.floor(pos), hi = Math.ceil(pos);
	return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

export function median(values: number[]): number {
	if (values.length === 0) return NaN;
	const s = [...values].sort((a, b) => a - b);
	const m = s.length >> 1;
	return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
