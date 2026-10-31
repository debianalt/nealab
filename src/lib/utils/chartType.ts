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

export function median(values: number[]): number {
	if (values.length === 0) return NaN;
	const s = [...values].sort((a, b) => a - b);
	const m = s.length >> 1;
	return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
