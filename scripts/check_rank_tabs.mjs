// Ranking tabs that would paint the same map (5-nov-2026: in «actividad económica»,
// Ocupados, Tasa de empleo and Desocupación painted the employment rate in an open
// department). For every layer with deptRank.metrics, each tab resolves to what the
// map paints with a department open:
//   - hexNone      → the departments (deptFill), no cells: always distinct;
//   - hexVariable  → that column, which must be one of the layer's variables (else
//                    it is never loaded and the map silently falls back to the primary);
//   - neither      → the layer's primaryVariable;
//   - temporalMode → that column in the tab's temporal mode (its _baseline/_delta).
// Two tabs of one layer resolving to the same column (and the same macizo focus) fail.
//
//   node scripts/check_rank_tabs.mjs        (npm run check:tabs)
import { build } from 'esbuild';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const out = await build({
	stdin: { contents: "export { HEX_LAYER_REGISTRY } from '$lib/config';", resolveDir: root, loader: 'ts' },
	bundle: true, platform: 'node', format: 'esm', write: false, logLevel: 'error',
	alias: { $lib: path.join(root, 'src/lib') },
	loader: { '.json': 'json' },
	define: { 'import.meta.env.DEV': 'false' },
	// config.ts imports the i18n store, which uses Svelte runes at module level.
	banner: { js: 'globalThis.$state ??= (v) => v; globalThis.$derived ??= (v) => v; globalThis.$effect ??= () => {};' },
});
const { HEX_LAYER_REGISTRY } = await import('data:text/javascript;base64,' + Buffer.from(out.outputFiles[0].text).toString('base64'));

const errors = [];
for (const [id, layer] of Object.entries(HEX_LAYER_REGISTRY)) {
	const base = layer.deptRank;
	if (!base?.metrics?.length) continue;
	const cols = new Set((layer.variables ?? []).flatMap((v) => [v.col, v.rawCol].filter(Boolean)));
	const seen = new Map();
	for (const m of base.metrics) {
		const c = { ...base, ...m };
		if (c.hexNone) continue;
		if (c.hexVariable && !layer.macizos && !cols.has(c.hexVariable)) {
			errors.push(`${id} · ${m.tabKey}: hexVariable '${c.hexVariable}' no está en variables (no se carga; el mapa pinta la primaria)`);
		}
		// temporalMode (deforestation, Aceleración): same column, painted as its change.
		const paints = (c.hexVariable ?? layer.primaryVariable) + ' ' + JSON.stringify(c.macizoFocus ?? null) + ' ' + (c.temporalMode ?? 'current');
		if (seen.has(paints)) {
			errors.push(`${id}: ${seen.get(paints)} y ${m.tabKey} pintan lo mismo por hexágono (${paints.split(' ')[0]}); marcar hexNone o darle su hexVariable`);
		} else seen.set(paints, m.tabKey);
	}
}
if (errors.length) {
	console.error(errors.join('\n'));
	process.exit(1);
}
console.log('check_rank_tabs: cada pestaña del ranking pinta su propio mapa.');
