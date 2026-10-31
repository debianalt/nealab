// Hexoma — export of what the web app reads, so the local `hexoma` database loads
// exactly the files and columns the hexagon fact sheet (src/lib/utils/ficha.ts) and
// the layer panels read. Runs the real TypeScript through Vite SSR (config.ts,
// deptSummaries.ts, units.ts, i18n), so URLs carry the same ?v cache-busters and
// per-department parquetKeys as production; nothing is re-typed by hand.
//
// Usage (from neahub/):  node pipeline/hexoma_registry.mjs <out.json>
// Called by pipeline/build_hexoma.py.
import { createServer } from 'vite';
import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

const out = process.argv[2] ?? 'pipeline/output/hexoma/registry.json';

// Same exclusions as the hex panel and the ficha (HIDDEN in ficha.ts).
const HIDDEN = new Set(['score', 'type', 'type_label', 'pca_1', 'pca_2']);

const server = await createServer({ server: { middlewareMode: true, hmr: false }, appType: 'custom', logLevel: 'error' });
try {
	const cfg = await server.ssrLoadModule('/src/lib/config.ts');
	const ds = await server.ssrLoadModule('/src/lib/utils/deptSummaries.ts');
	const units = await server.ssrLoadModule('/src/lib/utils/units.ts');
	const { i18n } = await server.ssrLoadModule('/src/lib/stores/i18n.svelte.ts');
	i18n.setLocale('es');

	// Per-department URL dispatch: mirrors hexStore.deptUrlFor (src/lib/stores/hex.svelte.ts).
	const deptUrlFor = (layer, key, prefix) => {
		if (layer.id === 'flood_risk') return cfg.getFloodDptoUrl(key, prefix);
		if (layer.parquet?.startsWith('sat_')) return cfg.getSatDptoUrl(layer.id, key, prefix);
		return cfg.getScoresDptoUrl(key, prefix);
	};

	const layers = [];
	for (const a of cfg.ANALYSIS_REGISTRY) {
		const l = cfg.HEX_LAYER_REGISTRY[a.id];
		if (!l) continue;
		const pv = l.primaryVariable;
		layers.push({
			id: a.id,
			title: i18n.t(a.titleKey),
			lens: a.lensId,
			comparable: a.comparable === true,
			rigor: a.rigorBadge ?? null,
			perDepartment: !!l.perDepartment,
			temporal: !!l.temporal,
			parquet: l.parquet ?? `sat_${a.id}`,
			primary: pv,
			coverage: a.coverage ?? {},
			// displaySpec in ficha.ts: shown column = rawCol ?? col, × scale, unit or '/100'.
			variables: l.variables.map((v) => ({
				col: v.col,
				rawCol: v.rawCol ?? null,
				scale: v.scale ?? 1,
				unit: v.unit ? units.unitLabel(v.unit) : '/100',
				label: i18n.t(v.labelKey),
				inFicha: !HIDDEN.has(v.col) || v.col === pv || v.rawCol === pv,
			})),
		});
	}

	const territories = {};
	const loads = {};
	for (const t of Object.values(cfg.TERRITORY_REGISTRY)) {
		territories[t.id] = { label: t.label, country: t.country, prefix: t.parquetPrefix };
		loads[t.id] = {};
		for (const layer of layers) {
			if (layer.coverage?.[t.id] === 'unavailable') continue;
			const l = cfg.HEX_LAYER_REGISTRY[layer.id];
			if (l.perDepartment) {
				const s = await ds.loadDeptSummary(layer.id, t.parquetPrefix);
				if (!s?.departments) continue;
				const seen = new Set();
				const files = [];
				for (const d of s.departments) {
					const name = d.dpto ?? d.distrito ?? d.municipio ?? null;
					if (!d.parquetKey || seen.has(d.parquetKey)) continue; // name variants share a key
					seen.add(d.parquetKey);
					files.push({ url: deptUrlFor(l, d.parquetKey, t.parquetPrefix), parquetKey: d.parquetKey, dept: name });
				}
				loads[t.id][layer.id] = { scope: 'dept', files };
			} else {
				loads[t.id][layer.id] = { scope: 'territory', files: [{ url: cfg.getSatGlobalUrl(layer.id, t.parquetPrefix), parquetKey: null, dept: null }] };
			}
		}
	}

	// Which department file the fact sheet reads for a cell: openFichaFromLink
	// (src/routes/+page.svelte) takes the AR department whose polygon in
	// ar_dept_boundaries.json contains the cell centroid, then layerDeptUrl maps
	// its name to each layer's parquetKey (normDeptName match in the summary,
	// else normDeptName(nombre)). Border cells sit in two department files with
	// different values in some layers, so the Hexoma needs this to pick the
	// row the fact sheet shows. codprov → territory as AR_CODPROV in +page.svelte.
	const { normDeptName } = await server.ssrLoadModule('/src/lib/utils/deptBoundaries.ts');
	const AR_CODPROV = { '54': 'misiones', '18': 'corrientes', '22': 'chaco', '34': 'formosa' };
	const { readFileSync } = await import('node:fs');
	const gj = JSON.parse(readFileSync('src/lib/data/ar_dept_boundaries.json', 'utf8'));
	const fichaKeys = {}; // redcode → { territory, nombre, keys: { layerId: parquetKey } }
	for (const f of gj.features) {
		const redcode = String(f.properties.redcode);
		const tid = AR_CODPROV[redcode.slice(0, 2)];
		if (!tid) continue;
		const nombre = f.properties.nombre;
		const keys = {};
		for (const layer of layers) {
			if (!cfg.HEX_LAYER_REGISTRY[layer.id].perDepartment) continue;
			const s = await ds.loadDeptSummary(layer.id, territories[tid].prefix);
			const want = normDeptName(nombre);
			const d = s?.departments?.find((x) => normDeptName(x.dpto ?? x.distrito ?? x.municipio) === want);
			keys[layer.id] = d?.parquetKey ?? normDeptName(nombre);
		}
		fichaKeys[redcode] = { territory: tid, nombre, keys };
	}

	mkdirSync(dirname(out), { recursive: true });
	writeFileSync(out, JSON.stringify({ generated: new Date().toISOString(), layers, territories, loads, fichaKeys,
		boundaries: { ar: 'src/lib/data/ar_dept_boundaries.json' } }, null, 1));
	console.log(`hexoma registry: ${layers.length} layers, ${Object.keys(territories).length} territories → ${out}`);
} finally {
	await server.close();
}
