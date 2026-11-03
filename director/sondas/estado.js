// Estado de la vista actual, calculado dentro de la página (window.__spatia, gancho de
// +page.svelte con localStorage spatia_sonda=1). Devuelve un resumen chico: las
// comparaciones pesadas (miles de hexágonos) se hacen acá y no viajan a Python.
(opts) => {
	const S = window.__spatia;
	if (!S) return { sinGancho: true };
	const h = S.hexStore, l = S.lensStore, m = S.map, reg = S.registros;
	const txt = (sel, root = document) => [...root.querySelectorAll(sel)].map(e => e.innerText.trim());
	const norm = (c) => {
		if (!c) return null;
		const d = document.createElement('div'); d.style.color = c; document.body.appendChild(d);
		const r = getComputedStyle(d).color; d.remove();
		const n = r.match(/[\d.]+/g)?.map(Number) ?? [];
		return n.length >= 3 ? n.slice(0, 3).map(Math.round).join(',') : null;
	};
	const layer = h.activeLayer;
	const pv = h.effectivePrimary;
	const v = layer?.variables?.find(x => x.col === pv || x.rawCol === pv) ?? null;

	// Leyenda tal como se ve
	const leg = document.querySelector('.legend');
	const leyenda = leg ? {
		titulo: leg.querySelector('.legend-title')?.innerText.trim() ?? '',
		sub: leg.querySelector('.legend-sub')?.innerText.trim() ?? '',
		items: [...leg.querySelectorAll('.legend-item')].map(it => ({
			color: norm(it.querySelector('.legend-swatch')?.style.background),
			label: it.querySelector('.legend-label')?.innerText.trim() ?? ''
		})),
		gradiente: leg.querySelector('.gradient-bar') ? (getComputedStyle(leg.querySelector('.gradient-bar')).backgroundImage.match(/rgba?\([^)]+\)/g) || []).map(norm) : null,
		extremos: txt('.gradient-labels span', leg),
		texto: leg.innerText.trim()
	} : null;

	// Hexágonos pintados (fuente GeoJSON, no hace falta que estén en pantalla)
	let hex = null;
	const src = m?.getSource('hexagons');
	const data = src?.serialize?.().data;
	const feats = data?.features ?? [];
	if (feats.length) {
		const claro = document.documentElement.dataset.theme !== 'oscuro';
		const key = claro ? 'color_c' : 'color';
		const vals = [], porColor = new Map();
		let nodata = 0, cero = 0, ceroComoNodata = 0, nodataColor = null;
		for (const f of feats) {
			const p = f.properties; const c = norm(p[key]);
			if (p.nodata === true || p.nodata === 'true') { nodata++; nodataColor = c; continue; }
			const val = p.value == null ? null : Number(p.value);
			if (val === 0) cero++;
			vals.push([val, c]);
			const e = porColor.get(c) ?? { n: 0, min: Infinity, max: -Infinity };
			e.n++; if (val != null) { e.min = Math.min(e.min, val); e.max = Math.max(e.max, val); }
			porColor.set(c, e);
		}
		if (nodataColor) for (const [val, c] of vals) if (val === 0 && c === nodataColor) ceroComoNodata++;
		// Monotonía valor → posición en la rampa de la leyenda (Spearman aproximado)
		let spearman = null;
		const g = leyenda?.gradiente?.filter(Boolean);
		if (g && g.length >= 2 && vals.length > 20) {
			const rgb = s => s.split(',').map(Number);
			const G = g.map(rgb);
			const pos = c => { const x = rgb(c); let best = 0, bd = Infinity;
				for (let i = 0; i < G.length - 1; i++) for (let k = 0; k <= 10; k++) {
					const t = k / 10, q = G[i].map((a, j) => a + (G[i + 1][j] - a) * t);
					const d = q.reduce((s, a, j) => s + (a - x[j]) ** 2, 0);
					if (d < bd) { bd = d; best = (i + t) / (G.length - 1); } }
				return [best, Math.sqrt(bd)]; };
			const muestra = vals.filter(([val]) => val != null).filter((_, i, a) => i % Math.max(1, Math.floor(a.length / 400)) === 0);
			const pares = muestra.map(([val, c]) => { const [p, d] = pos(c); return [val, p, d]; });
			const rank = arr => { const idx = arr.map((x, i) => [x, i]).sort((a, b) => a[0] - b[0]); const r = new Array(arr.length); idx.forEach(([, i], k) => r[i] = k); return r; };
			const rv = rank(pares.map(p => p[0])), rp = rank(pares.map(p => p[1]));
			const n = pares.length, mean = (n - 1) / 2;
			let num = 0, dv = 0, dp = 0;
			for (let i = 0; i < n; i++) { num += (rv[i] - mean) * (rp[i] - mean); dv += (rv[i] - mean) ** 2; dp += (rp[i] - mean) ** 2; }
			spearman = { rho: dv && dp ? num / Math.sqrt(dv * dp) : null, n, fueraDeRampa: pares.filter(p => p[2] > 40).length };
		}
		hex = { n: feats.length, nodata, cero, ceroComoNodata, nodataColor, colores: [...porColor.entries()].map(([c, e]) => ({ c, ...e })).slice(0, 30), spearman,
			min: Math.min(...vals.map(x => x[0]).filter(x => x != null)), max: Math.max(...vals.map(x => x[0]).filter(x => x != null)) };
	}

	// Muestra de hexágonos en pantalla, para pasar el mouse (tooltip)
	let enPantalla = [];
	if (opts?.muestraHover && m) {
		const r = m.getCanvas().getBoundingClientRect();
		const rf = m.queryRenderedFeatures({ layers: ['hex-fill'] });
		const paso = Math.max(1, Math.floor(rf.length / opts.muestraHover));
		for (let i = 0; i < rf.length && enPantalla.length < opts.muestraHover; i += paso) {
			const f = rf[i]; const ring = f.geometry.type === 'Polygon' ? f.geometry.coordinates[0] : f.geometry.coordinates?.[0]?.[0];
			if (!ring) continue;
			const ctr = ring.slice(0, -1).reduce((a, b, _, A) => [a[0] + b[0] / A.length, a[1] + b[1] / A.length], [0, 0]);
			const pt = m.project(ctr);
			if (pt.x < 20 || pt.y < 20 || pt.x > r.width - 20 || pt.y > r.height - 20) continue;
			enPantalla.push({ x: pt.x + r.left, y: pt.y + r.top, value: f.properties.value, nodata: f.properties.nodata, type_label: f.properties.type_label });
		}
	}

	const panel = document.querySelector('.dept-panel, aside, .sidebar');
	return {
		capa: layer?.id ?? null, analisis: l.activeAnalysis?.id ?? null, lente: l.activeLens ?? null,
		territorio: h.territoryId, cargando: h.loading, errorCarga: h.loadError,
		primaria: pv, colorVar: h.colorVariable, escalaColor: layer?.colorScale, modoTemporal: h.temporalMode,
		variable: v ? { col: v.col, unit: v.unit ?? null, rawUnit: v.rawUnit ?? null, scale: v.scale ?? null, label: S.i18n.t(v.labelKey) } : null,
		dominio: h.colorDomain, clases: h.effectiveColorClasses ? {
			zero: h.effectiveColorClasses.zero ? S.i18n.t(h.effectiveColorClasses.zero.labelKey) : null,
			steps: h.effectiveColorClasses.steps.map(s => ({ from: s.from, label: S.i18n.t(s.labelKey) })) } : null,
		deptSel: h.selectedDpto, deptFill: h.deptFill ? { titulo: h.deptFill.title, unidad: h.deptFill.unit, max: h.deptFill.max,
			decimales: h.deptFill.decimals, valores: h.deptFill.values.slice(0, 80) } : null,
		pestanas: txt('.rank-tab'), pestanaActiva: txt('.rank-tab.active')[0] ?? null,
		ranking: [...document.querySelectorAll('.rank-row')].slice(0, 80).map(r => ({
			nombre: r.querySelector('.rank-name')?.innerText.trim(), valor: r.querySelector('.rank-value')?.innerText.trim() })),
		notaRanking: txt('.rank-note').join(' '),
		graficos: [...document.querySelectorAll('.chart-frame')].map(f => ({
			titulo: f.querySelector('.chart-frame-title')?.innerText.replace('▸', '').replace('▾', '').trim(),
			abierto: !!f.querySelector('svg, canvas'), marcas: f.querySelectorAll('svg rect, svg path, svg circle').length,
			texto: f.innerText.slice(0, 1500) })),
		leyenda, hex, enPantalla,
		toasts: txt('[class*=toast], [role=alert]'),
		textoPanel: (panel?.innerText ?? '').slice(0, 20000),
		url: location.href
	};
}
