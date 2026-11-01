"""Hexoma A1 — ficha local de una celda H3: suelo, relieve y series agroclimáticas.

Arma un HTML autocontenido (sin bibliotecas externas) por celda en pipeline/output/hexoma/fichas/<h3>.html,
leído directo de la base `hexoma` (esquemas clima y suelo). Cada serie se dibuja con su normal
1991–2020 (línea punteada) y la banda P10–P90 del mismo píxel, y debajo va la tabla de anomalías de
las últimas campañas. Las variables que la validación contra estaciones marcó como sesgadas llevan
el aviso con la cifra medida (clima.validacion).

Uso (desde neahub/):
    python pipeline/hexoma/hexoma_ficha.py 89a946191b3ffff [89a95099b8fffff …]
"""
from __future__ import annotations

import html
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SALIDA = ROOT / 'pipeline' / 'output' / 'hexoma' / 'fichas'

# (tabla, variable, título, unidad, forma, aviso)
SERIES = [
    ('chirps_campania', 'p_mm', 'Lluvia por campaña', 'mm', 'linea', None),
    ('balance_campania', 'p_menos_et0', 'Balance hídrico: lluvia − ET0', 'mm', 'signo', None),
    ('chirps_campania', 'racha_seca_max_oct_mar', 'Racha seca más larga, octubre–marzo', 'días', 'linea',
     'CHIRPS diario es una desagregación: da ~36 días de lluvia por campaña más que los pluviómetros; las rachas son orientativas.'),
    ('era5_campania', 'gdd10', 'Grados-día base 10 °C', '°C·d', 'linea', None),
    ('era5_campania', 'et0_mm', 'Evapotranspiración de referencia (FAO-56)', 'mm', 'linea', None),
    ('era5_campania', 'tmax_media', 'Media de las máximas', '°C', 'linea',
     'ERA5-Land da máximas ~2,1 °C más bajas que las estaciones del NEA (mediana de 48).'),
    ('era5_campania', 'tmin_media', 'Media de las mínimas', '°C', 'linea',
     'ERA5-Land da mínimas ~1,5 °C más altas que las estaciones del NEA (mediana de 48).'),
    ('era5_campania', 'dias_tmax35', 'Días con máxima > 35 °C', 'días', 'barra',
     'Subestimado: las máximas de ERA5-Land vienen ~2,1 °C bajas.'),
    ('era5_campania', 'sw2', 'Humedad del suelo, 7–28 cm', 'm³/m³', 'linea',
     'Modelo de ERA5-Land con textura genérica: sirve para la variación entre años, no como valor absoluto.'),
    ('era5_helada', 'dias_tmin_le3', 'Días con mínima ≤ 3 °C (helada agrometeorológica), por año', 'días', 'barra',
     'Muy subestimado: ERA5-Land marca ~9 días por año menos que las estaciones (−78 %).'),
    ('era5_helada', 'dias_tmin_lt0', 'Días con mínima < 0 °C, por año', 'días', 'barra',
     'Casi nulo en ERA5-Land: las estaciones registran 0,3–6 por año en abrigo y 8–24 a 5 cm del suelo; el modelo ~0. No usar como conteo de heladas.'),
]


def datos(cur, h3: str) -> dict:
    cur.execute("""SELECT h.h3::text, t.etiqueta, l.nombre,
                          ST_Y(h3_cell_to_geometry(h.h3)), ST_X(h3_cell_to_geometry(h.h3)),
                          p.chirps_pixel, p.era5_pixel, p.par_id, e.z_m
                   FROM hexagonos h JOIN territorios t USING (territorio_id)
                   JOIN clima.hex_pixel p ON p.h3 = h.h3
                   LEFT JOIN clima.pixel e ON e.fuente = 'era5' AND e.pixel_id = p.era5_pixel
                   LEFT JOIN limites l ON l.territorio_id = h.territorio_id
                        AND ST_Contains(l.geom, h3_cell_to_geometry(h.h3))
                   WHERE h.h3 = %s::h3index LIMIT 1""", (h3,))
    r = cur.fetchone()
    if not r:
        raise SystemExit(f'{h3}: no está en el Hexoma')
    d = dict(zip(['h3', 'territorio', 'departamento', 'lat', 'lng', 'chirps', 'era5', 'par', 'z_era5'], r))
    cur.execute('SELECT * FROM suelo.h3 WHERE h3 = %s::h3index', (h3,))
    cols = [c.name for c in cur.description]
    d['suelo'] = dict(zip(cols, cur.fetchone() or [None] * len(cols)))
    d['suelo'].pop('h3', None)
    unidad = {'chirps_campania': d['chirps'], 'era5_campania': d['era5'], 'era5_helada': d['era5'],
              'balance_campania': d['par']}
    d['series'] = []
    for tabla, var, titulo, u, forma, aviso in SERIES:
        per = 'anio' if tabla == 'era5_helada' else 'campania'
        cur.execute(f'SELECT {per}, {var} FROM clima.{tabla} WHERE {"par_id" if tabla.startswith("balance") else "pixel_id"} = %s ORDER BY 1',
                    (unidad[tabla],))
        pts = [[int(a), None if b is None else float(b)] for a, b in cur.fetchall()]
        cur.execute("""SELECT media, p10, p90 FROM clima.normal_1991_2020
                       WHERE tabla = %s AND unidad_id = %s AND variable = %s AND estacion IS NULL""",
                    (tabla, unidad[tabla], var))
        n = cur.fetchone()
        d['series'].append({'tabla': tabla, 'var': var, 'titulo': titulo, 'unidad': u, 'forma': forma, 'nd': ND.get(var, 0),
                            'aviso': aviso, 'periodo': per, 'puntos': pts,
                            'normal': None if not n else {'media': n[0], 'p10': n[1], 'p90': n[2]}})
    ult = 2025
    cur.execute("""SELECT tabla, variable, periodo, valor, normal, anom, anom_pct, pct_ref
                   FROM clima.anomalia WHERE ((tabla = 'chirps_campania' AND unidad_id = %s)
                      OR (tabla IN ('era5_campania', 'era5_helada') AND unidad_id = %s)
                      OR (tabla = 'balance_campania' AND unidad_id = %s))
                     AND periodo BETWEEN %s AND %s""", (d['chirps'], d['era5'], d['par'], ult - 4, ult))
    d['anomalias'] = cur.fetchall()
    return d


# decimales por variable (por defecto 0: mm, días, °C·d)
ND = {'tmax_media': 1, 'tmin_media': 1, 'sw2': 3}

SUELO_ROT = {
    'arcilla_pct': ('Arcilla', '%'), 'arena_pct': ('Arena', '%'), 'limo_pct': ('Limo', '%'),
    'cos_g_kg': ('Carbono orgánico', 'g/kg'), 'ph_h2o': ('pH en agua', ''), 'cic_cmol_kg': ('CIC', 'cmol(+)/kg'),
    'dap_g_cm3': ('Densidad aparente', 'g/cm³'), 'fragmentos_pct': ('Fragmentos gruesos', '% vol.'),
    'n_g_kg': ('Nitrógeno total', 'g/kg'), 'pendiente_media_grados': ('Pendiente media', '°'),
    'elev_media_m': ('Elevación media', 'm'), 'elev_min_m': ('Elevación mínima', 'm'),
    'elev_max_m': ('Elevación máxima', 'm'), 'n_px_relieve': ('Píxeles de relieve en la celda', ''),
}


def num(x, nd=1) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return '—'
    s = f'{x:,.{nd}f}'
    return s.replace(',', ' ').replace('.', ',')


def pagina(d: dict) -> str:
    e = html.escape
    filas_suelo = ''.join(
        f'<tr><td>{e(SUELO_ROT[k][0])}</td><td class="n">{num(v, 0 if k.startswith(("elev", "n_px")) else (2 if k in ("dap_g_cm3", "ph_h2o", "n_g_kg") else 1))}</td>'
        f'<td class="u">{e(SUELO_ROT[k][1])}</td></tr>'
        for k, v in d['suelo'].items() if k in SUELO_ROT)
    titulos = {(s['tabla'], s['var']): (s['titulo'], s['unidad']) for s in d['series']}
    an = sorted(d['anomalias'], key=lambda r: (list(titulos).index((r[0], r[1])) if (r[0], r[1]) in titulos else 99, r[2]))
    filas_an = ''
    for tabla, var, per, val, nor, anom, pct, ref in an:
        if (tabla, var) not in titulos:
            continue
        t, u = titulos[(tabla, var)]
        etq = str(per) if tabla == 'era5_helada' else f'{per}/{str(per + 1)[2:]}'
        nd = ND.get(var, 0)
        filas_an += (f'<tr><td>{e(t)}</td><td>{etq}</td><td class="n">{num(val, nd)}</td><td class="n">{num(nor, nd)}</td>'
                     f'<td class="n">{num(anom, nd)}</td><td class="n">{num(pct, 0)}</td><td class="n">{num(ref, 0)}</td>'
                     f'<td class="u">{e(u)}</td></tr>')
    datos_js = json.dumps(d['series'], ensure_ascii=False)
    return f"""<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hexoma {e(d['h3'])}</title>
<style>
:root {{ color-scheme: light; --surface:#fcfcfb; --surface-2:#f2f1ee; --text:#0b0b0b; --text-2:#52514e; --muted:#8a8984;
  --grid:#e4e3df; --series:#2a78d6; --neg:#eb6834; --band:rgba(82,81,78,.12); --normal:#52514e; --warn-bg:#fdf3e1; --warn:#7a4b00; }}
@media (prefers-color-scheme: dark) {{ :root:where(:not([data-theme="light"])) {{ color-scheme: dark; --surface:#1a1a19; --surface-2:#242422;
  --text:#f0efec; --text-2:#c3c2b7; --muted:#8f8e86; --grid:#33322f; --series:#3987e5; --neg:#d95926; --band:rgba(195,194,183,.14);
  --normal:#c3c2b7; --warn-bg:#3a2c12; --warn:#f3c77a; }} }}
:root[data-theme="dark"] {{ color-scheme: dark; --surface:#1a1a19; --surface-2:#242422; --text:#f0efec; --text-2:#c3c2b7; --muted:#8f8e86;
  --grid:#33322f; --series:#3987e5; --neg:#d95926; --band:rgba(195,194,183,.14); --normal:#c3c2b7; --warn-bg:#3a2c12; --warn:#f3c77a; }}
* {{ box-sizing: border-box; }}
body {{ margin:0; background:var(--surface); color:var(--text); font:15px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }}
main {{ max-width:1100px; margin:0 auto; padding:24px 16px 48px; }}
h1 {{ font-size:22px; margin:0 0 4px; font-family: ui-monospace, "Cascadia Mono", monospace; }}
h2 {{ font-size:17px; margin:32px 0 8px; }}
.sub {{ color:var(--text-2); margin:0 0 4px; }}
.nota {{ color:var(--text-2); font-size:13px; }}
table {{ border-collapse:collapse; width:100%; font-size:14px; }}
td, th {{ padding:4px 8px; border-bottom:1px solid var(--grid); text-align:left; }}
th {{ color:var(--text-2); font-weight:600; }}
td.n, th.n {{ text-align:right; font-variant-numeric: tabular-nums; }}
td.u {{ color:var(--muted); }}
.grid {{ display:grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap:16px; }}
.carta {{ background:var(--surface-2); border-radius:8px; padding:12px; position:relative; }}
.carta h3 {{ font-size:14px; margin:0 0 2px; }}
.carta .u {{ color:var(--muted); font-size:12px; margin:0 0 6px; }}
.aviso {{ background:var(--warn-bg); color:var(--warn); font-size:12px; padding:6px 8px; border-radius:6px; margin-top:6px; }}
svg {{ display:block; width:100%; height:auto; overflow:visible; }}
svg text {{ fill:var(--muted); font-size:10px; }}
.tip {{ position:absolute; pointer-events:none; background:var(--surface); color:var(--text); border:1px solid var(--grid);
  border-radius:6px; padding:6px 8px; font-size:12px; box-shadow:0 2px 8px rgba(0,0,0,.15); display:none; white-space:nowrap; z-index:2; }}
.ley {{ display:flex; gap:14px; flex-wrap:wrap; font-size:12px; color:var(--text-2); margin:4px 0 12px; }}
.ley span::before {{ content:""; display:inline-block; width:14px; height:8px; margin-right:5px; vertical-align:middle; border-radius:2px; }}
.ley .v::before {{ background:var(--series); height:2px; }} .ley .nm::before {{ border-top:2px dashed var(--normal); height:0; }}
.ley .b::before {{ background:var(--band); }}
.tw {{ overflow-x:auto; }}
</style></head><body><main>
<h1>{e(d['h3'])}</h1>
<p class="sub">{e(d['territorio'] or '')}{' · ' + e(d['departamento']) if d['departamento'] else ''} · {num(d['lat'], 4)}, {num(d['lng'], 4)}</p>
<p class="nota">Píxel CHIRPS {d['chirps']} (0,05°, ~70 celdas) · píxel ERA5-Land {d['era5']} (0,1°, altitud del modelo {num(d['z_era5'], 0)} m) ·
cada valor es el del píxel que contiene el centro de la celda, sin interpolar. Ficha del Hexoma (base local, pipeline/hexoma/hexoma_ficha.py).</p>

<h2>Suelo y relieve</h2>
<p class="nota">Suelo: SoilGrids 2.0, 0–30 cm, píxel de 250 m. <b>Es un modelo</b> (predicción con aprendizaje automático), no una medición en esta celda.
Relieve: FABDEM (sin dosel ni edificios), media de los píxeles de ~90 m dentro de la celda.</p>
<div class="tw"><table><tbody>{filas_suelo}</tbody></table></div>

<h2>Series por campaña (julio–junio) y por año</h2>
<div class="ley"><span class="v">valor de la campaña</span><span class="nm">normal 1991–2020 de este píxel</span><span class="b">P10–P90 de 1991–2020</span></div>
<div class="grid" id="series"></div>

<h2>Últimas campañas frente a su normal</h2>
<p class="nota">Anomalía = valor − normal 1991–2020 del mismo píxel. «% bajo» = de los 30 años de referencia, cuántos tuvieron un valor menor.
No es un goalpost: la celda se compara con su propia historia.</p>
<div class="tw"><table><thead><tr><th>Variable</th><th>Período</th><th class="n">Valor</th><th class="n">Normal</th>
<th class="n">Anomalía</th><th class="n">% de la normal</th><th class="n">% bajo</th><th>Unidad</th></tr></thead><tbody>{filas_an}</tbody></table></div>
<p class="nota">Fuentes: CHIRPS v3 (estimación satelital calibrada con pluviómetros, desde 1981/82); ERA5-Land (reanálisis, desde 1950/51; antes de 1979 asimila menos observaciones).
Validación contra 48 estaciones (GHCN-Daily e INTA SIGA): pipeline/hexoma/validacion_clima.md.</p>
</main>
<script>
const SERIES = {datos_js};
const fmt = (v, nd) => v == null ? '—' : v.toLocaleString('es-AR', {{maximumFractionDigits: nd, minimumFractionDigits: nd}});
const etq = (s, x) => s.periodo === 'anio' ? String(x) : `${{x}}/${{String(x + 1).slice(2)}}`;
const W = 340, H = 170, M = {{l: 40, r: 8, t: 8, b: 20}};
function carta(s) {{
  const div = document.createElement('div'); div.className = 'carta';
  const pts = s.puntos.filter(p => p[1] != null);
  const nd = s.nd;
  div.innerHTML = `<h3>${{s.titulo}}</h3><p class="u">${{s.unidad}} · ${{etq(s, pts[0][0])}} a ${{etq(s, pts[pts.length - 1][0])}}</p>`;
  const xs = pts.map(p => p[0]), ys = pts.map(p => p[1]);
  const n = s.normal;
  let lo = Math.min(...ys, n ? n.p10 : Infinity), hi = Math.max(...ys, n ? n.p90 : -Infinity);
  if (s.forma !== 'linea') {{ lo = Math.min(0, lo); hi = Math.max(0, hi); }}
  if (hi === lo) hi = lo + 1;
  const pad = (hi - lo) * 0.06; if (s.forma === 'linea') lo -= pad; hi += pad;
  const x0 = Math.min(...xs) - 0.5, x1 = Math.max(...xs) + 0.5;
  const X = x => M.l + (x - x0) / (x1 - x0) * (W - M.l - M.r);
  const Y = y => M.t + (hi - y) / (hi - lo) * (H - M.t - M.b);
  const ns = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(ns, 'svg'); svg.setAttribute('viewBox', `0 0 ${{W}} ${{H}}`);
  svg.setAttribute('role', 'img'); svg.setAttribute('aria-label', s.titulo);
  let g = '';
  const ticks = 4;
  for (let i = 0; i <= ticks; i++) {{
    const v = lo + (hi - lo) * i / ticks;
    g += `<line x1="${{M.l}}" x2="${{W - M.r}}" y1="${{Y(v)}}" y2="${{Y(v)}}" stroke="var(--grid)" stroke-width="1"/>`;
    g += `<text x="${{M.l - 4}}" y="${{Y(v) + 3}}" text-anchor="end">${{fmt(v, (hi - lo) < 1 ? 2 : (hi - lo) < 20 ? 1 : 0)}}</text>`;
  }}
  const dec = xs.filter(x => x % 10 === 0);
  dec.forEach(x => g += `<text x="${{X(x)}}" y="${{H - 4}}" text-anchor="middle">${{x}}</text>`);
  if (n) {{
    g += `<rect x="${{X(1990.5)}}" width="${{X(2020.5) - X(1990.5)}}" y="${{Y(n.p90)}}" height="${{Math.max(1, Y(n.p10) - Y(n.p90))}}" fill="var(--band)"/>`;
    g += `<line x1="${{M.l}}" x2="${{W - M.r}}" y1="${{Y(n.media)}}" y2="${{Y(n.media)}}" stroke="var(--normal)" stroke-width="1.5" stroke-dasharray="4 3"/>`;
  }}
  const bw = Math.max(1, (W - M.l - M.r) / (x1 - x0) - 2);
  if (s.forma === 'linea') {{
    g += `<path d="${{pts.map((p, i) => (i ? 'L' : 'M') + X(p[0]) + ' ' + Y(p[1])).join('')}}" fill="none" stroke="var(--series)" stroke-width="2" stroke-linejoin="round"/>`;
  }} else {{
    pts.forEach(p => {{
      const y0 = Y(0), y1 = Y(p[1]); const c = (s.forma === 'signo' && p[1] < 0) ? 'var(--neg)' : 'var(--series)';
      g += `<rect x="${{X(p[0]) - bw / 2}}" width="${{bw}}" y="${{Math.min(y0, y1)}}" height="${{Math.max(Math.abs(y1 - y0), p[1] ? 1 : 0)}}" rx="1" fill="${{c}}"/>`;
    }});
    g += `<line x1="${{M.l}}" x2="${{W - M.r}}" y1="${{Y(0)}}" y2="${{Y(0)}}" stroke="var(--text-2)" stroke-width="1"/>`;
  }}
  g += `<line class="cruz" x1="0" x2="0" y1="${{M.t}}" y2="${{H - M.b}}" stroke="var(--text-2)" stroke-width="1" visibility="hidden"/>`;
  svg.innerHTML = g;
  div.appendChild(svg);
  const tip = document.createElement('div'); tip.className = 'tip'; div.appendChild(tip);
  svg.addEventListener('mousemove', ev => {{
    const r = svg.getBoundingClientRect(); const sx = (ev.clientX - r.left) / r.width * W;
    let best = pts[0]; pts.forEach(p => {{ if (Math.abs(X(p[0]) - sx) < Math.abs(X(best[0]) - sx)) best = p; }});
    const cr = svg.querySelector('.cruz'); cr.setAttribute('x1', X(best[0])); cr.setAttribute('x2', X(best[0])); cr.setAttribute('visibility', 'visible');
    const an = n ? best[1] - n.media : null;
    tip.innerHTML = `<b>${{etq(s, best[0])}}</b>: ${{fmt(best[1], nd)}} ${{s.unidad}}` +
      (n ? `<br>normal ${{fmt(n.media, nd)}} · anomalía ${{an >= 0 ? '+' : ''}}${{fmt(an, nd)}}` : '');
    tip.style.display = 'block';
    const px = X(best[0]) / W * r.width;
    tip.style.left = Math.min(px + 14, r.width - tip.offsetWidth) + 'px'; tip.style.top = '40px';
  }});
  svg.addEventListener('mouseleave', () => {{ tip.style.display = 'none'; svg.querySelector('.cruz').setAttribute('visibility', 'hidden'); }});
  if (s.aviso) {{ const a = document.createElement('div'); a.className = 'aviso'; a.textContent = s.aviso; div.appendChild(a); }}
  return div;
}}
const cont = document.getElementById('series');
SERIES.filter(s => s.puntos.length).forEach(s => cont.appendChild(carta(s)));
</script></body></html>"""


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    SALIDA.mkdir(parents=True, exist_ok=True)
    conn = connect(DB)
    with conn.cursor() as cur:
        for h3 in sys.argv[1:]:
            d = datos(cur, h3)
            f = SALIDA / f'{h3}.html'
            f.write_text(pagina(d), encoding='utf-8')
            print(f)
    return 0


if __name__ == '__main__':
    sys.exit(main())
