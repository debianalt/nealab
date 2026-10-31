"""Hexoma H2 — distribution of every fact-sheet variable per scope, and each hexagon's
position within it. Runs on the base that build_hexoma.py loaded (which calls this at
the end of a full load); nothing is read from R2 here.

Scopes:
  departamento  rows of the department file (depto_id), border cells included, exactly
                the set the fact sheet ranks against (src/lib/utils/ficha.ts)
  provincia     the territory (AR: province; PY: departamento; BR: estado), principal rows
  region        only layers with comparable: true. Physical/modelled layers pool every
                loaded territory; census layers pool only their own country (INDEC,
                DGEEC and IBGE are never mixed). A cell present in two territories counts
                once, in the territory the ?h3= link opens (ficha_enlace, else the polygon
                holding its centre, else the lowest territorio_id); its other row has NULL
                region rank.

Values in fact-sheet units: (rawCol ?? col) × scale, NULL excluded. Quantiles with
percentile_cont = quantile() in src/lib/utils/chartType.ts (linear on (n-1)·q).
Position = rankOf() in ficha.ts: lt = % of cells strictly lower, eq = % equal, both over
n (not n-1), stored as smallint hundredths of a point (0-10000).

Usage (from neahub/):
    python pipeline/build_hexoma_distribuciones.py [--layers a,b] [--workers 4]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from psycopg2 import sql
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, REGISTRY, connect, log, table_columns  # noqa: E402

SCOPES = ('dep', 'prov', 'reg')
SCOPE_NAME = {'dep': 'departamento', 'prov': 'provincia', 'reg': 'region'}
QS = (0.1, 0.25, 0.5, 0.75, 0.9)

SCHEMA = """
CREATE TABLE IF NOT EXISTS distribuciones (
    capa          text NOT NULL,
    variable      text NOT NULL,
    columna       text NOT NULL,
    unidad        text,
    ambito        text NOT NULL CHECK (ambito IN ('departamento', 'provincia', 'region')),
    ambito_id     text NOT NULL,
    ambito_nombre text,
    n             integer NOT NULL,
    p10 double precision, p25 double precision, p50 double precision,
    p75 double precision, p90 double precision,
    media double precision, min double precision, max double precision,
    calculado_en  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (capa, variable, ambito, ambito_id)
);
COMMENT ON TABLE distribuciones IS 'Distribución de cada variable de la ficha por ámbito, en las unidades de la ficha ((rawCol ?? col) × scale; sin dato excluido). Cuantiles = percentile_cont (como quantile() de la web). ambito_id: departamento = depto_id; provincia = territorio; region = todos | censo_<pais>. columna = nombre en v_hexoma.';
"""


def numeric_cols(cur, table: str) -> set[str]:
    cur.execute("""SELECT column_name FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = %s
                     AND data_type IN ('smallint','integer','bigint','real','double precision','numeric')""", (table,))
    return {r[0] for r in cur.fetchall()}


def layer_vars(cur, layer: dict) -> list[dict]:
    """Fact-sheet variables (plus the primary) with the column actually present and numeric."""
    table = f'capa_{layer["id"]}'
    num = numeric_cols(cur, table)
    out, seen = [], set()
    for v in layer['variables']:
        if not (v['inFicha'] or layer['primary'] in (v['col'], v['rawCol'])) or v['col'] in seen:
            continue
        col = v['rawCol'] if v['rawCol'] and v['rawCol'] in num else v['col']
        if col not in num:
            continue
        seen.add(v['col'])
        out.append({**v, 'src': col})
    return out


def scopes_of(layer: dict) -> list[str]:
    s = ['prov']
    if layer['perDepartment']:
        s.insert(0, 'dep')
    if layer['comparable']:
        s.append('reg')
    return s


def base_sql(layer: dict, vars_: list[dict]) -> sql.Composed:
    """Every row of the layer with display values, scope keys and the region flag."""
    table = sql.Identifier(f'capa_{layer["id"]}')
    vals = []
    for i, v in enumerate(vars_):
        e = sql.SQL('c.{}::double precision').format(sql.Identifier(v['src']))
        if v['scale'] != 1:
            e = sql.SQL('{} * {}').format(e, sql.Literal(v['scale']))
        vals.append(sql.SQL('{} AS {}').format(e, sql.Identifier(f'v{i}')))
    regkey = sql.SQL("'censo_' || t.pais") if layer['rigor'] == 'census' else sql.SQL("'todos'::text")
    return sql.SQL("""
        WITH rep AS (
            SELECT DISTINCT ON (c.h3) c.h3, c.territorio_id
            FROM {t} c
            LEFT JOIN ficha_enlace fe ON fe.h3 = c.h3
            LEFT JOIN hexagonos hx ON hx.h3 = c.h3 AND hx.territorio_id = c.territorio_id
            LEFT JOIN limites l ON l.redcode = hx.redcode
            WHERE c.principal
            ORDER BY c.h3, (c.territorio_id = fe.territorio_id) DESC NULLS LAST,
                     (c.territorio_id = l.territorio_id) DESC NULLS LAST, c.territorio_id)
        SELECT c.h3, c.territorio_id, c.depto_id, c.principal, {regkey} AS regkey,
               (c.principal AND rep.h3 IS NOT NULL) AS reg_ok, {vals}
        FROM {t} c JOIN territorios t USING (territorio_id)
        LEFT JOIN rep ON rep.h3 = c.h3 AND rep.territorio_id = c.territorio_id""").format(
        t=table, regkey=regkey, vals=sql.SQL(', ').join(vals))


# the value each scope ranks / summarises (NULL = row not in that scope's population)
def scoped(s: str, i: int) -> sql.Composed:
    v = sql.Identifier(f'v{i}')
    return {
        'dep': sql.SQL('{}').format(v),
        'prov': sql.SQL('CASE WHEN principal THEN {} END').format(v),
        'reg': sql.SQL('CASE WHEN reg_ok THEN {} END').format(v),
    }[s]


PART = {'dep': sql.SQL('depto_id'), 'prov': sql.SQL('territorio_id'), 'reg': sql.SQL('regkey')}


def build_layer(layer: dict) -> dict:
    t0 = time.time()
    conn = connect(DB)
    lid = layer['id']
    with conn.cursor() as cur:
        cur.execute("SET work_mem = '512MB'")
        vars_ = layer_vars(cur, layer)
        scopes = scopes_of(layer)
        base = base_sql(layer, vars_)

        # ── per-hexagon position: lt / eq in hundredths of a point ──
        cols = []
        for i, v in enumerate(vars_):
            for s in scopes:
                x = scoped(s, i)
                w = sql.SQL('PARTITION BY {p} ORDER BY {x}').format(p=PART[s], x=x)
                n = sql.SQL('count({x}) OVER (PARTITION BY {p})').format(x=x, p=PART[s])
                lt = sql.SQL('rank() OVER ({w}) - 1').format(w=w)
                le = sql.SQL('count({x}) OVER ({w})').format(x=x, w=w)  # non-null values <= x (peers included)
                cols.append(sql.SQL('CASE WHEN {x} IS NOT NULL THEN round(10000.0 * ({lt}) / {n})::smallint END AS {a}').format(
                    x=x, lt=lt, n=n, a=sql.Identifier(f'{v["col"]}__{s}_lt')))
                cols.append(sql.SQL('CASE WHEN {x} IS NOT NULL THEN round(10000.0 * ({le} - ({lt})) / {n})::smallint END AS {a}').format(
                    x=x, le=le, lt=lt, n=n, a=sql.Identifier(f'{v["col"]}__{s}_eq')))
        new = sql.Identifier(f'rango_{lid}_new')
        cur.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(new))
        cur.execute(sql.SQL("""CREATE TABLE {new} AS
            SELECT h3, territorio_id, {cols} FROM (SELECT b.*, {wins} FROM ({base}) b) w WHERE principal""").format(
            new=new, base=base,
            cols=sql.SQL(', ').join(sql.Identifier(f'{v["col"]}__{s}_{k}') for v in vars_ for s in scopes for k in ('lt', 'eq')),
            wins=sql.SQL(', ').join(cols)))
        n_rows = cur.rowcount
        cur.execute(sql.SQL('ALTER TABLE {} ADD PRIMARY KEY (h3, territorio_id)').format(new))

        # ── distributions ──
        dist = []
        for s in scopes:
            aggs = []
            for i, v in enumerate(vars_):
                x = scoped(s, i)
                aggs.append(sql.SQL("""count({x}), percentile_cont(ARRAY[{qs}]) WITHIN GROUP (ORDER BY {x}),
                                       avg({x}), min({x}), max({x})""").format(
                    x=x, qs=sql.SQL(', ').join(sql.Literal(q) for q in QS)))
            if s == 'dep':
                key = sql.SQL("depto_id::text, max(d.departamento) || ' (' || max(tt.territorio) || ')'")
                join = sql.SQL('JOIN departamentos d USING (depto_id) JOIN territorios tt ON tt.territorio_id = d.territorio_id')
                grp = sql.SQL('depto_id')
            elif s == 'prov':
                key = sql.SQL('max(tt.territorio), max(tt.etiqueta)')
                join = sql.SQL('JOIN territorios tt USING (territorio_id)')
                grp = sql.SQL('territorio_id')
            else:
                key = sql.SQL("regkey, string_agg(DISTINCT tt.etiqueta, ', ')")
                join = sql.SQL('JOIN territorios tt USING (territorio_id)')
                grp = sql.SQL('regkey')
            cur.execute(sql.SQL('SELECT {key}, {aggs} FROM ({base}) b {join} GROUP BY {grp}').format(
                key=key, aggs=sql.SQL(', ').join(aggs), base=base, join=join, grp=grp))
            for row in cur.fetchall():
                aid, aname, rest = row[0], row[1], row[2:]
                for i, v in enumerate(vars_):
                    n, qs, mean, mn, mx = rest[5 * i: 5 * i + 5]
                    if not n:
                        continue
                    dist.append((lid, v['col'], f'{lid}__{v["col"]}', v['unit'], SCOPE_NAME[s], aid, aname,
                                 n, *qs, mean, mn, mx))
        cur.execute('DELETE FROM distribuciones WHERE capa = %s', (lid,))
        execute_values(cur, """INSERT INTO distribuciones (capa, variable, columna, unidad, ambito, ambito_id, ambito_nombre,
                               n, p10, p25, p50, p75, p90, media, min, max) VALUES %s""", dist, page_size=2000)
    conn.commit()
    conn.close()
    return {'id': lid, 'vars': vars_, 'scopes': scopes, 'rows': n_rows, 'dist': len(dist), 'secs': time.time() - t0}


def swap_and_view(conn, results: list[dict], all_layers: list[dict]) -> int:
    """Replace rango_* with the new tables and rebuild v_hexoma_rangos in one transaction."""
    titles = {l['id']: l['title'] for l in all_layers}
    with conn.cursor() as cur:
        cur.execute('DROP VIEW IF EXISTS v_hexoma_rangos')
        for r in results:
            old, new = f'rango_{r["id"]}', f'rango_{r["id"]}_new'
            cur.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(sql.Identifier(old)))
            cur.execute(sql.SQL('ALTER TABLE {} RENAME TO {}').format(sql.Identifier(new), sql.Identifier(old)))
            cur.execute(sql.SQL('ALTER INDEX {} RENAME TO {}').format(sql.Identifier(f'{new}_pkey'), sql.Identifier(f'{old}_pkey')))
            cur.execute(sql.SQL('COMMENT ON TABLE {} IS %s').format(sql.Identifier(old)), (
                f'Posición de cada hexágono (filas principales de capa_{r["id"]}) en cada ámbito, como la ficha: '
                '<var>__<ámbito>_lt = % de celdas con valor estrictamente menor, _eq = % con el mismo valor, '
                'en centésimos de punto (0-10000; error ±0,005 pt). Ámbitos: dep = archivo del departamento, '
                'prov = territorio, reg = región (sólo capas comparables; censales sólo dentro de su país; '
                'una celda en dos territorios tiene rango regional sólo en el del enlace). Sin dato = NULL.',))
        # the view: one column per (layer, variable, scope) = % below, over every rango_* table present
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'rango\\_%%' AND tablename NOT LIKE '%%\\_new'")
        tables = sorted(r[0] for r in cur.fetchall())
        sel, joins, comments = [sql.SQL('hx.h3, t.territorio')], [], []
        for j, tab in enumerate(tables):
            lid = tab[len('rango_'):]
            a = sql.Identifier(f'r{j}')
            joins.append(sql.SQL('LEFT JOIN {t} {a} ON {a}.h3 = hx.h3 AND {a}.territorio_id = hx.territorio_id').format(
                t=sql.Identifier(tab), a=a))
            for c in sorted(table_columns(cur, tab) - {'h3', 'territorio_id'}):
                if not c.endswith('_lt'):
                    continue
                var, s = c[:-3].rsplit('__', 1)
                name = f'{lid}__{var}__pr_{s}'
                sel.append(sql.SQL('({a}.{c} / 100.0)::real AS {n}').format(a=a, c=sql.Identifier(c), n=sql.Identifier(name)))
                comments.append((name, f'% de celdas por debajo, ámbito {SCOPE_NAME[s]} — {titles.get(lid, lid)} · {var}'))
        cur.execute(sql.SQL("""CREATE VIEW v_hexoma_rangos AS SELECT {} FROM hexagonos hx
            JOIN territorios t USING (territorio_id) {}""").format(sql.SQL(', ').join(sel), sql.SQL('\n').join(joins)))
        cur.execute("COMMENT ON VIEW v_hexoma_rangos IS 'Posición de cada hexágono en cada ámbito: <capa>__<variable>__pr_<dep|prov|reg> = % de celdas con valor estrictamente menor (la ficha: «más alto que N de cada 100»). El % de iguales está en las tablas rango_*. Distribuciones en distribuciones.'")
        for name, txt in comments:
            cur.execute(sql.SQL('COMMENT ON COLUMN v_hexoma_rangos.{} IS %s').format(sql.Identifier(name)), (txt,))
    conn.commit()
    return len(sel) - 2


def run(layer_ids: list[str] | None = None, workers: int = 4) -> None:
    t0 = time.time()
    reg = json.loads(REGISTRY.read_text(encoding='utf-8'))
    conn = connect(DB)
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
        layers = [l for l in reg['layers'] if (not layer_ids or l['id'] in layer_ids) and table_columns(cur, f'capa_{l["id"]}')]
    conn.commit()
    log(f'distribuciones: {len(layers)} capas, {workers} en paralelo')
    results = list(ThreadPoolExecutor(workers).map(build_layer, layers))
    for r in results:
        log(f"  {r['id']:<24} {len(r['vars']):>2} vars × {'/'.join(r['scopes']):<12} {r['rows']:>10,} filas  "
            f"{r['dist']:>5} distribuciones  {r['secs']:6.1f} s")
    ncols = swap_and_view(conn, results, reg['layers'])
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('ANALYZE distribuciones')
        for r in results:
            cur.execute(sql.SQL('ANALYZE {}').format(sql.Identifier(f'rango_{r["id"]}')))
        cur.execute("""SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c
                       WHERE c.relkind = 'r' AND (c.relname LIKE 'rango\\_%%' OR c.relname = 'distribuciones')""")
        size = cur.fetchone()[0]
        cur.execute('SELECT pg_size_pretty(pg_database_size(current_database()))')
        log(f'v_hexoma_rangos: {ncols} columnas · rangos + distribuciones {size} · base {cur.fetchone()[0]} · {time.time() - t0:.0f} s')
    conn.close()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--layers', default='')
    ap.add_argument('--workers', type=int, default=4)
    a = ap.parse_args()
    run([x for x in a.layers.split(',') if x] or None, a.workers)
