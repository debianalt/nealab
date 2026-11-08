"""Oraciones y menciones de cortes en las notas de prensa (fase 6: sólo datos, reglas transparentes, sin modelo de lenguaje).

Para cada nota de noticias.notas (patrones v2: ver el comentario de TIPOS):
  1. oraciones (punto, signo o salto de línea seguido de mayúscula); sólo se guardan las que tienen mención, como
     texto dentro de noticias.menciones (la tabla noticias.oraciones, 231 MB, se borró el 7-nov-2026)
  2. tipo de evento por patrones en la MISMA oración (puede haber varios por oración):
       corte_puente       puente/alcantarilla/vado + cortado, intransitable, bajo agua, anegado, tapado, colapso,
                          derrumbe, (in)habilitado, clausurado, interrumpido
       corte_ruta_camino  ruta/camino/calle/tramo/RP/RN + los mismos verbos
       crecida            crecida, creciente, desborde, nivel/altura del río, inundación
       evacuados_aislados evacuados, aislados, incomunicados
  3. lugar con el nomenclador del Hexoma (texto normalizado sin acentos; el nombre propio tiene que estar con
     mayúscula en el original): municipios (visor), asentamientos BAHRA, arroyos y ríos con nombre (IGN y red
     derivada, precedidos de «arroyo»/«río»/«A°»), hidrómetros; rutas «RP/RN/ruta (provincial|nacional) N» y «km X».
     Un nombre presente en más de un lugar del nomenclador se marca ambiguo.
  4. altura del río citada (número + metros/mts en una oración con río/arroyo/altura/nivel/escala) y fecha relativa
     («ayer», «anoche», día de la semana) como TEXTO: la fecha cierta es la de la nota.
Una oración con evento y sin lugar también queda (lugar NULL).

El corte de luz salió el 7-nov-2026: contra Black Marble no hay señal (probar_utilidad.py, prueba B; razón de
radiancia 0,90 en días con corte en prensa contra 0,87 de control). Las notas siguen en noticias.notas.

  noticias.menciones   mencion_id, nota_id, n_oracion, oracion, medio, fecha, tipo, lugar_tipo, lugar_nombre,
                       lugar_ref, ambiguo, ruta, km, altura_m, hidrometro, fecha_relativa, geom (punto del lugar)

Uso (desde neahub/, después de bajar_notas.py):
    python pipeline/noticias/extraer.py
"""
from __future__ import annotations

import io
import re
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect, log  # noqa: E402

# Versión 2 de los patrones (7-nov-2026). La v1 tuvo 52 % de precisión de tipo en la muestra (semilla 7; regla ≥ 80 %):
# «aislados» en fútbol/virus, cualquier nota de Energía de Misiones como corte de luz, cortes de ruta de protesta,
# «habilitado» de obras, desbordes en el fútbol y frases hipotéticas. Ver pipeline/noticias/validacion_noticias.md.
VERBOS = (r'(cortad[oa]s?|cortes?\b|intransitables?|bajo (el )?agua|anegad[oa]s?|tapad[oa]s?|cubiert[oa]s? por el agua|'
          r'colaps\w*|derrumb\w*|inhabilitad[oa]s?|clausurad[oa]s?|interrumpid[oa]s?|restringid[oa]s?|'
          r'socavad[oa]s?|se cayo|se vino abajo|cerrad[oa]s? (al transito|preventivamente)|cierre preventivo)')
AGUA = (r'(\brio\b|\brios\b|arroyo|agua|lluvia|temporal|tormenta|crecida|creciente|desbord|inundac|anegad|precipitac|'
        r'cauce|damnificad)')
TIPOS = {   # nombre: (requiere, además requiere, excluye)
    'corte_puente': (re.compile(r'\b(puentes?|alcantarillas?|vados?|pasarelas?)\b'), re.compile(VERBOS),
                     re.compile(r'(alumbrado|iluminacion|licitac|inaugur|pavimentac)')),
    'corte_ruta_camino': (re.compile(r'\b(rutas?|caminos?|calles?|tramos?|rp|rn|ruta costera|transito)\b'), re.compile(VERBOS),
                          re.compile(r'(protesta|manifestant|piquet|reclam|colonos|tareferos|medida de fuerza|productores|'
                                     r'moviliza|marcha|gremi|sindica|repavimentac|pavimentac|bacheo|licitac)')),
    'crecida': (re.compile(r'(crecida|creciente|desbord\w*|inundac\w*|anegad\w*)'),
                re.compile(r'(\brio\b|\brios\b|arroyo|agua|lluvia|temporal|tormenta|cauce|calles|barrio|vecinos|familias|'
                           r'precipitac|nivel|altura|puente|ruta)'),
                re.compile(r'(\bgol\b|pelota|partido|delanter|volante|arquero|cancha|\bclub\b|equipo|emocion|alegria)')),
    'evacuados_aislados': (re.compile(r'\b(evacuad\w*|aislad\w*|incomunicad\w*)'), re.compile(AGUA + r'|puente|camino|ruta'),
                           re.compile(r'(virus|covid|coronavirus|cuarentena|contagi|preso|carcel|penal|guerra|hamas|israel|'
                                      r'gaza|ucrania|(lluvias|tormentas|chaparrones|precipitaciones|lloviznas) aislad|'
                                      r'rafagas|casos aislad|hechos aislad|episodios aislad|acciones aislad)')),
}
HIPOTETICO = re.compile(r'\b(podria|podrian|pudiera|posibles?|eventual(es|mente)?|prevenir|anticipar|en caso de|'
                        r'fueran necesarios|pronostic\w*|se espera|se esperan)\b')
RE_RUTA = re.compile(r'\b(?:ruta\s+(provincial|nacional)|rp|rn)\s*(?:n(?:ro|°|º|\.)?\s*)?(\d{1,3})\b')
RE_KM = re.compile(r'\bkm\.?\s*(\d{1,4}(?:[.,]\d+)?)')
RE_ALTURA = re.compile(r'(\d{1,2}(?:[.,]\d{1,2})?)\s*(?:metros|mts\.?|m)\b')
RE_ALT_CTX = re.compile(r'\b(rio|arroyo|altura|nivel|escala|hidrometro|prefectura)\b')
RE_REL = re.compile(r'\b(ayer|anoche|anteayer|esta madrugada|hoy|el (lunes|martes|miercoles|jueves|viernes|sabado|domingo))\b')
RE_ORACION = re.compile(r'(?<=[.!?;])\s+(?=[A-ZÁÉÍÓÚÑ¿¡"“«])|\n+')
GENERICOS = {'centro', 'union', 'paraiso', 'progreso', 'esperanza', 'libertad', 'independencia', 'porvenir', 'triunfo',
             'victoria', 'la plata', 'santa rosa', 'el soberbio arroyo', 'puerto', 'colonia', 'barrio', 'ruta', 'km',
             'san martin', 'belgrano', 'mayo', 'florida', 'aurora', 'tres esquinas', 'cuatro bocas', 'la paz'}


def norm(s: str) -> str:
    """minúsculas sin acentos, cada espacio → ' ' (1 a 1: conserva el largo para alinear con el original)"""
    s = unicodedata.normalize('NFC', s or '')
    s = ''.join(unicodedata.normalize('NFD', c)[0] for c in s)
    return re.sub(r'\s', ' ', s.lower())


def clave(s: str) -> str:
    return ' '.join(norm(s).split())


def nomenclador(conn) -> pd.DataFrame:
    g = pd.read_sql("""
        SELECT 'municipio' tipo, regexp_replace(nombre, '^Municipio de ', '') nombre, gid::text ref,
               ST_X(ST_PointOnSurface(geom)) lon, ST_Y(ST_PointOnSurface(geom)) lat FROM infra.visor_municipios
        UNION ALL SELECT 'asentamiento', nombre, gid::text, ST_X(geom), ST_Y(geom) FROM infra.asentamientos
        UNION ALL SELECT 'hidrometro', nombre, gid::text, ST_X(geom), ST_Y(geom) FROM infra.visor_hidrometros
        UNION ALL
        (SELECT DISTINCT ON (s.n) 'cauce', s.n, s.n, ST_X(s.p), ST_Y(s.p) FROM (
          SELECT regexp_replace(nombre, '^(Arroyo|Río|Rio|A°|Aº)\\s+', '', 'i') n,
                 ST_LineInterpolatePoint(geom, 0.5) p, area_abajo_km2 a
          FROM infra.arroyos WHERE nombre ~* '^(arroyo|r[ií]o)\\s') s ORDER BY s.n, s.a DESC)""", conn)
    g['clave'] = g.nombre.map(clave)
    g = g[(g.clave.str.len() >= 4) & ~g.clave.isin(GENERICOS)]
    g['n_lugares'] = g.groupby('clave').clave.transform('size')
    return g


def main() -> int:
    t0 = time.time()
    conn = connect(DB)
    notas = pd.read_sql('SELECT nota_id, medio, fecha, titulo, texto FROM noticias.notas', conn)
    gaz = nomenclador(conn)
    lugares = gaz[gaz.tipo != 'cauce']
    cauces = gaz[gaz.tipo == 'cauce']
    # una sola regex por tipo de lugar (más largos primero, para que «San Pedro» no tape «Colonia San Pedro»)
    def regex_de(claves, prefijo=''):
        alt = '|'.join(re.escape(c) for c in sorted(set(claves), key=len, reverse=True))
        return re.compile(rf'\b{prefijo}({alt})\b')
    re_lug = regex_de(lugares.clave)
    re_cau = regex_de(cauces.clave, r'(?:(?:arroyo|rio|a°|aº)\s+(?:el\s+|la\s+|los\s+|las\s+)?|puentes?\s+(?:sobre\s+(?:el\s+)?)?)')
    por_clave = gaz.groupby('clave')
    log(f'nomenclador: {len(gaz):,} lugares ({gaz.tipo.value_counts().to_dict()}); notas {len(notas):,}')

    menciones = []
    n_oraciones = 0
    for r in notas.itertuples():
        partes = [p.strip() for p in RE_ORACION.split(f'{r.titulo}.\n{r.texto}') if p and p.strip()]
        n_oraciones += len(partes)
        for k, orig in enumerate(partes):
            texto_orig = orig
            orig = unicodedata.normalize('NFC', orig)
            t = norm(orig)
            if HIPOTETICO.search(t):
                continue
            tipos = [tp for tp, (a, b, x) in TIPOS.items()
                     if a.search(t) and (b is None or b.search(t)) and (x is None or not x.search(t))]
            if not tipos:
                continue
            rutas = [f"{'RN' if (m.group(1) == 'nacional' or m.group(0).startswith('rn')) else 'RP'} {m.group(2)}"
                     for m in RE_RUTA.finditer(t)]
            km = RE_KM.search(t)
            alt = RE_ALTURA.search(t) if RE_ALT_CTX.search(t) else None
            rel = RE_REL.search(t)
            hallados, tramos_cauce = [], []
            for m in re_cau.finditer(t):
                hallados.append(('cauce', m.group(1)))
                tramos_cauce.append(m.span(1))
            for m in re_lug.finditer(t):
                if any(a <= m.start(1) < b for a, b in tramos_cauce):   # «puente Piray Guazú»: gana el río, no el paraje Piray
                    continue
                # nombre propio: en el original empieza con mayúscula en la misma posición aproximada
                frag = orig[m.start(1):m.start(1) + 1]
                if not frag.isupper():
                    continue
                hallados.append(('lugar', m.group(1)))
            hid = [c for _, c in hallados if (por_clave.get_group(c).tipo == 'hidrometro').any()]
            base = dict(nota_id=r.nota_id, n_oracion=k, oracion=texto_orig, medio=r.medio, fecha=r.fecha,
                        ruta=rutas[0] if rutas else None, km=km.group(1).replace(',', '.') if km else None,
                        altura_m=float(alt.group(1).replace(',', '.')) if alt else None,
                        hidrometro=hid[0] if hid else None, fecha_relativa=rel.group(0) if rel else None)
            for tp in tipos:
                if not hallados:
                    menciones.append(dict(base, tipo=tp, lugar_tipo=None, lugar_nombre=None, lugar_ref=None,
                                          ambiguo=None, lon=None, lat=None))
                for clase, c in dict.fromkeys(hallados):
                    gg = por_clave.get_group(c)
                    if clase == 'cauce':
                        gg = gg[gg.tipo == 'cauce']
                    else:
                        gg = gg[gg.tipo != 'cauce']
                    if gg.empty:
                        continue
                    pref = gg.sort_values('tipo', key=lambda s: s.map({'municipio': 0, 'hidrometro': 1, 'asentamiento': 2, 'cauce': 3})).iloc[0]
                    menciones.append(dict(base, tipo=tp, lugar_tipo=pref.tipo, lugar_nombre=pref.nombre, lugar_ref=pref.ref,
                                          ambiguo=bool(pref.n_lugares > 1), lon=pref.lon, lat=pref.lat))
    men = pd.DataFrame(menciones)
    log(f'oraciones {n_oraciones:,}; menciones {len(men):,} ({time.time() - t0:.0f} s)')

    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS noticias.menciones')
    cur.execute('DROP TABLE IF EXISTS noticias.oraciones')
    cols = ['nota_id', 'n_oracion', 'oracion', 'medio', 'fecha', 'tipo', 'lugar_tipo', 'lugar_nombre', 'lugar_ref', 'ambiguo',
            'ruta', 'km', 'altura_m', 'hidrometro', 'fecha_relativa', 'lon', 'lat']
    cur.execute("""CREATE TABLE noticias.menciones (mencion_id serial PRIMARY KEY, nota_id int, n_oracion int, oracion text, medio text,
                   fecha timestamp, tipo text, lugar_tipo text, lugar_nombre text, lugar_ref text, ambiguo boolean,
                   ruta text, km text, altura_m real, hidrometro text, fecha_relativa text, lon double precision,
                   lat double precision)""")
    buf = io.StringIO()
    men[cols].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert(f"COPY noticias.menciones ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute('ALTER TABLE noticias.menciones ADD COLUMN geom geometry(Point, 4326)')
    cur.execute('UPDATE noticias.menciones SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326) WHERE lon IS NOT NULL')
    cur.execute('CREATE INDEX ON noticias.menciones USING gist (geom)')
    cur.execute('CREATE INDEX ON noticias.menciones (tipo, fecha)')
    cur.execute("COMMENT ON TABLE noticias.menciones IS 'Menciones de cortes de puente y de ruta/camino, crecidas y "
                "evacuados/aislados por oración (texto en oracion), con lugar del nomenclador del Hexoma (escala municipio "
                "y día); reglas en pipeline/noticias/extraer.py'")
    conn.commit()
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s; por tipo: {men.tipo.value_counts().to_dict()}; '
        f'con lugar {men.lugar_tipo.notna().mean():.0%}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
