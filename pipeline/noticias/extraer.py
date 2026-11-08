"""Menciones de eventos de infraestructura en las notas de prensa (v3: sólo datos, reglas transparentes, sin modelo de lenguaje).

Para cada oración de cada nota de noticias.notas (texto ya sin menús: comun.limpiar_texto):
  1. tipo de evento por patrones en la MISMA oración (puede haber varios por oración):
       corte_puente       puente/alcantarilla/vado/pasarela + cortado, intransitable, bajo agua, anegado, tapado, colapso,
                          derrumbe, (in)habilitado, clausurado, interrumpido
       corte_ruta_camino  ruta/camino/calle/tramo/RP/RN/tránsito + los mismos verbos (piquetes INCLUIDOS desde v3)
       crecida            crecida, creciente, desborde, inundación + contexto de agua
       evacuados_aislados evacuados, aislados, incomunicados + contexto de agua o vía
  2. motivo (v3): protesta | accidente | agua | obra | otro, por palabras de la oración, en ese orden de precedencia.
     Crecida y evacuados son siempre «agua».
  3. fechas: fecha_nota (la de publicación: para citar) y fecha_evento con su fuente:
       explicita  «el 17 de febrero», «14 de octubre de 2023» (sin año: el de la nota, o el anterior si queda adelante)
       relativa   ayer, anteayer, anoche, hoy, esta madrugada, día de la semana (el último anterior o igual a la nota)
       nota       sin indicación: la de la nota
  4. nombres encontrados (con mayúscula en el original), para que ubicar.py elija el punto:
       cauces      «arroyo X», «río X», «A° X», «puente (sobre el) X» con X del nomenclador de cauces
       lugares     municipios, asentamientos BAHRA, hidrómetros
       toponimos   noticias.toponimos (toponimos.py): parajes, picadas, puentes con nombre; barrios sólo detrás de
                   «barrio»/«B°»; escuelas como «escuela <número>»
       candidatos  nombres nuevos detrás de paraje/barrio/colonia/picada/línea/aldea/comunidad/villa que no están en
                   ningún nomenclador (ubicar.py los busca afuera)
       contexto    el municipio o asentamiento más nombrado en toda la nota (para elegir entre homónimos)
  5. ruta («RP/RN/ruta (provincial|nacional) N»), km, altura del río citada.
Sólo Misiones: se descarta la oración que nombra otra provincia o un lugar lejano (OTRA_REGION) sin nombrar a
Misiones; ubicar.py descarta además todo punto fuera de la provincia.

El corte de luz salió el 7-nov-2026: contra Black Marble no hay señal (probar_utilidad.py, prueba B). Las notas
siguen en noticias.notas.

  noticias.menciones   mencion_id, nota_id, n_oracion, oracion, medio, fecha_nota, fecha_evento, fecha_evento_fuente,
                       tipo, motivo, ruta, km, altura_m, hidrometro, cauces, lugares, toponimos, candidatos, contexto,
                       + lo que agrega ubicar.py (precision, lugar_nombre, fuente_ubicacion, ambiguo, geom)

Uso (desde neahub/, después de bajar_notas.py / bajar_sin_api.py y toponimos.py):
    python pipeline/noticias/extraer.py && python pipeline/noticias/ubicar.py
"""
from __future__ import annotations

import datetime as dt
import io
import re
import sys
import time
import unicodedata
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect, log  # noqa: E402

# Patrones v2 (7-nov-2026; la v1 tuvo 52 % de precisión de tipo). v3 (7-nov-2026): los piquetes y las obras ya no se
# excluyen del corte de ruta, pasan a la variable motivo. Ver pipeline/noticias/validacion_noticias.md.
VERBOS = (r'(cortad[oa]s?|cortes?\b|intransitables?|bajo (el )?agua|anegad[oa]s?|tapad[oa]s?|cubiert[oa]s? por el agua|'
          r'colaps\w*|derrumb\w*|inhabilitad[oa]s?|clausurad[oa]s?|interrumpid[oa]s?|restringid[oa]s?|'
          r'socavad[oa]s?|se cayo|se vino abajo|cerrad[oa]s? (al transito|preventivamente)|cierre preventivo|'
          r'cortaron|bloque\w*)')
AGUA = (r'(\brio\b|\brios\b|arroyo|agua|lluvia|temporal|tormenta|crecida|creciente|desbord|inundac|anegad|precipitac|'
        r'cauce|damnificad)')
TIPOS = {   # nombre: (requiere, además requiere, excluye)
    'corte_puente': (re.compile(r'\b(puentes?|alcantarillas?|vados?|pasarelas?)\b'), re.compile(VERBOS),
                     re.compile(r'(alumbrado|iluminacion|licitac|inaugur)')),
    'corte_ruta_camino': (re.compile(r'\b(rutas?|caminos?|calles?|tramos?|rp|rn|ruta costera|transito)\b'), re.compile(VERBOS),
                          re.compile(r'(licitac|inaugur|corte de cinta|cortes? de (luz|energia|agua potable|suministro)|'
                                     # v3.1: el corte de ruta como tema, no como hecho
                                     r'cortes? de rutas? (representan?|son un|afectan?|perjudican?|generan?)|'
                                     r'son los cortes de ruta|problema de los cortes)')),
    'crecida': (re.compile(r'(crecida|creciente|desbord\w*|inundac\w*|anegad\w*)'),
                re.compile(r'(\brio\b|\brios\b|arroyo|agua|lluvia|temporal|tormenta|cauce|calles|barrio|vecinos|familias|'
                           r'precipitac|nivel|altura|puente|ruta)'),
                re.compile(r'(\bgol\b|pelota|partido|delanter|volante|arquero|cancha|\bclub\b|equipo|emocion|alegria|'
                           r'creciente (necesidad|demanda|interes|preocupacion|numero|cantidad))')),
    'evacuados_aislados': (re.compile(r'\b(evacuad\w*|aislad\w*|incomunicad\w*)'), re.compile(AGUA + r'|puente|camino|ruta'),
                           re.compile(r'(virus|covid|coronavirus|cuarentena|contagi|preso|carcel|penal|guerra|hamas|israel|'
                                      r'gaza|ucrania|(lluvias|tormentas|chaparrones|precipitaciones|lloviznas) aislad|'
                                      r'rafagas|casos aislad|hechos aislad|episodios aislad|acciones aislad|evacuo \d|'
                                      # v3.1: negaciones
                                      r'siniestros aislad|sin (registrarse )?(personas )?evacuad|no hay evacuad|'
                                      r'ningun evacuad|no se registraron evacuad|no hubo evacuad)')),
}
MOTIVOS = [   # el primero que aparece manda
    ('protesta', re.compile(r'(protesta|manifestant|piquet|reclam|colonos|tareferos|medida de fuerza|moviliza|marcha\b|'
                            r'gremi|sindica|autoconvocad|docentes|acampe|cortaron la ruta en|asamblea|trabajadores de|'
                            r'productores (yerbateros|tabacaleros|tealeros)|cooperativistas|organizaciones sociales|'
                            r'agrupaciones|movimiento teresa|\bmtr\b|polo obrero|ctep|barrios de pie|piqueter)')),
    ('accidente', re.compile(r'(choque|chocaron|vuelco|volco|colision|accidente|siniestro vial|despiste|camion (volcado|'
                             r'atravesado|cruzado)|incendio de (un|una) (camion|vehiculo|auto)|'
                             r'(camion|unidad|acoplado|vehiculo) quedo (obstruyendo|atravesad|cruzad|varad))')),
    ('agua', re.compile(AGUA + r'|socav|barro|desmoron|deslizamiento|alud|caudal|repunte|altura del|el nino|'
                        r'fenomeno climatico')),
    ('obra', re.compile(r'(repavimentac|pavimentac|bacheo|\bobras?\b|trabajos de|mantenimiento|reparac|reconstruc|'
                        r'construcc|asfalt|ensanche|demolic|voladura)')),
]
HIPOTETICO = re.compile(r'\b(podria|podrian|pudiera|posibles?|eventual(es|mente)?|prevenir|anticipar|en caso de|'
                        r'fueran necesarios|pronostic\w*|se espera|se esperan|'
                        # v3.1 (muestra semilla 12: 15 de 60 eran temas, futuros o negaciones, no hechos)
                        r'posibilidad|pueden? (llegar|superar|provocar|generar|causar)|habra|habria|podra|podran|'
                        r'suelen|cada vez que|vulnerab\w*|contra (las |los )?(crecidas|inundaciones|desbordes)|mitigar)\b')
# sólo Misiones: oración que nombra otra provincia o un lugar lejano y no nombra a Misiones → fuera. Las ciudades de la
# frontera (Encarnación, Foz, Ciudad del Este, Chapecó) NO están: sus puentes y pasos son infraestructura de Misiones.
OTRA_REGION = re.compile(
    r'\b(buenos aires|cordoba|santa fe|entre rios|provincia de corrientes|correntin\w*|ituzaingo|paso de los libres|'
    r'santo tome|virasoro|chaco|chaque\w*|formosa|salta|jujuy|tucuman|catamarca|santiago del estero|la rioja|san juan|'
    r'mendoza|san luis|neuquen|rio negro|chubut|santa cruz|tierra del fuego|la pampa|bahia blanca|rosario|concordia|'
    r'gualeguaychu|rio grande do sul|santa catarina|porto alegre|sao paulo|rio de janeiro|asuncion|uruguayana|'
    r'bolivia|chile|peru|venezuela|ecuador|colombia|mexico|estados unidos|espana|europa|israel|gaza|ucrania|'
    r'valle de punilla|traslasierra|neuquin\w*|cordobes\w*|santafesin\w*|porten\w*|bonaerense\w*|mendocin\w*|'
    r'tucuman\w*|saltena?s?|jujen\w*|entrerrian\w*|formosen\w*|rionegrin\w*|catamarquen\w*|riojan\w*|'
    r'sanjuanin\w*|puntan[oa]s?|pampean\w*|fueguin\w*|santiaguen\w*|edenor|edesur|caba|conurbano|amba|'
    r'capital federal|comunidad valenciana|valencia|carcarana|epecuen|carhue|corrientes(?=\s*:)|'
    r'en (la provincia de )?corrientes|itati|yacarei|scorza cue|iribu cua|texas|imola|paraibuna)\b')
MISIONES = re.compile(r'\b(misiones|misioner[oa]s?)\b')
RE_RUTA = re.compile(r'\b(?:ruta\s+(provincial|nacional)|rp|rn)\s*(?:n(?:ro|°|º|\.)?\s*)?(\d{1,3})\b')
RE_KM = re.compile(r'\bkm\.?\s*(\d{1,4}(?:[.,]\d+)?)')
RE_ALTURA = re.compile(r'(\d{1,2}(?:[.,]\d{1,2})?)\s*(?:metros|mts\.?|m)\b')
RE_ALT_CTX = re.compile(r'\b(rio|arroyo|altura|nivel|escala|hidrometro|prefectura)\b')
RE_ORACION = re.compile(r'(?<=[.!?;])\s+(?=[A-ZÁÉÍÓÚÑ¿¡"“«])|\n+')
RE_ESCUELA = re.compile(r'\b(?:escuela|epet|instituto|colegio|bop|cep|nenis?)\b[^\d.]{0,25}?n[°º.]?\s*(\d{1,4})')
# nombres nuevos detrás de una palabra de lugar (en el original, con mayúscula)
RE_CANDIDATO = re.compile(r'\b([Pp]araje|[Bb]arrio|B[°º]|[Cc]olonia|[Pp]icada|[Ll][íi]nea|[Aa]ldea|[Cc]omunidad|[Vv]illa)\s+'
                          r'((?:[A-ZÁÉÍÓÚÑ][\wáéíóúñü]+|\d{1,4})(?:\s+(?:de|del|la|las|los|[A-ZÁÉÍÓÚÑ][\wáéíóúñü]+|\d{1,4})){0,3})')
GENERICOS = {'centro', 'union', 'paraiso', 'progreso', 'esperanza', 'libertad', 'independencia', 'porvenir', 'triunfo',
             'victoria', 'la plata', 'santa rosa', 'el soberbio arroyo', 'puerto', 'colonia', 'barrio', 'ruta', 'km',
             'san martin', 'belgrano', 'mayo', 'florida', 'aurora', 'tres esquinas', 'cuatro bocas', 'la paz',
             'escuela', 'norte', 'sur', 'este', 'oeste', 'nuevo', 'nueva', 'santa', 'san', 'jardin', 'el cruce',
             'cruce', 'la cancha', 'el puente', 'puente', 'la loma', 'kilometro', 'linea', 'picada', 'acceso'}
MESES = {m: i for i, m in enumerate(['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto',
                                     'septiembre', 'octubre', 'noviembre', 'diciembre'], 1)} | {'setiembre': 9}
DIAS = ['lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo']
RE_FECHA = re.compile(r'\b(\d{1,2})\s+de\s+(' + '|'.join(MESES) + r')(?:\s+(?:de|del)\s+((?:19|20)\d\d))?\b')
RE_REL = re.compile(r'\b(anteayer|ayer|anoche|esta madrugada|esta manana|esta tarde|esta noche|hoy|'
                    r'(?:el|este|del|la noche del|la madrugada del)\s+(lunes|martes|miercoles|jueves|viernes|sabado|domingo))\b')


def norm(s: str) -> str:
    """minúsculas sin acentos, cada espacio → ' ' (1 a 1: conserva el largo para alinear con el original)"""
    s = unicodedata.normalize('NFC', s or '')
    s = ''.join(unicodedata.normalize('NFD', c)[0] for c in s)
    return re.sub(r'\s', ' ', s.lower())


def clave(s: str) -> str:
    """clave de un NOMBRE: sin acentos ni mayúsculas, y la «y» como «i» (IGN «Pindaití», prensa «Pindaytí»)"""
    return ' '.join(norm(s).split()).replace('y', 'i')


def para_nombres(t: str) -> str:
    return t.replace('y', 'i')     # el texto normalizado, para buscar nombres (mismo largo: alinea con el original)


GENERICOS = {clave(x) for x in GENERICOS}   # «mayo» → «maio»: misma clave que los nombres


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


def regex_de(claves, prefijo=''):
    """una sola regex (más largos primero, para que «San Pedro» no tape «Colonia San Pedro»)"""
    alt = '|'.join(re.escape(c) for c in sorted(set(claves), key=len, reverse=True))
    return re.compile(rf'\b{prefijo}({alt})\b')


def con_mayuscula(orig: str, i: int) -> bool:
    return orig[i:i + 1].isupper() or orig[i:i + 1].isdigit()


RE_MES_ANIO = re.compile(r'\ben (' + '|'.join(MESES) + r')\s+(?:de|del)\s+((?:19|20)\d\d)\b')


def fecha_evento(t: str, nota: pd.Timestamp, orig: str = '') -> tuple[dt.date | None, str]:
    if pd.isna(nota):
        return None, 'nota'
    f0 = nota.date()
    for m in RE_FECHA.finditer(t):
        # «9 de Julio», «25 de Mayo»: con mayúscula en el original es el municipio, no una fecha (v3.1)
        if orig and orig[m.start(2):m.start(2) + 1].isupper():
            continue
        dia, mes, anio = int(m.group(1)), MESES[m.group(2)], m.group(3)
        try:
            f = dt.date(int(anio) if anio else f0.year, mes, dia)
            if not anio and f > f0 + dt.timedelta(days=2):
                f = f.replace(year=f.year - 1)
            return f, 'explicita'
        except ValueError:
            continue
    m = RE_MES_ANIO.search(t)     # «en junio del 2014»: mitad del mes
    if m and not (orig and orig[m.start(1):m.start(1) + 1].isupper()):
        return dt.date(int(m.group(2)), MESES[m.group(1)], 15), 'explicita_mes'
    m = RE_REL.search(t)
    if m:
        w = m.group(1)
        if w == 'anteayer':
            return f0 - dt.timedelta(days=2), 'relativa'
        if w == 'ayer':
            return f0 - dt.timedelta(days=1), 'relativa'
        if w == 'anoche':
            return f0 - dt.timedelta(days=1 if nota.hour < 19 else 0), 'relativa'
        if m.group(2):
            return f0 - dt.timedelta(days=(f0.weekday() - DIAS.index(m.group(2))) % 7), 'relativa'
        return f0, 'relativa'
    return f0, 'nota'


def main() -> int:
    t0 = time.time()
    conn = connect(DB)
    notas = pd.read_sql('SELECT nota_id, medio, fecha, titulo, texto FROM noticias.notas', conn)
    gaz = nomenclador(conn)
    top = pd.read_sql("SELECT clase, clave FROM noticias.toponimos", conn)
    top['clave'] = top.clave.str.replace(r'^(barrio|b°|bº|paraje)\s+', '', regex=True)
    top = top[(top.clave.str.len() >= 5) & ~top.clave.isin(GENERICOS) & ~top.clave.isin(set(gaz.clave))]
    # v3.1: los alias de toponimos_manual.csv también se buscan (y por ser más largos ganan: «San Roque González»
    # le gana al paraje «San Roque»)
    man = ROOT / 'pipeline' / 'noticias' / 'toponimos_manual.csv'
    if man.exists():
        mm = pd.read_csv(man)
        al = [clave(a) for r in mm.itertuples() for a in [r.nombre] + str(r.alias).split('|')]
        top = pd.concat([top, pd.DataFrame({'clase': 'paraje', 'clave': [a for a in al if len(a) >= 5]})], ignore_index=True)
    re_lug = regex_de(gaz[gaz.tipo != 'cauce'].clave)
    re_cau = regex_de(gaz[gaz.tipo == 'cauce'].clave,
                      r'(?:(?:arroio|rio|a°|aº)\s+(?:el\s+|la\s+|los\s+|las\s+)?|puentes?\s+(?:sobre\s+(?:el\s+)?)?)')
    re_top = regex_de(top[top.clase.isin(['paraje', 'picada', 'puente'])].clave)
    re_bar = regex_de(top[top.clase == 'barrio'].clave, r'(?:barrio|b°|bº)\s+')
    hid = set(gaz[gaz.tipo == 'hidrometro'].clave)
    conocidos = set(gaz.clave) | set(top.clave)
    log(f'nomenclador: {len(gaz):,} base ({gaz.tipo.value_counts().to_dict()}) + {len(top):,} finos; notas {len(notas):,}')

    menciones = []
    n_oraciones = n_fuera = 0
    for r in notas.itertuples():
        partes = [p.strip() for p in RE_ORACION.split(f'{r.titulo}.\n{r.texto}') if p and p.strip()]
        n_oraciones += len(partes)
        contexto, nota_fuera = None, False
        for k, orig in enumerate(partes):
            orig = unicodedata.normalize('NFC', orig)
            t = norm(orig)
            if HIPOTETICO.search(t):
                continue
            tipos = [tp for tp, (a, b, x) in TIPOS.items()
                     if a.search(t) and (b is None or b.search(t)) and (x is None or not x.search(t))]
            if not tipos:
                continue
            if OTRA_REGION.search(t) and not MISIONES.search(t):
                n_fuera += 1
                continue
            if contexto is None:   # el lugar más nombrado de la nota, una vez por nota
                todo = unicodedata.normalize('NFC', f'{r.titulo}\n{r.texto}')
                tn = norm(todo)
                # nota de otra región (la nombra y nunca dice «Misiones»): sin contexto, y sólo cuentan las oraciones
                # que nombran un lugar de Misiones (si no, el «lugar principal» sería un homónimo: Avellaneda, San Martín)
                nota_fuera = bool(OTRA_REGION.search(tn)) and not MISIONES.search(tn)
                c = Counter(m.group(1) for m in re_lug.finditer(para_nombres(tn)) if con_mayuscula(todo, m.start(1)) and m.group(1) not in hid)
                contexto = '' if nota_fuera else (c.most_common(1)[0][0] if c else '')
            rutas = [f"{'RN' if (m.group(1) == 'nacional' or m.group(0).startswith('rn')) else 'RP'} {m.group(2)}"
                     for m in RE_RUTA.finditer(t)]
            km = RE_KM.search(t)
            alt = RE_ALTURA.search(t) if RE_ALT_CTX.search(t) else None
            cauces, tramos = [], []
            tf = para_nombres(t)
            for m in re_cau.finditer(tf):
                cauces.append(m.group(1))
                tramos.append(m.span(1))
            lugares = [m.group(1) for m in re_lug.finditer(tf)
                       if not any(a <= m.start(1) < b for a, b in tramos) and con_mayuscula(orig, m.start(1))]
            # v3.1: un paraje detrás de otra palabra con mayúscula es un apellido («Héctor Manuel Piñeyro»), no un lugar
            topo = [m.group(1) for m in re_top.finditer(tf)
                    if not any(a <= m.start(1) < b for a, b in tramos) and con_mayuscula(orig, m.start(1))
                    and not re.search(r'[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+\s+$', orig[max(0, m.start(1) - 25):m.start(1)])]
            topo += [m.group(1) for m in re_bar.finditer(tf)]
            topo += [f'escuela {m.group(1)}' for m in RE_ESCUELA.finditer(t)]
            cand = []
            for m in RE_CANDIDATO.finditer(orig):
                nombre = re.sub(r'\s+(de|del|la|las|los)$', '', m.group(2))
                completo = clave(f'{m.group(1)} {nombre}')
                if (clave(nombre) not in conocidos and completo not in conocidos and clave(nombre) not in GENERICOS
                        and len(nombre) >= 4):
                    cand.append(f'{m.group(1).lower().replace("b°", "barrio").replace("bº", "barrio")} {nombre}')
            if nota_fuera and not (lugares or topo):
                n_fuera += 1
                continue
            fe, ffe = fecha_evento(t, r.fecha, orig)
            motivo = next((mo for mo, rx in MOTIVOS if rx.search(t)), 'otro')
            base = dict(nota_id=r.nota_id, n_oracion=k, oracion=orig, medio=r.medio, fecha_nota=r.fecha,
                        fecha_evento=fe, fecha_evento_fuente=ffe,
                        ruta=rutas[0] if rutas else None, km=km.group(1).replace(',', '.') if km else None,
                        altura_m=float(alt.group(1).replace(',', '.')) if alt else None,
                        hidrometro=next((x for x in lugares if x in hid), None),
                        cauces='|'.join(dict.fromkeys(cauces)) or None, lugares='|'.join(dict.fromkeys(lugares)) or None,
                        toponimos='|'.join(dict.fromkeys(topo)) or None, candidatos='|'.join(dict.fromkeys(cand)) or None,
                        contexto=contexto or None)
            for tp in tipos:
                menciones.append(dict(base, tipo=tp, motivo='agua' if tp in ('crecida', 'evacuados_aislados') else motivo))
    men = pd.DataFrame(menciones)
    log(f'oraciones {n_oraciones:,}; menciones {len(men):,}; descartadas por otra región {n_fuera:,} '
        f'({time.time() - t0:.0f} s)')

    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS noticias.menciones CASCADE')
    cur.execute('DROP TABLE IF EXISTS noticias.oraciones')
    cols = ['nota_id', 'n_oracion', 'oracion', 'medio', 'fecha_nota', 'fecha_evento', 'fecha_evento_fuente', 'tipo', 'motivo',
            'ruta', 'km', 'altura_m', 'hidrometro', 'cauces', 'lugares', 'toponimos', 'candidatos', 'contexto']
    cur.execute("""CREATE TABLE noticias.menciones (mencion_id serial PRIMARY KEY, nota_id int, n_oracion int, oracion text,
                   medio text, fecha_nota timestamp, fecha_evento date, fecha_evento_fuente text, tipo text, motivo text,
                   ruta text, km text, altura_m real, hidrometro text, cauces text, lugares text, toponimos text,
                   candidatos text, contexto text)""")
    buf = io.StringIO()
    men[cols].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert(f"COPY noticias.menciones ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute('CREATE INDEX ON noticias.menciones (tipo, fecha_evento)')
    cur.execute('CREATE INDEX ON noticias.menciones (nota_id)')
    cur.execute("COMMENT ON TABLE noticias.menciones IS 'Menciones de cortes de puente y de ruta/camino (con motivo: agua, "
                "protesta, accidente, obra), crecidas y evacuados/aislados por oración, con fecha de la nota y del evento "
                "y los nombres encontrados; ubicación en ubicar.py (pipeline/noticias/extraer.py, patrones v3)'")
    conn.commit()
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s; por tipo: {men.tipo.value_counts().to_dict()}; '
        f'motivo: {men.motivo.value_counts().to_dict()}; fecha: {men.fecha_evento_fuente.value_counts().to_dict()}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
