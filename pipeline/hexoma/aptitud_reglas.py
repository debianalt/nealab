"""A4 — umbrales validados (umbrales_cultivos.md, 1-nov-2026) en forma de código.

Cada factor: (id, variable, tipo, parámetros, fuente). Clases: 1 = S1, 2 = S2, 3 = S3, 4 = N.
Tipos:
    rango   s1 = (lo, hi), lim = (lo, hi): S1 dentro de s1; S2 dentro de lim; N fuera de lim. None = sin cota.
    min     s1 = valor mínimo para S1; s2 = mínimo para S2 (None = todo lo demás es S2).
    cero    S1 si el conteo es 0; si no, S2 (heladas por estado: las fuentes no dan un límite de exclusión).
    max_n   N si el valor ≥ umbral (anegamiento); si no, S1.
    pend    pendiente (%): S1 ≤ s1 (óptimo propio o 8 de Sys); S2 ≤ s2 (16 de Sys, o el propio); S3 ≤ 30; N > 30.
    hidro   arroz: S1 si inundable ≥ 50 % o arcilla ≥ 35 %; si no, S2.
Variables climáticas por píxel ERA5 y campaña (aptitud_clima.py) o por par CHIRPS×ERA5 (p_mm); de suelo por celda.

Reglas del autor que vinculan (meta.umbrales_reglas): factor más limitante; calor sin clase; arroz «sin dato de agua
de riego»; campo natural sin aptitud; pendiente de Sys con el óptimo propio (ninguna propia excluye por sí sola).
Decisiones de aplicación tomadas al codificar (se informan en la validación):
- Cítricos y pasturas: el rango validado es de temperatura «de crecimiento»; con la ventana aprobada (año) se compara
  con la media anual, que en el NEA queda debajo del óptimo: la temperatura los deja en S2 en casi todo el NEA.
- Arroz: por decenios de ene, feb y 1.º de mar (Steinmetz & Amaral 2002): S1 si algún decenio no tiene noches ≤ 17 °C;
  S2 si alguno no tiene noches ≤ 15 °C; N si todos tienen alguna. Contar una sola noche fría en dos meses (primera
  versión) daba N en el 70–85 % de las campañas, igual que las estaciones, y contradecía las 95 mil ha sembradas.
- Arroz, pendiente: S1 ≤ 1 % (RENARE); S2 ≤ 3 % (RENARE); S3 ≤ 30 %; N > 30 % (Sys excluye).
"""
INF = float('inf')

CICLOS = {
    'soja': 'nov–mar', 'maiz': 'sep–feb', 'algodon': 'oct–mar', 'girasol': 'ago–dic',
    'arroz': 'ene–feb (frío y calor)', 'yerba': 'año (campaña)', 'te': 'año (campaña)', 'citricos': 'año (campaña)',
    'pasturas': 'año (campaña)',
}
NOMBRES = {'yerba': 'Yerba mate', 'te': 'Té', 'citricos': 'Cítricos (naranja)', 'soja': 'Soja', 'maiz': 'Maíz',
           'algodon': 'Algodón', 'girasol': 'Girasol', 'arroz': 'Arroz (sin dato de agua de riego)',
           'pasturas': 'Pasturas megatérmicas'}
ROTULO = {'arroz': 'sin dato de agua de riego'}

REGLAS = {
    'yerba': [
        ('lluvia', 'p_mm', 'rango', dict(s1=(1500, 2500), lim=(1200, None)), 'Zelada & González 2019; Oliveira & Rotta 1985; Carvalho 2003'),
        ('t_media', 'tm_anual', 'rango', dict(s1=(20, 23), lim=(15.5, 25.5)), 'Oliveira & Rotta 1985; Zelada & González 2019'),
        ('helada_severa', 'tmin_abs', 'rango', dict(s1=(-6, None), lim=(-11.6, None)), 'ECOCROP KTMPR; Carvalho 2003'),
        ('helada_brotacion', 'heladas5_sep_oct', 'cero', {}, 'ECOCROP KTMP (brotación)'),
        ('ph', 'ph', 'rango', dict(s1=(4.5, 5.5), lim=(4.0, None)), 'Zelada & González 2019; Embrapa 2014'),
        ('arcilla', 'arcilla', 'rango', dict(s1=(35, None), lim=(15, None)), 'Oliveira & Rotta 1985; Carvalho 2003'),
        ('anegamiento', 'inundable', 'max_n', dict(n=50), 'Oliveira & Rotta 1985; Embrapa 2014'),
        ('pendiente', 'pendiente', 'pend', dict(s1=4, s2=16), 'Burtnik 2006 (óptimo); Sys et al. 1993'),
    ],
    'te': [
        ('lluvia', 'p_mm', 'rango', dict(s1=(1800, 2200), lim=(1000, None)), 'Parra (MAGyP); Prat Kricun et al. 2020; ECOCROP'),
        ('t_media', 'tm_anual', 'rango', dict(s1=(20, 30), lim=(8, 35)), 'ECOCROP'),
        ('helada_brotacion', 'heladas5_sep_oct', 'cero', {}, 'ECOCROP KTMP (brotación)'),
        ('helada_severa', 'tmin_abs', 'rango', dict(s1=(-5, None), lim=(None, None)), 'ECOCROP KTMPR'),
        ('ph', 'ph', 'rango', dict(s1=(4.5, 5.5), lim=(4.0, 6.0)), 'Parra (MAGyP); ECOCROP'),
        ('anegamiento', 'inundable', 'max_n', dict(n=50), 'Crane & Balerdi (EDIS)'),
        ('pendiente', 'pendiente', 'pend', dict(s1=17.6, s2=17.6), 'Mwesige et al. 2026 (óptimo 5–10°); Sys et al. 1993'),
    ],
    'citricos': [
        ('lluvia', 'p_mm', 'rango', dict(s1=(1200, 2000), lim=(450, None)), 'INTA EEA Concordia; ECOCROP'),
        ('t_media', 'tm_anual', 'rango', dict(s1=(23, 34), lim=(12.8, 37)), 'INTA EEA Concordia; Oliveira et al. 2012'),
        ('helada_floracion', 'heladas5_sep_oct', 'cero', {}, 'INTA EEA Concordia (−1 °C en flor)'),
        ('helada_severa', 'tmin_abs', 'rango', dict(s1=(-3, None), lim=(-8, None)), 'INTA EEA Concordia; Oliveira et al. 2012'),
        ('ph', 'ph', 'rango', dict(s1=(5.0, 6.0), lim=(4.0, 8.3)), 'ECOCROP (naranja)'),
        ('pendiente', 'pendiente', 'pend', dict(s1=5, s2=16), 'Veloso et al. 2000 (óptimo); Sys et al. 1993'),
    ],
    'soja': [
        ('t_media', 'tm_nov_mar', 'rango', dict(s1=(20, 33), lim=(10, 38)), 'ECOCROP'),
        ('lluvia', 'p_mm', 'rango', dict(s1=(600, 1500), lim=(450, None)), 'ECOCROP; INTA Las Breñas'),
        ('libre_heladas', 'libre_heladas', 'min', dict(s1=130, s2=100), 'FAO Crop info (ciclo)'),
        ('ph', 'ph', 'rango', dict(s1=(5.5, 6.5), lim=(4.5, 8.4)), 'ECOCROP'),
        ('arcilla', 'arcilla', 'rango', dict(s1=(20, 60), lim=(None, None)), 'Verdoodt & Van Ranst 2003'),
        ('anegamiento', 'inundable', 'max_n', dict(n=50), 'FAO 66'),
        ('pendiente', 'pendiente', 'pend', dict(s1=8, s2=16), 'Sys et al. 1993'),
    ],
    'maiz': [
        ('t_media', 'tm_sep_feb', 'rango', dict(s1=(18, 33), lim=(10, 47)), 'ECOCROP'),
        ('lluvia', 'p_mm', 'rango', dict(s1=(600, 1200), lim=(400, None)), 'ECOCROP'),
        ('ph', 'ph', 'rango', dict(s1=(5.0, 7.0), lim=(4.5, 8.5)), 'ECOCROP'),
        ('pendiente', 'pendiente', 'pend', dict(s1=8, s2=16), 'Sys et al. 1993'),
    ],
    'algodon': [
        ('t_media', 'tm_oct_mar', 'rango', dict(s1=(22, 36), lim=(15, 42)), 'ECOCROP'),
        ('grados_dia', 'gdd12_oct_mar', 'min', dict(s1=1645, s2=None), 'Colombo 2019 (INTA Reconquista)'),
        ('libre_heladas', 'libre_heladas', 'min', dict(s1=200, s2=None), 'FAO Crop info'),
        ('lluvia', 'p_mm', 'rango', dict(s1=(750, 1200), lim=(450, None)), 'ECOCROP'),
        ('ph', 'ph', 'rango', dict(s1=(6.0, 7.5), lim=(5.0, 9.5)), 'ECOCROP'),
        ('pendiente', 'pendiente', 'pend', dict(s1=8, s2=16), 'Sys et al. 1993'),
    ],
    'girasol': [
        ('t_media', 'tm_ago_dic', 'rango', dict(s1=(17, 34), lim=(5, 45)), 'ECOCROP'),
        ('grados_dia', 'gdd7_ago_dic', 'min', dict(s1=1600, s2=None), 'ASAGIR'),
        ('helada_tardia', 'heladas5_oct', 'cero', {}, 'ASAGIR (6–8 hojas y botón)'),
        ('lluvia', 'p_mm', 'rango', dict(s1=(600, 1000), lim=(300, None)), 'ECOCROP; INTA Las Breñas'),
        ('ph', 'ph', 'rango', dict(s1=(6.0, 7.5), lim=(5.5, 8.0)), 'ECOCROP'),
        ('pendiente', 'pendiente', 'pend', dict(s1=8, s2=16), 'Sys et al. 1993'),
    ],
    'arroz': [
        ('frio_microsporogenesis', 'frio_decendio', 'rango', dict(s1=(None, 0), lim=(None, 1)), 'SOSBAI 2025; Steinmetz et al. 2004; Steinmetz & Amaral 2002 (decenios)'),
        ('ph', 'ph', 'rango', dict(s1=(5.5, 7.0), lim=(4.5, 9.0)), 'ECOCROP; SOSBAI'),
        ('suelo_hidromorfico', 'inundable', 'hidro', {}, 'SOSBAI 2025'),
        ('pendiente', 'pendiente', 'pend', dict(s1=1, s2=3), 'RENARE (Uruguay); Sys et al. 1993'),
    ],
    'pasturas': [
        ('t_media', 'tm_anual', 'rango', dict(s1=(28, 35), lim=(15, None)), 'Veneciano et al. 2002 (INTA)'),
        ('lluvia', 'p_mm', 'rango', dict(s1=(800, None), lim=(500, None)), 'Pérego 1999; Mc Lean et al. 2015; INTA Las Breñas 2005'),
        ('pendiente', 'pendiente', 'pend', dict(s1=8, s2=16), 'Sys et al. 1993'),
    ],
}
# frio_decendio codifica el frío del arroz por decenios (aptitud_clima.py): 0 = algún decenio sin noches ≤ 17 °C; 1 = alguno sin ≤ 15 °C; 2 = ninguno
CALOR = {'soja': 'dias35_nov_mar', 'maiz': 'dias35_sep_feb', 'algodon': 'dias35_oct_mar', 'girasol': 'dias35_ago_dic',
         'arroz': 'dias35_ene_feb'}
VAR_SUELO = {'ph', 'arcilla', 'inundable', 'pendiente'}


def clasificar(tipo: str, p: dict, x, arcilla=None):
    """Clase 1–4 por elemento (numpy). NaN → 0 (sin dato)."""
    import numpy as np
    x = np.asarray(x, dtype=float)
    c = np.zeros(x.shape, np.int8)
    ok = ~np.isnan(x)

    def dentro(r):
        lo, hi = r
        m = np.ones(x.shape, bool)
        if lo is not None:
            m &= x >= lo
        if hi is not None:
            m &= x <= hi
        return m
    if tipo == 'rango':
        c[ok] = 4
        c[ok & dentro(p['lim'])] = 2
        c[ok & dentro(p['s1'])] = 1
    elif tipo == 'min':
        c[ok] = 2 if p.get('s2') is None else 4
        if p.get('s2') is not None:
            c[ok & (x >= p['s2'])] = 2
        c[ok & (x >= p['s1'])] = 1
    elif tipo == 'cero':
        c[ok] = np.where(x[ok] > 0, 2, 1)
    elif tipo == 'max_n':
        c[ok] = np.where(x[ok] >= p['n'], 4, 1)
    elif tipo == 'pend':
        c[ok] = 4
        c[ok & (x <= 30)] = 3
        c[ok & (x <= max(p['s2'], p['s1']))] = 2
        c[ok & (x <= p['s1'])] = 1
    elif tipo == 'hidro':
        a = np.asarray(arcilla, dtype=float)
        c[ok] = 2
        c[ok & ((x >= 50) | (a >= 35))] = 1
    return c
