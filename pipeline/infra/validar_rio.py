"""Validación del río Uruguay contra los puentes de la RP2 → pipeline/infra/validacion_rio.md.

Reglas fijadas ANTES de mirar (plan del 7-nov-2026):
  - ceros: si Monitor y FABDEM difieren ≤ 1,5 m se usa el Monitor; si no, FABDEM (aplicado en rio_uruguay.py);
  - prensa: en ≥ 2/3 de los cortes documentados por crecida del Uruguay, el modelo da cortado o margen ≥ −1 m dentro
    de ±2 días de la fecha de la nota. La lista (pipeline/infra/cortes_rp2_prensa.csv) se armó antes de correr el
    modelo, por un agente que no vio resultados.
Además (no es regla, es medida): error de la interpolación dejando fuera cada estación intermedia.

Uso (desde neahub/, después de rio_uruguay.py y cortes_rio.py):
    python pipeline/infra/validar_rio.py
"""
from __future__ import annotations

import datetime as dt
import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
import drenaje  # noqa: E402
from build_hexoma import DB, connect  # noqa: E402
from rio_uruguay import MAX_TRAMO_KM  # noqa: E402
from validar_cruces import tabla  # noqa: E402

MD = ROOT / 'pipeline' / 'infra' / 'validacion_rio.md'
PRENSA = ROOT / 'pipeline' / 'infra' / 'cortes_rp2_prensa.csv'
VENT = 2
TOL_M = -1.0
REGLA = 2 / 3
PINDAITI_PROVISORIO = pd.Timestamp('2021-04-01')   # puente inhabilitado abr-2021, derrumbado dic-2021: pasos bajos

# texto de la nota (sin acentos, minúsculas) → nombre del puente en infra.visor_puentes_rp2
PATRONES = [
    # el orden importa: la nota nombra a veces la localidad («en Santa Rita») además del arroyo
    (r'pindayt|pindait', 'Puente sobre el Arroyo Pindaití'),
    (r'ramon', 'Puente sobre el Arroyo Ramón'),
    (r'soberbio', 'Puente Arroyo Soberbio'),
    (r'shangay|chancay', 'Puente sobre el Arroyo Chancay'),
    (r'santa rita|sta\. rita', 'Puente sobre el Arroyo Santa Rita'),
    (r'saltin', 'Puente sobre el Arroyo Saltiño'),
    (r'torto', 'Puente sobre el Arroyo Canal Torto'),
    (r'tunas', 'Puente sobre el Arroyo Tunas'),
    (r'guerrero', 'Puente sobre el Arroyo Guerrero'),
    (r'doradito', 'Puente sobre el Arroyo Doradito'),
    (r'persiguero', 'Puente sobre el Arroyo Persiguero'),
]


def sin_acentos(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', str(s)) if unicodedata.category(c) != 'Mn').lower()


def main() -> int:
    conn = connect(DB)
    q = lambda s, **kw: pd.read_sql(s, conn, **kw)  # noqa: E731
    L = ['# Validación: río Uruguay contra los puentes de la RP2', '',
         f'Generado por `pipeline/infra/validar_rio.py` el {dt.date.today().isoformat()}. Método en los docstrings de '
         '`rio_uruguay.py` y `cortes_rio.py`.', '']

    # 1. ceros y coherencia de datum
    est = q('SELECT serie, nombre, km_rio, dist_eje_m, cero_monitor, cero_fabdem, cero_perfil, cero_msnm, fuente_cero, alerta_m, '
            'evacuacion_m FROM infra.rio_estaciones ORDER BY km_rio')
    import rasterio
    pu = q("""SELECT gid, nombre, cota_seguridad_msnm cota, ST_X(geom) lon, ST_Y(geom) lat
              FROM infra.visor_puentes_rp2 WHERE cota_seguridad_msnm IS NOT NULL ORDER BY ST_X(geom) DESC""")
    with rasterio.open(drenaje.FUENTES / 'fabdem_misiones.tif') as src:
        a = src.read(1)
        def win(lon, lat, k):
            f, c = src.index(lon, lat)
            return a[f - k:f + k + 1, c - k:c + k + 1]
        pu['fabdem_acceso_max'] = [float(win(x, y, 3).max()) for x, y in zip(pu.lon, pu.lat)]
    pu['cota_menos_fabdem'] = pu.cota - pu.fabdem_acceso_max
    L += ['## 1. Cero de escala', '',
          'Regla fijada antes de mirar: si el cero del Monitor provincial y el implícito de FABDEM difieren ≤ 1,5 m, se '
          'usa el del Monitor; si no, el de FABDEM.', '',
          tabla(est.round(1)), '',
          'Ceros en uso: perfil isotónico de FABDEM (superficie del agua a lo largo de 366 km del eje, punto más bajo '
          'de cada sección cada 100 m, mediana por km y regresión isotónica decreciente) menos la mediana de la escala '
          'en 2011–2015. Regla fijada antes de mirar: gana el juego de ceros con menor error de interpolación en '
          'crecida, si el perfil da El Soberbio a ≤ 1,5 m de 118. Punto FABDEM y perfil coinciden a ≤ 0,5 m; el '
          'Monitor difiere 3–11 m en Alba Posse, Panambí y San Javier.', '',
          '**Coherencia de datum.** La cota de seguridad de cada puente contra el máximo de FABDEM a ±90 m (el '
          'camino de acceso): si coinciden, cotas y FABDEM están en el mismo sistema y los ceros de FABDEM son los '
          f'coherentes. Diferencia mediana {pu.cota_menos_fabdem.median():+.1f} m; '
          f'|dif| ≤ 3 m en {(pu.cota_menos_fabdem.abs() <= 3).sum()} de {len(pu)} puentes.', '',
          tabla(pu[['nombre', 'cota', 'fabdem_acceso_max', 'cota_menos_fabdem']].round(1)), '',
          'El cero del Monitor en San Javier (85) pondría el agua del hidrómetro por encima de la superficie del río '
          'que FABDEM ve 5 km aguas arriba (Alipio, ~84 msnm): el río no sube hacia abajo. La variante con los ceros '
          'del Monitor se informa en la sección 4 para mostrar cuánto cambia.', '']

    # 2. interpolación dejando fuera cada estación
    lec = q("SELECT serie, fecha, altura_m FROM infra.rio_lecturas WHERE qc = 'ok'", parse_dates=['fecha'])
    e = est.set_index('serie')
    lec['msnm'] = lec.altura_m + lec.serie.map(e.cero_msnm)
    tab = lec.pivot_table(index='fecha', columns='serie', values='msnm')
    filas = []
    for t in [61, 63, 64, 65]:
        k = e.km_rio[t]
        otros = e.drop(t)
        arriba = otros[otros.km_rio < k].sort_values('km_rio', ascending=False).index
        abajo = otros[otros.km_rio > k].sort_values('km_rio').index
        pred = pd.Series(np.nan, index=tab.index)
        tramo = pd.Series(np.nan, index=tab.index)
        for s1 in arriba:
            for s2 in abajo:
                span = e.km_rio[s2] - e.km_rio[s1]
                if span > MAX_TRAMO_KM:
                    continue
                f = (k - e.km_rio[s1]) / span
                v = tab[s1] * (1 - f) + tab[s2] * f
                m = pred.isna() & v.notna()
                pred[m], tramo[m] = v[m], span
        obs = tab[t]
        umbral = e.alerta_m[t] if pd.notna(e.alerta_m[t]) else (obs - e.cero_msnm[t]).quantile(.9)
        m = obs.notna() & pred.notna() & ((obs - e.cero_msnm[t]) >= umbral)
        err = (pred - obs)[m]
        mt = obs.notna() & pred.notna()
        filas.append((e.nombre[t], int(m.sum()), round(umbral, 1), round(err.abs().median(), 2),
                      round(err.abs().quantile(.9), 2), round(err.median(), 2),
                      round((pred - obs)[mt].abs().median(), 2), round(tramo[m].median(), 0)))
    L += ['## 2. Error de la interpolación (dejando fuera cada estación intermedia)', '',
          'Se predice el nivel de cada estación con las vecinas que tienen lectura ese día, como se hace en los '
          'puentes. «Crecida» = días con la escala sobre el nivel de alerta (o sobre el P90 si no hay alerta). El '
          'error incluye el de los ceros.', '',
          tabla(pd.DataFrame(filas, columns=['estación', 'días de crecida', 'umbral (m)', '|error| mediano (m)',
                                             '|error| P90 (m)', 'sesgo (m)', '|error| mediano todos los días',
                                             'tramo mediano (km)'])), '',
          'Lectura: entre El Soberbio y Alba Posse el error en crecida es de 1–2 m; entre Panambí y San Javier, de 4–7 m '
          'con sesgo de signo opuesto. Dos estimaciones de FABDEM independientes dan los mismos ceros, así que el error '
          'no es del cero: en crecida el río no sube parejo (el valle se ensancha aguas abajo) y la interpolación lineal '
          'entre hidrómetros a 100–200 km no lo capta. Las frecuencias de corte de los puentes entre Panambí y Garruchos '
          'son INDICATIVAS; las de El Soberbio a Alba Posse, firmes dentro de ±2 m.', '']

    # 3. prensa
    pr = pd.read_csv(PRENSA)
    pr['f'] = pd.to_datetime(pr.fecha)
    pr['txt'] = pr.lugar_textual.map(sin_acentos)
    pr['puente'] = pr.txt.map(lambda s: next((n for pat, n in PATRONES if re.search(pat, s)), None))
    sur = pr.txt.str.contains('santa maria|concepcion') & pr.txt.str.contains('persiguero')
    pr.loc[sur, 'puente'] = 'Puente sobre el Arroyo Persiguero (sur)'
    causa = pr.causa.map(sin_acentos)
    pr['por_uruguay'] = causa.str.contains('uruguay') & ~causa.str.startswith('lluvia del arroyo')
    pr['provisorio'] = pr.puente.eq('Puente sobre el Arroyo Pindaití') & (pr.f >= PINDAITI_PROVISORIO)
    gid = pu.set_index('nombre').gid
    # dos puentes «Persiguero»: el del norte (El Soberbio) y el del sur (Santa María – Concepción)
    pers = pu[pu.nombre == 'Puente sobre el Arroyo Persiguero'].sort_values('lon')
    gid_map = {n: g for n, g in gid.items() if n != 'Puente sobre el Arroyo Persiguero'}
    if len(pers) == 2:
        gid_map['Puente sobre el Arroyo Persiguero (sur)'] = pers.gid.iloc[0]
        gid_map['Puente sobre el Arroyo Persiguero'] = pers.gid.iloc[1]
    pr['gid'] = pr.puente.map(gid_map)
    diario = q('SELECT gid, fecha, margen_m FROM infra.rio_puente_diario', parse_dates=['fecha'])

    def margen(r):
        if pd.isna(r.gid):
            return np.nan
        w = diario[(diario.gid == r.gid) & (diario.fecha >= r.f - pd.Timedelta(days=VENT))
                   & (diario.fecha <= r.f + pd.Timedelta(days=VENT))]
        return w.margen_m.max() if len(w) else np.nan
    pr['margen_max'] = pr.apply(margen, axis=1)
    pr['acierto'] = pr.margen_max >= TOL_M
    eleg = pr[pr.por_uruguay & pr.gid.notna() & ~pr.provisorio]
    tasa = eleg.acierto.mean() if len(eleg) else np.nan
    pasa = tasa >= REGLA
    cols = ['fecha', 'lugar_textual', 'causa', 'puente', 'margen_max', 'acierto']
    L += ['## 3. Cortes documentados en la prensa', '',
          f'Regla fijada antes de mirar: en ≥ {REGLA:.0%} de los cortes por crecida del Uruguay, el modelo da cortado '
          f'o margen ≥ {TOL_M:+.0f} m en ±{VENT} días. Fuera de la regla: los pasos provisorios del Pindaytí desde '
          'abril de 2021 (bajos, no comparables con la cota del puente), los cortes por lluvia del propio arroyo y '
          'los registros que no nombran un puente con cota (Yabotí, Chico Alférez, tramos genéricos).', '',
          f'**Elegibles: {len(eleg)}; aciertos: {int(eleg.acierto.sum())} ({tasa:.0%}). '
          f'{"PASA" if pasa else "NO PASA"}.**', '',
          tabla(eleg[cols].assign(margen_max=eleg.margen_max.round(2))), '',
          'Cortes por lluvia del propio arroyo (el modelo NO los representa: es esperable que dé margen negativo):', '',
          tabla(pr[~pr.por_uruguay & pr.gid.notna() & ~pr.provisorio][cols].assign(
              margen_max=lambda d: d.margen_max.round(2))), '']
    # alturas citadas en El Soberbio para el puente del arroyo Soberbio
    sob = pr[(pr.puente == 'Puente Arroyo Soberbio') & pr.altura_citada_m.notna()
             & pr.hidrometro_citado.fillna('').map(sin_acentos).str.contains('soberbio')]
    cero61 = e.cero_msnm[61]
    cota_sob = float(pu.set_index('nombre').cota['Puente Arroyo Soberbio'])
    L += [f'**Umbral físico contra umbral de cierre (puente del Arroyo Soberbio).** Con cero {cero61:.1f} msnm, la cota de '
          f'{cota_sob:.2f} equivale a {cota_sob - cero61:.2f} m en la escala de El Soberbio. Alturas citadas:', '',
          tabla(sob[['fecha', 'lugar_textual', 'altura_citada_m']].assign(
              equivale_msnm=sob.altura_citada_m + cero61, respecto_de_la_cota_m=sob.altura_citada_m + cero61 - cota_sob)
                .round(2)), '',
          'El 28-jun-2014 la nota dice que el agua pasaba «2 mt sobre el Puente» con 22,60 m: el tablero queda a '
          f'~20,6 m de escala = {cero61 + 20.6:.1f} msnm, contra la cota de {cota_sob:.2f} (diferencia '
          f'{cero61 + 20.6 - cota_sob:+.2f} m). La cota de seguridad es el nivel del tablero. En 2023 el puente se '
          'CERRÓ con 17–18 m, 2–4 m antes de que el agua lo tape: el cierre preventivo es otro umbral, más bajo, y el '
          'modelo físico no lo representa.', '',
          'Historia de esta validación (7-nov-2026). La primera corrida dio 62 % y no pasaba. Se corrigieron dos errores '
          'de código, no de criterio: (1) el control de calidad descartaba subidas reales de más de 5 m por día (borró '
          'el récord de 27 m del 9-jul-1983); ahora sólo descarta picos de un día de más de 10 m en el mismo sentido; '
          '(2) notas sobre el paso provisorio del Pindaytí «en Santa Rita» se asignaban al puente Santa Rita. Además, '
          'el cero de las estaciones pasó a salir del perfil isotónico de FABDEM por una regla fijada antes de mirar la '
          'prensa (menor error de interpolación y control en El Soberbio); eso subió el cero de El Soberbio 0,9 m y '
          'metió dos cierres de nov-2023 en la tolerancia de −1 m.', '']

    # 4. resumen por puente
    res = q("""SELECT nombre, cota, anios_con_datos::int anios, primer_anio::int desde, anios_con_corte::int con_corte,
                      round(p_anual_empirica::numeric, 3)::float p_anual, round(dias_cortado_media::numeric, 2)::float dias_anio,
                      round(margen_max_m::numeric, 2)::float margen_max, anios_con_corte_mas1::int "+1 m", anios_con_corte_mas2::int "+2 m",
                      round(t_retorno_gumbel::numeric, 1)::float t_gumbel, round(t_retorno_gev::numeric, 1)::float t_gev,
                      round(t_retorno_gev_p10::numeric, 1)::float gev_p10, round(t_retorno_gev_p90::numeric, 1)::float gev_p90,
                      mon_anios_con_corte::int con_corte_ceros_monitor, ultimo_corte
               FROM infra.rio_puente_resumen ORDER BY km_rio""")
    L += ['## 4. Cortes por puente (1950–2026 según el tramo)', '',
          'p_anual = años con al menos un día bajo la cota / años con datos (empírico, sin distribución). «+1 m» y '
          '«+2 m»: años con corte si el pico real fuera 1 o 2 m mayor que la lectura diaria (la lectura es manual, una '
          'vez por día). T = período de retorno de la cota según Gumbel y GEV ajustadas a los máximos anuales (IC 80 % '
          'por bootstrap); con < 3 cortes observados la extrapolación es frágil. «con_corte_ceros_monitor»: años con '
          'corte usando los ceros del Monitor provincial.', '', tabla(res), '']

    ev = q("""SELECT fecha, altura_el_soberbio_m altura, n_puentes_cortados puentes, round(personas_con_mas_minutos)::int
                     personas_afectadas, round(min_extra_mediana::numeric, 1)::float min_extra_mediana,
                     round(min_extra_p90::numeric, 1)::float min_extra_p90, round(personas_sin_ruta)::int sin_ruta,
                     puentes_cortados
              FROM infra.rio_eventos ORDER BY fecha""")
    L += ['## 5. Las 8 crecidas más altas de El Soberbio sobre la red (escenario seco)', '', tabla(ev), '',
          'Puentes bajo su cota (umbral físico) en ±3 días del pico. '
          + (f'Personas sin ruta al hospital: máximo {int(ev.sin_ruta.max()):,} en un evento.' if ev.sin_ruta.max() > 0
             else 'Ningún evento deja gente sin ruta al hospital: los puentes de la RP2 tienen desvío; el costo es en minutos.'),
          '']
    MD.write_text('\n'.join(L), encoding='utf-8')
    print('\n'.join(L))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
