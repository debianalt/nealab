"""Alturas diarias de los hidrómetros del Paraná y del Iguazú del visor de emergencias (fase 4: sólo datos).

Mismas series del INA (API pública a5, escalas de Prefectura u otras redes) y mismo control de calidad que
rio_uruguay.py (marca sin borrar: fuera de rango, pico de un día > 10 m en el mismo sentido). Sin ceros ni modelo.

  infra.rio_estaciones_otras   serie, nombre, río, punto, niveles de alerta/evacuación/aguas bajas del visor,
                               lecturas válidas, primera y última fecha
  infra.rio_lecturas_otras     serie, fecha, altura_m, qc

Tablas aparte de las del Uruguay porque rio_uruguay.py reescribe las suyas.

Uso (desde neahub/, después de importar_visor.py):
    python pipeline/infra/rios_parana_iguazu.py [--rebajar]
"""
from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from build_hexoma import DB, connect, log  # noqa: E402
from rio_uruguay import bajar_serie, qc  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--rebajar', action='store_true')
    a = ap.parse_args()
    conn = connect(DB)
    est = pd.read_sql("""SELECT series_id serie, nombre, rio, ST_X(geom) lon, ST_Y(geom) lat, nivel_alerta_m,
                                nivel_evacuacion_m, nivel_aguas_bajas_m, tabla
                         FROM infra.visor_hidrometros WHERE rio <> 'URUGUAY' AND series_id IS NOT NULL""", conn)
    lect = []
    for s in est.serie:
        try:
            lect.append(bajar_serie(int(s), a.rebajar))
        except Exception as e:  # noqa: BLE001
            log(f'  serie {s}: {str(e)[:100]}')
    lect = qc(pd.concat(lect, ignore_index=True))
    ok = lect[lect.qc == 'ok'].groupby('serie').agg(lecturas=('fecha', 'size'), desde=('fecha', 'min'),
                                                    hasta=('fecha', 'max'), max_m=('altura_m', 'max'))
    est = est.merge(ok, left_on='serie', right_index=True, how='left')
    log('\n' + est[['serie', 'nombre', 'rio', 'lecturas', 'desde', 'hasta', 'max_m', 'nivel_alerta_m']].to_string(index=False))
    cur = conn.cursor()
    for t, df, ddl in [
        ('infra.rio_estaciones_otras', est,
         'serie int PRIMARY KEY, nombre text, rio text, lon double precision, lat double precision, nivel_alerta_m real, '
         'nivel_evacuacion_m real, nivel_aguas_bajas_m real, tabla text, lecturas int, desde date, hasta date, max_m real'),
        ('infra.rio_lecturas_otras', lect[['serie', 'fecha', 'altura_m', 'qc']], 'serie int, fecha date, altura_m real, qc text')]:
        cur.execute(f'DROP TABLE IF EXISTS {t}')
        cur.execute(f'CREATE TABLE {t} ({ddl})')
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep='\\N', date_format='%Y-%m-%d')
        buf.seek(0)
        cur.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute("COMMENT ON TABLE infra.rio_lecturas_otras IS 'Alturas diarias (INA a5) de los hidrómetros del Paraná e "
                "Iguazú del visor de Misiones; máximo del día; qc marca sin borrar (rios_parana_iguazu.py)'")
    cur.execute('CREATE INDEX ON infra.rio_lecturas_otras (serie, fecha)')
    cur.execute("""INSERT INTO infra.fuentes VALUES ('infra.rio_lecturas_otras', 'INA – API a5 (alerta.ina.gob.ar)',
                   'https://alerta.ina.gob.ar/a5/obs/puntual/observaciones', current_date::text, now(), %s,
                   'Paraná e Iguazú, series del visor de emergencias')
                   ON CONFLICT (tabla) DO UPDATE SET cargado=now(), n=EXCLUDED.n""", (int((lect.qc == 'ok').sum()),))
    conn.commit()
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
