"""
Sustituye el area quemada cruda por el area quemada sobre vegetacion nativa lenosa
dentro del score de riesgo EUDR, para las provincias con cobertura MapBiomas.

    risk_score = 0,70 x perdida_post_2020 + 0,20 x FUEGO + 0,10 x (100 - cobertura_2020)

Donde exista `fire_native_post_2020_pct` (Misiones, Corrientes, Chaco, Formosa) FUEGO
pasa a ser el fuego filtrado; en el resto del area de estudio -Paraguay, sur de Brasil y
las demas provincias argentinas- el score queda intacto, porque alli no hay linea base
de cobertura cargada para filtrar. Es la misma asimetria ya documentada para la
distincion plantacion/nativo.

El peso del fuego NO cambia: solo cambia la entrada, para que la comparacion antes/despues
sea interpretable.

Antes de tocar nada, el script verifica que puede REPRODUCIR el score publicado a partir
del fuego crudo. Si esa reproduccion falla, la formula o los pesos no son los que se
usaron para generar la capa y el reemplazo seria invalido, asi que aborta.

Uso:
  python pipeline/apply_fire_native_to_risk.py [--dry-run]
"""
import argparse
import os
import shutil
import sys

import duckdb

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config_eudr import (WEIGHT_LOSS_POST_2020, WEIGHT_FIRE_POST_2020,
                         WEIGHT_NO_FOREST_2020)

EUDR = os.path.join(SCRIPT_DIR, "output", "eudr")
HIRES = os.path.join(EUDR, "hires")

# (capa servida, parquet de fuego nativo en la misma resolucion)
TARGETS = [
    (os.path.join(EUDR, "eudr_deforestation.parquet"),
     os.path.join(HIRES, "eudr_fire_native_res7.parquet")),
    (os.path.join(HIRES, "eudr_res9_combined.parquet"),
     os.path.join(HIRES, "eudr_fire_native_res9.parquet")),
    (os.path.join(HIRES, "eudr_ar_res7.parquet"),
     os.path.join(HIRES, "eudr_fire_native_res7.parquet")),
]

SCORE = ("round(least(greatest({w_loss} * least(e.loss_post_2020_pct, 100)"
         " + {w_fire} * least({fire}, 100)"
         " + {w_nf} * greatest(100 - e.forest_cover_2020, 0), 0), 100), 1)")

# Una celda H3 del borde puede figurar en la capa servida bajo una provincia vecina
# (Salta, Entre Rios, Nembucu...) y a la vez solaparse unos pocos pixeles con el area
# medida. Su fuego filtrado saldria de esa astilla y no representaria la celda, asi que
# el reemplazo se restringe a las provincias efectivamente cubiertas por la cobertura.
COVERED = ("ar_misiones", "ar_corrientes", "ar_chaco", "ar_formosa")


def score_expr(fire_col):
    return SCORE.format(w_loss=WEIGHT_LOSS_POST_2020, w_fire=WEIGHT_FIRE_POST_2020,
                        w_nf=WEIGHT_NO_FOREST_2020, fire=fire_col)


def check_reproduces(con, layer):
    """El score publicado tiene que salir de la formula con el fuego crudo."""
    q = f"""
    SELECT count(*) n,
           count(*) FILTER (WHERE abs({score_expr('e.fire_post_2020_pct')} - e.risk_score) > 0.11) malos,
           max(abs({score_expr('e.fire_post_2020_pct')} - e.risk_score)) peor
    FROM read_parquet('{layer}') e
    WHERE e.forest_cover_2020 IS NOT NULL
    """
    n, malos, peor = con.execute(q).fetchone()
    pct = 100.0 * malos / n if n else 0
    print(f"    reproduccion del score con fuego crudo: {n - malos:,}/{n:,} exactos "
          f"({pct:.3f}% fuera de tolerancia, peor desvio {peor:.2f})")
    return pct < 0.5


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="solo valida, no escribe")
    args = ap.parse_args()

    print(f"pesos: perdida {WEIGHT_LOSS_POST_2020} | fuego {WEIGHT_FIRE_POST_2020} | "
          f"sin bosque {WEIGHT_NO_FOREST_2020}")
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false;")

    for layer, fire in TARGETS:
        name = os.path.basename(layer)
        if not (os.path.exists(layer) and os.path.exists(fire)):
            print(f"  SALTEADA {name}: falta la capa o el fuego nativo")
            continue
        print(f"\n  {name}")
        if not check_reproduces(con, layer):
            print("    ABORTA: la formula no reproduce el score publicado.")
            return 1
        if args.dry_run:
            continue

        bak = layer + ".bak_fireraw"
        if not os.path.exists(bak):
            shutil.copy2(layer, bak)

        cols = [c[0] for c in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{layer}')").fetchall()]
        keep = [c for c in cols if c not in ("risk_score",)]
        sel = ", ".join(f"e.{c}" for c in keep)
        tmp = layer + ".tmp"
        cov = ", ".join(f"'{p}'" for p in COVERED)
        con.execute(f"""
        COPY (
          SELECT {sel},
                 CASE WHEN e.province IN ({cov}) THEN f.fire_native_post_2020_pct
                      ELSE CAST(NULL AS DOUBLE) END AS fire_native_post_2020_pct,
                 CASE WHEN e.province IN ({cov}) AND f.fire_native_post_2020_pct IS NOT NULL
                      THEN {score_expr('f.fire_native_post_2020_pct')}
                      ELSE e.risk_score END AS risk_score
          FROM read_parquet('{layer}') e
          LEFT JOIN read_parquet('{fire}') f USING (h3index)
          ORDER BY e.h3index
        ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 50000)
        """)
        os.replace(tmp, layer)

        r = con.execute(f"""
          SELECT province,
                 round(avg(fire_post_2020_pct),2) crudo,
                 round(avg(fire_native_post_2020_pct),2) nativo,
                 round(avg(risk_score),2) score
          FROM read_parquet('{layer}')
          WHERE fire_native_post_2020_pct IS NOT NULL
          GROUP BY 1 ORDER BY 4 DESC""").fetchall()
        for p in r:
            print(f"      {p[0]:<16} fuego {p[1]:6.2f} -> {p[2]:6.2f}   score {p[3]:.2f}")
        sin = con.execute(f"""SELECT count(*) FROM read_parquet('{layer}')
                              WHERE fire_native_post_2020_pct IS NULL""").fetchone()[0]
        print(f"      celdas sin filtrar (score intacto): {sin:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
