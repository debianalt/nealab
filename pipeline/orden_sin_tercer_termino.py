"""
Orden provincial del riesgo EUDR con y sin el tercer termino del score.

Por que existe este script
--------------------------
El score combina tres terminos (config_eudr.py):

  risk_score = 0,70 x perdida_post_2020 + 0,20 x FUEGO + 0,10 x (100 - cobertura_2000)

El tercero NO es "perdida previa al corte". Es la ausencia de cobertura arborea
en la linea base de 2000, que proviene del producto global y no de MapBiomas.
Un pastizal o un humedal naturales puntuan alto ahi sin que haya ocurrido
conversion alguna. Como los otros dos terminos se mueven en unidades
porcentuales y este ronda las decenas, domina el compuesto: sobre los 68.517
hexagonos del NEA aporta el 69,0 % del score medio pese a su peso nominal
del 10 %.

Cuidado al recalcular a mano: dividir cifras ya redondeadas a dos decimales
(6,76 / 9,79) da 69,1 %, y el valor sobre las medias sin redondear es 69,0 %.

Consecuencia: el orden provincial del indice publicado sigue, sobre todo, cuan
poco arbolada estaba cada provincia en 2000. Este script lo mide y reporta el
orden que resulta de excluir el termino y renormalizar los dos restantes, que
son los que miden lo que el indice dice medir.

El paso previo es la verificacion: si la formula no reproduce el score servido,
los pesos o las columnas no son los que generaron la capa y el contrafactico no
significaria nada. Misma logica que apply_fire_native_to_risk.py.

Alcance: las cuatro provincias del NEA, unicas con fuego filtrado por cobertura
nativa lenosa.

Reproducible desde un clon limpio: si no encuentra el parquet local -que vive
bajo pipeline/output/ y esta gitignoreado- descarga la MISMA capa que sirve el
visor, publica y sin credenciales. Verificado: las dos rutas dan identicos los
cuatro promedios provinciales y el total.

Uso:
  python pipeline/orden_sin_tercer_termino.py
"""
import io
import os
import sys
import urllib.request

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config_eudr import (WEIGHT_LOSS_POST_2020, WEIGHT_FIRE_POST_2020,
                         WEIGHT_NO_FOREST_2020)

LOCAL = os.path.join(SCRIPT_DIR, "output", "eudr", "hires", "eudr_ar_res7.parquet")
SERVIDA = "https://cdn.spatia.ar/data/eudr/eudr_deforestation.parquet"
NEA = ["ar_misiones", "ar_chaco", "ar_corrientes", "ar_formosa"]
NOMBRE = {"ar_misiones": "Misiones", "ar_chaco": "Chaco",
          "ar_corrientes": "Corrientes", "ar_formosa": "Formosa"}
# el score servido se redondea a un decimal, asi que la reproduccion tolera medio paso
TOL = 0.11


def cargar_capa():
    """Parquet local si esta; si no, el que sirve el visor."""
    if os.path.exists(LOCAL):
        return pd.read_parquet(LOCAL), os.path.relpath(LOCAL, SCRIPT_DIR)
    print(f"Sin parquet local, descargando {SERVIDA}")
    # el CDN rechaza con 403 el user-agent por defecto de urllib
    req = urllib.request.Request(SERVIDA, headers={"User-Agent": "nealab-pipeline"})
    with urllib.request.urlopen(req, timeout=180) as resp:
        return pd.read_parquet(io.BytesIO(resp.read())), SERVIDA


def main():
    d, origen = cargar_capa()
    d = d[d["province"].isin(NEA) & d["forest_cover_2020"].notna()].copy()
    print(f"Capa: {origen}  ({len(d):,} hexagonos res-7)")

    loss = d["loss_post_2020_pct"].clip(upper=100)
    fire = d["fire_native_post_2020_pct"].clip(upper=100)
    no_forest = (100 - d["forest_cover_2020"]).clip(lower=0)

    d["t_perdida"] = WEIGHT_LOSS_POST_2020 * loss
    d["t_fuego"] = WEIGHT_FIRE_POST_2020 * fire
    d["t_sin_arboles"] = WEIGHT_NO_FOREST_2020 * no_forest

    # 1. reproducir el score servido antes de tocar nada
    rep = (d.t_perdida + d.t_fuego + d.t_sin_arboles).clip(0, 100).round(1)
    malos = int((rep - d["risk_score"]).abs().gt(TOL).sum())
    peor = float((rep - d["risk_score"]).abs().max())
    print(f"Reproduccion del score servido: {len(d) - malos:,}/{len(d):,} exactos "
          f"(peor desvio {peor:.2f})")
    if malos / len(d) > 0.005:
        raise SystemExit("La formula no reproduce la capa servida: revisar pesos y columnas.")

    # 2. contrafactico: sin el tercer termino, renormalizando los otros dos a peso 1
    peso_restante = WEIGHT_LOSS_POST_2020 + WEIGHT_FIRE_POST_2020
    # se reporta el score tal como esta servido, no el reproducido: es el que
    # cita el informe. La diferencia entre ambos es el redondeo a un decimal.
    d["score_publicado"] = d["risk_score"]
    d["score_sin_tercero"] = ((d.t_perdida + d.t_fuego) / peso_restante).clip(0, 100).round(1)

    g = d.groupby("province").agg(
        n=("h3index", "size"),
        publicado=("score_publicado", "mean"),
        sin_tercero=("score_sin_tercero", "mean"),
        aporte_tercero=("t_sin_arboles", "mean"),
        perdida_pct=("loss_post_2020_pct", "mean"),
        fuego_nativo_pct=("fire_native_post_2020_pct", "mean"),
        cobertura_2000=("forest_cover_2020", "mean"),
    )
    g["peso_tercero_pct"] = 100 * g.aporte_tercero / g.publicado
    g["orden_publicado"] = g.publicado.rank(ascending=False).astype(int)
    g["orden_sin_tercero"] = g.sin_tercero.rank(ascending=False).astype(int)
    g = g.sort_values("publicado", ascending=False)

    print(f"\n{'Provincia':<12}{'n':>8}{'publicado':>11}{'orden':>7}"
          f"{'sin 3er':>9}{'orden':>7}{'peso 3er':>10}{'cob.2000':>10}")
    for prov, r in g.iterrows():
        print(f"{NOMBRE[prov]:<12}{r.n:>8,.0f}{r.publicado:>11.2f}{r.orden_publicado:>7.0f}"
              f"{r.sin_tercero:>9.2f}{r.orden_sin_tercero:>7.0f}"
              f"{r.peso_tercero_pct:>9.1f}%{r.cobertura_2000:>9.1f}%")

    tot_pub = d.score_publicado.mean()
    tot_sin = d.score_sin_tercero.mean()
    tot_peso = 100 * d.t_sin_arboles.mean() / tot_pub
    print(f"{'Total':<12}{len(d):>8,}{tot_pub:>11.2f}{'':>7}{tot_sin:>9.2f}{'':>7}"
          f"{tot_peso:>9.1f}%{d.forest_cover_2020.mean():>9.1f}%")

    subio = g.index[(g.orden_sin_tercero < g.orden_publicado)]
    bajo = g.index[(g.orden_sin_tercero > g.orden_publicado)]
    print("\nSuben al excluir el termino: " + ", ".join(NOMBRE[p] for p in subio))
    print("Bajan: " + ", ".join(NOMBRE[p] for p in bajo))


if __name__ == "__main__":
    main()
