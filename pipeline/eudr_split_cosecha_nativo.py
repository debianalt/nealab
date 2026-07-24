"""
Separa la perdida forestal posterior al corte EUDR en cosecha de plantacion y
conversion de vegetacion nativa, atribuyendo cada pixel a la cobertura que
MapBiomas registra en el anio de corte.

Por que existe este script
--------------------------
Hansen detecta la desaparicion de cobertura arborea pero no su causa: cosechar
una plantacion de pino y desmontar un bosque nativo producen la misma senal.
Para el Reglamento (UE) 2023/1115 lo primero es admisible y lo segundo no.
MapBiomas Argentina tiene una clase propia de silvicultura, separada de las
formaciones nativas; leida en 2020 aporta la linea base que Hansen no trae.

Este script produce las cifras titulares del informe EUDR: el reparto
cosecha/nativo de la perdida 2021-2025, su estabilidad ante el anio final de la
serie, y la superficie que paso de nativa en 2020 a plantacion en 2024.

Metodo
------
Atribucion a nivel de PIXEL, en la resolucion nativa de ambos productos (30 m).
Cada pixel de perdida se cruza con la clase de cobertura de 2020 y se cuenta
sobre la geometria provincial. No usa la grilla H3 y no promedia composiciones
dentro de una unidad de agregacion, de modo que el resultado no depende de como
se distribuya la perdida dentro de ella.

El denominador del reparto es la perdida sobre plantacion mas la perdida sobre
nativo; la perdida sobre otras clases (agricultura, pastizal) queda fuera porque
no es interpretable bajo la regla del reglamento.

`bestEffort=True` esta puesto como red de seguridad, pero con maxPixels=1e12 no
llega a activarse: la provincia mas extensa ronda 1,1e8 pixeles a 30 m, asi que
la reduccion corre a la escala pedida y no a una degradada.

Uso:
  python pipeline/eudr_split_cosecha_nativo.py
"""
import json
import os
import sys

import ee

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config_eudr import (BOUNDARY_PATH, HANSEN_ASSET, MAPBIOMAS_C2_ASSET,
                         MB_CLASS_FORESTRY, MB_CLASSES_NATIVE)

# 30 m x 30 m = 900 m2 = 0,09 ha
PIX_HA = 0.09
# Codigo lossyear de Hansen: 21 = 2021, primer anio posterior al corte EUDR.
FIRST_LOSS_CODE = 21
# Anio final alternativo, para el chequeo de estabilidad de la serie.
PENULTIMATE_LOSS_CODE = 24

BANDS = ("p25", "n25", "p24", "n24", "conv", "plant20")


def build_stack():
    """Apila las mascaras binarias que se suman por provincia."""
    cov = ee.Image(MAPBIOMAS_C2_ASSET)
    cov20 = cov.select("classification_2020")
    cov24 = cov.select("classification_2024")

    plant20 = cov20.eq(MB_CLASS_FORESTRY)
    plant24 = cov24.eq(MB_CLASS_FORESTRY)
    nat20 = cov20.eq(MB_CLASSES_NATIVE[0])
    for code in MB_CLASSES_NATIVE[1:]:
        nat20 = nat20.Or(cov20.eq(code))

    lossyear = ee.Image(HANSEN_ASSET).select("lossyear").unmask(0)
    loss25 = lossyear.gte(FIRST_LOSS_CODE)
    loss24 = loss25.And(lossyear.lte(PENULTIMATE_LOSS_CODE))

    return (loss25.And(plant20).rename("p25")
            .addBands(loss25.And(nat20).rename("n25"))
            .addBands(loss24.And(plant20).rename("p24"))
            .addBands(loss24.And(nat20).rename("n24"))
            .addBands(nat20.And(plant24).rename("conv"))
            .addBands(plant20.rename("plant20")))


def share(plantation, native):
    """Porcentaje de la perdida atribuible a cosecha de plantacion."""
    total = plantation + native
    return 100.0 * plantation / total if total else 0.0


def main():
    if not os.path.exists(BOUNDARY_PATH):
        print("ERROR: falta el geojson de limites provinciales en %s"
              % BOUNDARY_PATH)
        return 1

    ee.Initialize()
    stack = build_stack()

    with open(BOUNDARY_PATH, encoding="utf-8") as fh:
        boundaries = json.load(fh)

    header = "%-12s %8s %8s %10s %12s" % (
        "provincia", "cos_25", "cos_24", "conv_ha", "plant20_ha")
    print(header)
    print("-" * len(header), flush=True)

    totals = {band: 0.0 for band in BANDS}
    for feature in boundaries["features"]:
        name = feature["properties"]["name"]
        geom = ee.Geometry(feature["geometry"])
        reduced = stack.reduceRegion(
            ee.Reducer.sum(), geom, scale=30, maxPixels=1e12, bestEffort=True
        ).getInfo()
        values = {band: (reduced.get(band) or 0.0) for band in BANDS}
        for band in BANDS:
            totals[band] += values[band]
        print("%-12s %7.1f%% %7.1f%% %10.0f %12.0f"
              % (name,
                 share(values["p25"], values["n25"]),
                 share(values["p24"], values["n24"]),
                 values["conv"] * PIX_HA,
                 values["plant20"] * PIX_HA), flush=True)

    print("-" * len(header))
    print("%-12s %7.1f%% %7.1f%% %10.0f %12.0f"
          % ("TOTAL",
             share(totals["p25"], totals["n25"]),
             share(totals["p24"], totals["n24"]),
             totals["conv"] * PIX_HA,
             totals["plant20"] * PIX_HA))
    print()
    print("cos_25  = % de la perdida 2021-2025 sobre plantacion 2020 (resto: nativo)")
    print("cos_24  = idem restringido a 2021-2024 (chequeo de estabilidad)")
    print("conv_ha = hectareas nativo(2020) -> plantacion(2024)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
