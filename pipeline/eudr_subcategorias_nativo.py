"""
Desagrega la perdida posterior al corte que recae sobre vegetacion nativa en las
subcategorias de MapBiomas, y la contrasta con la composicion de la cobertura
de 2020.

Por que existe este script
--------------------------
Decir que el 77,2 % de la perdida cae sobre "vegetacion nativa" deja abierta la
pregunta de que vegetacion es. MapBiomas la separa en formacion forestal y
formacion sabanica, y la distincion importa: el Gran Chaco combina bosque seco
denso con formaciones abiertas, y una caracterizacion que circulaba como obvia
-que Chaco y Corrientes son mayormente monte o sabana- no resiste el dato.
MapBiomas clasifica el bosque seco chaqueno como forestal, y Corrientes no es
monte sino humedal y pastizal.

  clase 3   = formacion forestal (bosque y selva, incluido el chaqueno seco)
  clases 4+6 = formacion sabanica (monte, bosque inundable)

La clase 5 (manglar) no tiene presencia en Argentina -cero pixeles, verificado
sobre el raster-, de modo que el conjunto nativo {3,4,6} equivale a {3,4,5,6}.

Metodo
------
Atribucion por pixel a 30 m sobre la geometria provincial, igual que
eudr_split_cosecha_nativo.py: se cuentan pixeles de perdida por clase de
cobertura de 2020 y se convierten a hectareas. La columna de cobertura da la
superficie de cada subcategoria en 2020, para leer la perdida contra su base.

Uso:
  python pipeline/eudr_subcategorias_nativo.py
"""
import json
import os
import sys

import ee

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config_eudr import (BOUNDARY_PATH, HANSEN_ASSET, MAPBIOMAS_C2_ASSET,
                         MB_CLASS_FOREST, MB_CLASS_FORESTRY,
                         MB_CLASSES_SAVANNA)

PIX_HA = 0.09
FIRST_LOSS_CODE = 21

# Las cuatro provincias con linea base de cobertura cargada.
PROVINCES = ("Misiones", "Corrientes", "Chaco", "Formosa")
BANDS = ("l_for", "l_sav", "l_pla", "c_for", "c_sav")


def build_stack():
    cov20 = ee.Image(MAPBIOMAS_C2_ASSET).select("classification_2020")
    forest20 = cov20.eq(MB_CLASS_FOREST)
    savanna20 = cov20.eq(MB_CLASSES_SAVANNA[0])
    for code in MB_CLASSES_SAVANNA[1:]:
        savanna20 = savanna20.Or(cov20.eq(code))
    plant20 = cov20.eq(MB_CLASS_FORESTRY)

    loss = ee.Image(HANSEN_ASSET).select("lossyear").unmask(0).gte(FIRST_LOSS_CODE)

    return (loss.And(forest20).rename("l_for")
            .addBands(loss.And(savanna20).rename("l_sav"))
            .addBands(loss.And(plant20).rename("l_pla"))
            .addBands(forest20.rename("c_for"))
            .addBands(savanna20.rename("c_sav")))


def forest_share(values):
    """Share de la perdida NATIVA que recae sobre formacion forestal."""
    native = values["l_for"] + values["l_sav"]
    return 100.0 * values["l_for"] / native if native else 0.0


def emit(name, values):
    print("%-12s %10.0f %10.0f %7.1f%% | %10.0f %10.0f"
          % (name,
             values["l_for"] * PIX_HA,
             values["l_sav"] * PIX_HA,
             forest_share(values),
             values["c_for"] * PIX_HA,
             values["c_sav"] * PIX_HA), flush=True)


def main():
    if not os.path.exists(BOUNDARY_PATH):
        print("ERROR: falta el geojson de limites provinciales en %s"
              % BOUNDARY_PATH)
        return 1

    ee.Initialize()
    stack = build_stack()

    with open(BOUNDARY_PATH, encoding="utf-8") as fh:
        boundaries = json.load(fh)

    print("%-12s | %-30s | %-21s"
          % ("", "PERDIDA sobre nativo (2021-25)", "COBERTURA nativa 2020"))
    header = "%-12s %10s %10s %8s | %10s %10s" % (
        "provincia", "forestal", "sabanica", "%forest", "forestal", "sabanica")
    print(header)
    print("-" * len(header), flush=True)

    totals = {band: 0.0 for band in BANDS}
    for feature in boundaries["features"]:
        name = feature["properties"]["name"]
        if name not in PROVINCES:
            continue
        geom = ee.Geometry(feature["geometry"])
        reduced = stack.reduceRegion(
            ee.Reducer.sum(), geom, scale=30, maxPixels=1e12, bestEffort=True
        ).getInfo()
        values = {band: (reduced.get(band) or 0.0) for band in BANDS}
        for band in BANDS:
            totals[band] += values[band]
        emit(name, values)

    print("-" * len(header))
    emit("TOTAL", totals)
    print()
    print("Hectareas. %forest = share de la perdida NATIVA sobre formacion forestal.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
