"""
Contrasta la senal de perdida que usa el visor EUDR (Hansen) contra el Modulo de
Perdida de Vegetacion de MapBiomas Argentina, sin recurrir a Hansen para validar
a Hansen.

Por que existe este script
--------------------------
El argumento central del informe es que una parte sustantiva de la "perdida de
bosque" que reportan los productos globales en el nordeste argentino no es
conversion de bosque nativo. Ese argumento se apoya en la clase silvicultura de
MapBiomas, asi que conviene corroborarlo con un producto independiente.

El Modulo de Perdida de Vegetacion distingue perdida de vegetacion PRIMARIA de
perdida de secundaria, con anio de deteccion. Hansen no puede hacer esa
distincion. Si la brecha entre ambos productos siguiera un patron aleatorio, no
diria nada; lo informativo es que su magnitud acompana a la prevalencia de
plantacion forestal, que es lo que este script mide.

Los dos productos miden cosas distintas por definicion, de modo que una
diferencia es esperable y no constituye por si sola un error de ninguno de los
dos. La lectura correcta esta en el patron provincial, no en la brecha.

Leyenda del modulo, verificada empiricamente cruzando cada clase de 2021 con la
transicion de cobertura 2020->2021 y tomando la moda (no esta bien documentada
en linea, asi que no conviene asumirla):

  3 = vegetacion secundaria
  4 = perdida de vegetacion PRIMARIA      <- la que se usa aca
  5 = rebrote de vegetacion secundaria
  6 = perdida de vegetacion secundaria

Metodo
------
Periodo comun 2021-2024: la Coleccion 2 termina en 2024, asi que Hansen se
restringe al mismo rango para que la comparacion sea legitima.

A diferencia de los otros dos scripts EUDR, este reduce a escala 100 m y con
reducer `mean`, porque el resultado se expresa como porcentaje del area
provincial y no como recuento de pixeles. Los 100 m son ademas el piso de
precision efectivo de la capa servida.

Uso:
  python pipeline/eudr_validacion_modulo_mapbiomas.py
"""
import json
import os
import sys

import ee

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config_eudr import (BOUNDARY_PATH, HANSEN_ASSET, MAPBIOMAS_C2_ASSET,
                         MAPBIOMAS_C2_DEFOR_ASSET, MB_CLASS_FOREST)

# Periodo comun a los dos productos.
YEARS = (2021, 2022, 2023, 2024)
# Clase 4 del modulo = perdida de vegetacion primaria.
MB_PRIMARY_LOSS = 4
# Codigos lossyear de Hansen equivalentes a YEARS.
FIRST_LOSS_CODE = 21
LAST_LOSS_CODE = 24

BANDS = ("hansen_loss", "mb_primary_loss", "mb_primary_loss_forest", "forest2020")


def build_stack():
    module = ee.Image(MAPBIOMAS_C2_DEFOR_ASSET)
    lulc = ee.Image(MAPBIOMAS_C2_ASSET)

    # Perdida de vegetacion primaria en cualquier anio del periodo.
    mb_loss = ee.Image(0)
    for year in YEARS:
        mb_loss = mb_loss.Or(module.select("classification_%d" % year).eq(MB_PRIMARY_LOSS))
    mb_loss = mb_loss.rename("mb_primary_loss")

    # Idem, restringida a lo que en 2020 era formacion forestal: lectura mas
    # estricta y directamente comparable con la cobertura arborea de Hansen.
    was_forest = lulc.select("classification_2020").eq(MB_CLASS_FOREST)
    mb_loss_forest = mb_loss.And(was_forest).rename("mb_primary_loss_forest")

    lossyear = ee.Image(HANSEN_ASSET).select("lossyear").unmask(0)
    hansen = (lossyear.gte(FIRST_LOSS_CODE)
              .And(lossyear.lte(LAST_LOSS_CODE)).rename("hansen_loss"))

    return (hansen.addBands(mb_loss).addBands(mb_loss_forest)
            .addBands(was_forest.rename("forest2020")))


def main():
    if not os.path.exists(BOUNDARY_PATH):
        print("ERROR: falta el geojson de limites provinciales en %s"
              % BOUNDARY_PATH)
        return 1

    ee.Initialize()
    stack = build_stack()

    with open(BOUNDARY_PATH, encoding="utf-8") as fh:
        boundaries = json.load(fh)

    header = "%-12s %10s %10s %10s %10s" % (
        "provincia", "hansen%", "mb_prim%", "mb_bosq%", "bosq2020%")
    print(header)
    print("-" * len(header), flush=True)

    for feature in boundaries["features"]:
        name = feature["properties"]["name"]
        geom = ee.Geometry(feature["geometry"])
        reduced = stack.reduceRegion(
            ee.Reducer.mean(), geom, scale=100, maxPixels=1e11, bestEffort=True
        ).getInfo()
        values = [(reduced.get(band) or 0.0) * 100 for band in BANDS]
        print("%-12s %10.2f %10.2f %10.2f %10.2f" % (name, *values), flush=True)

    print()
    print("hansen%%   = perdida de cobertura arborea %d-%d, %% del area provincial"
          % (YEARS[0], YEARS[-1]))
    print("mb_prim%  = perdida de vegetacion PRIMARIA (MapBiomas Col.2, clase 4)")
    print("mb_bosq%  = idem, restringida a lo que era formacion forestal en 2020")
    print("bosq2020% = formacion forestal en 2020 (referencia)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
