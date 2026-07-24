"""
Agrega a H3 el area quemada post-2020 que ocurre SOBRE VEGETACION NATIVA LENOSA.

Por que existe este script
--------------------------
El score de riesgo EUDR pondera al 20 % el area quemada cruda (banda `fire_post_2020`
de MODIS MCD64A1). En el nordeste argentino esa aproximacion se sostiene mal: el fuego
se concentra en pastizal y humedal, donde es regimen natural y no indicio de conversion
de bosque. Corrientes es el caso limite: reune la mayor area quemada de la region y a la
vez la mayor proporcion de perdida explicada por cosecha de plantacion.

La misma linea base de MapBiomas que separa cosecha de conversion permite filtrar el
fuego. Este script cuenta solo el area quemada que cae sobre vegetacion nativa lenosa
(clases 3 formacion forestal + 4/5/6 sabanica e inundable), y deja fuera pastizal (12)
y humedal (11).

Metodo
------
La banda de fuego es binaria a 100 m (hubo cicatriz de quema al menos una vez desde
2021). La cobertura MapBiomas esta a 30 m. Para cada pixel de 100 m se calcula que
fraccion de su superficie es nativa lenosa, promediando la mascara de 30 m sobre la
grilla de fuego, y se pondera:

    fuego_nativo = fuego x fraccion_nativa

de modo que el resultado sigue siendo una fraccion de superficie del hexagono, en la
misma escala que el fuego crudo, y por lo tanto sustituible dentro del score sin tocar
los pesos.

Solo cubre las cuatro provincias con cobertura MapBiomas cargada (Misiones, Corrientes,
Chaco, Formosa). Paraguay y Brasil conservan fuego crudo, la misma asimetria ya
documentada para la distincion plantacion/nativo.

Uso:
  python pipeline/aggregate_fire_native.py
"""
import os
import sys

# La instalacion de PROJ que trae PostgreSQL tapa la de rasterio y reproject aborta con
# CRSError ("DATABASE.LAYOUT.VERSION.MINOR = 2 whereas a number >= 6 is expected").
# Hay que apuntar PROJ a los datos de rasterio ANTES de importar rasterio.warp.
try:
    import rasterio as _rio
    _proj = os.path.join(os.path.dirname(_rio.__file__), "proj_data")
    if os.path.isdir(_proj):
        os.environ["PROJ_DATA"] = _proj
        os.environ["PROJ_LIB"] = _proj
except ImportError:
    pass

import glob
import json
import time

import numpy as np
import pandas as pd
import rasterio
from rasterio.merge import merge as rio_merge
from rasterio.warp import reproject, Resampling
from rasterio.features import geometry_mask
from shapely.geometry import shape
import h3.api.basic_int as h3i

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NEA = os.path.dirname(SCRIPT_DIR)
EUDR_DIR = os.path.join(SCRIPT_DIR, "output", "eudr")
OUT_DIR = os.path.join(EUDR_DIR, "hires")

FIRE_SHARDS = sorted(glob.glob(os.path.join(EUDR_DIR, "eudr_deforestation_combined_2025*.tif")))
COVER = os.path.join(EUDR_DIR, "mapbiomas_col2_nea.tif")
# MISMO archivo de limites con el que se genero la capa servida (via aggregate_geom).
# El de src/lib/data/ tiene geometrias distintas: usarlo daba un conjunto de pixeles
# diferente y el fuego crudo llegaba a diferir 98 pp contra el publicado.
BOUNDARY = os.path.join(SCRIPT_DIR, "data", "ar_eudr_provinces.geojson")

TREECOVER_BAND = 1               # treecover_2000: define que pixeles son validos
FIRE_BAND = 5                    # fire_post_2020
NATIVE_CLASSES = (3, 4, 5, 6)    # formacion forestal + sabanica/inundable
# provincias con cobertura MapBiomas cargada (el raster mapbiomas_col2_nea cubre el NEA)
PROV = ("Misiones", "Corrientes", "Chaco", "Formosa")
RESOLUTIONS = (9, 7)


def native_fraction(dst_transform, dst_shape, bounds):
    """Fraccion de vegetacion nativa lenosa (30 m) llevada a la grilla de fuego (100 m).

    Resampling.average sobre una mascara 0/1 devuelve exactamente la fraccion de
    superficie de cada pixel de destino que cumple la condicion.
    """
    with rasterio.open(COVER) as src:
        win = rasterio.windows.from_bounds(*bounds, transform=src.transform)
        win = win.round_offsets().round_lengths()
        cov = src.read(1, window=win)
        cov_transform = rasterio.windows.transform(win, src.transform)
    mask = np.isin(cov, NATIVE_CLASSES).astype("float32")
    del cov
    out = np.zeros(dst_shape, dtype="float32")
    reproject(mask, out,
              src_transform=cov_transform, src_crs="EPSG:4326",
              dst_transform=dst_transform, dst_crs="EPSG:4326",
              resampling=Resampling.average)
    del mask
    return out


def province(geom, label, srcs):
    t0 = time.time()
    # nodata=np.nan, igual que aggregate_eudr_hires.py: con nodata=0 los huecos del
    # mosaico entrarian como pixeles validos sin fuego e inflarian el denominador.
    stack, transform = rio_merge(srcs, bounds=geom.bounds,
                                 indexes=[TREECOVER_BAND, FIRE_BAND], nodata=np.nan)
    stack = stack.astype("float32")
    tc = stack[0]
    fire = stack[1]
    del stack
    rows, cols = fire.shape
    inside = geometry_mask([geom], out_shape=(rows, cols), transform=transform, invert=True)
    # MISMA compuerta de validez que aggregate_eudr_hires.py: si aca se usara solo la
    # geometria, el denominador no coincidiria con el de las demas bandas y el fuego
    # filtrado no seria sustituible dentro del score (llego a diferir 98 pp en el borde).
    valid = inside & np.isfinite(tc) & (tc >= 0)
    del inside, tc
    natf = native_fraction(transform, (rows, cols), geom.bounds)

    ri, ci = np.nonzero(valid)
    lons = transform.c + (ci + 0.5) * transform.a
    lats = transform.f + (ri + 0.5) * transform.e
    # el fuego se desenmascara DESPUES de fijar los pixeles validos: MODIS sin quema
    # llega como NaN y cuenta como cero, no como dato faltante
    burned = np.nan_to_num(fire[valid], nan=0.0)
    native = natf[valid]
    del fire, natf, valid

    out = {}
    for res in RESOLUTIONS:
        cells = np.fromiter((h3i.latlng_to_cell(la, lo, res) for la, lo in zip(lats, lons)),
                            dtype=np.int64, count=len(lats))
        df = pd.DataFrame({"h3index": cells,
                           "raw": burned,
                           "nat": burned * native})
        g = df.groupby("h3index", as_index=False).mean()
        g["h3index"] = [h3i.int_to_str(int(c)) for c in g.h3index]
        g["fire_native_post_2020_pct"] = (100.0 * g.pop("nat")).round(2)
        g["fire_raw_check_pct"] = (100.0 * g.pop("raw")).round(2)
        g["province"] = label
        out[res] = g
        del cells, df

    r9 = out[9]
    print("  %-14s res9=%-8d res7=%-6d  crudo=%5.2f%%  nativo=%5.2f%%  (-%.0f%%)  (%.0fs)"
          % (label, len(out[9]), len(out[7]),
             r9.fire_raw_check_pct.mean(), r9.fire_native_post_2020_pct.mean(),
             100 * (1 - r9.fire_native_post_2020_pct.mean() / max(r9.fire_raw_check_pct.mean(), 1e-9)),
             time.time() - t0), flush=True)
    return out


def main():
    if not FIRE_SHARDS:
        print("ERROR: no hay rasters eudr_deforestation_combined_2025*.tif")
        return 1
    if not os.path.exists(COVER):
        print(f"ERROR: falta la cobertura MapBiomas: {COVER}")
        return 1
    os.makedirs(OUT_DIR, exist_ok=True)
    print("fuego:", [os.path.basename(f) for f in FIRE_SHARDS])
    print("cobertura:", os.path.basename(COVER), "| nativa lenosa = clases", NATIVE_CLASSES)

    fc = json.load(open(BOUNDARY, encoding="utf-8"))
    srcs = [rasterio.open(f) for f in FIRE_SHARDS]
    acc = {r: [] for r in RESOLUTIONS}
    try:
        for feat in fc["features"]:
            name = feat["properties"]["NAME_1"]
            if name not in PROV:
                continue
            # mismo etiquetado que regen_ar_res7.py, para que el join por h3index cierre
            label = ("ar_" + name).strip("_").lower().replace(" ", "_")
            res_out = province(shape(feat["geometry"]), label, srcs)
            for r in RESOLUTIONS:
                acc[r].append(res_out[r])
    finally:
        for s in srcs:
            s.close()

    for r in RESOLUTIONS:
        df = pd.concat(acc[r], ignore_index=True)
        # Una celda de borde aparece en dos provincias. Se conserva la PRIMERA en el
        # orden del geojson, que es la regla de regen_ar_res7.py; desempatar por valor
        # dejaria estas celdas midiendo una provincia distinta a la del dato publicado.
        df = (df.drop_duplicates(subset="h3index", keep="first")
                .sort_values("h3index").reset_index(drop=True))
        out = os.path.join(OUT_DIR, f"eudr_fire_native_res{r}.parquet")
        df.to_parquet(out, index=False)
        print(f"  -> {out}  ({len(df):,} celdas, {os.path.getsize(out)/1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
