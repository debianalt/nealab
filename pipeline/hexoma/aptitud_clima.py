"""A4 — indicadores climáticos por cultivo, por píxel ERA5-Land y campaña jul–jun, con la temperatura CORREGIDA.

Lee los diarios corregidos de corregir_temperatura.py (era5_tmin/tmax/suelo_corr_diario.npy) y escribe
%HEXOMA_FUENTES%/clima/derivados/aptitud_clima.parquet (pixel_id, campania, indicadores). Ventanas aprobadas el
1-nov-2026 (aptitud_reglas.CICLOS). La campaña Y va de jul-Y a jun-(Y+1); los meses de ene–jun son del año Y+1.

    tm_anual            media de (Tmax+Tmin)/2 en la campaña, °C
    tm_<ventana>        ídem en la ventana del cultivo (nov_mar, sep_feb, oct_mar, ago_dic) y en la estación de
                        crecimiento sep_mar (cítricos y pasturas: su rango es de temperatura de crecimiento)
    gdd8_sep_feb        Σ max(0, min(Tm, 30) − 8) sep–feb (maíz; FAO 66: Tbase 8, Tupper 30)
    gdd12_oct_mar       Σ max(0, Tm − 12) oct–mar (algodón)
    gdd7_ago_dic        Σ max(0, Tm − 7) ago–dic (girasol; ASAGIR 6–8 °C)
    libre_heladas       días entre la última Tmin < 0 °C de jul–dic y la primera de ene–jun (sin helada: borde de la campaña)
    tmin_abs            mínima absoluta de la campaña, °C
    heladas5_sep_oct    días con mínima a 5 cm < 0 °C en sep–oct (brotación / floración)
    heladas5_oct        ídem en octubre (girasol con 6–8 hojas o botón)
    frio_decendio       arroz, por decenios de ene, feb y el 1.º de mar (Steinmetz & Amaral 2002, Embrapa Doc. 108: la siembra
                        se elige para que la fase crítica caiga en el decenio de menor riesgo): 0 = hay algún decenio sin
                        noches ≤ 17 °C; 1 = hay alguno sin noches ≤ 15 °C; 2 = todos tienen alguna noche ≤ 15 °C
    dias35_<ventana>    días con Tmax > 35 °C en la ventana (informativo, sin clase)

Uso (desde neahub/):
    python pipeline/hexoma/aptitud_clima.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import log  # noqa: E402
from clima_a_pixel import DERIV  # noqa: E402

VENTANAS = {'sep_mar': [9, 10, 11, 12, 1, 2, 3], 'nov_mar': [11, 12, 1, 2, 3], 'sep_feb': [9, 10, 11, 12, 1, 2], 'oct_mar': [10, 11, 12, 1, 2, 3],
            'ago_dic': [8, 9, 10, 11, 12], 'ene_feb': [1, 2], 'sep_oct': [9, 10], 'oct': [10]}


def main() -> None:
    t0 = time.time()
    pix = pd.read_parquet(DERIV / 'pixel_era5.parquet')
    f = pd.DatetimeIndex(pd.read_parquet(DERIV / 'era5_fechas.parquet').fecha)
    tmin = np.load(DERIV / 'era5_tmin_corr_diario.npy', mmap_mode='r')
    tmax = np.load(DERIV / 'era5_tmax_corr_diario.npy', mmap_mode='r')
    suelo = np.load(DERIV / 'era5_suelo_corr_diario.npy', mmap_mode='r')
    camp = np.where(f.month >= 7, f.year, f.year - 1)
    mes = f.month.values
    filas = []
    for c in range(camp.min() + 1, camp.max() + 1):  # campañas completas: la primera arranca incompleta
        i = np.nonzero(camp == c)[0]
        if len(i) < 365:
            continue
        tn, tx, ts = np.asarray(tmin[i]), np.asarray(tmax[i]), np.asarray(suelo[i])
        tm = (tn + tx) / 2
        m = mes[i]
        d = {'pixel_id': pix.pixel_id.values, 'campania': np.full(len(pix), c, np.int16)}
        d['tm_anual'] = tm.mean(axis=0)
        for v in ('sep_mar', 'nov_mar', 'sep_feb', 'oct_mar', 'ago_dic'):
            sel = np.isin(m, VENTANAS[v])
            d[f'tm_{v}'] = tm[sel].mean(axis=0)
            d[f'dias35_{v}'] = (tx[sel] > 35).sum(axis=0).astype(np.int16)
        sel = np.isin(m, VENTANAS['ene_feb'])
        d['dias35_ene_feb'] = (tx[sel] > 35).sum(axis=0).astype(np.int16)
        d['gdd8_sep_feb'] = np.maximum(np.minimum(tm[np.isin(m, VENTANAS['sep_feb'])], 30) - 8, 0).sum(axis=0)
        d['gdd12_oct_mar'] = np.maximum(tm[np.isin(m, VENTANAS['oct_mar'])] - 12, 0).sum(axis=0)
        d['gdd7_ago_dic'] = np.maximum(tm[np.isin(m, VENTANAS['ago_dic'])] - 7, 0).sum(axis=0)
        # período libre de heladas (abrigo) dentro de la campaña
        prim = i[m >= 7]  # jul–dic
        seg = i[m <= 6]   # ene–jun
        hp = np.asarray(tmin[prim]) < 0
        hs = np.asarray(tmin[seg]) < 0
        ult = np.where(hp.any(axis=0), len(prim) - 1 - hp[::-1].argmax(axis=0), -1)       # índice en la campaña
        pri = np.where(hs.any(axis=0), len(prim) + hs.argmax(axis=0), len(i))
        d['libre_heladas'] = (pri - ult - 1).astype(np.int16)
        d['tmin_abs'] = tn.min(axis=0)
        d['heladas5_sep_oct'] = (ts[np.isin(m, VENTANAS['sep_oct'])] < 0).sum(axis=0).astype(np.int16)
        d['heladas5_oct'] = (ts[np.isin(m, VENTANAS['oct'])] < 0).sum(axis=0).astype(np.int16)
        dia = f.day.values[i]
        decenios = [(mm, lo, hi) for mm in (1, 2) for lo, hi in ((1, 10), (11, 20), (21, 31))] + [(3, 1, 10)]
        sin17 = np.zeros(len(pix), bool)
        sin15 = np.zeros(len(pix), bool)
        for mm, lo, hi in decenios:
            sd = (m == mm) & (dia >= lo) & (dia <= hi)
            if sd.any():
                sin17 |= ~(tn[sd] <= 17).any(axis=0)
                sin15 |= ~(tn[sd] <= 15).any(axis=0)
        d['frio_decendio'] = np.where(sin17, 0, np.where(sin15, 1, 2)).astype(np.int8)
        filas.append(pd.DataFrame(d))
    out = pd.concat(filas, ignore_index=True)
    out.to_parquet(DERIV / 'aptitud_clima.parquet', index=False)
    log(f'{len(out):,} filas ({out.campania.min()}–{out.campania.max()}), {time.time() - t0:.0f} s')
    print(out.describe().T[['mean', 'min', 'max']].round(1).to_string())
    print('arroz, frío por decenios (0 S1 / 1 S2 / 2 N):', out.frio_decendio.value_counts(normalize=True).round(3).to_dict())


if __name__ == '__main__':
    main()
