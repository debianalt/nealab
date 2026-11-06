# Validación de la serie Hansen por celda (`series`)

Generado por `build_hexoma_hansen.py --validar` el 2026-11-06.

## Totales 2001–2025 por provincia contra GEE

Celdas: suma de `series.hansen_h3_anio` sobre las celdas de cada territorio (píxel → celda de su centro). GEE: `reduceRegion` de `pixelArea` sobre la provincia de FAO GAUL 2015 nivel 1, a la escala nativa. Los bordes no son los mismos (GAUL contra los radios INDEC de las celdas) y las celdas de borde entre provincias cuentan en las dos: la diferencia esperable es de pocos por ciento.

| Territorio | Celdas, dosel ≥ 30 % (ha) | GEE (ha) | Dif. | Celdas, todo (ha) | GEE (ha) | Dif. | Dif. anual mín./máx./mediana abs. (≥ 30 %) |
|---|---|---|---|---|---|---|---|
| chaco | 1,070,405 | 1,067,568 | +0.3 % | 1,140,282 | 1,137,437 | +0.3 % | -0.2 / +0.6 / 0.2 % |
| corrientes | 189,988 | 190,077 | -0.0 % | 366,413 | 366,389 | +0.0 % | -0.8 / +0.2 / 0.1 % |
| formosa | 812,296 | 809,516 | +0.3 % | 884,432 | 881,022 | +0.4 % | +0.0 / +2.4 / 0.3 % |
| misiones | 617,552 | 616,410 | +0.2 % | 669,078 | 667,932 | +0.2 % | +0.1 / +0.3 / 0.2 % |

## Contra la capa web (`capa_deforestation_dynamics`)

Misma fuente, otro código (zonal por polígono de la celda en `process_hansen_to_h3.py`; acá, centro del píxel). 3,287,567 celdas.

- r de `c_loss_rate` (2016–2025) contra `tasa_2016_2025`: **0.9985**
- r de `c_loss_rate_baseline` (2001–2010) contra `tasa_2001_2010`: **0.9983**
- diferencia absoluta 2016–2025: mediana 0.0000, P99 0.4522 puntos de %/año

## Por año

| Territorio | Año | Celdas ≥ 30 % (ha) | GEE ≥ 30 % (ha) | Dif. |
|---|---|---|---|---|
| chaco | 2001 | 22,815 | 22,748 | +0.3 % |
| chaco | 2002 | 5,647 | 5,655 | -0.2 % |
| chaco | 2003 | 22,382 | 22,351 | +0.1 % |
| chaco | 2004 | 59,039 | 58,724 | +0.5 % |
| chaco | 2005 | 54,334 | 54,013 | +0.6 % |
| chaco | 2006 | 33,174 | 32,979 | +0.6 % |
| chaco | 2007 | 51,566 | 51,566 | -0.0 % |
| chaco | 2008 | 49,627 | 49,588 | +0.1 % |
| chaco | 2009 | 37,651 | 37,643 | +0.0 % |
| chaco | 2010 | 21,676 | 21,605 | +0.3 % |
| chaco | 2011 | 101,232 | 100,821 | +0.4 % |
| chaco | 2012 | 98,007 | 97,709 | +0.3 % |
| chaco | 2013 | 42,003 | 41,952 | +0.1 % |
| chaco | 2014 | 27,687 | 27,596 | +0.3 % |
| chaco | 2015 | 24,450 | 24,388 | +0.3 % |
| chaco | 2016 | 39,885 | 39,761 | +0.3 % |
| chaco | 2017 | 57,179 | 57,072 | +0.2 % |
| chaco | 2018 | 43,044 | 42,937 | +0.2 % |
| chaco | 2019 | 27,194 | 27,158 | +0.1 % |
| chaco | 2020 | 32,298 | 32,251 | +0.1 % |
| chaco | 2021 | 29,772 | 29,699 | +0.2 % |
| chaco | 2022 | 51,878 | 51,772 | +0.2 % |
| chaco | 2023 | 68,189 | 68,074 | +0.2 % |
| chaco | 2024 | 46,783 | 46,658 | +0.3 % |
| chaco | 2025 | 22,895 | 22,849 | +0.2 % |
| corrientes | 2001 | 4,761 | 4,756 | +0.1 % |
| corrientes | 2002 | 4,215 | 4,212 | +0.1 % |
| corrientes | 2003 | 5,216 | 5,209 | +0.1 % |
| corrientes | 2004 | 7,910 | 7,922 | -0.2 % |
| corrientes | 2005 | 7,329 | 7,339 | -0.1 % |
| corrientes | 2006 | 8,704 | 8,710 | -0.1 % |
| corrientes | 2007 | 8,931 | 8,918 | +0.2 % |
| corrientes | 2008 | 10,396 | 10,417 | -0.2 % |
| corrientes | 2009 | 8,820 | 8,835 | -0.2 % |
| corrientes | 2010 | 8,245 | 8,250 | -0.1 % |
| corrientes | 2011 | 10,500 | 10,486 | +0.1 % |
| corrientes | 2012 | 16,790 | 16,785 | +0.0 % |
| corrientes | 2013 | 11,883 | 11,878 | +0.0 % |
| corrientes | 2014 | 8,965 | 8,988 | -0.3 % |
| corrientes | 2015 | 6,471 | 6,469 | +0.0 % |
| corrientes | 2016 | 7,857 | 7,852 | +0.1 % |
| corrientes | 2017 | 6,360 | 6,356 | +0.1 % |
| corrientes | 2018 | 4,529 | 4,527 | +0.0 % |
| corrientes | 2019 | 4,259 | 4,264 | -0.1 % |
| corrientes | 2020 | 4,075 | 4,080 | -0.1 % |
| corrientes | 2021 | 6,302 | 6,300 | +0.0 % |
| corrientes | 2022 | 7,840 | 7,849 | -0.1 % |
| corrientes | 2023 | 9,069 | 9,062 | +0.1 % |
| corrientes | 2024 | 7,186 | 7,207 | -0.3 % |
| corrientes | 2025 | 3,377 | 3,406 | -0.8 % |
| formosa | 2001 | 16,865 | 16,741 | +0.7 % |
| formosa | 2002 | 4,234 | 4,133 | +2.4 % |
| formosa | 2003 | 10,658 | 10,551 | +1.0 % |
| formosa | 2004 | 13,908 | 13,890 | +0.1 % |
| formosa | 2005 | 22,299 | 22,181 | +0.5 % |
| formosa | 2006 | 18,643 | 18,554 | +0.5 % |
| formosa | 2007 | 43,161 | 42,882 | +0.7 % |
| formosa | 2008 | 47,956 | 47,762 | +0.4 % |
| formosa | 2009 | 44,836 | 44,643 | +0.4 % |
| formosa | 2010 | 60,063 | 59,907 | +0.3 % |
| formosa | 2011 | 39,994 | 39,905 | +0.2 % |
| formosa | 2012 | 74,149 | 74,019 | +0.2 % |
| formosa | 2013 | 58,916 | 58,697 | +0.4 % |
| formosa | 2014 | 27,008 | 26,940 | +0.3 % |
| formosa | 2015 | 20,535 | 20,431 | +0.5 % |
| formosa | 2016 | 34,856 | 34,793 | +0.2 % |
| formosa | 2017 | 29,599 | 29,496 | +0.4 % |
| formosa | 2018 | 27,380 | 27,314 | +0.2 % |
| formosa | 2019 | 26,665 | 26,585 | +0.3 % |
| formosa | 2020 | 38,340 | 38,218 | +0.3 % |
| formosa | 2021 | 34,692 | 34,573 | +0.3 % |
| formosa | 2022 | 43,770 | 43,752 | +0.0 % |
| formosa | 2023 | 33,841 | 33,753 | +0.3 % |
| formosa | 2024 | 25,707 | 25,631 | +0.3 % |
| formosa | 2025 | 14,221 | 14,166 | +0.4 % |
| misiones | 2001 | 18,297 | 18,266 | +0.2 % |
| misiones | 2002 | 17,676 | 17,644 | +0.2 % |
| misiones | 2003 | 22,744 | 22,714 | +0.1 % |
| misiones | 2004 | 28,961 | 28,899 | +0.2 % |
| misiones | 2005 | 31,733 | 31,667 | +0.2 % |
| misiones | 2006 | 34,444 | 34,369 | +0.2 % |
| misiones | 2007 | 29,383 | 29,331 | +0.2 % |
| misiones | 2008 | 35,120 | 35,031 | +0.3 % |
| misiones | 2009 | 19,913 | 19,851 | +0.3 % |
| misiones | 2010 | 24,056 | 23,989 | +0.3 % |
| misiones | 2011 | 22,580 | 22,538 | +0.2 % |
| misiones | 2012 | 28,342 | 28,273 | +0.2 % |
| misiones | 2013 | 21,547 | 21,504 | +0.2 % |
| misiones | 2014 | 20,466 | 20,418 | +0.2 % |
| misiones | 2015 | 13,763 | 13,741 | +0.2 % |
| misiones | 2016 | 26,786 | 26,754 | +0.1 % |
| misiones | 2017 | 38,487 | 38,437 | +0.1 % |
| misiones | 2018 | 22,336 | 22,300 | +0.2 % |
| misiones | 2019 | 23,425 | 23,385 | +0.2 % |
| misiones | 2020 | 26,844 | 26,795 | +0.2 % |
| misiones | 2021 | 27,197 | 27,180 | +0.1 % |
| misiones | 2022 | 28,872 | 28,840 | +0.1 % |
| misiones | 2023 | 16,766 | 16,749 | +0.1 % |
| misiones | 2024 | 18,611 | 18,585 | +0.1 % |
| misiones | 2025 | 19,203 | 19,150 | +0.3 % |
