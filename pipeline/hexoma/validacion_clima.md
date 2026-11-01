# Validación del clima del Hexoma contra estaciones

Generado por `pipeline/hexoma/validar_clima.py` el 01-11-2026. Período 1991–2025; cada estación contra el píxel que la contiene (ERA5-Land 0,1°, CHIRPS v3 0,05°). **Sesgo = modelo − estación.** Estaciones de GHCN-Daily (NOAA) y del INTA (SIGA, marcadas «INTA»). En A1 el sesgo se mide, no se corrige.

Sólo entran años y campañas con al menos 95 % de días válidos en la estación.

## Tmin diaria (°C)

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Posadas | 9670 | 16,9 | 17,7 | 0,8 | 1,2 | 0,97 |
| Iguazú Aero | 9234 | 16,2 | 18,2 | 2,0 | 2,1 | 0,96 |
| Foz do Iguaçu (Cataratas) | 5230 | 16,8 | 18,4 | 1,6 | 1,7 | 0,96 |
| Encarnación (PY) | 9449 | 15,9 | 17,3 | 1,4 | 1,8 | 0,95 |
| Capitán Meza (PY) | 6475 | 15,6 | 17,0 | 1,4 | 1,6 | 0,96 |
| Ciudad del Este (PY) | 1888 | 17,8 | 18,8 | 0,9 | 1,2 | 0,97 |
| Corrientes | 8667 | 15,7 | 17,5 | 1,8 | 1,9 | 0,97 |
| Paso de los Libres | 9791 | 14,6 | 15,8 | 1,2 | 1,4 | 0,97 |
| Monte Caseros | 10265 | 14,5 | 15,3 | 0,8 | 1,1 | 0,97 |
| Curuzú Cuatiá | 1359 | 13,0 | 14,7 | 1,8 | 2,0 | 0,96 |
| Resistencia | 10486 | 15,2 | 17,3 | 2,1 | 2,2 | 0,96 |
| Presidencia Roque Sáenz Peña | 6450 | 15,2 | 17,7 | 2,6 | 2,7 | 0,96 |
| Formosa | 9124 | 17,0 | 18,4 | 1,4 | 1,6 | 0,97 |
| Las Lomitas | 7087 | 16,8 | 18,7 | 1,9 | 2,1 | 0,96 |
| Rivadavia (Salta) | 1818 | 17,5 | 19,6 | 2,1 | 2,4 | 0,93 |
| Reconquista (Santa Fe) | 10659 | 15,1 | 16,4 | 1,3 | 1,5 | 0,97 |
| Ceres (Santa Fe) | 11693 | 13,0 | 15,4 | 2,4 | 2,5 | 0,96 |
| Santa Sylvina - EEA Sáenz Peña (INTA, 90 m) | 1100 | 15,0 | 17,0 | 2,0 | 2,1 | 0,97 |
| Col Popular - EEA Col Benitez (INTA, 82 m) | 4502 | 16,3 | 18,0 | 1,7 | 2,4 | 0,94 |
| Cerro Azul - EEA Cerro Azul (INTA, 292 m) | 4493 | 16,3 | 16,9 | 0,5 | 1,0 | 0,97 |
| Basail - EEA Col Benitez (INTA, 55 m) | 2112 | 16,4 | 17,3 | 0,9 | 1,5 | 0,94 |
| V Miguel Lanus -EEA Cerro Azul (INTA, 120 m) | 3502 | 15,7 | 17,8 | 2,0 | 2,3 | 0,90 |
| Andresito - EEA Montecarlo (INTA, 261 m) | 3372 | 16,7 | 18,7 | 2,0 | 2,1 | 0,95 |
| B de Irigoyen - EEA Montecarlo (INTA, 815 m) | 3999 | 15,8 | 15,6 | -0,2 | 1,2 | 0,94 |
| Goya - EEA Bella Vista (INTA, 84 m) | 3854 | 15,4 | 16,6 | 1,2 | 1,5 | 0,97 |
| Montecarlo - EEA Montecarlo (INTA, 200 m) | 3545 | 16,0 | 17,2 | 1,2 | 1,5 | 0,96 |
| INTA - Corrientes (EMC) (INTA, 57 m) | 2560 | 15,5 | 18,2 | 2,7 | 2,8 | 0,93 |
| San Vicente - EEA Cerro Azul (INTA, 534 m) | 2515 | 16,0 | 16,3 | 0,4 | 1,2 | 0,95 |
| Esquina - EEA Bella Vista (INTA, 53 m) | 3956 | 14,8 | 15,8 | 1,0 | 1,3 | 0,97 |
| INTA - Bella Vista (EMC) (INTA, 70 m) | 12780 | 16,0 | 16,8 | 0,8 | 1,3 | 0,96 |
| Sombrerito - EEA Corrientes (INTA, 79 m) | 3765 | 16,0 | 17,8 | 1,8 | 1,9 | 0,97 |
| El Colorado - EEA El Colorado (INTA, 106 m) | 2743 | 16,7 | 18,1 | 1,4 | 1,6 | 0,95 |
| Bme de las Casas -EEA Colorado (INTA, 116 m) | 1696 | 17,7 | 19,6 | 1,8 | 2,0 | 0,96 |
| Caa Cati - EEA Corrientes (INTA, 74 m) | 3103 | 17,6 | 18,1 | 0,5 | 1,4 | 0,94 |
| INTA - Mercedes Ctes (EMC) (INTA, 100 m) | 10838 | 13,3 | 15,7 | 2,4 | 3,0 | 0,88 |
| Mercedes - EEA Mercedes (INTA, 99 m) | 4011 | 15,0 | 16,2 | 1,1 | 1,3 | 0,97 |
| P. de la Plaza -EEA Sáenz Peña (INTA, 75 m) | 1293 | 15,8 | 17,8 | 2,1 | 2,2 | 0,96 |
| INTA - Colonia Benitez (EMC) (INTA, 54 m) | 11971 | 15,9 | 17,8 | 2,0 | 2,1 | 0,96 |
| INTA - Las Breñas (EMC) (INTA, 102 m) | 12124 | 15,7 | 17,6 | 1,9 | 2,1 | 0,95 |
| INTA - Roque Saenz Peña (EMC) (INTA, 90 m) | 12221 | 15,3 | 17,6 | 2,3 | 2,4 | 0,96 |
| Bella Vista - EEA Bella Vista (INTA, 70 m) | 3632 | 15,8 | 17,3 | 1,5 | 1,7 | 0,94 |
| Du Graty - EEA Sáenz Peña (INTA, 89 m) | 2712 | 15,6 | 17,4 | 1,9 | 2,0 | 0,97 |
| INTA - El Colorado (EMC) (INTA, 78 m) | 5940 | 16,3 | 17,8 | 1,5 | 1,7 | 0,96 |
| INTA - Cerro Azul (EMC) (INTA, 283 m) | 12779 | 15,9 | 16,7 | 0,7 | 1,3 | 0,96 |
| Ituzaingo - EEA Corrientes (INTA, 20 m) | 1874 | 15,8 | 19,1 | 3,2 | 3,4 | 0,79 |
| Pje. Desmochado - EEA B Vista (INTA, 50 m) | 1645 | 15,3 | 16,8 | 1,5 | 1,7 | 0,97 |
| Col. Tabay - EEA Bella Vista (INTA, 10 m) | 1371 | 15,8 | 17,3 | 1,5 | 1,6 | 0,97 |
| J J Castelli - EEA Sáenz Peña (INTA, 0 m) | 845 | 17,4 | 18,8 | 1,4 | 1,7 | 0,97 |

Mediana del sesgo: 1,5; mediana de r: 0,96.

## Tmax diaria (°C)

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Posadas | 5364 | 29,3 | 25,4 | -3,9 | 4,0 | 0,90 |
| Iguazú Aero | 5787 | 29,1 | 26,3 | -2,7 | 2,9 | 0,89 |
| Foz do Iguaçu (Cataratas) | 3837 | 29,2 | 26,1 | -3,1 | 3,3 | 0,88 |
| Encarnación (PY) | 5609 | 29,2 | 25,5 | -3,6 | 3,7 | 0,89 |
| Capitán Meza (PY) | 3353 | 28,6 | 25,8 | -2,8 | 3,1 | 0,88 |
| Ciudad del Este (PY) | 1095 | 29,8 | 26,9 | -2,9 | 3,1 | 0,89 |
| Corrientes | 4892 | 28,7 | 25,3 | -3,3 | 3,5 | 0,90 |
| Paso de los Libres | 5494 | 27,1 | 24,0 | -3,1 | 3,2 | 0,91 |
| Monte Caseros | 5268 | 26,9 | 23,9 | -3,0 | 3,2 | 0,92 |
| Curuzú Cuatiá | 520 | 26,2 | 23,1 | -3,1 | 3,3 | 0,91 |
| Resistencia | 8238 | 28,2 | 25,8 | -2,4 | 2,7 | 0,93 |
| Presidencia Roque Sáenz Peña | 3990 | 29,7 | 26,9 | -2,9 | 3,2 | 0,91 |
| Formosa | 5171 | 30,3 | 26,7 | -3,6 | 3,7 | 0,91 |
| Las Lomitas | 5818 | 30,4 | 28,2 | -2,2 | 2,6 | 0,93 |
| Rivadavia (Salta) | 3161 | 30,1 | 28,9 | -1,2 | 2,1 | 0,93 |
| Reconquista (Santa Fe) | 7716 | 26,6 | 24,7 | -1,9 | 2,3 | 0,93 |
| Ceres (Santa Fe) | 7402 | 26,4 | 24,5 | -1,8 | 2,3 | 0,93 |
| Santa Sylvina - EEA Sáenz Peña (INTA, 90 m) | 1100 | 28,7 | 26,7 | -2,1 | 2,5 | 0,95 |
| Col Popular - EEA Col Benitez (INTA, 82 m) | 4502 | 30,2 | 27,2 | -3,0 | 3,3 | 0,93 |
| Cerro Azul - EEA Cerro Azul (INTA, 292 m) | 4493 | 27,6 | 25,7 | -1,9 | 2,3 | 0,95 |
| Basail - EEA Col Benitez (INTA, 55 m) | 2112 | 29,2 | 26,3 | -2,9 | 3,2 | 0,93 |
| V Miguel Lanus -EEA Cerro Azul (INTA, 120 m) | 3502 | 29,6 | 26,3 | -3,3 | 3,5 | 0,91 |
| Andresito - EEA Montecarlo (INTA, 261 m) | 3372 | 31,1 | 27,5 | -3,6 | 3,9 | 0,92 |
| B de Irigoyen - EEA Montecarlo (INTA, 815 m) | 3999 | 26,0 | 24,0 | -2,0 | 2,3 | 0,94 |
| Goya - EEA Bella Vista (INTA, 84 m) | 3854 | 27,5 | 26,0 | -1,5 | 2,0 | 0,96 |
| Montecarlo - EEA Montecarlo (INTA, 200 m) | 3545 | 29,1 | 26,6 | -2,5 | 3,0 | 0,93 |
| INTA - Corrientes (EMC) (INTA, 57 m) | 2560 | 28,4 | 26,9 | -1,4 | 2,1 | 0,94 |
| San Vicente - EEA Cerro Azul (INTA, 534 m) | 2515 | 27,5 | 25,4 | -2,1 | 2,5 | 0,94 |
| Esquina - EEA Bella Vista (INTA, 53 m) | 3956 | 26,7 | 25,1 | -1,6 | 2,0 | 0,96 |
| INTA - Bella Vista (EMC) (INTA, 70 m) | 12780 | 26,8 | 25,6 | -1,1 | 1,7 | 0,96 |
| Sombrerito - EEA Corrientes (INTA, 79 m) | 3765 | 28,5 | 26,5 | -2,0 | 2,4 | 0,96 |
| El Colorado - EEA El Colorado (INTA, 106 m) | 2743 | 29,7 | 27,5 | -2,1 | 2,6 | 0,94 |
| Bme de las Casas -EEA Colorado (INTA, 116 m) | 1696 | 31,2 | 28,4 | -2,9 | 3,2 | 0,94 |
| Caa Cati - EEA Corrientes (INTA, 74 m) | 3103 | 29,0 | 27,3 | -1,7 | 2,2 | 0,94 |
| INTA - Mercedes Ctes (EMC) (INTA, 100 m) | 10838 | 27,1 | 24,8 | -2,2 | 2,9 | 0,87 |
| Mercedes - EEA Mercedes (INTA, 99 m) | 4011 | 27,5 | 25,7 | -1,8 | 2,3 | 0,95 |
| P. de la Plaza -EEA Sáenz Peña (INTA, 75 m) | 1293 | 29,8 | 27,2 | -2,6 | 2,9 | 0,95 |
| INTA - Colonia Benitez (EMC) (INTA, 54 m) | 11976 | 27,5 | 26,2 | -1,3 | 1,9 | 0,95 |
| INTA - Las Breñas (EMC) (INTA, 102 m) | 12119 | 28,6 | 27,1 | -1,5 | 2,1 | 0,94 |
| INTA - Roque Saenz Peña (EMC) (INTA, 90 m) | 12474 | 28,5 | 27,1 | -1,3 | 2,1 | 0,95 |
| Bella Vista - EEA Bella Vista (INTA, 70 m) | 3638 | 28,1 | 26,0 | -2,1 | 2,5 | 0,94 |
| Du Graty - EEA Sáenz Peña (INTA, 89 m) | 2712 | 29,6 | 27,3 | -2,3 | 2,7 | 0,96 |
| INTA - El Colorado (EMC) (INTA, 78 m) | 5941 | 28,0 | 26,6 | -1,5 | 2,1 | 0,94 |
| INTA - Cerro Azul (EMC) (INTA, 283 m) | 12777 | 26,4 | 25,4 | -1,0 | 1,7 | 0,95 |
| Ituzaingo - EEA Corrientes (INTA, 20 m) | 1874 | 27,4 | 26,2 | -1,2 | 3,1 | 0,83 |
| Pje. Desmochado - EEA B Vista (INTA, 50 m) | 1645 | 27,8 | 26,2 | -1,6 | 2,0 | 0,97 |
| Col. Tabay - EEA Bella Vista (INTA, 10 m) | 1371 | 28,3 | 26,7 | -1,6 | 2,2 | 0,95 |
| J J Castelli - EEA Sáenz Peña (INTA, 0 m) | 845 | 30,8 | 28,9 | -2,0 | 2,6 | 0,95 |

Mediana del sesgo: -2,1; mediana de r: 0,94.

## Lluvia por campaña (mm)

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Iguazú Aero | 7 | 1 947,4 | 2 062,7 | 115,3 | 139,4 | 0,96 |
| Foz do Iguaçu (Cataratas) | 6 | 1 430,6 | 1 567,6 | 137,0 | 137,0 | 0,96 |
| Encarnación (PY) | 8 | 1 892,6 | 2 091,5 | 199,0 | 200,2 | 0,98 |
| Ciudad del Este (PY) | 8 | 2 010,8 | 2 029,5 | 18,7 | 83,8 | 0,98 |
| Monte Caseros | 8 | 1 411,3 | 1 585,4 | 174,1 | 185,7 | 0,98 |
| Curuzú Cuatiá | 8 | 1 323,9 | 1 382,5 | 58,6 | 160,2 | 0,95 |
| Col Popular - EEA Col Benitez (INTA, 82 m) | 8 | 1 189,5 | 1 416,8 | 227,3 | 227,3 | 0,94 |
| Cerro Azul - EEA Cerro Azul (INTA, 292 m) | 8 | 2 112,9 | 2 137,9 | 25,0 | 220,1 | 0,80 |
| V Miguel Lanus -EEA Cerro Azul (INTA, 120 m) | 6 | 1 767,4 | 2 183,1 | 415,7 | 500,5 | 0,63 |
| B de Irigoyen - EEA Montecarlo (INTA, 815 m) | 6 | 1 953,8 | 2 245,5 | 291,7 | 389,1 | 0,95 |
| Goya - EEA Bella Vista (INTA, 84 m) | 6 | 1 105,5 | 1 106,0 | 0,5 | 33,2 | 0,99 |
| Esquina - EEA Bella Vista (INTA, 53 m) | 7 | 1 028,1 | 1 107,7 | 79,5 | 260,7 | 0,88 |
| INTA - Bella Vista (EMC) (INTA, 70 m) | 34 | 1 366,1 | 1 355,9 | -10,2 | 120,0 | 0,92 |
| Sombrerito - EEA Corrientes (INTA, 79 m) | 6 | 1 222,5 | 1 419,4 | 197,0 | 227,3 | 0,87 |
| INTA - Mercedes Ctes (EMC) (INTA, 100 m) | 31 | 1 518,9 | 1 571,7 | 52,8 | 125,9 | 0,94 |
| Mercedes - EEA Mercedes (INTA, 99 m) | 7 | 1 366,5 | 1 553,3 | 186,8 | 256,1 | 0,52 |
| INTA - Colonia Benitez (EMC) (INTA, 54 m) | 31 | 1 344,5 | 1 411,6 | 67,1 | 135,4 | 0,85 |
| INTA - Las Breñas (EMC) (INTA, 102 m) | 30 | 957,0 | 1 042,0 | 85,0 | 141,0 | 0,83 |
| INTA - Roque Saenz Peña (EMC) (INTA, 90 m) | 34 | 907,1 | 1 074,7 | 167,6 | 194,4 | 0,74 |
| Bella Vista - EEA Bella Vista (INTA, 70 m) | 8 | 1 027,5 | 1 267,7 | 240,2 | 298,3 | 0,80 |
| Du Graty - EEA Sáenz Peña (INTA, 89 m) | 6 | 722,0 | 1 168,9 | 446,9 | 446,9 | 0,75 |
| INTA - El Colorado (EMC) (INTA, 78 m) | 22 | 1 088,6 | 1 267,0 | 178,4 | 191,2 | 0,85 |
| INTA - Cerro Azul (EMC) (INTA, 283 m) | 34 | 2 009,3 | 2 133,3 | 124,0 | 163,4 | 0,95 |

Mediana del sesgo: 137,0; mediana de r: 0,92.

## Días de lluvia ≥ 1 mm por campaña

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Iguazú Aero | 7 | 98,6 | 144,0 | 45,4 | 45,4 | 0,52 |
| Foz do Iguaçu (Cataratas) | 6 | 93,7 | 122,0 | 28,3 | 28,3 | 0,97 |
| Encarnación (PY) | 8 | 98,4 | 134,1 | 35,8 | 35,8 | 0,86 |
| Ciudad del Este (PY) | 8 | 106,1 | 146,4 | 40,2 | 40,2 | 0,57 |
| Monte Caseros | 8 | 74,6 | 104,6 | 30,0 | 30,0 | 0,85 |
| Curuzú Cuatiá | 8 | 71,8 | 97,0 | 25,2 | 25,2 | 0,89 |
| Col Popular - EEA Col Benitez (INTA, 82 m) | 8 | 70,9 | 102,0 | 31,1 | 31,1 | 0,90 |
| Cerro Azul - EEA Cerro Azul (INTA, 292 m) | 8 | 98,2 | 137,9 | 39,6 | 39,6 | 0,85 |
| V Miguel Lanus -EEA Cerro Azul (INTA, 120 m) | 6 | 84,5 | 130,5 | 46,0 | 46,0 | 0,24 |
| B de Irigoyen - EEA Montecarlo (INTA, 815 m) | 6 | 107,3 | 155,7 | 48,3 | 48,3 | 0,92 |
| Goya - EEA Bella Vista (INTA, 84 m) | 6 | 67,8 | 86,3 | 18,5 | 18,5 | 0,88 |
| Esquina - EEA Bella Vista (INTA, 53 m) | 7 | 62,1 | 87,6 | 25,4 | 25,4 | 0,62 |
| INTA - Bella Vista (EMC) (INTA, 70 m) | 34 | 69,0 | 102,5 | 33,6 | 33,6 | 0,67 |
| Sombrerito - EEA Corrientes (INTA, 79 m) | 6 | 67,8 | 101,5 | 33,7 | 33,7 | 0,53 |
| INTA - Mercedes Ctes (EMC) (INTA, 100 m) | 31 | 73,9 | 108,8 | 34,9 | 34,9 | 0,69 |
| Mercedes - EEA Mercedes (INTA, 99 m) | 7 | 68,7 | 107,7 | 39,0 | 39,0 | 0,15 |
| INTA - Colonia Benitez (EMC) (INTA, 54 m) | 31 | 71,1 | 106,4 | 35,3 | 35,3 | 0,53 |
| INTA - Las Breñas (EMC) (INTA, 102 m) | 30 | 62,0 | 92,0 | 30,0 | 30,0 | 0,57 |
| INTA - Roque Saenz Peña (EMC) (INTA, 90 m) | 34 | 57,2 | 93,7 | 36,5 | 36,5 | 0,40 |
| Bella Vista - EEA Bella Vista (INTA, 70 m) | 8 | 57,1 | 93,0 | 35,9 | 35,9 | 0,84 |
| Du Graty - EEA Sáenz Peña (INTA, 89 m) | 6 | 44,2 | 89,3 | 45,2 | 45,2 | 0,66 |
| INTA - El Colorado (EMC) (INTA, 78 m) | 22 | 61,9 | 105,8 | 43,9 | 43,9 | 0,50 |
| INTA - Cerro Azul (EMC) (INTA, 283 m) | 34 | 98,4 | 138,7 | 40,3 | 40,3 | 0,84 |

Mediana del sesgo: 35,8; mediana de r: 0,67.

## Lluvia diaria (mm)

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Iguazú Aero | 6948 | 8,2 | 7,3 | -0,8 | 7,2 | 0,56 |
| Foz do Iguaçu (Cataratas) | 7917 | 5,3 | 5,6 | 0,3 | 5,0 | 0,63 |
| Encarnación (PY) | 7082 | 7,4 | 7,7 | 0,3 | 7,2 | 0,57 |
| Ciudad del Este (PY) | 4415 | 5,3 | 5,5 | 0,2 | 5,5 | 0,52 |
| Monte Caseros | 6718 | 6,3 | 6,3 | 0,0 | 6,1 | 0,59 |
| Curuzú Cuatiá | 3420 | 3,7 | 3,8 | 0,1 | 4,3 | 0,51 |
| Col Popular - EEA Col Benitez (INTA, 82 m) | 4495 | 3,3 | 4,1 | 0,8 | 4,8 | 0,32 |
| Cerro Azul - EEA Cerro Azul (INTA, 292 m) | 4464 | 6,2 | 6,0 | -0,2 | 6,8 | 0,45 |
| V Miguel Lanus -EEA Cerro Azul (INTA, 120 m) | 3515 | 4,7 | 5,8 | 1,0 | 6,1 | 0,41 |
| B de Irigoyen - EEA Montecarlo (INTA, 815 m) | 3951 | 5,2 | 6,3 | 1,0 | 6,1 | 0,46 |
| Goya - EEA Bella Vista (INTA, 84 m) | 3687 | 2,7 | 3,4 | 0,6 | 4,1 | 0,33 |
| Esquina - EEA Bella Vista (INTA, 53 m) | 3943 | 2,7 | 3,2 | 0,5 | 4,0 | 0,32 |
| INTA - Bella Vista (EMC) (INTA, 70 m) | 12771 | 3,7 | 3,7 | -0,0 | 4,4 | 0,43 |
| Sombrerito - EEA Corrientes (INTA, 79 m) | 3755 | 3,5 | 4,3 | 0,7 | 5,1 | 0,31 |
| INTA - Mercedes Ctes (EMC) (INTA, 100 m) | 11560 | 4,1 | 4,3 | 0,1 | 4,5 | 0,55 |
| Mercedes - EEA Mercedes (INTA, 99 m) | 4013 | 3,8 | 4,3 | 0,5 | 5,0 | 0,38 |
| INTA - Colonia Benitez (EMC) (INTA, 54 m) | 12171 | 3,7 | 3,9 | 0,2 | 4,5 | 0,43 |
| INTA - Las Breñas (EMC) (INTA, 102 m) | 12166 | 2,7 | 2,9 | 0,2 | 3,8 | 0,29 |
| INTA - Roque Saenz Peña (EMC) (INTA, 90 m) | 12774 | 2,5 | 3,0 | 0,5 | 3,2 | 0,47 |
| Bella Vista - EEA Bella Vista (INTA, 70 m) | 3630 | 2,9 | 3,6 | 0,7 | 4,4 | 0,34 |
| Du Graty - EEA Sáenz Peña (INTA, 89 m) | 2711 | 2,1 | 3,1 | 1,1 | 3,9 | 0,20 |
| INTA - El Colorado (EMC) (INTA, 78 m) | 8879 | 3,1 | 3,5 | 0,5 | 3,8 | 0,52 |
| INTA - Cerro Azul (EMC) (INTA, 283 m) | 12771 | 5,5 | 5,8 | 0,3 | 6,1 | 0,47 |

Mediana del sesgo: 0,3; mediana de r: 0,45.

## Días con Tmin < 0 °C por año

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Resistencia | 12 | 3,8 | 0,0 | -3,8 | 3,8 |  |
| Las Lomitas | 8 | 4,8 | 0,0 | -4,8 | 4,8 |  |
| Reconquista (Santa Fe) | 15 | 2,3 | 0,1 | -2,2 | 2,2 | -0,03 |
| Ceres (Santa Fe) | 10 | 7,5 | 0,8 | -6,7 | 6,7 | 0,51 |
| Col Popular - EEA Col Benitez (INTA, 82 m) | 9 | 6,2 | 0,0 | -6,2 | 6,2 |  |
| Cerro Azul - EEA Cerro Azul (INTA, 292 m) | 10 | 0,8 | 0,0 | -0,8 | 0,8 |  |
| V Miguel Lanus -EEA Cerro Azul (INTA, 120 m) | 7 | 1,9 | 0,0 | -1,9 | 1,9 |  |
| Andresito - EEA Montecarlo (INTA, 261 m) | 5 | 1,0 | 0,0 | -1,0 | 1,0 |  |
| B de Irigoyen - EEA Montecarlo (INTA, 815 m) | 7 | 1,1 | 0,6 | -0,6 | 0,6 | 0,85 |
| Goya - EEA Bella Vista (INTA, 84 m) | 7 | 0,9 | 0,1 | -0,7 | 1,0 | -0,31 |
| INTA - Corrientes (EMC) (INTA, 57 m) | 6 | 2,0 | 0,0 | -2,0 | 2,0 |  |
| Esquina - EEA Bella Vista (INTA, 53 m) | 6 | 0,8 | 0,2 | -0,7 | 0,7 | 0,11 |
| INTA - Bella Vista (EMC) (INTA, 70 m) | 35 | 0,3 | 0,1 | -0,2 | 0,3 | 0,01 |
| Sombrerito - EEA Corrientes (INTA, 79 m) | 7 | 1,4 | 0,0 | -1,4 | 1,4 |  |
| El Colorado - EEA El Colorado (INTA, 106 m) | 5 | 1,8 | 0,0 | -1,8 | 1,8 |  |
| INTA - Mercedes Ctes (EMC) (INTA, 100 m) | 26 | 5,2 | 0,7 | -4,5 | 4,7 | 0,10 |
| Mercedes - EEA Mercedes (INTA, 99 m) | 6 | 2,5 | 0,5 | -2,0 | 2,0 | -0,08 |
| INTA - Colonia Benitez (EMC) (INTA, 54 m) | 31 | 3,3 | 0,0 | -3,3 | 3,3 |  |
| INTA - Las Breñas (EMC) (INTA, 102 m) | 32 | 4,5 | 0,1 | -4,3 | 4,3 | 0,29 |
| INTA - Roque Saenz Peña (EMC) (INTA, 90 m) | 31 | 5,5 | 0,2 | -5,4 | 5,4 | 0,28 |
| Bella Vista - EEA Bella Vista (INTA, 70 m) | 5 | 0,4 | 0,0 | -0,4 | 0,4 |  |
| Du Graty - EEA Sáenz Peña (INTA, 89 m) | 7 | 3,9 | 0,0 | -3,9 | 3,9 |  |
| INTA - El Colorado (EMC) (INTA, 78 m) | 11 | 1,5 | 0,0 | -1,5 | 1,5 |  |
| INTA - Cerro Azul (EMC) (INTA, 283 m) | 35 | 1,0 | 0,1 | -0,9 | 0,9 | 0,67 |

Mediana del sesgo: -1,9; mediana de r: 0,11.

## Días con Tmin ≤ 3 °C por año

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Resistencia | 12 | 16,2 | 2,1 | -14,1 | 14,1 | 0,25 |
| Las Lomitas | 8 | 12,5 | 3,0 | -9,5 | 9,5 | 0,64 |
| Reconquista (Santa Fe) | 15 | 12,3 | 3,2 | -9,1 | 9,1 | 0,47 |
| Ceres (Santa Fe) | 10 | 27,5 | 6,9 | -20,6 | 20,6 | 0,64 |
| Col Popular - EEA Col Benitez (INTA, 82 m) | 9 | 19,0 | 1,9 | -17,1 | 17,1 | 0,23 |
| Cerro Azul - EEA Cerro Azul (INTA, 292 m) | 10 | 4,9 | 2,3 | -2,6 | 2,6 | 0,90 |
| V Miguel Lanus -EEA Cerro Azul (INTA, 120 m) | 7 | 9,1 | 0,7 | -8,4 | 8,4 | 0,37 |
| Andresito - EEA Montecarlo (INTA, 261 m) | 5 | 4,2 | 0,2 | -4,0 | 4,0 | 0,65 |
| B de Irigoyen - EEA Montecarlo (INTA, 815 m) | 7 | 5,0 | 4,0 | -1,0 | 1,9 | 0,62 |
| Goya - EEA Bella Vista (INTA, 84 m) | 7 | 8,6 | 2,3 | -6,3 | 6,3 | 0,09 |
| INTA - Corrientes (EMC) (INTA, 57 m) | 6 | 11,2 | 1,8 | -9,3 | 9,3 | 0,77 |
| Esquina - EEA Bella Vista (INTA, 53 m) | 6 | 7,8 | 2,5 | -5,3 | 5,3 | 0,43 |
| INTA - Bella Vista (EMC) (INTA, 70 m) | 35 | 5,7 | 2,3 | -3,4 | 3,4 | 0,65 |
| Sombrerito - EEA Corrientes (INTA, 79 m) | 7 | 12,1 | 0,9 | -11,3 | 11,3 | 0,22 |
| El Colorado - EEA El Colorado (INTA, 106 m) | 5 | 6,2 | 1,4 | -4,8 | 4,8 | 0,39 |
| INTA - Mercedes Ctes (EMC) (INTA, 100 m) | 26 | 23,8 | 6,4 | -17,4 | 17,5 | 0,48 |
| Mercedes - EEA Mercedes (INTA, 99 m) | 6 | 14,0 | 6,5 | -7,5 | 7,5 | -0,17 |
| INTA - Colonia Benitez (EMC) (INTA, 54 m) | 31 | 14,0 | 1,0 | -13,0 | 13,0 | 0,27 |
| INTA - Las Breñas (EMC) (INTA, 102 m) | 32 | 15,5 | 2,6 | -12,9 | 12,9 | 0,42 |
| INTA - Roque Saenz Peña (EMC) (INTA, 90 m) | 31 | 19,4 | 2,6 | -16,8 | 16,8 | 0,28 |
| Bella Vista - EEA Bella Vista (INTA, 70 m) | 5 | 6,8 | 0,2 | -6,6 | 6,6 | -0,12 |
| Du Graty - EEA Sáenz Peña (INTA, 89 m) | 7 | 17,6 | 2,3 | -15,3 | 15,3 | 0,42 |
| INTA - El Colorado (EMC) (INTA, 78 m) | 11 | 7,9 | 1,6 | -6,3 | 6,3 | 0,63 |
| INTA - Cerro Azul (EMC) (INTA, 283 m) | 35 | 6,7 | 2,3 | -4,5 | 4,5 | 0,54 |

Mediana del sesgo: -8,8; mediana de r: 0,43.

## Grados-día base 10 por campaña (°C·d)

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Resistencia | 11 | 4 187,0 | 4 247,4 | 60,4 | 81,1 | 0,84 |
| Las Lomitas | 8 | 4 696,3 | 4 734,5 | 38,2 | 60,4 | 0,82 |
| Reconquista (Santa Fe) | 12 | 3 864,8 | 3 832,5 | -32,3 | 63,0 | 0,85 |
| Ceres (Santa Fe) | 9 | 3 367,5 | 3 565,1 | 197,7 | 197,7 | 0,75 |
| Col Popular - EEA Col Benitez (INTA, 82 m) | 8 | 4 690,5 | 4 509,7 | -180,7 | 345,0 | -0,40 |
| Cerro Azul - EEA Cerro Azul (INTA, 292 m) | 9 | 4 323,3 | 4 068,8 | -254,5 | 254,5 | 0,92 |
| V Miguel Lanus -EEA Cerro Azul (INTA, 120 m) | 7 | 4 534,5 | 4 340,0 | -194,4 | 198,0 | -0,54 |
| Andresito - EEA Montecarlo (INTA, 261 m) | 5 | 4 879,3 | 4 595,0 | -284,3 | 284,3 | 0,99 |
| B de Irigoyen - EEA Montecarlo (INTA, 815 m) | 7 | 3 873,7 | 3 501,0 | -372,7 | 372,7 | 0,97 |
| Goya - EEA Bella Vista (INTA, 84 m) | 6 | 4 007,1 | 3 988,2 | -18,9 | 106,3 | 0,84 |
| Esquina - EEA Bella Vista (INTA, 53 m) | 7 | 3 921,2 | 3 806,0 | -115,2 | 132,3 | 0,87 |
| INTA - Bella Vista (EMC) (INTA, 70 m) | 34 | 4 170,1 | 4 112,4 | -57,6 | 77,9 | 0,93 |
| Sombrerito - EEA Corrientes (INTA, 79 m) | 6 | 4 359,7 | 4 329,1 | -30,6 | 69,7 | 0,74 |
| El Colorado - EEA El Colorado (INTA, 106 m) | 5 | 4 865,6 | 4 665,8 | -199,7 | 199,7 | 0,44 |
| INTA - Mercedes Ctes (EMC) (INTA, 100 m) | 27 | 3 795,4 | 3 806,1 | 10,7 | 212,3 | 0,46 |
| Mercedes - EEA Mercedes (INTA, 99 m) | 7 | 4 083,5 | 3 963,9 | -119,6 | 128,2 | 0,85 |
| INTA - Colonia Benitez (EMC) (INTA, 54 m) | 29 | 4 245,9 | 4 340,4 | 94,5 | 105,6 | 0,82 |
| INTA - Las Breñas (EMC) (INTA, 102 m) | 30 | 4 407,6 | 4 458,7 | 51,1 | 99,5 | 0,85 |
| INTA - Roque Saenz Peña (EMC) (INTA, 90 m) | 29 | 4 285,4 | 4 455,4 | 170,0 | 176,7 | 0,85 |
| Bella Vista - EEA Bella Vista (INTA, 70 m) | 7 | 4 304,4 | 4 141,8 | -162,6 | 162,6 | 0,88 |
| Du Graty - EEA Sáenz Peña (INTA, 89 m) | 6 | 4 481,7 | 4 426,4 | -55,3 | 80,6 | 0,76 |
| INTA - El Colorado (EMC) (INTA, 78 m) | 12 | 4 453,7 | 4 470,8 | 17,1 | 77,3 | 0,87 |
| INTA - Cerro Azul (EMC) (INTA, 283 m) | 34 | 4 098,9 | 4 041,7 | -57,2 | 86,8 | 0,77 |

Mediana del sesgo: -55,3; mediana de r: 0,84.

## Heladas a 5 cm (estación) frente a Tmin < 0 °C del modelo, por año

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| INTA - Bella Vista (EMC) (INTA, 70 m) | 28 | 7,7 | 0,1 | -7,6 | 7,6 | 0,01 |
| INTA - Colonia Benitez (EMC) (INTA, 54 m) | 21 | 24,4 | 0,0 | -24,4 | 24,4 |  |
| INTA - Las Breñas (EMC) (INTA, 102 m) | 22 | 20,9 | 0,1 | -20,7 | 20,7 | -0,07 |
| INTA - Roque Saenz Peña (EMC) (INTA, 90 m) | 8 | 19,4 | 0,1 | -19,2 | 19,2 | 0,48 |
| INTA - Cerro Azul (EMC) (INTA, 283 m) | 32 | 9,5 | 0,1 | -9,4 | 9,4 | 0,15 |

Mediana del sesgo: -19,2; mediana de r: 0,08.

