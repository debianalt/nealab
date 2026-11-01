# Validación del clima del Hexoma contra estaciones

Generado por `pipeline/hexoma/validar_clima.py` el 01-11-2026. Período 1991–2025; cada estación contra el píxel que la contiene (ERA5-Land 0,1°, CHIRPS v3 0,05°). **Sesgo = modelo − estación.** Estaciones de GHCN-Daily (NOAA). En A1 el sesgo se mide, no se corrige.

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

Mediana del sesgo: 1,6; mediana de r: 0,96.

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

Mediana del sesgo: -2,9; mediana de r: 0,91.

## Lluvia por campaña (mm)

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Iguazú Aero | 7 | 1 947,4 | 2 062,7 | 115,3 | 139,4 | 0,96 |
| Foz do Iguaçu (Cataratas) | 6 | 1 430,6 | 1 567,6 | 137,0 | 137,0 | 0,96 |
| Encarnación (PY) | 8 | 1 892,6 | 2 091,5 | 199,0 | 200,2 | 0,98 |
| Ciudad del Este (PY) | 8 | 2 010,8 | 2 029,5 | 18,7 | 83,8 | 0,98 |
| Monte Caseros | 8 | 1 411,3 | 1 585,4 | 174,1 | 185,7 | 0,98 |
| Curuzú Cuatiá | 8 | 1 323,9 | 1 382,5 | 58,6 | 160,2 | 0,95 |

Mediana del sesgo: 126,2; mediana de r: 0,97.

## Días de lluvia ≥ 1 mm por campaña

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Iguazú Aero | 7 | 98,6 | 144,0 | 45,4 | 45,4 | 0,52 |
| Foz do Iguaçu (Cataratas) | 6 | 93,7 | 122,0 | 28,3 | 28,3 | 0,97 |
| Encarnación (PY) | 8 | 98,4 | 134,1 | 35,8 | 35,8 | 0,86 |
| Ciudad del Este (PY) | 8 | 106,1 | 146,4 | 40,2 | 40,2 | 0,57 |
| Monte Caseros | 8 | 74,6 | 104,6 | 30,0 | 30,0 | 0,85 |
| Curuzú Cuatiá | 8 | 71,8 | 97,0 | 25,2 | 25,2 | 0,89 |

Mediana del sesgo: 32,9; mediana de r: 0,85.

## Lluvia diaria (mm)

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Iguazú Aero | 6948 | 8,2 | 7,3 | -0,8 | 7,2 | 0,56 |
| Foz do Iguaçu (Cataratas) | 7917 | 5,3 | 5,6 | 0,3 | 5,0 | 0,63 |
| Encarnación (PY) | 7082 | 7,4 | 7,7 | 0,3 | 7,2 | 0,57 |
| Ciudad del Este (PY) | 4415 | 5,3 | 5,5 | 0,2 | 5,5 | 0,52 |
| Monte Caseros | 6718 | 6,3 | 6,3 | 0,0 | 6,1 | 0,59 |
| Curuzú Cuatiá | 3420 | 3,7 | 3,8 | 0,1 | 4,3 | 0,51 |

Mediana del sesgo: 0,2; mediana de r: 0,56.

## Días con Tmin < 0 °C por año

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Resistencia | 12 | 3,8 | 0,0 | -3,8 | 3,8 |  |
| Las Lomitas | 8 | 4,8 | 0,0 | -4,8 | 4,8 |  |
| Reconquista (Santa Fe) | 15 | 2,3 | 0,1 | -2,2 | 2,2 | -0,03 |
| Ceres (Santa Fe) | 10 | 7,5 | 0,8 | -6,7 | 6,7 | 0,51 |

Mediana del sesgo: -4,3; mediana de r: 0,24.

## Días con Tmin ≤ 3 °C por año

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Resistencia | 12 | 16,2 | 2,1 | -14,1 | 14,1 | 0,25 |
| Las Lomitas | 8 | 12,5 | 3,0 | -9,5 | 9,5 | 0,64 |
| Reconquista (Santa Fe) | 15 | 12,3 | 3,2 | -9,1 | 9,1 | 0,47 |
| Ceres (Santa Fe) | 10 | 27,5 | 6,9 | -20,6 | 20,6 | 0,64 |

Mediana del sesgo: -11,8; mediana de r: 0,56.

## Grados-día base 10 por campaña (°C·d)

| Estación | n | Estación | Modelo | Sesgo | MAE | r |
|---|---:|---:|---:|---:|---:|---:|
| Resistencia | 11 | 4 187,0 | 4 247,4 | 60,4 | 81,1 | 0,84 |
| Las Lomitas | 8 | 4 696,3 | 4 734,5 | 38,2 | 60,4 | 0,82 |
| Reconquista (Santa Fe) | 12 | 3 864,8 | 3 832,5 | -32,3 | 63,0 | 0,85 |
| Ceres (Santa Fe) | 9 | 3 367,5 | 3 565,1 | 197,7 | 197,7 | 0,75 |

Mediana del sesgo: 49,3; mediana de r: 0,83.

## Huecos

- Las sierras de Misiones (Oberá, Bernardo de Irigoyen, Cerro Azul) no están en GHCN-Daily. INTA SIGA tiene esas series, pero su sitio responde 403 a la descarga automática. Para sumarlas: bajarlas a mano y dejarlas en `%HEXOMA_FUENTES%/estaciones/extra/` (formato en el docstring del script).
