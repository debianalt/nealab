# Validación del cruce radio → celda

Generado por `pipeline/hexoma/build_hexoma_cruce.py` el 2026-10-31. Método en el docstring del script.

Para cada año y método se reparte la población de cada fracción censal (de más de un radio) entre sus
radios según la suma de pesos de cada radio, y se compara con la población real del radio.
`WAPE` = Σ|est − real| / Σ real (%); `mediano` = error relativo mediano por radio (%).
Regla fijada antes: gana el WAPE total más bajo; a menos de 1 punto, el más simple (área < edificios < ghsl < edificios_ghsl).
Un tamaño de radio usa otro método sólo si ese método le baja el WAPE más de 2 puntos.
2022: donde gana edificios, se usa el cruce de la web (cada edificio asignado a su radio por punto, `censo_2022.cruce`).

## 1991 (GHSL 1990)

Elegido — urbano (< 1 km²): **ghsl**; intermedio (1–10 km²): **ghsl**; rural (> 10 km²): **ghsl**

| Radios | n | area WAPE / mediano | edificios WAPE / mediano | ghsl WAPE / mediano | edificios_ghsl WAPE / mediano |
|---|---|---|---|---|---|
| total | 4,127 | 71.1 / 52.5 | 40.1 / 30.3 | 35.5 / 29.5 | 39.9 / 30.2 |
| urbano (< 1 km²) | 1,679 | 51.5 / 44.2 | 31.1 / 23.5 | 32.7 / 28.0 | 31.0 / 23.2 |
| intermedio (1–10 km²) | 414 | 125.9 / 95.7 | 57.8 / 39.2 | 40.4 / 32.0 | 58.1 / 39.2 |
| rural (> 10 km²) | 2,034 | 88.2 / 51.9 | 51.6 / 37.0 | 39.3 / 30.5 | 51.1 / 37.2 |

Fracciones sin peso con cada método (caen a área): area 0, edificios 16, ghsl 0, edificios_ghsl 18

## 2001 (GHSL 2000)

Elegido — urbano (< 1 km²): **edificios_ghsl**; intermedio (1–10 km²): **ghsl**; rural (> 10 km²): **ghsl**

| Radios | n | area WAPE / mediano | edificios WAPE / mediano | ghsl WAPE / mediano | edificios_ghsl WAPE / mediano |
|---|---|---|---|---|---|
| total | 4,738 | 76.6 / 54.5 | 35.7 / 28.0 | 34.5 / 28.5 | 35.1 / 27.9 |
| urbano (< 1 km²) | 2,301 | 52.3 / 44.3 | 29.5 / 24.8 | 32.4 / 28.5 | 29.3 / 24.3 |
| intermedio (1–10 km²) | 406 | 136.6 / 96.0 | 49.4 / 33.8 | 39.8 / 30.9 | 49.2 / 32.6 |
| rural (> 10 km²) | 2,031 | 123.8 / 63.1 | 48.6 / 33.7 | 38.3 / 28.2 | 46.8 / 32.9 |

Fracciones sin peso con cada método (caen a área): area 0, edificios 16, ghsl 0, edificios_ghsl 18

## 2010 (GHSL 2010)

Elegido — urbano (< 1 km²): **edificios**; intermedio (1–10 km²): **ghsl**; rural (> 10 km²): **ghsl**

| Radios | n | area WAPE / mediano | edificios WAPE / mediano | ghsl WAPE / mediano | edificios_ghsl WAPE / mediano |
|---|---|---|---|---|---|
| total | 5,307 | 80.4 / 59.5 | 34.3 / 27.8 | 34.4 / 29.6 | 33.6 / 27.2 |
| urbano (< 1 km²) | 2,789 | 54.2 / 47.2 | 28.3 / 24.1 | 31.4 / 27.9 | 28.1 / 23.7 |
| intermedio (1–10 km²) | 429 | 133.3 / 96.0 | 45.3 / 33.3 | 40.1 / 33.7 | 45.1 / 33.3 |
| rural (> 10 km²) | 2,089 | 157.3 / 73.2 | 52.2 / 37.3 | 43.5 / 32.4 | 49.5 / 35.2 |

Fracciones sin peso con cada método (caen a área): area 0, edificios 17, ghsl 0, edificios_ghsl 19

## 2022 (GHSL 2020)

Elegido — urbano (< 1 km²): **edificios**; intermedio (1–10 km²): **edificios**; rural (> 10 km²): **ghsl**

| Radios | n | area WAPE / mediano | edificios WAPE / mediano | ghsl WAPE / mediano | edificios_ghsl WAPE / mediano |
|---|---|---|---|---|---|
| total | 6,898 | 81.6 / 61.8 | 32.6 / 27.3 | 33.6 / 29.1 | 31.7 / 26.7 |
| urbano (< 1 km²) | 4,302 | 54.5 / 48.7 | 25.8 / 21.6 | 28.6 / 24.7 | 25.5 / 21.2 |
| intermedio (1–10 km²) | 486 | 153.6 / 95.4 | 43.2 / 32.7 | 45.2 / 36.0 | 42.8 / 31.6 |
| rural (> 10 km²) | 2,110 | 206.6 / 97.2 | 68.1 / 54.7 | 57.2 / 46.5 | 63.6 / 50.7 |

Fracciones sin peso con cada método (caen a área): area 0, edificios 19, ghsl 0, edificios_ghsl 21
