# Validación del cruce radio → celda

Generado por `pipeline/hexoma/build_hexoma_cruce.py` el 2026-11-06. Método en el docstring del script.

Para cada año y método se reparte la población de cada fracción censal (de más de un radio) entre sus
radios según la suma de pesos de cada radio, y se compara con la población real del radio.
`WAPE` = Σ|est − real| / Σ real (%); `mediano` = error relativo mediano por radio (%).
Regla fijada antes: gana el WAPE total más bajo; a menos de 1 punto, el más simple (área < edificios < ghsl < edificios_ghsl).
Un tamaño de radio usa otro método sólo si ese método le baja el WAPE más de 2 puntos.
2022: donde gana edificios, se usa el cruce de la web (cada edificio asignado a su radio por punto, `censo_2022.cruce`).

## 1991 (GHSL 1990)

Elegido — urbano (< 1 km²): **edificios_ghsl**; intermedio (1–10 km²): **ghsl**; rural (> 10 km²): **ghsl**

| Radios | n | area WAPE / mediano | edificios WAPE / mediano | ghsl WAPE / mediano | edificios_ghsl WAPE / mediano |
|---|---|---|---|---|---|
| total | 4,127 | 71.1 / 52.5 | 38.6 / 29.3 | 35.5 / 29.5 | 38.4 / 29.2 |
| urbano (< 1 km²) | 1,679 | 51.5 / 44.2 | 30.4 / 22.5 | 32.7 / 28.0 | 30.2 / 22.3 |
| intermedio (1–10 km²) | 414 | 125.9 / 95.7 | 54.6 / 38.5 | 40.4 / 32.0 | 54.7 / 38.5 |
| rural (> 10 km²) | 2,034 | 88.2 / 51.9 | 49.2 / 35.5 | 39.3 / 30.5 | 48.9 / 35.9 |

Fracciones sin peso con cada método (caen a área): area 0, edificios 0, ghsl 0, edificios_ghsl 0

## 2001 (GHSL 2000)

Elegido — urbano (< 1 km²): **edificios**; intermedio (1–10 km²): **ghsl**; rural (> 10 km²): **ghsl**

| Radios | n | area WAPE / mediano | edificios WAPE / mediano | ghsl WAPE / mediano | edificios_ghsl WAPE / mediano |
|---|---|---|---|---|---|
| total | 4,738 | 76.6 / 54.5 | 34.1 / 27.0 | 34.5 / 28.5 | 33.6 / 26.9 |
| urbano (< 1 km²) | 2,301 | 52.3 / 44.3 | 28.8 / 23.9 | 32.4 / 28.5 | 28.5 / 23.5 |
| intermedio (1–10 km²) | 406 | 136.6 / 96.0 | 46.0 / 32.8 | 39.8 / 30.9 | 45.8 / 31.8 |
| rural (> 10 km²) | 2,031 | 123.8 / 63.1 | 45.2 / 31.6 | 38.3 / 28.2 | 43.6 / 31.8 |

Fracciones sin peso con cada método (caen a área): area 0, edificios 0, ghsl 0, edificios_ghsl 0

## 2010 (GHSL 2010)

Elegido — urbano (< 1 km²): **edificios**; intermedio (1–10 km²): **ghsl**; rural (> 10 km²): **ghsl**

| Radios | n | area WAPE / mediano | edificios WAPE / mediano | ghsl WAPE / mediano | edificios_ghsl WAPE / mediano |
|---|---|---|---|---|---|
| total | 5,307 | 80.4 / 59.5 | 32.9 / 27.2 | 34.4 / 29.6 | 32.2 / 26.4 |
| urbano (< 1 km²) | 2,789 | 54.2 / 47.2 | 27.7 / 23.8 | 31.4 / 27.9 | 27.4 / 23.1 |
| intermedio (1–10 km²) | 429 | 133.3 / 96.0 | 42.3 / 31.8 | 40.1 / 33.7 | 42.0 / 31.8 |
| rural (> 10 km²) | 2,089 | 157.3 / 73.2 | 48.5 / 35.3 | 43.5 / 32.4 | 45.9 / 33.0 |

Fracciones sin peso con cada método (caen a área): area 0, edificios 0, ghsl 0, edificios_ghsl 0

## 2022 (GHSL 2020)

Elegido — urbano (< 1 km²): **edificios**; intermedio (1–10 km²): **edificios**; rural (> 10 km²): **ghsl**

| Radios | n | area WAPE / mediano | edificios WAPE / mediano | ghsl WAPE / mediano | edificios_ghsl WAPE / mediano |
|---|---|---|---|---|---|
| total | 6,898 | 81.6 / 61.8 | 31.0 / 26.8 | 33.6 / 29.0 | 30.1 / 26.2 |
| urbano (< 1 km²) | 4,302 | 54.5 / 48.7 | 25.1 / 21.2 | 28.6 / 24.7 | 24.7 / 20.9 |
| intermedio (1–10 km²) | 486 | 153.6 / 95.4 | 38.7 / 31.8 | 45.2 / 36.0 | 38.4 / 30.8 |
| rural (> 10 km²) | 2,110 | 206.6 / 97.2 | 63.0 / 50.6 | 57.2 / 46.6 | 58.7 / 48.7 |

Fracciones sin peso con cada método (caen a área): area 0, edificios 0, ghsl 0, edificios_ghsl 0
