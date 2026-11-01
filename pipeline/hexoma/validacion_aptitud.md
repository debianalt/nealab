# Validación de la aptitud por cultivo

Generado por `pipeline/hexoma/validar_aptitud.py` el 01-11-2026. Aptitud no es uso: se espera lift > 1 y correlación positiva, no coincidencia. Clase = clase mediana 1991/92–2024/25 (aptitud.resumen).

## Lift contra MapBiomas 2025

| Cultivo | Celdas cultivadas | Clase | % de cultivadas en la clase | % de todas | Lift |
|---|---:|---|---:|---:|---:|
| Soja | 262,041 | S1 | 77,3 | 51,5 | 1,50 |
| Soja | 262,041 | S1+S2 | 99,7 | 73,5 | 1,36 |
| Maíz | 262,041 | S1 | 69,2 | 42,5 | 1,63 |
| Maíz | 262,041 | S1+S2 | 99,7 | 97,0 | 1,03 |
| Algodón | 262,041 | S1 | 82,5 | 35,4 | 2,33 |
| Algodón | 262,041 | S1+S2 | 98,8 | 93,5 | 1,06 |
| Girasol | 262,041 | S1 | 39,3 | 22,5 | 1,75 |
| Girasol | 262,041 | S1+S2 | 96,7 | 81,6 | 1,19 |
| Yerba mate | 39,415 | S1 | 1,8 | 0,6 | 3,18 |
| Yerba mate | 39,415 | S1+S2 | 99,2 | 60,4 | 1,64 |
| Té | 39,415 | S1 | 6,7 | 3,7 | 1,81 |
| Té | 39,415 | S1+S2 | 98,6 | 48,6 | 2,03 |
| Cítricos (naranja) | 39,415 | S1 | 0,2 | 3,9 | 0,05 |
| Cítricos (naranja) | 39,415 | S1+S2 | 94,5 | 92,8 | 1,02 |
| Pasturas megatérmicas | 78,385 | S1 | 0,0 | 0,0 |  |
| Pasturas megatérmicas | 78,385 | S1+S2 | 93,0 | 96,8 | 0,96 |

Yerba, té y cítricos se comparan con el «cultivo perenne» de MapBiomas, que en Misiones y Corrientes es sobre todo yerba y té: para cítricos el lift no es una prueba válida; vale la de departamentos.

## Departamentos contra el MAGyP

| Cultivo | Campañas MAGyP | Deptos. | Clase | r | ρ |
|---|---|---:|---|---:|---:|
| Soja | 2019/2020–2024/2025 | 64 | S1 | 0,33 | 0,42 |
| Soja | 2019/2020–2024/2025 | 75 | S1+S2 | 0,33 | 0,37 |
| Maíz | 2019/2020–2024/2025 | 67 | S1 | 0,50 | 0,39 |
| Maíz | 2019/2020–2024/2025 | 75 | S1+S2 | 0,41 | 0,25 |
| Algodón | 2019/2020–2024/2025 | 32 | S1 | 0,14 | 0,52 |
| Algodón | 2019/2020–2024/2025 | 75 | S1+S2 | 0,15 | 0,31 |
| Girasol | 2019/2020–2024/2025 | 31 | S1 | -0,04 | 0,02 |
| Girasol | 2019/2020–2024/2025 | 75 | S1+S2 | 0,08 | 0,44 |
| Arroz (sin dato de agua de riego) | 2019/2020–2024/2025 | 68 | S1 | 0,07 | 0,15 |
| Arroz (sin dato de agua de riego) | 2019/2020–2024/2025 | 75 | S1+S2 | 0,13 | 0,28 |
| Yerba mate | 2016/2017–2023/2024 | 21 | S1 | 0,06 | 0,34 |
| Yerba mate | 2016/2017–2023/2024 | 64 | S1+S2 | 0,15 | 0,29 |
| Té | 2018/2019–2023/2024 | 26 | S1 | -0,14 | 0,03 |
| Té | 2018/2019–2023/2024 | 72 | S1+S2 | 0,17 | 0,52 |
| Cítricos (naranja) | 1991/1992–1996/1997 | 47 | S1 | -0,07 | -0,03 |
| Cítricos (naranja) | 1991/1992–1996/1997 | 75 | S1+S2 | -0,08 | -0,15 |
