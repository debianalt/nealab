# Validación de la aptitud por cultivo

Generado por `pipeline/hexoma/validar_aptitud.py` el 07-11-2026. Aptitud no es uso: se espera lift > 1 y correlación positiva, no coincidencia. Clase = clase mediana 1991/92–2024/25 (aptitud.resumen).

## Lift contra MapBiomas 2025

| Cultivo | Celdas cultivadas | Clase | % de cultivadas en la clase | % de todas | Lift |
|---|---:|---|---:|---:|---:|
| Soja | 262,041 | S1 | 77,3 | 51,5 | 1,50 |
| Soja | 262,041 | S1+S2 | 98,3 | 65,9 | 1,49 |
| Maíz | 262,041 | S1 | 69,2 | 42,4 | 1,63 |
| Maíz | 262,041 | S1+S2 | 97,7 | 86,4 | 1,13 |
| Algodón | 262,041 | S1 | 82,5 | 35,4 | 2,33 |
| Algodón | 262,041 | S1+S2 | 95,6 | 76,6 | 1,25 |
| Girasol | 262,041 | S1 | 39,3 | 22,5 | 1,75 |
| Girasol | 262,041 | S1+S2 | 94,8 | 73,7 | 1,29 |
| Yerba mate | 39,415 | S1 | 1,8 | 0,6 | 3,18 |
| Yerba mate | 39,415 | S1+S2 | 99,2 | 60,5 | 1,64 |
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
| Soja | 2019/2020–2024/2025 | 65 | S1 | 0,34 | 0,40 |
| Soja | 2019/2020–2024/2025 | 73 | S1+S2 | 0,34 | 0,33 |
| Maíz | 2019/2020–2024/2025 | 68 | S1 | 0,51 | 0,39 |
| Maíz | 2019/2020–2024/2025 | 76 | S1+S2 | 0,44 | 0,20 |
| Algodón | 2019/2020–2024/2025 | 33 | S1 | 0,12 | 0,49 |
| Algodón | 2019/2020–2024/2025 | 55 | S1+S2 | 0,09 | 0,22 |
| Girasol | 2019/2020–2024/2025 | 32 | S1 | -0,05 | 0,04 |
| Girasol | 2019/2020–2024/2025 | 57 | S1+S2 | 0,00 | 0,29 |
| Arroz (sin dato de agua de riego) | 2019/2020–2024/2025 | 76 | S1 | 0,09 | 0,27 |
| Arroz (sin dato de agua de riego) | 2019/2020–2024/2025 | 76 | S1+S2 | 0,13 | 0,30 |
| Yerba mate | 2016/2017–2023/2024 | 21 | S1 | 0,05 | 0,35 |
| Yerba mate | 2016/2017–2023/2024 | 64 | S1+S2 | 0,15 | 0,29 |
| Té | 2018/2019–2023/2024 | 26 | S1 | -0,14 | 0,04 |
| Té | 2018/2019–2023/2024 | 73 | S1+S2 | 0,18 | 0,53 |
| Cítricos (naranja) | 1991/1992–1996/1997 | 47 | S1 | -0,07 | -0,04 |
| Cítricos (naranja) | 1991/1992–1996/1997 | 76 | S1+S2 | -0,07 | -0,13 |
