# Validación de la capa web «uso del suelo» (MapBiomas Argentina Col. 3)

Generado el 03-11-2026 con `pipeline/hexoma/exportar_uso_web.py` (tablas de umbral con `--evaluar`). Reemplaza a
Dynamic World 2024 en spatia.ar para Misiones, Corrientes, Chaco y Formosa; Itapúa y Alto Paraná quedan sin la capa
(MapBiomas Argentina no los cubre y mezclar colecciones daría leyendas incompatibles).

## Umbral de «mosaico» (ninguna clase llega al X % del hexágono), 2025, 11 clases finales

| Provincia | celdas | sin clase ≥40 % | ≥50 % | ≥60 % |
|---|---:|---:|---:|---:|
| misiones | 319,871 | 4.5 % | 14.4 % | 28.2 % |
| formosa | 825,760 | 0.9 % | 3.3 % | 9.8 % |
| corrientes | 1,033,548 | 1.3 % | 4.7 % | 13.5 % |
| chaco | 1,116,998 | 0.9 % | 4.1 % | 13.1 % |
| Departamento de Misiones | mosaico al 50 % |
|---|---:|
| Apóstoles | 30.0 % |
| Capital | 23.4 % |
| San Javier | 21.8 % |
| 25 de Mayo | 20.7 % |
| L. N. Alem | 20.2 % |
| Concepción | 19.7 % |
| Guaraní | 18.6 % |
| Candelaria | 18.2 % |
| L.G. San Martín | 15.7 % |
| Oberá | 14.3 % |
| San Ignacio | 13.4 % |
| G. M. Belgrano | 13.3 % |
| Cainguás | 12.4 % |
| San Pedro | 10.1 % |
| Montecarlo | 9.1 % |
| Eldorado | 8.4 % |
| Iguazú | 4.0 % |

**Elegido: 50 %.** Con 60 % el mosaico cubre el 28 % de Misiones y domina el mapa; con 40 % casi desaparece (4,5 %).
Al 50 %, los mosaicos de Misiones son sobre todo monte + perenne (24 %), monte + pastura (16 %), perenne + pastura
(15 %) y monte + plantación (10 %); la primera y la segunda clase suman en promedio el 75 % del hexágono. Se concentran
en la zona de colonias (Apóstoles, San Javier, 25 de Mayo, Alem) y casi no aparecen en Iguazú. Están agrupados: el
30 % de los vecinos de un mosaico es también mosaico, contra el 14 % de base; no son bordes sueltos entre manchas.
(Evaluado el 3-nov con 9 grupos y repetido con las 11 clases finales: Misiones 14,0 → 14,4 %.)

## Plantación forestal contra el Inventario Nacional de Plantaciones (DNDFI, 23-04-2026), Misiones

| | MapBiomas 2025 | DNDFI |
|---|---:|---:|
| Superficie | 478 mil ha | 327 mil ha |

Cociente 1,46; r entre departamentos 0,976; r entre hexágonos 0,65. De los hexágonos con plantación dominante (≥ 50 %)
según el DNDFI, el 73 % también la tiene en MapBiomas. MapBiomas ubica bien dónde está el pino y el eucalipto y marca
más superficie de la inventariada (la diferencia mayor está en Iguazú: 89 mil contra 47 mil ha).

## Cultivo perenne (yerba, té, cítricos) contra el MAGyP

Ver `validacion_uso.md`: MapBiomas marca de 2,8 a 3,3 veces la superficie plantada del MAGyP (2000–2020), con
r 0,81–0,84 entre departamentos. Por eso la capa lo llama «yerba, té y otros perennes», lo ordena sólo por porcentaje
y no publica hectáreas de yerba. MapBiomas no separa yerba de té.

## Monte 1985 → 2025

Misiones: 1,94 M ha en 1985 → 1,61 M ha en 2025, saldo neto −330 mil ha (−17 %). La suma de las bajas de cada
hexágono da 474 mil ha porque incluye el ruido de clasificación entre años; el ranking usa el saldo neto del
departamento. Plantación: 130 mil → 427 mil ha (×3,3). Chaco −1,49 M ha (−21 %), Formosa −0,63 M ha (−10 %),
Corrientes +33 mil ha. La trayectoria por hexágono «volvió el monte» (5 % de Misiones) incluye ese ruido.

## Lo que la capa no ve
Lotes de 1–3 ha (tabaco, huerta) a 30 m terminan en mosaico o cultivo anual; yerba y té van juntos; superficie
de MapBiomas ≠ superficie sembrada.
