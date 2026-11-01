# Predicciones registradas — El Soberbio, río Uruguay (2026-11-01)

Predicciones de la demo El Niño (El Soberbio) registradas antes de las crecidas de la temporada 2026-27. Escenarios condicionales: qué pasaría si el río Uruguay llega a cada altura; no pronostican la altura.

Escala: El Soberbio (escala de Prefectura, serie INA 61). Rango: ajuste del terreno Δ 1,5 a 2,0 m. Arriba de 19 m, techo.

| Altura | Viviendas bajo agua | Personas | Aisladas | Escuelas bajo agua | Barrios alcanzados (viviendas) |
|---|---|---|---|---|---|
| 13 m | 10 a 18 | 26 a 47 | 21 a 24 | 0 | Maracaná, Chivilcoy 3 a 9; Puerto Mata Ojos 2 a 3; Fray Luis Beltrán, Mariano Moreno, Paraje Fray Luis Beltrán 1 a 2; Paraje Barra El Soberbio 1; Unión, Hertman 1; Unión, Barrio Unión 1 |
| 15 m | 66 a 78 | 177 a 209 | 61 a 66 | 0 | Maracaná, Chivilcoy 42 a 48; Unión, Barrio Unión 14 a 17; Fray Luis Beltrán, Mariano Moreno, Paraje Fray Luis Beltrán 3 a 4; Puerto Mata Ojos 4; Paraje Barra El Soberbio 2 a 3; Unión, Hertman 1 y 1 más |
| 17 m | 139 a 165 | 371 a 441 | 117 a 142 | 0 | Maracaná, Chivilcoy 86 a 95; Unión, Barrio Unión 31 a 38; Fray Luis Beltrán, Mariano Moreno, Paraje Fray Luis Beltrán 7 a 8; cerca de El Soberbio 0 a 7; Paraje Barra El Soberbio 6 a 7; Puerto Mata Ojos 6 y 4 más |
| 19 m | 257 a 310 | 681 a 824 | 170 a 225 | 0 | Maracaná, Chivilcoy 117 a 121; Unión, Barrio Unión 64 a 88; cerca de El Soberbio 29 a 42; Paraje Barra El Soberbio 12 a 18; Fray Luis Beltrán, Mariano Moreno, Paraje Fray Luis Beltrán 9 a 10; Puerto Mata Ojos 8 a 9 y 7 más |
| 21 m (techo) | 485 a 525 | 1.282 a 1.383 | 215 a 226 | 0 a 1 | Maracaná, Chivilcoy 142 a 151; Unión, Barrio Unión 146 a 148; cerca de El Soberbio 95 a 109; Paraje Barra El Soberbio 31 a 33; Unión, Hertman 20 a 25; Unión, Agua Potable, Telecom 18 a 20 y 8 más |
| 23 m (techo) | 710 a 768 | 1.865 a 2.015 | 187 a 192 | 2 | Maracaná, Chivilcoy 182 a 197; Unión, Barrio Unión 172 a 179; cerca de El Soberbio 135 a 146; Unión, Hertman 49 a 53; Hospital, Itatí 40 a 50; Paraje Barra El Soberbio 40 a 42 y 12 más |

La tabla completa (11 a 30 m, cada 0,5 m, por barrio) está en el JSON del mismo nombre.

## Cómo se verifica (fijado antes de las crecidas)

1. Barrios: cada barrio que la prensa nombre como afectado tiene que estar alcanzado (punta alta) a la altura pico.
2. Cantidad: familias evacuadas o casas bajo agua informadas contra la banda: dentro, modelo alto (x < bajo) o modelo bajo (x > alto). Arriba de 19 m se espera modelo alto.
3. Agua: pasada NISAR a 4 días o menos del pico, orilla observada contra la predicha con la lectura INA del día.
4. Altura del evento: pico informado en la escala de Prefectura; si no hay, lectura diaria INA 61 (se dice cuál).

## Insumos (SHA-256)

- `tabla_altura.json` d9ea46a855605c180d5d8cc0c1889c4e9cfc38ee3bc5683a6a3c5e363030b5c5
- `edificios_pts.json` ffdaf62d524993edf06aa63a72301e91e5b93b200c15ca512d64e87e3ddef874
- `radios.geojson` f8fb5e16967ca359d8b563b8f82786e788103fc82500b99010d65f42bd6c33e8
- `puntos.geojson` a7807a69e089c845835efb2a07a2b54be43d4219737d830e986095b0c119b9ef
- `agua.png` 05b4a8465515d53b36f5980ae0b8ef08223e704b5fd7d9e2ab35cc03b2ee86a1
