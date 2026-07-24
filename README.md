# nealab

Plataforma reproducible de análisis geoespacial subnacional. Publica capas
hexagonales (H3) sobre el norte argentino, Paraguay y el sur de Brasil, servidas
como parquet y consultadas desde el navegador con DuckDB-WASM.

**Sitio:** [spatia.ar](https://spatia.ar) · **Licencia:** AGPL-3.0-only ·
**Archivo citable:** [10.5281/zenodo.19483040](https://doi.org/10.5281/zenodo.19483040)

---

## EUDR — riesgo de no-conformidad con el Reglamento (UE) 2023/1115

El trabajo principal de este repositorio. Estima, por hexágono y por parcela, el
riesgo de que un predio del nordeste argentino no cumpla con el reglamento
europeo sobre productos libres de deforestación, y **separa la cosecha de una
plantación forestal de la conversión de bosque nativo** usando la Serie Anual de
MapBiomas Argentina como línea base del año de corte.

| Qué | Dónde |
|---|---|
| Informe interactivo (storymap) | [spatia.ar/eudr/informe](https://spatia.ar/eudr/informe) |
| Visor por parcela (polígono, coordenadas o CSV) | [spatia.ar/eudr/check](https://spatia.ar/eudr/check) |
| Metodología completa | [spatia.ar/metodologia/eudr](https://spatia.ar/metodologia/eudr) |
| Scripts que producen cada cifra publicada | [`pipeline/README.md`](pipeline/README.md) |
| Constantes compartidas (assets, clases, vintage) | [`pipeline/config_eudr.py`](pipeline/config_eudr.py) |

### Resultado principal

El 22,8 % de la pérdida de cobertura arbórea posterior al 31/12/2020 en las
cuatro provincias del nordeste recae sobre superficie que **ya era plantación en
2020**, de modo que es compatible con un ciclo de cosecha y no con deforestación.
La proporción llega al 91,2 % en Corrientes y al 28,9 % en Misiones. Sin una
línea base nacional de cobertura, esa fracción se contabilizaría como
deforestación.

Un segundo resultado: restringir el área quemada a la que ocurre sobre
vegetación nativa leñosa baja la media regional de 13,40 % a 5,34 % y reordena el
riesgo provincial, porque en Corrientes el fuego es del humedal del Iberá y
pertenece al régimen natural del ecosistema.

### Cómo se construye el score

```
score (0–100) = 0,70 × pérdida forestal 2021–2025   (Hansen GFC v1.13, 100 m)
              + 0,20 × área quemada sobre nativa leñosa (MODIS MCD64A1, 500 m
                                                          × MapBiomas Col. 2)
              + 0,10 × ausencia de cobertura arbórea en la línea base Hansen
                       (treecover2000; aproxima el paisaje ya convertido)
```

Los pesos son coeficientes, no participaciones: el tercer término tiene el peso
nominal más bajo y, por la escala de su variable, es el que más aporta al valor
medio del score. Someterlo al mismo filtro por clase de cobertura que ya recibe
el fuego es la extensión pendiente del método.

### Fuente de cobertura y cita

> MapBiomas – Colección 2 de la Serie Anual de Mapas de Cobertura y Uso del Suelo
> de Argentina, consultada el 24 de julio de 2026 a través del enlace:
> <https://argentina.mapbiomas.org> — licencia **CC-BY**.

La leyenda usada es la **argentina**: clase 9 silvicultura; clases 3, 4 y 6
bosques cerrados, abiertos e inundables, las tres dentro de la categoría
*Bosques*. La clase 5 no existe en la leyenda argentina, y los nombres
brasileños ("formação savânica") no se aplican a productos de Argentina.

---

## Otras capas

- **Riesgo de inundación** — Sentinel-1 SAR, pipeline documentado en
  [`pipeline/README.md`](pipeline/README.md).
- **Accesibilidad** — tiempos de viaje y clasificación multivariada por hexágono.
- **Actividad económica** — variables censales, radiancia nocturna y densidad
  edilicia agregadas a H3.

## Stack

SvelteKit 5 + adapter-static · MapLibre GL 5 + PMTiles · DuckDB-WASM sobre
parquet en Cloudflare R2 · pipeline en Python con Google Earth Engine, rasterio y
h3 · deploy en Cloudflare Pages.

```bash
npm install
npm run dev
```

## Cómo citar

Ver [`CITATION.cff`](CITATION.cff). El DOI de concepto
[10.5281/zenodo.19483040](https://doi.org/10.5281/zenodo.19483040) resuelve
siempre a la última versión publicada.
