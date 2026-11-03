# nealab

Plataforma reproducible de análisis geoespacial subnacional. Publica capas
hexagonales (H3) de análisis geoespacial sobre el noreste argentino, Itapúa y
Alto Paraná; edificios, censo y límites en el resto de Paraguay y el sur de
Brasil. Las capas se sirven como parquet y se consultan desde el navegador con
DuckDB-WASM.

**Sitio:** [spatia.ar](https://spatia.ar) · **Licencia:** AGPL-3.0-only ·
**Archivo citable:** [10.5281/zenodo.19483040](https://doi.org/10.5281/zenodo.19483040)

---

## EUDR — pérdida de cobertura arbórea posterior al corte del Reglamento (UE) 2023/1115

El trabajo principal de este repositorio. Mide, por hexágono y por parcela, la
pérdida de cobertura arbórea posterior al 31/12/2020, la fecha de corte del
reglamento europeo sobre productos libres de deforestación, y **separa la
cosecha de una plantación forestal de la conversión de bosque nativo** usando la
Serie Anual de MapBiomas Argentina como línea base del año de corte. El visor
por parcela cubre el nordeste argentino y además Paraguay (salvo el Chaco
paraguayo norte) y el sur de Brasil.

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
vegetación nativa leñosa baja la media regional de 13,40 % a 5,34 % y reordena las
provincias, porque en Corrientes el fuego es del humedal del Iberá y
pertenece al régimen natural del ecosistema.

### Qué informa el chequeo

La pérdida de cobertura arbórea posterior al 31/12/2020 (Hansen GFC v1.13),
como porcentaje de la superficie del hexágono. El área quemada sobre vegetación
nativa leñosa (MODIS MCD64A1 × MapBiomas Col. 2) se informa como dato aparte y
no se combina con la pérdida en un puntaje.

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

- **Inundación** — edificios y tierra a menos de 5 m sobre el drenaje (HAND,
  MERIT Hydro + Google Open Buildings); JRC GSW para la presencia histórica y
  Sentinel-1 para la extensión actual. Pipeline documentado en
  [`pipeline/README.md`](pipeline/README.md).
- **Accesibilidad** — tiempos de viaje y clasificación multivariada por hexágono.
- **Actividad económica** — variables censales, radiancia nocturna y densidad
  edilicia agregadas a H3.

## Stack

SvelteKit 5 + adapter-static · MapLibre GL 6 + PMTiles · DuckDB-WASM sobre
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
