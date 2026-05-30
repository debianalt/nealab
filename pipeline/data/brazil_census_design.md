# Brazil Census Integration — Design Report (PR / SC / RS)

**Scope:** Add IBGE Censo Demográfico 2022 disaggregated census data for three Brazilian
states — Paraná (PR), Santa Catarina (SC), Rio Grande do Sul (RS) — to feed Spatia's
dasymetric population + deprivation (NBI-equivalent) layers at H3 res-9, anchored to the
finest census geography (setores censitários) and weighted by building footprints (GBA, Zhu
et al. 2025).

**Status:** Research / design only. No pipeline or frontend code changed.
Date: 2026-05-30.

---

## 1. Summary & Feasibility Verdict

**Feasible now.** The data we need is already released and downloadable:

- **Geometry:** the 2022 census-tract mesh (*malha de setores censitários 2022*) is public,
  in Shapefile / GeoPackage / KML, downloadable per UF — directly analogous to INDEC radio
  shapefiles. CRS is SIRGAS 2000 (EPSG:4674), IBGE's standard geodetic frame.
- **Population & households:** the *Agregados por Setores Censitários — Resultados do Universo*
  (tract-level aggregates) are released, including the `básico` and `demografia` themes
  (population, household counts) and — critically — the three `características_domicilio`
  files that hold sanitation infrastructure. These were the later release waves; as of early
  2026 they are out (file timestamps 2024-11 to 2025-04, dictionary re-issued 2026-05-20).

**Two caveats that shape the design:**

1. **No official NBI.** Brazil does not publish an NBI index. We are *reconstructing* a
   deprivation index from raw `Domicílio`/`Pessoa` variables. This is the same situation that
   already forced Spatia's census layers into `--mode local` (INDEC vs DGEEC schemas don't
   align). The Brazil layer **must be `--mode local`, not `comparable`** — a Brazil-derived
   deprivation score is not numerically comparable to an AR/PY NBI and should not be presented
   as such. (See `neahub/CLAUDE.md` → "Scoring Modes — Dual Architecture".)

2. **One NBI component does not translate.** Spatia's *capacidad de subsistencia* (economic
   dependency: 4+ dependents per low-education earner) has no clean IBGE tract equivalent.
   Income at tract level lives in a *separate, structurally different* table
   (`DomicilioRenda` / `PessoaRenda`) — arguably better data, but not the same construct.
   Mapping it is lossy; flagged in §3 and §7.

Bottom line: population dasymetric + a 3-component deprivation proxy (housing/sanitation +
education, no subsistence) is solidly feasible. A faithful 4-component AR-NBI clone is not.

---

## 2. Data Sources (concrete URLs)

All paths verified against IBGE FTP listings, May 2026.

### 2.1 Census-tract geometry (malha de setores censitários 2022)
- Landing page: `https://www.ibge.gov.br/geociencias/organizacao-do-territorio/estrutura-territorial/26565-malhas-de-setores-censitarios-divisoes-intramunicipais.html`
- FTP (geoftp), shapefile per UF, "setores" level:
  `https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/setores/shp/UF/`
  (sibling dirs exist for `bairros/`, `distritos/`, `subdistritos/`; formats `shp/`, `gpkg/`, `kml/`).
- Coverage: 468,097 census tracts nationally (final mesh; an earlier preliminary count of
  452,338 circulates — preliminary-vs-final, immaterial for design).

### 2.2 Tract-level aggregates (Agregados por Setores Censitários — Resultados do Universo)
- FTP root:
  `https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/`
- CSV by setor (national ZIPs — **not** split by UF):
  `.../Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv/`
  Relevant files observed:
  | File (national `_BR`) | Size | Date | Content |
  |---|---|---|---|
  | `Agregados_por_setores_basico_BR_20260520.zip` | 15M | 2026-05-20 | population, households (tract base record) |
  | `Agregados_por_setores_demografia_BR.zip` | 22M | 2024-11-12 | age/sex demographics |
  | `Agregados_por_setores_alfabetizacao_BR.zip` | 136M | 2024-11-12 | literacy by age/sex |
  | `Agregados_por_setores_caracteristicas_domicilio1_BR.zip` | 23M | 2024-11-12 | household characteristics (incl. sanitation) |
  | `Agregados_por_setores_caracteristicas_domicilio2_BR_20250417.zip` | 80M | 2025-04-17 | household characteristics (cont.) |
  | `Agregados_por_setores_caracteristicas_domicilio3_BR_20250417.zip` | 50M | 2025-04-17 | household characteristics (cont.) |
  | `Agregados_por_setores_cor_ou_raca_BR.zip` | 42M | 2024-11-12 | race/colour |
  | `Agregados_por_setores_obitos_BR.zip` | 20M | 2024-11-12 | deaths |
  - XLSX equivalents under `Agregados_por_Setor_xlsx/`; prefer CSV.
- **Geometry-with-attributes** (mesh already joined to selected attributes), if we want to
  skip the join: `.../Agregados_por_Setores_Censitarios/malha_com_atributos/` (shp by UF/BR).
- **Data dictionary** (authoritative variable codes/labels):
  `.../Agregados_por_Setores_Censitarios/dicionario_de_dados_agregados_por_setores_censitarios_20260520.xlsx`
  (XLSX — must be opened/parsed to lock exact V-codes; see §7).

### 2.3 SIDRA API (alternative to flat files)
- SIDRA "Universo — Características dos Domicílios":
  `https://sidra.ibge.gov.br/pesquisa/censo-demografico/demografico-2022/universo-caracteristicas-dos-domicilios`
- REST: `https://apisidra.ibge.gov.br/values/...`. **But:** SIDRA's published tables for the
  sanitation theme bottom out at **município** level, not tract. For tract granularity, use
  the flat-file aggregates (§2.2), not SIDRA.

### 2.4 Favelas e Comunidades Urbanas (formerly Aglomerados Subnormais)
- Landing: `https://www.ibge.gov.br/geociencias/organizacao-do-territorio/tipologias-do-territorio/15788-favelas-e-comunidades-urbanas.html`
- 2022: 12,348 favelas/urban communities, 16.39M residents nationally. Boundaries are a
  separate territorial tipology layer (polygon), and tracts carry a situation/type flag.

### 2.5 R packages (reference only, not for the pipeline)
- `{censobr}` (IPEA) wraps the same files: `read_tracts(year=2022, ...)`,
  `data_dictionary(year=2022, dataset='tracts')`. Useful to *cross-check* variable codes
  without parsing the XLSX. Pipeline stays Python.

---

## 3. Variable Mapping — IBGE → Spatia NBI components

Spatia's existing NBI (AR/PY) has four `pct_*` components plus the composite `pct_nbi`
(see `pipeline/data/itapua_nbi_2022.csv`, `pipeline/build_censo_corrientes.py`):
`pct_vivienda` (housing quality), `pct_sanitario` (sanitation infra), `pct_educacion`
(education access), `pct_subsistencia` (economic dependency).

IBGE does **not** ship these as ready percentages; we compute them as
*share of tract households/persons lacking X*. Variable codes below are described by IBGE
theme + label; **exact `V0xxxx` codes must be locked from the dictionary XLSX (§7)** — IBGE
uses opaque codes (e.g. `V0001` = population), not semantic names.

| Spatia component | IBGE source (theme / file) | Construct | Translates? |
|---|---|---|---|
| **pct_sanitario** (sanitation) | `características_domicilio` — abastecimento de água (rede geral), esgotamento sanitário (rede geral / fossa séptica vs. inadequado), coleta de lixo (direta/indireta vs. outro), canalização interna de água | % households without piped general-network water OR without adequate sewage OR without garbage collection | **Strong.** Direct conceptual match to AR's `h_cloaca`/`h_agua_red`. Best-mapped component. |
| **pct_vivienda** (housing quality) | `características_domicilio` — existência de banheiro de uso exclusivo; material/condition fields; + Favelas/Comunidades Urbanas flag (§2.4) as informal-settlement proxy | % households without exclusive bathroom OR in a favela/comunidade urbana | **Partial.** IBGE does not publish AR-style "piso inadecuado"/"hacinamiento por cuarto" identically. Use bathroom + favela flag + any density field present; document the substitution. |
| **pct_educacion** (education access) | `alfabetização` (taxa de analfabetismo, persons ≥ a threshold age) ; `básico`/`demografia` for denominators | % persons (age-bounded) illiterate / not literate | **Partial.** AR uses *non-attendance of school-age children + low adult schooling*. IBGE tract universe gives literacy robustly; school-attendance by age is thinner at tract level than INDEC's. Map to literacy-based deprivation, note the construct shift. |
| **pct_subsistencia** (economic dependency) | `DomicilioRenda` / `PessoaRenda` (rendimento) — **separate table, separate structure** | — | **Does NOT translate.** No "4 dependents per low-education earner" equivalent. Income data is continuous and household/person-level, not a dependency ratio. **Recommendation: drop this component for Brazil** (or substitute a low-income-household share as a clearly-labelled, non-equivalent proxy). Do not fake parity. |
| **pct_nbi** (composite) | derived | Composite of the above. With subsistencia dropped, the Brazil composite is a **3-component** index over a different variable basis. | **Not comparable** to AR/PY `pct_nbi`. Must be `--mode local`; label distinctly in UI. |
| (population, for dasymetric) | `básico` (V0001 total population) + `demografia`; weights = GBA building footprints | tract population redistributed to H3 by building-footprint area | **Strong.** This is the cleanest, highest-value deliverable. |

**Net:** sanitation maps cleanly; housing and education map partially (with documented
substitutions); subsistence does not map. Treat the Brazil "NBI" as a *deprivation proxy*,
not an NBI clone.

---

## 4. Geometry & Download Mechanics

- **Tract geometry:** download per-UF shapefile/gpkg from §2.1 (`setores/shp/UF/`). One file
  each for PR, SC, RS. CRS **SIRGAS 2000 / EPSG:4674**; reproject to the pipeline's working
  CRS as done for INDEC/DGEEC. Tract key = `CD_SETOR` (15-digit code: UF(2)+município(5)+
  distrito+subdistrito+setor); UF prefixes — PR=`41`, SC=`42`, RS=`43`.
- **Attribute join:** the CSV aggregates are **national** (one `_BR.zip` per theme), so the
  workflow is: download national ZIP → read CSV → **filter rows by `CD_SETOR` prefix
  (41/42/43)** → join to the per-UF geometry on `CD_SETOR`. (Unlike INDEC where we query
  PostGIS by `codprov`, here we pre-filter flat files.)
- **Encoding:** IBGE CSVs are typically Latin-1 / `;`-delimited — set explicitly on read.
- **Dasymetric step:** intersect GBA footprints with tracts → per-tract footprint area →
  redistribute tract population/household counts to H3 res-9 proportional to footprint area
  within each tract (same dasymetric logic already used for AR census = "dasymetric-only,
  69K hex" per project conventions; Brazil follows the building-weighted variant).
- **Mesh-with-attributes shortcut:** `malha_com_atributos/` already joins basic attributes to
  geometry — usable to skip the join for the `básico` theme, but the sanitation themes still
  need the separate CSVs, so a clean "geometry + manual CSV join" pipeline is more uniform.

---

## 5. Volume & Complexity Estimate

**Tract counts (order of magnitude):**
- PR ≈ 23,000 tracts (IBGE-stated).
- SC and RS not separately confirmed in sources; estimate by population scaling from PR
  (PR ~11.4M, RS ~10.9M, SC ~7.6M): **RS ≈ 28–32k, SC ≈ 16–18k**.
- **Three-state total ≈ 65–70k tracts.**

**Data size:** the national thematic ZIPs total ~**500 MB** zipped (the `_BR` files in §2.2);
once filtered to UF 41/42/43 the working tables are a fraction of that. Geometry: three UF
shapefiles, manageable.

**Complexity vs AR/PY already done:**
- *Higher* than PY (DGEEC district-level NBI was a small CSV, §built from district stats).
- *Comparable-to-higher* than AR per-province (INDEC was already in local PostGIS; here we
  ingest raw IBGE flat files + national-scope filtering + a fresh deprivation computation).
- Main new work: (a) parsing the IBGE dictionary to lock V-codes, (b) building the
  deprivation index from scratch (AR's came pre-computed as NBI), (c) GBA footprint
  intersection at ~70k-tract scale (tractable; comparable to the satellite full-raster 320K
  hex workflows already run).
- Three states ≈ low–mid hundreds of thousands of H3 res-9 hexes after dasymetric expansion;
  within the envelope of existing territories.

---

## 6. Recommended Implementation Phases (future task)

1. **Phase 0 — Lock the schema.** Parse `dicionario...20260520.xlsx`; extract exact V-codes
   for population, households, água/esgoto/lixo/banheiro, literacy, income. Cross-check
   against `{censobr}` `data_dictionary`. Output: a `br_variable_map.md` / config block.
2. **Phase 1 — Geometry + population dasymetric (highest ROI, lowest risk).** Ingest PR/SC/RS
   tract meshes; join `básico` population; GBA footprint intersection; produce H3 res-9
   dasymetric population. Ship this first — it is the clean, defensible deliverable.
3. **Phase 2 — Deprivation proxy (sanitation + housing).** Compute `pct_sanitario`,
   `pct_vivienda` from `características_domicilio`. Produce a 2-component deprivation layer.
4. **Phase 3 — Education + composite.** Add `pct_educacion` (literacy); assemble a
   3-component Brazil deprivation composite via the established PCA + geometric-mean scoring
   (`pipeline/scoring.py`), in **`--mode local`**. Explicitly label it non-comparable.
5. **Phase 4 (optional) — Favelas/Comunidades Urbanas overlay** as a categorical
   informal-settlement layer for the dasymetric/deprivation narrative.
6. **Phase 5 (optional) — income proxy** from `DomicilioRenda`, clearly labelled as a
   distinct construct (not `pct_subsistencia`).

Phases 1–3 are the core; 4–6 are enrichment.

---

## 7. Open Questions & Risks

- **Exact V-codes unverified.** The dictionary is a binary XLSX that web tooling could not
  parse here. IBGE uses opaque codes; **§3 mapping is at the theme/label level and must be
  resolved to concrete `V0xxxx` columns in Phase 0** before any computation. Treat current
  code-level claims as provisional.
- **Schema heterogeneity (warn loudly).** Three census systems, three structures:
  INDEC = radios with a *pre-built* NBI and Spanish categorical labels, in local PostGIS;
  DGEEC/INE = district-level NBI CSV; IBGE = tracts with *no NBI*, ~3,000 raw variables split
  across themed national CSVs, opaque V-codes, Latin-1/`;` files, SIRGAS 2000. **These do not
  translate 1:1.** Any "NBI" comparison across countries is methodologically invalid; the
  three are only comparable at the *conceptual* (interpretive) level, which is exactly why
  Spatia keeps census layers in `--mode local`.
- **`pct_subsistencia` gap.** No tract-level dependency-ratio equivalent. Either drop the
  component (recommended) or substitute a clearly-different income proxy — never silently.
- **Granularity asymmetry in education.** Tract-level school-attendance-by-age is thinner than
  INDEC's; literacy is the robust IBGE signal. The education component will be a different
  (weaker-construct) measure than AR's.
- **SIDRA ≠ tract for sanitation.** Don't expect the SIDRA API to deliver sanitation at tract
  level — it stops at município. Flat files are the only tract-level path.
- **National-file filtering correctness.** Must filter by `CD_SETOR` UF prefix (41/42/43)
  *before* join; an off-by-prefix bug would silently pull wrong-state rows.
- **Favela boundaries vs tract flag.** Decide whether to use the separate favela polygon layer
  or the tract-level situation flag; they have different update cadence.

---

## Sources

- [Malha de Setores Censitários — IBGE](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/estrutura-territorial/26565-malhas-de-setores-censitarios-divisoes-intramunicipais.html)
- [geoftp — malhas de setores censitários 2022](https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_de_setores_censitarios__divisoes_intramunicipais/censo_2022/)
- [ftp — Censo_Demografico_2022 / Agregados_por_Setores_Censitarios](https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/)
- [Censo 2022 — informações por setores censitários (Agência IBGE)](https://agenciadenoticias.ibge.gov.br/agencia-noticias/2012-agencia-de-noticias/noticias/39525-censo-2022-informacoes-de-populacao-e-domicilios-por-setores-censitarios-auxiliam-gestao-publica)
- [Censo 2022 — características dos domicílios / saneamento (Agência IBGE)](https://agenciadenoticias.ibge.gov.br/agencia-noticias/2012-agencia-de-noticias/noticias/39237-censo-2022-rede-de-esgoto-alcanca-62-5-da-populacao-mas-desigualdades-regionais-e-por-cor-e-raca-persistem)
- [SIDRA — Universo / Características dos Domicílios](https://sidra.ibge.gov.br/pesquisa/censo-demografico/demografico-2022/universo-caracteristicas-dos-domicilios)
- [Favelas e Comunidades Urbanas — IBGE](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/tipologias-do-territorio/15788-favelas-e-comunidades-urbanas.html)
- [Censo 2022 — Favelas e Comunidades Urbanas: 16,4 milhões (Agência IBGE)](https://agenciadenoticias.ibge.gov.br/agencia-noticias/2012-agencia-de-noticias/noticias/41797-censo-2022-brasil-tinha-16-4-milhoes-de-pessoas-morando-em-favelas-e-comunidades-urbanas)
- [{censobr} — Agregados dos Setores Censitários (IPEA)](https://ipeagit.github.io/censobr_oficina_abep_2024/5_agregados_setores.html)
- [IBGE — Dados Abertos / Política de Dados Abertos (Decreto 8.777/2016)](https://www.ibge.gov.br/acesso-informacao/dados-abertos.html)
- [Paraná ~23 mil setores censitários (cidades.ibge.gov.br)](https://cidades.ibge.gov.br/brasil/pr)

## License / Terms

IBGE data is **open data** under the federal Open Data Policy (Decreto nº 8.777/2016); IBGE
geospatial products are released "aberta, livre e irrestrita" (open, free, unrestricted) and
all files on the relevant FTP dirs are explicitly marked *"Todos os arquivos aqui disponíveis
são públicos."* Practically equivalent to CC-BY-style reuse: free use with attribution to
IBGE / Censo Demográfico 2022. Microdata at individual level is confidential (Lei 5.534/1968),
but **tract aggregates are public** and are what we use. No licensing blocker for Spatia.
Building footprints come from GBA (Zhu et al. 2025) under its own (separate) terms — verify
GBA license independently for the dasymetric weights.
