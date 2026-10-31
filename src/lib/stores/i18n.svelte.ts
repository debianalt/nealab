export type Locale = 'es' | 'en' | 'gn' | 'pt';

const dict: Record<string, Record<Locale, string>> = {
	// Header
	'header.title': { es: 'Spatia', en: 'Spatia', gn: 'Spatia', pt: 'Spatia' },
	'header.home': { es: 'Volver al mapa inicial (vista del NEA)', en: 'Back to the start map (NEA view)', gn: 'Volver al mapa inicial (vista del NEA)', pt: 'Voltar ao mapa inicial (vista do NEA)' },
	'lens.close': { es: 'Cerrar: volver a los edificios y radios censales', en: 'Close: back to buildings and census tracts', gn: 'Cerrar: volver a los edificios y radios censales', pt: 'Fechar: voltar aos edifícios e setores censitários' },
	'header.whatIsThis': { es: '¿Qué es esto?', en: 'What is this?', gn: "Mba'épa ko?", pt: 'O que é isso?' },
	'header.subtitle': { es: 'Análisis geoespacial', en: 'Geospatial analysis', gn: "Yvy Rekokatu", pt: 'Análise geoespacial' },
	'header.nav.map': { es: 'Mapa', en: 'Map', gn: 'Mapa', pt: 'Mapa' },
	'header.nav.dashboards': { es: 'Trade', en: 'Trade', gn: 'Trade', pt: 'Trade' },

	// Controls
	'ctrl.tilt': { es: 'Inclinación', en: 'Tilt', gn: "Je'apy", pt: 'Inclinação' },
	'ctrl.clear': { es: 'Limpiar', en: 'Clear', gn: "Mopotĩ", pt: 'Limpar' },

	// Sidebar
	'side.hover': { es: 'Scroll para zoom · Click derecho + arrastrar para rotar el mapa', en: 'Scroll to zoom · Right-click + drag to rotate the map', gn: "Scroll zoom hag̃ua · Click derecho + arrastrar mapa mbojere hag̃ua", pt: 'Role para zoom · Clique direito + arraste para girar o mapa' },
	'side.deselect': { es: 'Click para quitar', en: 'Click to remove', gn: "Ehesakutu emboguete hag̃ua", pt: 'Clique para remover' },

	// Radio stats sections
	'stats.buildingEstimates': { es: 'Estimaciones por edificaciones', en: 'Building estimates', gn: "Óga rehegua", pt: 'Estimativas por edificações' },
	'stats.census': { es: 'Censo 2022', en: 'Census 2022', gn: "Papapy 2022", pt: 'Censo 2022' },
	'stats.socioeconomic': { es: 'Socioeconómico', en: 'Socioeconomic', gn: "Teko porã", pt: 'Socioeconômico' },
	'stats.comparison': { es: 'Comparación', en: 'Comparison', gn: "Jojaha", pt: 'Comparação' },
	'stats.selection': { es: 'Selección', en: 'Selection', gn: 'Jeporavo', pt: 'Seleção' },

	// Stat labels
	'label.population': { es: 'Población', en: 'Population', gn: 'Yvypóra', pt: 'População' },
	'label.males': { es: 'Varones', en: 'Males', gn: "Kuimba'e", pt: 'Homens' },
	'label.females': { es: 'Mujeres', en: 'Females', gn: 'Kuña', pt: 'Mulheres' },
	'label.dwellings': { es: 'Viviendas', en: 'Dwellings', gn: 'Óga', pt: 'Domicílios' },
	'label.households': { es: 'Hogares', en: 'Households', gn: 'Ógape', pt: 'Lares' },
	'label.area': { es: 'Área', en: 'Area', gn: "Yvy", pt: 'Área' },
	'label.activityRate': { es: 'Tasa de actividad', en: 'Econ. activity rate', gn: "Tembiapo jeku'e", pt: 'Taxa de atividade' },
	'label.employmentRate': { es: 'Tasa de empleo', en: 'Employment rate', gn: "Tembiapo reko", pt: 'Taxa de emprego' },
	'label.unemploymentRate': { es: 'Tasa de desocupación', en: 'Unemployment rate', gn: "Tembiapo'ỹ", pt: 'Taxa de desemprego' },
	'label.avgHousehold': { es: 'Tamaño medio hogar', en: 'Avg household size', gn: "Óga tuichakue", pt: 'Tamanho médio do lar' },
	'label.ubn': { es: 'NBI (%)', en: 'UBN (%)', gn: "NBI (%)", pt: 'NBI (%)' },
	'label.overcrowding': { es: 'Hacinamiento (%)', en: 'Overcrowding (%)', gn: "Ñembyaty (%)", pt: 'Superlotação (%)' },
	'label.masculinityRate': { es: 'Tasa de masculinidad', en: 'Masculinity rate', gn: "Kuimba'e jeku'e", pt: 'Taxa de masculinidade' },
	'label.coverage': { es: 'Cobertura', en: 'Coverage', gn: "Joapy", pt: 'Cobertura' },
	'label.buildings': { es: 'Edificaciones', en: 'Buildings', gn: 'Óga', pt: 'Edificações' },
	'label.totalArea': { es: 'Área total', en: 'Total area', gn: 'Yvy opavave', pt: 'Área total' },
	'label.avgHeight': { es: 'Altura promedio (pond.)', en: 'Avg height (weighted)', gn: "Yvatekue", pt: 'Altura média (pond.)' },

	// Tooltip
	'tip.building': { es: 'Edificación:', en: 'Building:', gn: 'Óga:', pt: 'Edificação:' },
	'tip.height': { es: 'Altura', en: 'Height', gn: 'Yvate', pt: 'Altura' },
	'tip.area': { es: 'Área', en: 'Area', gn: 'Yvy', pt: 'Área' },
	'tip.estPersons': { es: 'Personas est.:', en: 'Est. persons:', gn: "Yvypóra:", pt: 'Pessoas est.:' },
	'tip.radio': { es: 'Radio', en: 'Radio', gn: 'Radio', pt: 'Setor' },
	'tip.pop': { es: 'Pob.:', en: 'Pop:', gn: "Yvypóra:", pt: 'Pop.:' },
	'tip.density': { es: 'Densidad:', en: 'Density:', gn: "Yvypóra/km\u00B2:", pt: 'Densidade:' },

	// Legend
	'legend.estPersons': { es: 'Población estimada por edificación', en: 'Est. population per building', gn: "Yvypóra óga pegua", pt: 'População estimada por edificação' },
	'legend.estPersonsNote': { es: 'estimación dasimétrica (no medición)', en: 'dasymetric estimate (not measured)', gn: 'estimación dasimétrica', pt: 'estimativa dasimétrica (não medição)' },
	'legend.buildingVolume': { es: 'Volumen construido (área × altura)', en: 'Built volume (area × height)', gn: 'Óga tuichakue', pt: 'Volume construído (área × altura)' },
	'legend.buildingVolumeNote': { es: 'huellas: GBA, Google Open Buildings, Microsoft, Overture/OSM', en: 'footprints: GBA, Google Open Buildings, Microsoft, Overture/OSM', gn: 'huellas: GBA, Google Open Buildings, Microsoft, Overture/OSM', pt: 'pegadas: GBA, Google Open Buildings, Microsoft, Overture/OSM' },
	'legend.buildingsCanvas': { es: 'Edificios — GBA, Google/Microsoft, Overture', en: 'Buildings — GBA, Google/Microsoft, Overture', gn: 'Óga — GBA, Google/Microsoft, Overture', pt: 'Edifícios — GBA, Google/Microsoft, Overture' },
	'analysis.scores.score': { es: 'Score de consolidación', en: 'Consolidation score', gn: 'Score consolidación', pt: 'Score de consolidação' },
	'analysis.accessibility.score': { es: 'Score accesibilidad', en: 'Accessibility score', gn: 'Score accesibilidad', pt: 'Score de acessibilidade' },
	'legend.low': { es: 'Bajo', en: 'Low', gn: 'Michĩ', pt: 'Baixo' },
	'legend.medium': { es: 'Medio', en: 'Medium', gn: 'Mbyte', pt: 'Médio' },
	'legend.high': { es: 'Alto', en: 'High', gn: 'Yvate', pt: 'Alto' },
	'legend.veryLow': { es: 'Muy bajo', en: 'Very low', gn: 'Michĩmĩ', pt: 'Muito baixo' },
	'legend.lowRisk': { es: 'Bajo riesgo', en: 'Low risk', gn: 'Michĩ riesgo', pt: 'Baixo risco' },
	'legend.highRisk': { es: 'Alto riesgo', en: 'High risk', gn: 'Yvate riesgo', pt: 'Alto risco' },
	// Per-analysis legend labels
	'legend.flood.none': { es: 'Sin agua detectada (0 %)', en: 'No water detected (0%)', gn: 'Ndaipóri y ojehecháva (0 %)', pt: 'Sem água detectada (0 %)' },
	'legend.flood.low': { es: 'Baja (<5 %)', en: 'Low (<5%)', gn: 'Michĩ (<5 %)', pt: 'Baixa (<5 %)' },
	'legend.flood.mid': { es: 'Moderada (5–20 %)', en: 'Moderate (5–20%)', gn: 'Mbyte (5–20 %)', pt: 'Moderada (5–20 %)' },
	'legend.flood.high': { es: 'Alta (>20 %)', en: 'High (>20%)', gn: 'Guasu (>20 %)', pt: 'Alta (>20 %)' },
	'legend.locValue.low': { es: 'Peor ubicación', en: 'Worse location', gn: 'Michĩ valor', pt: 'Pior localização' },
	'legend.locValue.high': { es: 'Mejor ubicación', en: 'Better location', gn: 'Tuicha valor', pt: 'Melhor localização' },
	'legend.agri.low': { es: 'Menor aptitud', en: 'Less suitable', gn: 'Michĩ aptitud', pt: 'Menor aptidão' },
	'legend.agri.high': { es: 'Mayor aptitud', en: 'More suitable', gn: 'Tuicha aptitud', pt: 'Maior aptidão' },
	'legend.forestry.low': { es: 'Menor aptitud forestal', en: 'Lower forestry aptitude', gn: "Michĩ aptitud forestal", pt: 'Menor aptidão florestal' },
	'legend.forestry.high': { es: 'Mayor aptitud forestal', en: 'Higher forestry aptitude', gn: "Tuicha aptitud forestal", pt: 'Maior aptidão florestal' },
	'legend.deprivation.low': { es: 'Baja carencia', en: 'Low deprivation', gn: 'Michĩ mba\'e\'ỹ' , pt: 'Baixa carência' },
	'legend.deprivation.high': { es: 'Alta carencia', en: 'High deprivation', gn: 'Tuicha mba\'e\'ỹ' , pt: 'Alta carência' },
	'legend.isolation.low': { es: 'Bajo aislamiento', en: 'Low isolation', gn: 'Michĩ mombyry', pt: 'Baixo isolamento' },
	'legend.isolation.high': { es: 'Alto aislamiento', en: 'High isolation', gn: 'Tuicha mombyry', pt: 'Alto isolamento' },
	'legend.health.low': { es: 'Buen acceso', en: 'Good access', gn: 'Porã access', pt: 'Bom acesso' },
	'legend.health.high': { es: 'Mal acceso', en: 'Poor access', gn: 'Vai access', pt: 'Mau acesso' },
	'legend.eduCap.low': { es: 'Alto capital', en: 'High capital', gn: 'Tuicha capital', pt: 'Alto capital' },
	'legend.eduCap.high': { es: 'Bajo capital', en: 'Low capital', gn: 'Michĩ capital', pt: 'Baixo capital' },
	'legend.eduFlow.low': { es: 'Buen desempeño', en: 'Good performance', gn: 'Porã', pt: 'Bom desempenho' },
	'legend.eduFlow.high': { es: 'Mal desempeño', en: 'Poor performance', gn: 'Vai', pt: 'Mau desempenho' },
	'legend.eudr.low': { es: 'Sin pérdida post-2020', en: 'No post-2020 loss', gn: 'Pérdida post-2020 ỹre', pt: 'Sem perda pós-2020' },
	'legend.eudr.high': { es: 'Mayor pérdida post-2020', en: 'Higher post-2020 loss', gn: 'Pérdida post-2020 tuichave', pt: 'Maior perda pós-2020' },
	'legend.accessibility.low': { es: 'Cerca de la capital', en: 'Near capital', gn: 'Peteĩ', pt: 'Perto da capital' },
	'legend.accessibility.high': { es: 'Lejos de la capital', en: 'Far from capital', gn: 'Mombyry', pt: 'Longe da capital' },
	'legend.socio.low': { es: 'Bajo NBI', en: 'Low NBI', gn: 'Michĩ NBI', pt: 'Baixo NBI' },
	'legend.socio.high': { es: 'Alto NBI', en: 'High NBI', gn: 'Tuicha NBI', pt: 'Alto NBI' },
	'legend.economic.low': { es: 'Poca actividad nocturna', en: 'Low nighttime activity', gn: 'Michĩ', pt: 'Pouca atividade noturna' },
	'legend.economic.high': { es: 'Alta actividad nocturna', en: 'High nighttime activity', gn: 'Tuicha', pt: 'Alta atividade noturna' },
	'legend.scores.low': { es: 'Menor consolidación', en: 'Less consolidated', gn: 'Michĩ', pt: 'Menor consolidação' },
	'legend.scores.high': { es: 'Mayor consolidación', en: 'More consolidated', gn: 'Tuicha', pt: 'Maior consolidação' },
	'legend.range': { es: 'Score 0–100', en: 'Score 0–100', gn: 'Score 0–100', pt: 'Score 0–100' },
	'legend.highValue': { es: 'Valor alto', en: 'High value', gn: 'Valor yvate', pt: 'Valor alto' },
	'unit.pctlProv': { es: 'percentil prov.', en: 'prov. percentile', gn: 'percentil prov.', pt: 'percentil prov.' },
	'legend.deptFill.byDept': { es: 'por departamento', en: 'by department', gn: 'departamento rupi', pt: 'por departamento' },
	'legend.deptFill.byDist': { es: 'por distrito', en: 'by district', gn: 'distrito rupi', pt: 'por distrito' },
	'legend.deptFill.byMun': { es: 'por municipio', en: 'by municipality', gn: 'municipio rupi', pt: 'por município' },
	'legend.deptFill.na': { es: 'Sin dato', en: 'No value', gn: 'Ndaipóri dato', pt: 'Sem dado' },
	'legend.noData': { es: 'Sin cobertura', en: 'No data', gn: "Ndaipóri dato", pt: 'Sem cobertura' },
	// ── Analysis section headings ──
	// Generic spinner label for panels that had 'Cargando…' hardcoded in all four locales.
	// Values lifted verbatim from side.censoTemporal.loading — no new copy, just a key
	// whose name doesn't tie it to one panel.
	'common.loading': { es: 'Cargando…', en: 'Loading…', gn: 'Oñembohasahína…', pt: 'Carregando…' },

	// ── Analysis menu group headers ──
	// Were a Spanish-only lookup table in AnalysisMenu.svelte, rendered to all four
	// locales. `gn` here is the Spanish string — a declared fallback, not Guaraní: the
	// map's gn readers already saw exactly this text, so nothing regresses, and English
	// was never the right fallback for them. Grep `gn: pendiente` for the full list.
	'menu.group.comparable': { es: '↔ Comparables entre Argentina, Paraguay y Brasil', en: '↔ Comparable across Argentina, Paraguay and Brazil', gn: '↔ Comparables entre Argentina, Paraguay y Brasil', pt: '↔ Comparáveis entre Argentina, Paraguai e Brasil' },
	'menu.group.comparableAr': { es: '↔ Comparables entre provincias argentinas (censo INDEC)', en: '↔ Comparable across Argentine provinces (INDEC census)', gn: '↔ Comparables entre provincias argentinas (censo INDEC)', pt: '↔ Comparáveis entre províncias argentinas (censo INDEC)' }, // gn: pendiente
	'menu.group.localOnly': { es: 'Solo {inRef}', en: 'Only {inRef}', gn: 'Solo {inRef}', pt: 'Apenas {inRef}' },
	'menu.sub.waterRisk': { es: 'Riesgo hídrico', en: 'Water risk', gn: 'Riesgo hídrico', pt: 'Risco hídrico' }, // gn: pendiente
	'menu.sub.naturalCover': { es: 'Cobertura natural', en: 'Natural cover', gn: 'Cobertura natural', pt: 'Cobertura natural' }, // gn: pendiente
	'menu.sub.airQuality': { es: 'Calidad del aire', en: 'Air quality', gn: 'Calidad del aire', pt: 'Qualidade do ar' }, // gn: pendiente
	'menu.sub.soils': { es: 'Suelos', en: 'Soils', gn: 'Suelos', pt: 'Solos' }, // gn: pendiente
	'menu.sub.landUse': { es: 'Uso del suelo', en: 'Land use', gn: 'Uso del suelo', pt: 'Uso do solo' }, // gn: pendiente
	'menu.sub.accessibility': { es: 'Accesibilidad', en: 'Accessibility', gn: 'Accesibilidad', pt: 'Acessibilidade' }, // gn: pendiente
	'menu.sub.population': { es: 'Población', en: 'Population', gn: 'Población', pt: 'População' }, // gn: pendiente
	'menu.sub.basicServices': { es: 'Servicios básicos', en: 'Basic services', gn: 'Servicios básicos', pt: 'Serviços básicos' }, // gn: pendiente
	'menu.sub.education': { es: 'Educación', en: 'Education', gn: 'Educación', pt: 'Educação' }, // gn: pendiente
	'menu.sub.activity': { es: 'Actividad', en: 'Activity', gn: 'Actividad', pt: 'Atividade' }, // gn: pendiente
	'menu.sub.trade': { es: 'Comercio (EUDR)', en: 'Trade (EUDR)', gn: 'Comercio (EUDR)', pt: 'Comércio (EUDR)' }, // gn: pendiente
	'menu.sub.other': { es: 'Otros', en: 'Other', gn: 'Otros', pt: 'Outros' }, // gn: pendiente
	// Rigor badges under every analysis in the menu. Emoji stays in the value — it is
	// part of the badge, not decoration around it.
	'analysis.rigor.physical': { es: '🛰 Medición satelital', en: '🛰 Satellite measurement', gn: '🛰 Medición satelital', pt: '🛰 Medição por satélite' }, // gn: pendiente
	'analysis.rigor.modeled': { es: '📐 Aptitud modelada', en: '📐 Modelled suitability', gn: '📐 Aptitud modelada', pt: '📐 Aptidão modelada' }, // gn: pendiente
	'analysis.rigor.census': { es: '🏛 Indicador censal', en: '🏛 Census indicator', gn: '🏛 Indicador censal', pt: '🏛 Indicador censitário' }, // gn: pendiente

	// ── Map chrome ──
	// Were inline locale ternaries in +page.svelte. None of them had a gn branch, so a
	// Guaraní reader hit the English default on the two hints below — English was never
	// the right fallback for a reader in the NEA. Spanish is, until these are translated.
	'map.hover.pick': { es: '{name} · clic para elegir', en: '{name} · click to select', gn: '{name} · clic para elegir', pt: '{name} · clique para escolher' },
	'map.hint.zoomNeighbours': { es: 'Acercá para ver los hexágonos de las provincias, departamentos y estados vecinos', en: 'Zoom in to load the neighbouring provinces, departments and states', gn: 'Acercá para ver los hexágonos de las provincias, departamentos y estados vecinos', pt: 'Aproxime para ver os hexágonos das províncias, departamentos e estados vizinhos' },
	'map.hint.aggregated': { es: 'Vista agregada — promedios espaciales · acercá para ver el detalle por hexágono', en: 'Aggregated overview — spatial means · zoom in for per-hex detail', gn: 'Vista agregada — promedios espaciales · acercá para ver el detalle por hexágono', pt: 'Vista agregada — médias espaciais · aproxime para ver o detalhe por hexágono' }, // gn: pendiente
	'map.basemap.map': { es: 'Mapa', en: 'Map', gn: 'Mapa', pt: 'Mapa' }, // gn: pendiente
	'map.basemap.satellite': { es: 'Satélite', en: 'Satellite', gn: 'Satélite', pt: 'Satélite' }, // gn: pendiente
	'map.opacity': { es: 'Opacidad', en: 'Opacity', gn: 'Opacidad', pt: 'Opacidade' }, // gn: pendiente

	// ── Analysis panel ──
	'analysis.noDeptData': { es: 'No hay datos departamentales para {ref}.', en: 'No department-level data for {ref}.', gn: 'No hay datos departamentales para {ref}.', pt: 'Não há dados departamentais para {ref}.' },
	'analysis.loadingTerritory': { es: 'cargando datos {ofRef}…', en: 'loading data {ofRef}…', gn: 'cargando datos {ofRef}…', pt: 'carregando dados {ofRef}…' },
	'analysis.distByTerritory': { es: 'Distribución por provincia, departamento y estado /100', en: 'Distribution by province, department and state /100', gn: 'Distribución por provincia, departamento y estado /100', pt: 'Distribuição por província, departamento e estado /100' },

	// ── Chart panels ──
	// Hints and empty states that were Spanish literals in the markup. The two *.note
	// keys carry <em> and render through {@html}, same as the methodology blocks.
	'chart.hexDblClick': { es: 'hex · doble-clic para limpiar', en: 'hex · double-click to clear', gn: 'hex · doble-clic para limpiar', pt: 'hex · duplo clique para limpar' }, // gn: pendiente
	'chart.loadError': { es: 'error al cargar datos', en: 'error loading data', gn: 'error al cargar datos', pt: 'erro ao carregar dados' }, // gn: pendiente
	'chart.bivariate.pts': { es: 'pts · arrastrá para seleccionar', en: 'pts · drag to select', gn: 'pts · arrastrá para seleccionar', pt: 'pts · arraste para selecionar' }, // gn: pendiente
	'chart.bivariate.pickY': { es: 'elegí un eje Y abajo', en: 'pick a Y axis below', gn: 'elegí un eje Y abajo', pt: 'escolha um eixo Y abaixo' }, // gn: pendiente
	'chart.bivariate.intro': { es: 'Seleccioná un segundo análisis en "Eje Y" para explorar correlaciones y seleccionar hexágonos en el mapa.', en: 'Select a second analysis under "Y axis" to explore correlations and select hexagons on the map.', gn: 'Seleccioná un segundo análisis en "Eje Y" para explorar correlaciones y seleccionar hexágonos en el mapa.', pt: 'Selecione uma segunda análise em "Eixo Y" para explorar correlações e selecionar hexágonos no mapa.' }, // gn: pendiente
	'chart.bivariate.noCommon': { es: 'sin hexágonos en común', en: 'no hexagons in common', gn: 'sin hexágonos en común', pt: 'sem hexágonos em comum' }, // gn: pendiente
	'chart.bivariate.axisX': { es: 'Eje X (horizontal):', en: 'X axis (horizontal):', gn: 'Eje X (horizontal):', pt: 'Eixo X (horizontal):' }, // gn: pendiente
	'chart.bivariate.axisY': { es: 'Eje Y (vertical):', en: 'Y axis (vertical):', gn: 'Eje Y (vertical):', pt: 'Eixo Y (vertical):' }, // gn: pendiente
	'chart.bivariate.pick': { es: '— elegir capa —', en: '— pick a layer —', gn: '— elegir capa —', pt: '— escolher camada —' }, // gn: pendiente
	'chart.bivariate.medianNote': { es: 'Líneas punteadas: mediana de cada eje. Tocá una esquina para ver ese cuadrante en el mapa.', en: 'Dashed lines: median of each axis. Tap a corner to see that quadrant on the map.', gn: 'Líneas punteadas: mediana de cada eje. Tocá una esquina para ver ese cuadrante en el mapa.', pt: 'Linhas tracejadas: mediana de cada eixo. Toque um canto para ver esse quadrante no mapa.' }, // gn: pendiente
	'chart.bivariate.hi': { es: 'alto', en: 'high', gn: 'alto', pt: 'alto' }, // gn: pendiente
	'chart.bivariate.lo': { es: 'bajo', en: 'low', gn: 'bajo', pt: 'baixo' }, // gn: pendiente
	'chart.bivariate.paintMap': { es: 'Pintar el mapa con las dos capas (3×3)', en: 'Paint the map with both layers (3×3)', gn: 'Pintar el mapa con las dos capas (3×3)', pt: 'Pintar o mapa com as duas camadas (3×3)' }, // gn: pendiente
	'chart.bivariate.paintMapOff': { es: 'Volver al mapa de la capa', en: 'Back to the layer map', gn: 'Volver al mapa de la capa', pt: 'Voltar ao mapa da camada' }, // gn: pendiente
	'chart.bivariate.legendNote': { es: 'Cada eje en tercios de hexágonos (bajo, medio, alto). Tocá una celda para ver esos hexágonos; el número es cuántos hay.', en: 'Each axis split into thirds of hexagons (low, mid, high). Tap a cell to see those hexagons; the number is how many.', gn: 'Cada eje en tercios de hexágonos (bajo, medio, alto). Tocá una celda para ver esos hexágonos; el número es cuántos hay.', pt: 'Cada eixo em terços de hexágonos (baixo, médio, alto). Toque uma célula para ver esses hexágonos; o número é quantos há.' }, // gn: pendiente
	'chart.conc.title': { es: 'Concentración', en: 'Concentration', gn: 'Concentración', pt: 'Concentração' }, // gn: pendiente
	'chart.conc.top10': { es: 'El 10 % de los hexágonos concentra el {p} del total.', en: '10 % of the hexagons hold {p} of the total.', gn: 'El 10 % de los hexágonos concentra el {p} del total.', pt: '10 % dos hexágonos concentram {p} do total.' }, // gn: pendiente
	'chart.conc.all': { es: 'Todo el total está en el {p} de los hexágonos; el resto vale cero.', en: 'The whole total sits in {p} of the hexagons; the rest are zero.', gn: 'Todo el total está en el {p} de los hexágonos; el resto vale cero.', pt: 'Todo o total está em {p} dos hexágonos; o resto vale zero.' }, // gn: pendiente
	'chart.conc.half': { es: 'La mitad del total está en el {p} de los hexágonos.', en: 'Half of the total sits in {p} of the hexagons.', gn: 'La mitad del total está en el {p} de los hexágonos.', pt: 'Metade do total está em {p} dos hexágonos.' }, // gn: pendiente
	'chart.conc.hint': { es: 'tocá o arrastrá para ver en el mapa los que más aportan', en: 'tap or drag to see the top contributors on the map', gn: 'tocá o arrastrá para ver en el mapa los que más aportan', pt: 'toque ou arraste para ver no mapa os que mais contribuem' }, // gn: pendiente
	'chart.conc.sel': { es: 'El {k} % que más aporta: {n} hex · {p} % del total · doble clic limpia', en: 'Top {k} %: {n} hex · {p} % of the total · double-click clears', gn: 'El {k} % que más aporta: {n} hex · {p} % del total · doble clic limpia', pt: 'Os {k} % que mais contribuem: {n} hex · {p} % do total · duplo clique limpa' }, // gn: pendiente
	'chart.conc.even': { es: 'reparto parejo', en: 'even split', gn: 'reparto parejo', pt: 'divisão igual' }, // gn: pendiente
	'chart.conc.axisX': { es: '% de hexágonos, de mayor a menor', en: '% of hexagons, largest first', gn: '% de hexágonos, de mayor a menor', pt: '% de hexágonos, do maior ao menor' }, // gn: pendiente
	'chart.conc.axisY': { es: '% del total de {v}', en: '% of total {v}', gn: '% del total de {v}', pt: '% do total de {v}' }, // gn: pendiente
	'chart.conc.empty': { es: 'sin valores mayores que cero', en: 'no values above zero', gn: 'sin valores mayores que cero', pt: 'sem valores acima de zero' }, // gn: pendiente
	'chart.analysisCoarse': { es: 'Distrito muy grande: los gráficos se calculan sobre celdas agregadas (res {res}), no sobre cada hexágono.', en: 'Very large district: charts use aggregated cells (res {res}), not each hexagon.', gn: 'Distrito muy grande: los gráficos se calculan sobre celdas agregadas (res {res}), no sobre cada hexágono.', pt: 'Distrito muito grande: os gráficos usam células agregadas (res {res}), não cada hexágono.' }, // gn: pendiente
	'chart.dist.title': { es: 'Perfil por distancia', en: 'Distance profile', gn: 'Perfil por distancia', pt: 'Perfil por distância' }, // gn: pendiente
	'chart.dist.to': { es: 'Distancia a:', en: 'Distance to:', gn: 'Distancia a:', pt: 'Distância a:' }, // gn: pendiente
	'chart.dist.loading': { es: 'cargando distancias…', en: 'loading distances…', gn: 'cargando distancias…', pt: 'carregando distâncias…' }, // gn: pendiente
	'chart.dist.noData': { es: 'Esta distancia no está calculada en este territorio.', en: 'This distance is not computed for this territory.', gn: 'Esta distancia no está calculada en este territorio.', pt: 'Esta distância não está calculada neste território.' }, // gn: pendiente
	'chart.dist.near': { es: 'Cerca', en: 'Near', gn: 'Cerca', pt: 'Perto' }, // gn: pendiente
	'chart.dist.peak': { es: 'Máximo', en: 'Peak', gn: 'Máximo', pt: 'Máximo' }, // gn: pendiente
	'chart.dist.far': { es: 'lejos', en: 'far', gn: 'lejos', pt: 'longe' }, // gn: pendiente
	'chart.dist.hint': { es: 'tocá una barra para ver esos hexágonos en el mapa', en: 'tap a bar to see those hexagons on the map', gn: 'tocá una barra para ver esos hexágonos en el mapa', pt: 'toque uma barra para ver esses hexágonos no mapa' }, // gn: pendiente
	'chart.dist.clear': { es: 'tocá de nuevo para limpiar', en: 'tap again to clear', gn: 'tocá de nuevo para limpiar', pt: 'toque de novo para limpar' }, // gn: pendiente
	'chart.dist.note': { es: 'Barras: promedio de {v} en cada banda de distancia; debajo, cuántos hexágonos hay. Línea punteada: promedio del área.', en: 'Bars: mean {v} in each distance band; below, how many hexagons. Dashed line: area mean.', gn: 'Barras: promedio de {v} en cada banda de distancia; debajo, cuántos hexágonos hay. Línea punteada: promedio del área.', pt: 'Barras: média de {v} em cada faixa de distância; abaixo, quantos hexágonos. Linha tracejada: média da área.' }, // gn: pendiente
	'ficha.title': { es: 'Ficha del hexágono', en: 'Hexagon fact sheet', gn: 'Ficha del hexágono', pt: 'Ficha do hexágono' }, // gn: pendiente
	'ficha.open': { es: 'Ficha completa (todas las capas)', en: 'Full fact sheet (all layers)', gn: 'Ficha completa (todas las capas)', pt: 'Ficha completa (todas as camadas)' }, // gn: pendiente
	'ficha.loading': { es: 'Reuniendo todas las capas de este hexágono…', en: 'Gathering every layer for this hexagon…', gn: 'Reuniendo todas las capas de este hexágono…', pt: 'Reunindo todas as camadas deste hexágono…' }, // gn: pendiente
	'ficha.copyLink': { es: 'Copiar enlace', en: 'Copy link', gn: 'Copiar enlace', pt: 'Copiar link' }, // gn: pendiente
	'ficha.copied': { es: 'Enlace copiado', en: 'Link copied', gn: 'Enlace copiado', pt: 'Link copiado' }, // gn: pendiente
	'ficha.close': { es: 'Cerrar', en: 'Close', gn: 'Cerrar', pt: 'Fechar' }, // gn: pendiente
	'ficha.legend': { es: 'Cada capa con su valor en este hexágono. P = percentil en el departamento: qué parte de sus hexágonos tiene un valor menor (P90 = más alto que el 90 %). Tocá una capa para ver todas sus variables y el promedio del departamento.', en: 'Each layer with its value in this hexagon. P = percentile within the department: share of its hexagons with a lower value (P90 = higher than 90 %). Tap a layer for all its variables and the department mean.', gn: 'Cada capa con su valor en este hexágono. P = percentil en el departamento: qué parte de sus hexágonos tiene un valor menor (P90 = más alto que el 90 %). Tocá una capa para ver todas sus variables y el promedio del departamento.', pt: 'Cada camada com seu valor neste hexágono. P = percentil no departamento: parte dos hexágonos com valor menor (P90 = maior que 90 %). Toque uma camada para ver todas as variáveis e a média do departamento.' }, // gn: pendiente
	'ficha.pctlHint': { es: 'Percentil dentro del departamento', en: 'Percentile within the department', gn: 'Percentil dentro del departamento', pt: 'Percentil no departamento' }, // gn: pendiente
	'ficha.noData': { es: 'sin dato en este hexágono', en: 'no data for this hexagon', gn: 'sin dato en este hexágono', pt: 'sem dado neste hexágono' }, // gn: pendiente
	'ficha.noPop': { es: 'sin población estimada (sin edificios)', en: 'no estimated population (no buildings)', gn: 'sin población estimada (sin edificios)', pt: 'sem população estimada (sem edificações)' }, // gn: pendiente
	'ficha.colValue': { es: 'Valor', en: 'Value', gn: 'Valor', pt: 'Valor' }, // gn: pendiente
	'ficha.colPctlDept': { es: 'P depto.', en: 'P dept.', gn: 'P depto.', pt: 'P depto.' }, // gn: pendiente
	'ficha.colMeanDept': { es: 'Prom. depto.', en: 'Dept. mean', gn: 'Prom. depto.', pt: 'Média depto.' }, // gn: pendiente
	'ficha.colPctlTerr': { es: 'P territ.', en: 'P terr.', gn: 'P territ.', pt: 'P territ.' }, // gn: pendiente
	'ficha.colMeanTerr': { es: 'Prom. territ.', en: 'Terr. mean', gn: 'Prom. territ.', pt: 'Média territ.' }, // gn: pendiente
	'ficha.scopeTerr': { es: 'Esta capa no está dividida por departamento: percentil y promedio son del territorio entero.', en: 'This layer is not split by department: percentile and mean refer to the whole territory.', gn: 'Esta capa no está dividida por departamento: percentil y promedio son del territorio entero.', pt: 'Esta camada não é dividida por departamento: percentil e média são do território inteiro.' }, // gn: pendiente
	'ficha.pending': { es: 'Próximamente: posición en la provincia y en la región, e informe en PDF.', en: 'Coming soon: position within the province and the region, and a PDF report.', gn: 'Próximamente: posición en la provincia y en la región, e informe en PDF.', pt: 'Em breve: posição na província e na região, e relatório em PDF.' }, // gn: pendiente
	'ficha.legend2': { es: 'Cada capa: el valor de este hexágono, la mediana y el promedio del departamento, y dónde cae el hexágono entre todos los del departamento.', en: 'Each layer: the value in this hexagon, the department median and mean, and where the hexagon falls among all hexagons in the department.', gn: 'Cada capa: el valor de este hexágono, la mediana y el promedio del departamento, y dónde cae el hexágono entre todos los del departamento.', pt: 'Cada camada: o valor deste hexágono, a mediana e a média do departamento, e onde o hexágono fica entre todos os do departamento.' }, // gn: pendiente
	'ficha.lg.band': { es: '8 de cada 10 (P10–P90)', en: '8 in 10 (P10–P90)', gn: '8 de cada 10 (P10–P90)', pt: '8 de cada 10 (P10–P90)' }, // gn: pendiente
	'ficha.lg.box': { es: 'la mitad central (P25–P75)', en: 'middle half (P25–P75)', gn: 'la mitad central (P25–P75)', pt: 'metade central (P25–P75)' }, // gn: pendiente
	'ficha.lg.median': { es: 'mediana', en: 'median', gn: 'mediana', pt: 'mediana' }, // gn: pendiente
	'ficha.lg.mean': { es: 'promedio', en: 'mean', gn: 'promedio', pt: 'média' }, // gn: pendiente
	'ficha.lg.hex': { es: 'este hexágono', en: 'this hexagon', gn: 'este hexágono', pt: 'este hexágono' }, // gn: pendiente
	'ficha.ctx': { es: 'mediana {med} · promedio {mean}', en: 'median {med} · mean {mean}', gn: 'mediana {med} · promedio {mean}', pt: 'mediana {med} · média {mean}' }, // gn: pendiente
	'ficha.w.higher': { es: 'Más alto que {n} de cada 100 hexágonos {of}.', en: 'Higher than {n} in 100 hexagons {of}.', gn: 'Más alto que {n} de cada 100 hexágonos {of}.', pt: 'Maior que {n} de cada 100 hexágonos {of}.' }, // gn: pendiente
	'ficha.w.lower': { es: 'Más bajo que {n} de cada 100 hexágonos {of}.', en: 'Lower than {n} in 100 hexagons {of}.', gn: 'Más bajo que {n} de cada 100 hexágonos {of}.', pt: 'Menor que {n} de cada 100 hexágonos {of}.' }, // gn: pendiente
	'ficha.w.equal': { es: 'Igual que {n} de cada 100 hexágonos {of}', en: 'Same as {n} in 100 hexagons {of}', gn: 'Igual que {n} de cada 100 hexágonos {of}', pt: 'Igual a {n} de cada 100 hexágonos {of}' }, // gn: pendiente
	'ficha.w.andHigher': { es: '; más alto que {n}.', en: '; higher than {n}.', gn: '; más alto que {n}.', pt: '; maior que {n}.' }, // gn: pendiente
	'ficha.w.andLower': { es: '; más bajo que {n}.', en: '; lower than {n}.', gn: '; más bajo que {n}.', pt: '; menor que {n}.' }, // gn: pendiente
	'ficha.of.ar': { es: 'del departamento {name}', en: 'in {name} department', gn: 'del departamento {name}', pt: 'do departamento {name}' }, // gn: pendiente
	'ficha.of.py': { es: 'del distrito {name}', en: 'in {name} district', gn: 'del distrito {name}', pt: 'do distrito {name}' }, // gn: pendiente
	'ficha.of.br': { es: 'del municipio {name}', en: 'in the municipality of {name}', gn: 'del municipio {name}', pt: 'do município {name}' }, // gn: pendiente
	'ficha.sqrt': { es: 'escala √', en: '√ scale', gn: 'escala √', pt: 'escala √' }, // gn: pendiente
	'ficha.allVars': { es: 'Todas las variables ({n})', en: 'All variables ({n})', gn: 'Todas las variables ({n})', pt: 'Todas as variáveis ({n})' }, // gn: pendiente
	'ficha.colMedianDept': { es: 'Med. depto.', en: 'Dept. median', gn: 'Med. depto.', pt: 'Med. depto.' }, // gn: pendiente
	'ficha.colMedianTerr': { es: 'Med. territ.', en: 'Terr. median', gn: 'Med. territ.', pt: 'Med. territ.' }, // gn: pendiente
	'censo.deptScope': { es: 'Gráficos y mapa: departamento {name}', en: 'Charts and map: {name} department', gn: 'Gráficos y mapa: departamento {name}', pt: 'Gráficos e mapa: departamento {name}' }, // gn: pendiente
	'censo.deptScopeAll': { es: 'ver {ref} completa', en: 'show all of {ref}', gn: 'ver {ref} completa', pt: 'ver {ref} inteira' }, // gn: pendiente
	'chart.median': { es: 'mediana', en: 'median', gn: 'mediana', pt: 'mediana' }, // gn: pendiente
	'chart.hexShort': { es: 'hex.', en: 'hex.', gn: 'hex.', pt: 'hex.' }, // gn: pendiente
	'chart.bump.hint': { es: 'pasá el mouse para explorar', en: 'hover to explore', gn: 'pasá el mouse para explorar', pt: 'passe o mouse para explorar' }, // gn: pendiente
	'chart.bump.axis': { es: 'posición relativa (#1 = mejor)', en: 'relative rank (#1 = best)', gn: 'posición relativa (#1 = mejor)', pt: 'posição relativa (#1 = melhor)' }, // gn: pendiente
	'chart.bump.note': { es: '#1 = mejor posición siempre · En <em>riesgo/aislamiento</em> (Inund., Amb., Acceso): #1 = menos expuesto · En <em>potencial</em> (resto): #1 = mayor valor', en: '#1 = best rank always · Under <em>risk/isolation</em> (Flood, Env., Access): #1 = least exposed · Under <em>potential</em> (the rest): #1 = highest value', gn: '#1 = mejor posición siempre · En <em>riesgo/aislamiento</em> (Inund., Amb., Acceso): #1 = menos expuesto · En <em>potencial</em> (resto): #1 = mayor valor', pt: '#1 = melhor posição sempre · Em <em>risco/isolamento</em> (Inund., Amb., Acesso): #1 = menos exposto · Em <em>potencial</em> (o resto): #1 = maior valor' }, // gn: pendiente
	'chart.bump.noData': { es: 'sin datos comparables disponibles', en: 'no comparable data available', gn: 'sin datos comparables disponibles', pt: 'sem dados comparáveis disponíveis' }, // gn: pendiente
	'chart.histogram.dragRange': { es: 'arrastrá para seleccionar rango', en: 'drag to select a range', gn: 'arrastrá para seleccionar rango', pt: 'arraste para selecionar o intervalo' }, // gn: pendiente
	'chart.moran.title': { es: 'Autocorrelación espacial (Moran)', en: 'Spatial autocorrelation (Moran)', gn: 'Autocorrelación espacial (Moran)', pt: 'Autocorrelação espacial (Moran)' },
	'chart.moran.computing': { es: 'Calculando I de Moran…', en: 'Computing Moran’s I…', gn: 'Calculando I de Moran…', pt: 'Calculando o I de Moran…' },
	'chart.moran.lag': { es: 'Promedio de los vecinos', en: 'Neighbours’ average', gn: 'Promedio de los vecinos', pt: 'Média dos vizinhos' },
	'chart.moran.std': { es: '(en desvíos)', en: '(in SDs)', gn: '(en desvíos)', pt: '(em desvios)' },
	'chart.moran.qHH': { es: 'Foco alto', en: 'High cluster', gn: 'Foco alto', pt: 'Foco alto' },
	'chart.moran.qLL': { es: 'Foco bajo', en: 'Low cluster', gn: 'Foco bajo', pt: 'Foco baixo' },
	'chart.moran.qHL': { es: 'Alto aislado', en: 'High outlier', gn: 'Alto aislado', pt: 'Alto isolado' },
	'chart.moran.qLH': { es: 'Bajo aislado', en: 'Low outlier', gn: 'Bajo aislado', pt: 'Baixo isolado' },
	'chart.moran.nHex': { es: 'hexágonos', en: 'hexagons', gn: 'hexágonos', pt: 'hexágonos' },
	'chart.moran.nAggregated': { es: 'celdas (res {res}, vista agregada)', en: 'cells (res {res}, aggregated view)', gn: 'celdas (res {res}, vista agregada)', pt: 'células (res {res}, vista agregada)' },
	'chart.moran.read.strong': { es: 'Agrupamiento fuerte: los valores altos están junto a otros altos y los bajos junto a bajos.', en: 'Strong clustering: high values sit next to high values and low next to low.', gn: 'Agrupamiento fuerte: los valores altos están junto a otros altos y los bajos junto a bajos.', pt: 'Agrupamento forte: valores altos ficam junto de altos e baixos junto de baixos.' },
	'chart.moran.read.moderate': { es: 'Agrupamiento moderado: los valores parecidos tienden a estar juntos.', en: 'Moderate clustering: similar values tend to sit together.', gn: 'Agrupamiento moderado: los valores parecidos tienden a estar juntos.', pt: 'Agrupamento moderado: valores parecidos tendem a ficar juntos.' },
	'chart.moran.read.weak': { es: 'Agrupamiento débil: apenas más juntos que al azar.', en: 'Weak clustering: barely more grouped than random.', gn: 'Agrupamiento débil: apenas más juntos que al azar.', pt: 'Agrupamento fraco: pouco mais juntos que ao acaso.' },
	'chart.moran.read.none': { es: 'Sin patrón: los valores se reparten como al azar.', en: 'No pattern: values are spread as if at random.', gn: 'Sin patrón: los valores se reparten como al azar.', pt: 'Sem padrão: os valores se distribuem como ao acaso.' },
	'chart.moran.read.dispersed': { es: 'Disperso: los valores altos tienden a estar junto a bajos.', en: 'Dispersed: high values tend to sit next to low ones.', gn: 'Disperso: los valores altos tienden a estar junto a bajos.', pt: 'Disperso: valores altos tendem a ficar junto de baixos.' },
	'chart.moran.pNote': { es: 'Con miles de celdas contiguas el p casi siempre es < 0,001; lo que importa es el tamaño de I (0 = al azar, 1 = máximo agrupamiento).', en: 'With thousands of contiguous cells p is almost always < 0.001; what matters is the size of I (0 = random, 1 = maximum clustering).', gn: 'Con miles de celdas contiguas el p casi siempre es < 0,001; lo que importa es el tamaño de I (0 = al azar, 1 = máximo agrupamiento).', pt: 'Com milhares de células contíguas o p é quase sempre < 0,001; o que importa é o tamanho de I (0 = ao acaso, 1 = agrupamento máximo).' },
	'chart.moran.map': { es: 'Mapa:', en: 'Map:', gn: 'Mapa:', pt: 'Mapa:' },
	'chart.moran.choropleth': { es: 'coropleta', en: 'choropleth', gn: 'coropleta', pt: 'coropleta' },
	'chart.moran.quadrants': { es: 'Cuadrantes', en: 'Quadrants', gn: 'Cuadrantes', pt: 'Quadrantes' },
	'chart.moran.quadrantsHint': { es: 'Cuadrantes del diagrama de Moran (alto-alto, bajo-bajo…), sin prueba de significancia local', en: 'Moran scatterplot quadrants (high-high, low-low…), with no local significance test', gn: 'Cuadrantes del diagrama de Moran (alto-alto, bajo-bajo…), sin prueba de significancia local', pt: 'Quadrantes do diagrama de Moran (alto-alto, baixo-baixo…), sem teste de significância local' },
	'chart.moran.insufficient': { es: 'Datos insuficientes para calcular autocorrelación', en: 'Not enough data to compute autocorrelation', gn: 'Datos insuficientes para calcular autocorrelación', pt: 'Dados insuficientes para calcular autocorrelação' }, // gn: pendiente
	'chart.moran.loadLayer': { es: 'Cargá una capa para calcular autocorrelación espacial', en: 'Load a layer to compute spatial autocorrelation', gn: 'Cargá una capa para calcular autocorrelación espacial', pt: 'Carregue uma camada para calcular autocorrelação espacial' }, // gn: pendiente

	// ── Comparison panels ──
	'common.noMatches': { es: 'Sin coincidencias', en: 'No matches', gn: 'Sin coincidencias', pt: 'Sem correspondências' }, // gn: pendiente
	'common.noResults': { es: 'Sin resultados', en: 'No results', gn: 'Sin resultados', pt: 'Sem resultados' }, // gn: pendiente
	'panel.compareWith': { es: 'Comparar con…', en: 'Compare with…', gn: 'Comparar con…', pt: 'Comparar com…' }, // gn: pendiente
	'panel.selectDistrict': { es: 'Seleccioná un distrito', en: 'Select a district', gn: 'Seleccioná un distrito', pt: 'Selecione um distrito' }, // gn: pendiente
	'comparison.col.population': { es: 'Población', en: 'Population', gn: 'Población', pt: 'População' }, // gn: pendiente
	'export.radioCsv': { es: 'Datos del radio (CSV)', en: 'Census tract data (CSV)', gn: 'Datos del radio (CSV)', pt: 'Dados do setor (CSV)' }, // gn: pendiente
	'export.radioGeojson': { es: 'Polígono del radio (GeoJSON)', en: 'Census tract polygon (GeoJSON)', gn: 'Polígono del radio (GeoJSON)', pt: 'Polígono do setor (GeoJSON)' }, // gn: pendiente
	'export.districtGeojson': { es: 'Polígono del distrito (GeoJSON)', en: 'District polygon (GeoJSON)', gn: 'Polígono del distrito (GeoJSON)', pt: 'Polígono do distrito (GeoJSON)' }, // gn: pendiente
	'district.nbiNote': { es: 'NBI 2022 — 50 = promedio del departamento · mayor = más privación', en: 'UBN 2022 — 50 = department average · higher = more deprivation', gn: 'NBI 2022 — 50 = promedio del departamento · mayor = más privación', pt: 'NBI 2022 — 50 = média do departamento · maior = mais privação' }, // gn: pendiente
	'district.source': { es: 'INE Paraguay — CNPV 2022 (NBI por distrito)', en: 'INE Paraguay — CNPV 2022 (UBN by district)', gn: 'INE Paraguay — CNPV 2022 (NBI por distrito)', pt: 'INE Paraguai — CNPV 2022 (NBI por distrito)' }, // gn: pendiente

	// ── Flood vulnerability panel ──
	// The six definitions keep their <strong> and render via {@html}. Indicator
	// acronyms (NBI) stay as-is — they name the official INDEC indicator; the gloss
	// after the colon is what gets translated.
	'flood.loadingProfile': { es: 'Cargando perfil de vulnerabilidad...', en: 'Loading vulnerability profile...', gn: 'Cargando perfil de vulnerabilidad...', pt: 'Carregando perfil de vulnerabilidade...' }, // gn: pendiente
	'flood.profileTitle': { es: 'Perfil de vulnerabilidad hídrica', en: 'Water vulnerability profile', gn: 'Perfil de vulnerabilidad hídrica', pt: 'Perfil de vulnerabilidade hídrica' }, // gn: pendiente
	'flood.petalNote': { es: 'Relativo al promedio {ofRef}, ponderado por población (50 = promedio). Mayor extensión = mayor riesgo.', en: 'Relative to the population-weighted average {ofRef} (50 = average). Larger extent = higher risk.', gn: 'Relativo al promedio {ofRef}, ponderado por población (50 = promedio). Mayor extensión = mayor riesgo.', pt: 'Relativo à média {ofRef}, ponderada pela população (50 = média). Maior extensão = maior risco.' },
	'flood.def.freq': { es: '<strong>Frec. inundación:</strong> frecuencia histórica de anegamiento (satelital)', en: '<strong>Flood freq.:</strong> historical flooding frequency (satellite)', gn: '<strong>Frec. inundación:</strong> frecuencia histórica de anegamiento (satelital)', pt: '<strong>Freq. inundação:</strong> frequência histórica de alagamento (satélite)' }, // gn: pendiente
	'flood.def.hand': { es: '<strong>Altura s/ drenaje:</strong> elevación sobre el curso de agua más cercano', en: '<strong>Height above drainage:</strong> elevation above the nearest watercourse', gn: '<strong>Altura s/ drenaje:</strong> elevación sobre el curso de agua más cercano', pt: '<strong>Altura s/ drenagem:</strong> elevação acima do curso de água mais próximo' }, // gn: pendiente
	'flood.def.nbi': { es: '<strong>NBI:</strong> hogares con necesidades básicas insatisfechas (Censo 2022)', en: '<strong>NBI:</strong> households with unsatisfied basic needs (2022 Census)', gn: '<strong>NBI:</strong> hogares con necesidades básicas insatisfechas (Censo 2022)', pt: '<strong>NBI:</strong> domicílios com necessidades básicas insatisfeitas (Censo 2022)' }, // gn: pendiente
	'flood.def.sewer': { es: '<strong>Sin cloacas:</strong> hogares sin red cloacal (Censo 2022)', en: '<strong>No sewerage:</strong> households not connected to a sewer network (2022 Census)', gn: '<strong>Sin cloacas:</strong> hogares sin red cloacal (Censo 2022)', pt: '<strong>Sem esgoto:</strong> domicílios sem rede de esgoto (Censo 2022)' }, // gn: pendiente
	'flood.def.water': { es: '<strong>Sin agua de red:</strong> hogares sin agua de red pública (Censo 2022)', en: '<strong>No piped water:</strong> households without public piped water (2022 Census)', gn: '<strong>Sin agua de red:</strong> hogares sin agua de red pública (Censo 2022)', pt: '<strong>Sem água encanada:</strong> domicílios sem água da rede pública (Censo 2022)' }, // gn: pendiente
	'flood.def.infra': { es: '<strong>Déficit infra:</strong> índice compuesto de carencias en servicios básicos', en: '<strong>Infra. deficit:</strong> composite index of basic-service shortfalls', gn: '<strong>Déficit infra:</strong> índice compuesto de carencias en servicios básicos', pt: '<strong>Déficit infra:</strong> índice composto de carências em serviços básicos' }, // gn: pendiente
	'section.howToRead': { es: 'Cómo leer este mapa', en: 'How to read this map', gn: "Mba'eichapa ehechakuaa ko mapa", pt: 'Como ler este mapa' },
	'section.implications': { es: 'Implicancias', en: 'Implications', gn: 'Implicancia', pt: 'Implicações' },
	'section.methodology': { es: 'Metodología', en: 'Methodology', gn: 'Metodología', pt: 'Metodologia' },
	// Shown when resolveMethod() falls back — `method` is authored in es/en only, so a pt
	// or gn reader gets Spanish here and should be told rather than left guessing.
	// `gn` is the Spanish string on purpose: a notice saying "this section is in Spanish"
	// reads coherently in Spanish, and the alternative was inventing Guaraní that nobody
	// here can verify. Omitting gn is not an option — t() would fall back to English.
	'section.method_es_only': {
		es: 'Esta sección solo está disponible en español.',
		en: 'This section is only available in Spanish.',
		gn: 'Esta sección solo está disponible en español.',
		pt: 'Esta seção está disponível apenas em espanhol.',
	},
	'section.typeDistribution': { es: 'Distribución de tipos', en: 'Type distribution', gn: 'Laja papapy', pt: 'Distribuição de tipos' },
	'section.territorialProfile': { es: 'Perfil geoespacial', en: 'Geospatial profile', gn: "Yvy rekokatu", pt: 'Perfil geoespacial' },
	'section.zonesAnalyzed': { es: 'Zonas analizadas', en: 'Zones analysed', gn: "Zona ojestudiáva", pt: 'Zonas analisadas' },
	'section.requestReport': { es: 'Solicitar informe', en: 'Request report', gn: 'Ejerure informe', pt: 'Solicitar relatório' },
	'section.source': { es: 'Fuente', en: 'Source', gn: 'Fuente', pt: 'Fonte' },
	'section.processed': { es: 'Procesado', en: 'Processed', gn: 'Procesado', pt: 'Processado' },
	'section.sarImage': { es: 'Imagen SAR (extensión actual)', en: 'SAR image (current extent)', gn: 'SAR (extensión actual)', pt: 'Imagem SAR (extensão atual)' },
	'section.sarRevisit': { es: 'revisita ~6–12 días', en: '~6–12 day revisit', gn: 'revisita ~6–12 ára', pt: 'revisita ~6–12 dias' },
	'legend.highMeansDanger': { es: 'Score alto = mayor riesgo', en: 'High score = greater risk', gn: 'Score yvate = riesgo tuicha', pt: 'Score alto = maior risco' },
	'legend.highMeansGood': { es: 'Score alto = mejor aptitud', en: 'High score = better aptitude', gn: 'Score yvate = iporãve', pt: 'Score alto = melhor aptidão' },

	// Language selector
	'lang.note': { es: '', en: '', gn: '* Ñe\'ẽ guaraní: traducción aproximada' , pt: '' },

	// Partial warning
	'warn.partial': { es: '\u26A0 Algunas edificaciones pueden estar fuera de la vista actual.', en: '\u26A0 Some buildings may be outside the current view.', gn: "\u26A0 Oĩ óga oimeraẽva okápe.", pt: '⚠ Algumas edificações podem estar fora da vista atual.' },
	'warn.censusCrossCountry': { es: '⚠ Estás comparando radios de distintos países. Los datos censales (INDEC AR · DGEEC PY · IBGE BR) usan definiciones distintas.', en: '⚠ You are comparing tracts from different countries. Census data (INDEC AR · DGEEC PY · IBGE BR) uses different definitions.', gn: "⚠ Eñembojoja hína radio ambue tetãgui. Censo mba'ekuaa (INDEC AR · DGEEC PY · IBGE BR) oiporu definición iñambuéva.", pt: '⚠ Você está comparando setores de países diferentes. Os dados censitários (INDEC AR · DGEEC PY · IBGE BR) usam definições distintas.' },

	// Chart
	'chart.absolute': { es: 'Valores absolutos', en: 'Absolute values', gn: "Papapy", pt: 'Valores absolutos' },
	'chart.rates': { es: 'Tasas (%)', en: 'Rates (%)', gn: "Jeku'e (%)", pt: 'Taxas (%)' },

	'error.dataLoadFailed': { es: 'No se pudieron cargar los datos', en: 'Failed to load data', gn: "Ndaikatúi oñemyenyhẽ mba'ekuaa", pt: 'Não foi possível carregar os dados' },
	'error.engineFailed': { es: 'Error al iniciar el motor de datos', en: 'Data engine failed to start', gn: "Mba'ekuaa ndoikói", pt: 'Erro ao iniciar o motor de dados' },

	'label.waterNetwork': { es: 'Sin red de agua (%)', en: 'No water network (%)', gn: 'Y juru ỹre (%)', pt: 'Sem rede de água (%)' },
	// Brazil IBGE setor vars
	'label.br.waterNetwork':     { es: 'Agua de red (%)',      en: 'Piped water (%)',       gn: 'Y ñemopyrũ (%)',  pt: 'Água de rede (%)' },
	'label.br.sewerNetwork':     { es: 'Desagüe adecuado (%)',  en: 'Adequate sewage (%)',   gn: 'Ykua porã (%)',   pt: 'Esgoto adequado (%)' },
	'label.br.garbageCollected': { es: 'Basura recolectada (%)', en: 'Garbage collected (%)', gn: 'Taky jeheja (%)', pt: 'Lixo coletado (%)' },
	'label.br.literacy':         { es: 'Alfabetismo (%)',      en: 'Literacy (%)',          gn: 'Moñe\'ẽ porã (%)', pt: 'Alfabetismo (%)' },
	'label.br.noBathroom':       { es: 'Sin baño exclusivo (%)', en: 'No private bathroom (%)', gn: 'Ysyryrogue ỹre (%)', pt: 'Sem banheiro exclusivo (%)' },
	'label.dependencyIndex': { es: 'Índ. dependencia', en: 'Dependency index', gn: "Ñemomba'e", pt: 'Índ. dependência' },
	'label.university': { es: 'Universitario (%)', en: 'University (%)', gn: "Mbo'ehára (%)", pt: 'Universitário (%)' },
	'label.healthCoverage': { es: 'Cobertura salud (%)', en: 'Health coverage (%)', gn: "Tesãi joapy (%)", pt: 'Cobertura de saúde (%)' },
	'label.teenMotherhood': { es: 'Maternidad (%)', en: 'Motherhood (%)', gn: "Sy reko (%)", pt: 'Maternidade (%)' },

	// ── Lens system ──────────────────────────────────────────────────────────
	'lens.selectRadio': { es: 'Seleccioná un radio iluminado', en: 'Select a highlighted radio', gn: 'Eiporavo peteĩ radio', pt: 'Selecione um setor destacado' },

	'card.territory': { es: 'Huella espacial', en: 'Spatial footprint', gn: 'Yvy rapykuere', pt: 'Pegada espacial' },

	'lens.loading': { es: 'Cargando...', en: 'Loading...', gn: 'Oñemyenyhẽ...', pt: 'Carregando...' },

	// ── Analysis system ─────────────────────────────────────────────────────
	'analysis.menu.title': { es: 'Análisis disponibles', en: 'Available analyses', gn: "Mba'ekuaa oĩva", pt: 'Análises disponíveis' },
	'analysis.status.available': { es: 'Disponible', en: 'Available', gn: 'Oĩma', pt: 'Disponível' },
	'analysis.status.comingSoon': { es: 'En desarrollo', en: 'Coming soon', gn: 'Oguerahátama', pt: 'Em desenvolvimento' },
	'analysis.coverage.pending': { es: 'próximamente', en: 'Coming soon', gn: 'Oguerahátama', pt: 'em breve' },
	'analysis.coverage.unavailable': { es: 'no disponible {inRef}', en: 'not available {inRef}', gn: 'no disponible {inRef}', pt: 'não disponível {inRef}' },
	'analysis.petalHint.fixed': { es: 'Puntaje 0-100 en la escala fija regional (NEA y región transfronteriza): el mismo valor significa lo mismo en Argentina, Paraguay y Brasil. Mayor extensión = mayor intensidad.', en: 'Score 0-100 on the fixed regional scale (NEA and cross-border region): the same value means the same in Argentina, Paraguay and Brazil. Larger petal = higher intensity.', gn: 'Puntaje 0-100 en la escala fija regional (NEA y región transfronteriza): el mismo valor significa lo mismo en Argentina, Paraguay y Brasil. Mayor extensión = mayor intensidad.', pt: 'Pontuação 0-100 na escala fixa regional (NEA e região transfronteiriça): o mesmo valor significa o mesmo na Argentina, no Paraguai e no Brasil. Maior extensão = maior intensidade.' },
	'analysis.petalHint.local': { es: 'Percentil dentro {ofRef} (0-100). Mayor extensión = mayor intensidad relativa.', en: 'Percentile within {ref} (0-100). Larger petal = higher relative intensity.', gn: 'Percentil dentro {ofRef} (0-100). Mayor extensión = mayor intensidad relativa.', pt: 'Percentil dentro {ofRef} (0-100). Maior extensão = maior intensidade relativa.' },
	'analysis.back': { es: '← Análisis', en: '← Analyses', gn: "← Mba'ekuaa", pt: '← Análises' },
	'analysis.noData': { es: 'Sin datos para este radio', en: 'No data for this radio', gn: "Mba'ekuaa'ỹ ko radio-pe", pt: 'Sem dados para este setor' },
	'analysis.loading': { es: 'Cargando datos...', en: 'Loading data...', gn: "Oñemyenyhẽ mba'ekuaa...", pt: 'Carregando dados...' },
	'analysis.comingSoon.body': { es: 'Este análisis está en desarrollo. Próximamente disponible con datos actualizados.', en: 'This analysis is under development. Coming soon with updated data.', gn: "Ko mba'ekuaa oñemoĩhína. Oguerahátama.", pt: 'Esta análise está em desenvolvimento. Em breve disponível com dados atualizados.' },

	// Flood risk analysis
	'analysis.floodRisk.title': { es: '¿Dónde es mayor el riesgo de inundación?', en: 'Where is flood risk highest?', gn: "Moõpa ysoguy mba'asy tuichavéva?", pt: 'Onde o risco de inundação é maior?' },
	'analysis.floodRisk.desc': { es: 'Edificios y tierra en zona baja (relieve, MERIT Hydro), presencia histórica de agua (JRC 1984–2021) e inundación actual (Sentinel-1) por hexágono', en: 'Buildings and land on low ground (terrain, MERIT Hydro), historical water presence (JRC 1984–2021) and current flooding (Sentinel-1) per hexagon', gn: "Y rehegua historia guive (JRC 1984–2021) ha ko'ag̃a ysoguy (Sentinel-1 SAR) yvy rupi", pt: 'Edifícios e terra em terreno baixo (relevo, MERIT Hydro), presença histórica de água (JRC 1984–2021) e inundação atual (Sentinel-1) por hexágono' },
	'analysis.floodRisk.legend': { es: 'Presencia histórica (%)', en: 'Historical presence (%)', gn: "Y historia rehegua (%)", pt: 'Presença histórica (%)' },
	'analysis.flood.petal.occurrence': { es: 'Ocurrencia', en: 'Occurrence', gn: 'Oiko', pt: 'Ocorrência' },
	'analysis.flood.petal.recurrence': { es: 'Recurrencia', en: 'Recurrence', gn: 'Jey', pt: 'Recorrência' },
	'analysis.flood.petal.seasonality': { es: 'Estacionalidad', en: 'Seasonality', gn: "Ára rehe", pt: 'Sazonalidade' },
	'analysis.flood.petal.extent': { es: 'Extensión actual', en: 'Current extent', gn: "Ko'ãga", pt: 'Extensão atual' },
	'analysis.flood.riskScore': { es: 'Score de riesgo', en: 'Risk score', gn: "Mba'asy score", pt: 'Score de risco' },
	'analysis.flood.recurrence': { es: 'Recurrencia', en: 'Recurrence', gn: 'Jey', pt: 'Recorrência' },
	'analysis.flood.recurrenceDesc': { es: '% de meses con agua detectada', en: '% of months with water detected', gn: '% jasy y reheve', pt: '% de meses com água detectada' },
	'analysis.flood.jrcOccurrence': { es: 'Presencia histórica (%)', en: 'Historical presence (%)', gn: "Y rehegua historia (%)", pt: 'Presença histórica (%)' },
	'analysis.flood.jrcOccurrenceDesc': { es: '% del tiempo con agua detectada (Landsat 1984–2021)', en: '% of time with water detected (Landsat 1984–2021)', gn: '% ára y reheve (Landsat 1984–2021)', pt: '% do tempo com água detectada (Landsat 1984–2021)' },
	'analysis.flood.jrcRecurrence': { es: 'Recurrencia interanual (%)', en: 'Year-to-year recurrence (%)', gn: 'Jey ary ha ary (%)', pt: 'Recorrência interanual (%)' },
	'analysis.flood.jrcRecurrenceDesc': { es: '% de años en que el agua vuelve a aparecer', en: '% of years water reappears', gn: '% ary y ojekuaa jey', pt: '% de anos em que a água reaparece' },
	'analysis.flood.jrcSeasonality': { es: 'Estacionalidad (meses)', en: 'Seasonality (months)', gn: "Ára reípe (jasy)", pt: 'Sazonalidade (meses)' },
	'analysis.flood.jrcSeasonalityDesc': { es: 'Cantidad de meses con agua por año', en: 'Number of months with water per year', gn: 'Mboy jasy y reheve ary pe', pt: 'Número de meses com água por ano' },
	'analysis.flood.currentExtent': { es: 'Extensión actual', en: 'Current extent', gn: "Ko'ãga tuichakue", pt: 'Extensão atual' },
	'analysis.flood.currentExtentDesc': { es: '% de la zona inundada', en: '% of the area flooded', gn: '% yvy y guype oĩva', pt: '% da zona inundada' },
	'analysis.flood.riskHigh': { es: 'Riesgo alto', en: 'High risk', gn: "Mba'asy guasu", pt: 'Alto risco' },
	'analysis.flood.riskMedium': { es: 'Riesgo medio', en: 'Medium risk', gn: "Mba'asy mbyte", pt: 'Risco médio' },
	'analysis.flood.riskLow': { es: 'Riesgo bajo', en: 'Low risk', gn: "Mba'asy michĩ", pt: 'Baixo risco' },
	'analysis.flood.totalHex': { es: 'Zonas analizadas', en: 'Areas analysed', gn: 'Yvy ojehechauka', pt: 'Zonas analisadas' },
	'analysis.flood.highRecurrence': { es: 'Recurrencia >10%', en: 'Recurrence >10%', gn: 'Jey >10%', pt: 'Recorrência >10%' },
	'analysis.flood.avgScore': { es: 'Score promedio', en: 'Avg score', gn: 'Score mbytekue', pt: 'Score médio' },
	'analysis.flood.topDepts': { es: 'Seleccioná un departamento', en: 'Select a department', gn: "Eiporavo departamento", pt: 'Selecione um departamento' },
	'rank.flood.title': { es: '¿Dónde es mayor la parte de los edificios en zona baja?', en: 'Where is the share of buildings on low ground largest?', gn: '¿Dónde es mayor la parte de los edificios en zona baja?', pt: 'Onde é maior a parte dos edifícios em terreno baixo?' },
	'rank.flood.note': { es: '% de los edificios de cada departamento a menos de 5 m sobre el río o arroyo más cercano (HAND, MERIT Hydro). Al lado, % de la tierra en la misma condición, sin ríos ni embalses. Mide exposición por relieve, no frecuencia de inundación; las otras pestañas ordenan por cantidad de edificios y por % de tierra. Clic en una barra para abrir el departamento.', en: 'Share of each department’s buildings less than 5 m above the nearest river or stream (HAND, MERIT Hydro). Next to it, share of land in the same condition, rivers and reservoirs excluded. It measures exposure by terrain, not flood frequency; the other tabs rank by number of buildings and by share of land. Click a bar to open the department.', gn: '% de los edificios de cada departamento a menos de 5 m sobre el río o arroyo más cercano (HAND, MERIT Hydro). Al lado, % de la tierra en la misma condición, sin ríos ni embalses. Mide exposición por relieve, no frecuencia de inundación; las otras pestañas ordenan por cantidad de edificios y por % de tierra. Clic en una barra para abrir el departamento.', pt: '% dos edifícios de cada departamento a menos de 5 m acima do rio ou arroio mais próximo (HAND, MERIT Hydro). Ao lado, % da terra na mesma condição, sem rios nem represas. Mede exposição pelo relevo, não frequência de inundação; as outras abas ordenam por quantidade de edifícios e por % de terra. Clique numa barra para abrir o departamento.' },
	'rank.flood.saturated': { es: 'En la llanura casi todo está a menos de 5 m sobre algún curso de agua (HAND, MERIT Hydro), y el relieve no distingue un departamento de otro. Acá el anegamiento depende más de la lluvia y del drenaje del suelo que de la altura sobre el río.', en: 'On the plain nearly everything lies less than 5 m above some watercourse (HAND, MERIT Hydro), so terrain does not tell one department from another. Here waterlogging depends more on rainfall and soil drainage than on height above the river.', gn: 'En la llanura casi todo está a menos de 5 m sobre algún curso de agua (HAND, MERIT Hydro), y el relieve no distingue un departamento de otro. Acá el anegamiento depende más de la lluvia y del drenaje del suelo que de la altura sobre el río.', pt: 'Na planície quase tudo está a menos de 5 m acima de algum curso de água (HAND, MERIT Hydro), e o relevo não distingue um departamento de outro. Aqui o alagamento depende mais da chuva e da drenagem do solo do que da altura sobre o rio.' },
	'rank.saturated.head': { es: 'Sin ranking por relieve {inRef}', en: 'No terrain ranking {inRef}', gn: 'Sin ranking por relieve {inRef}', pt: 'Sem ranking por relevo {inRef}' },
	'rank.flood.plain.title': { es: '¿Dónde se anega más la tierra?', en: 'Where does the land waterlog most?', gn: '¿Dónde se anega más la tierra?', pt: 'Onde a terra mais alaga?' },
	'rank.flood.plain.note': { es: '% de la tierra de cada departamento —sin ríos ni lagunas permanentes— que tuvo agua en al menos el 10 % de los meses desde 2015 (radar Sentinel-1, que ve a través de las nubes). En la llanura el relieve no distingue departamentos, así que acá se mide el anegamiento recurrente; una crecida corta de río pesa poco. Clic en una barra para abrir el departamento.', en: 'Share of each department’s land —permanent rivers and lagoons excluded— that held water in at least 10% of months since 2015 (Sentinel-1 radar, which sees through clouds). On the plain terrain does not tell departments apart, so this measures recurrent waterlogging; a short river flood weighs little. Click a bar to open the department.', gn: '% de la tierra de cada departamento —sin ríos ni lagunas permanentes— que tuvo agua en al menos el 10 % de los meses desde 2015 (radar Sentinel-1, que ve a través de las nubes). En la llanura el relieve no distingue departamentos, así que acá se mide el anegamiento recurrente; una crecida corta de río pesa poco. Clic en una barra para abrir el departamento.', pt: '% da terra de cada departamento —sem rios nem lagoas permanentes— que teve água em pelo menos 10 % dos meses desde 2015 (radar Sentinel-1, que vê através das nuvens). Na planície o relevo não distingue departamentos, então aqui se mede o alagamento recorrente; uma cheia curta de rio pesa pouco. Clique numa barra para abrir o departamento.' },
	'rank.unit.haYr': { es: 'ha/año', en: 'ha/yr', gn: 'ha/año', pt: 'ha/ano' },
	'rank.unit.pctYr': { es: '%/año', en: '%/yr', gn: '%/año', pt: '%/ano' },
	'rank.deforest.tab.ha': { es: 'Hectáreas', en: 'Hectares', gn: 'Hectáreas', pt: 'Hectares' },
	'rank.deforest.tab.pct': { es: '% del bosque', en: '% of forest', gn: '% del bosque', pt: '% da floresta' },
	'rank.deforest.tab.change': { es: 'Cambio', en: 'Change', gn: 'Cambio', pt: 'Mudança' },
	'rank.deforest.title.ha': { es: '¿Dónde se pierde más bosque?', en: 'Where is most forest lost?', gn: '¿Dónde se pierde más bosque?', pt: 'Onde se perde mais floresta?' },
	'rank.deforest.title.pct': { es: '¿Dónde es más intensa la pérdida?', en: 'Where is loss most intense?', gn: '¿Dónde es más intensa la pérdida?', pt: 'Onde a perda é mais intensa?' },
	'rank.deforest.title.change': { es: '¿Dónde aceleró o frenó?', en: 'Where did it speed up or slow down?', gn: '¿Dónde aceleró o frenó?', pt: 'Onde acelerou ou freou?' },
	'rank.deforest.note.ha': { es: 'Hectáreas de cobertura arbórea perdidas por año en 2015–2024 (Hansen, píxeles Landsat de 30 m). Hansen cuenta las plantaciones como bosque: en Misiones y Corrientes parte de la pérdida es cosecha de pinos y eucaliptos. Clic en una barra para abrir el departamento.', en: 'Hectares of tree cover lost per year in 2015–2024 (Hansen, 30 m Landsat pixels). Hansen counts plantations as forest: in Misiones and Corrientes part of the loss is pine and eucalyptus harvest. Click a bar to open the department.', gn: 'Hectáreas de cobertura arbórea perdidas por año en 2015–2024 (Hansen, píxeles Landsat de 30 m). Hansen cuenta las plantaciones como bosque: en Misiones y Corrientes parte de la pérdida es cosecha de pinos y eucaliptos. Clic en una barra para abrir el departamento.', pt: 'Hectares de cobertura arbórea perdidos por ano em 2015–2024 (Hansen, pixels Landsat de 30 m). O Hansen conta plantações como floresta: em Misiones e Corrientes parte da perda é colheita de pinus e eucalipto. Clique numa barra para abrir o departamento.' },
	'rank.deforest.note.pct': { es: '% de la cobertura arbórea del año 2000 que se pierde por año en 2015–2024. Mide intensidad: un departamento con poco bosque puede quedar arriba con pocas hectáreas (al lado, las hectáreas por año). Hansen cuenta las plantaciones como bosque.', en: 'Share of the year-2000 tree cover lost per year in 2015–2024. It measures intensity: a department with little forest can rank high on few hectares (hectares per year alongside). Hansen counts plantations as forest.', gn: '% de la cobertura arbórea del año 2000 que se pierde por año en 2015–2024. Mide intensidad: un departamento con poco bosque puede quedar arriba con pocas hectáreas (al lado, las hectáreas por año). Hansen cuenta las plantaciones como bosque.', pt: '% da cobertura arbórea do ano 2000 perdida por ano em 2015–2024. Mede intensidade: um departamento com pouca floresta pode ficar no topo com poucos hectares (ao lado, os hectares por ano). O Hansen conta plantações como floresta.' },
	'rank.deforest.note.change': { es: 'Pérdida anual de 2015–2024 frente a la de 2001–2010 (dos períodos de 10 años). Rojo: aceleró; verde: frenó. Sólo donde la pérdida de 2001–2010 superó 50 ha/año; con bases más chicas el porcentaje no dice nada ("—"). Al lado, las hectáreas por año actuales.', en: 'Annual loss in 2015–2024 against 2001–2010 (two 10-year periods). Red: sped up; green: slowed down. Only where 2001–2010 loss exceeded 50 ha/yr; on smaller bases the percentage means nothing ("—"). Current hectares per year alongside.', gn: 'Pérdida anual de 2015–2024 frente a la de 2001–2010 (dos períodos de 10 años). Rojo: aceleró; verde: frenó. Sólo donde la pérdida de 2001–2010 superó 50 ha/año; con bases más chicas el porcentaje no dice nada ("—"). Al lado, las hectáreas por año actuales.', pt: 'Perda anual de 2015–2024 frente à de 2001–2010 (dois períodos de 10 anos). Vermelho: acelerou; verde: freou. Só onde a perda de 2001–2010 passou de 50 ha/ano; com bases menores a porcentagem não diz nada ("—"). Ao lado, os hectares por ano atuais.' },
	'rank.unit.pct': { es: '%', en: '%', gn: '%', pt: '%' },
	'rank.eudr.tab.fire': { es: 'Fuego nativo', en: 'Native fire', gn: 'Tata nativo', pt: 'Fogo nativo' },
	'rank.eudr.title.ha': { es: '¿Dónde se perdió más bosque después de 2020?', en: 'Where was most forest lost after 2020?', gn: '¿Dónde se perdió más bosque después de 2020?', pt: 'Onde se perdeu mais floresta depois de 2020?' },
	'rank.eudr.title.pct': { es: '¿Dónde pesa más esa pérdida?', en: 'Where does that loss weigh most?', gn: '¿Dónde pesa más esa pérdida?', pt: 'Onde essa perda pesa mais?' },
	'rank.eudr.title.fire': { es: '¿Dónde quemó vegetación nativa leñosa?', en: 'Where did fire hit native woody vegetation?', gn: '¿Dónde quemó vegetación nativa leñosa?', pt: 'Onde o fogo atingiu vegetação nativa lenhosa?' },
	'rank.eudr.note.ha': { es: 'Hectáreas de cobertura arbórea perdidas entre 2021 y 2025, después del corte del Reglamento (UE) 2023/1115 (31/12/2020). Hansen GFC, sumado sobre hexágonos de ~10 ha. Hansen cuenta las plantaciones como bosque: parte de la pérdida es cosecha de pinos y eucaliptos. Clic en una barra para abrir el departamento.', en: 'Hectares of tree cover lost in 2021–2025, after the cut-off of Regulation (EU) 2023/1115 (31/12/2020). Hansen GFC, summed over ~10 ha hexagons. Hansen counts plantations as forest: part of the loss is pine and eucalyptus harvest. Click a bar to open the department.', gn: 'Hectáreas de cobertura arbórea perdidas entre 2021 y 2025, después del corte del Reglamento (UE) 2023/1115 (31/12/2020). Hansen GFC, sumado sobre hexágonos de ~10 ha. Hansen cuenta las plantaciones como bosque: parte de la pérdida es cosecha de pinos y eucaliptos. Clic en una barra para abrir el departamento.', pt: 'Hectares de cobertura arbórea perdidos entre 2021 e 2025, depois do corte do Regulamento (UE) 2023/1115 (31/12/2020). Hansen GFC, somado sobre hexágonos de ~10 ha. O Hansen conta plantações como floresta: parte da perda é colheita de pinus e eucalipto. Clique numa barra para abrir o departamento.' },
	'rank.eudr.note.pct': { es: 'Hectáreas perdidas en 2021–2025 como % de la cobertura arbórea del año 2000 (Hansen). Mide intensidad: un departamento con poco bosque puede quedar arriba con pocas hectáreas (al lado, las hectáreas).', en: 'Hectares lost in 2021–2025 as a share of the year-2000 tree cover (Hansen). It measures intensity: a department with little forest can rank high on few hectares (hectares alongside).', gn: 'Hectáreas perdidas en 2021–2025 como % de la cobertura arbórea del año 2000 (Hansen). Mide intensidad: un departamento con poco bosque puede quedar arriba con pocas hectáreas (al lado, las hectáreas).', pt: 'Hectares perdidos em 2021–2025 como % da cobertura arbórea do ano 2000 (Hansen). Mede intensidade: um departamento com pouca floresta pode ficar no topo com poucos hectares (ao lado, os hectares).' },
	'rank.eudr.note.fire': { es: 'Hectáreas quemadas después de 2020 sobre vegetación nativa leñosa: área quemada MODIS (500 m) ponderada por la fracción de bosque y monte nativo de MapBiomas 2020. Sólo en las 4 provincias del NEA argentino (MapBiomas Argentina).', en: 'Hectares burned after 2020 on native woody vegetation: MODIS burned area (500 m) weighted by the MapBiomas 2020 native forest and woodland fraction. Only in the 4 NEA Argentine provinces (MapBiomas Argentina).', gn: 'Hectáreas quemadas después de 2020 sobre vegetación nativa leñosa: área quemada MODIS (500 m) ponderada por la fracción de bosque y monte nativo de MapBiomas 2020. Sólo en las 4 provincias del NEA argentino (MapBiomas Argentina).', pt: 'Hectares queimados depois de 2020 sobre vegetação nativa lenhosa: área queimada MODIS (500 m) ponderada pela fração de floresta e mata nativa do MapBiomas 2020. Só nas 4 províncias do NEA argentino (MapBiomas Argentina).' },
	'rank.unit.ugDelta': { es: 'µg/m³ vs 2001–10', en: 'µg/m³ vs 2001–10', gn: 'µg/m³ vs 2001–10', pt: 'µg/m³ vs 2001–10' },
	'rank.unit.ha': { es: 'ha', en: 'ha', gn: 'ha', pt: 'ha' },
	'rank.unit.pctArea': { es: '% del área', en: '% of area', gn: '% del área', pt: '% da área' },
	'rank.unit.hogares': { es: 'hogares', en: 'households', gn: 'hogares', pt: 'domicílios' },
	'rank.unit.pctCoverage': { es: '% con cobertura', en: '% covered', gn: '% con cobertura', pt: '% com cobertura' },
	'rank.pm25.title': { es: '¿Dónde es peor el aire?', en: 'Where is the air worst?', gn: '¿Dónde es peor el aire?', pt: 'Onde o ar é pior?' },
	'rank.pm25.note': { es: 'PM2.5 medio anual 2013–2022 (µg/m³, ACAG), promediado sobre el área del departamento: describe la superficie del departamento, no la exposición de la población. Al lado, el cambio respecto de 2001–2010 (negativo = mejoró). Clic en una barra para abrir el departamento.', en: 'Mean annual PM2.5 2013–2022 (µg/m³, ACAG), averaged over the department’s area: it describes the department’s surface, not population exposure. Alongside, the change from 2001–2010 (negative = improved). Click a bar to open the department.', gn: 'PM2.5 medio anual 2013–2022 (µg/m³, ACAG), promediado sobre el área del departamento: describe la superficie del departamento, no la exposición de la población. Al lado, el cambio respecto de 2001–2010 (negativo = mejoró). Clic en una barra para abrir el departamento.', pt: 'PM2.5 médio anual 2013–2022 (µg/m³, ACAG), médio sobre a área do departamento: descreve a superfície do departamento, não a exposição da população. Ao lado, a mudança desde 2001–2010 (negativo = melhorou). Clique numa barra para abrir o departamento.' },
	'rank.access.title': { es: '¿Dónde se tarda más en llegar?', en: 'Where does it take longest to get there?', gn: '¿Dónde se tarda más en llegar?', pt: 'Onde se demora mais para chegar?' },
	'rank.access.tab.cabecera': { es: 'A una ciudad', en: 'To a city', gn: 'A una ciudad', pt: 'A uma cidade' },
	'rank.access.tab.capital': { es: 'A la capital', en: 'To the capital', gn: 'A la capital', pt: 'À capital' },
	'rank.access.tab.hospital': { es: 'Al hospital', en: 'To a hospital', gn: 'Al hospital', pt: 'Ao hospital' },
	'rank.access.note': { es: 'Minutos de viaje promedio desde cada punto del departamento a la ciudad más cercana de 50.000 habitantes o más (Oxford) o a la capital de la provincia (en Paraguay, del departamento; en Brasil, del estado): describe la superficie del departamento, no a la población (un departamento grande y poco poblado queda arriba aunque su gente viva cerca de la ruta). Clic en una barra para abrir el departamento.', en: 'Average travel minutes from every point of the department to the nearest city of 50,000 or more (Oxford) or to the provincial capital (in Paraguay, the department’s; in Brazil, the state’s): it describes the department’s surface, not the population (a large, sparsely populated department ranks high even if its people live near the road). Click a bar to open the department.', gn: 'Minutos de viaje promedio desde cada punto del departamento a la ciudad más cercana de 50.000 habitantes o más (Oxford) o a la capital de la provincia (en Paraguay, del departamento; en Brasil, del estado): describe la superficie del departamento, no a la población (un departamento grande y poco poblado queda arriba aunque su gente viva cerca de la ruta). Clic en una barra para abrir el departamento.', pt: 'Minutos de viagem médios desde cada ponto do departamento até a cidade mais próxima de 50.000 habitantes ou mais (Oxford) ou até a capital da província (no Paraguai, do departamento; no Brasil, do estado): descreve a superfície do departamento, não a população. Clique numa barra para abrir o departamento.' },
	'rank.access.note.hospital': { es: 'Distancia promedio (km) desde cada punto del departamento al hospital más cercano: describe la superficie del departamento, no a la población. Clic en una barra para abrir el departamento.', en: 'Average distance (km) from every point of the department to the nearest hospital: it describes the department’s surface, not the population. Click a bar to open the department.', gn: 'Distancia promedio (km) desde cada punto del departamento al hospital más cercano: describe la superficie del departamento, no a la población. Clic en una barra para abrir el departamento.', pt: 'Distância média (km) desde cada ponto do departamento ao hospital mais próximo: descreve a superfície do departamento, não a população. Clique numa barra para abrir o departamento.' },
	'rank.landuse.title': { es: '¿Qué cubre el suelo?', en: 'What covers the land?', gn: '¿Qué cubre el suelo?', pt: 'O que cobre o solo?' },
	'rank.landuse.tab.trees': { es: 'Árboles', en: 'Trees', gn: 'Árboles', pt: 'Árvores' },
	'rank.landuse.tab.crops': { es: 'Cultivos', en: 'Crops', gn: 'Cultivos', pt: 'Cultivos' },
	'rank.landuse.tab.built': { es: 'Urbano', en: 'Built-up', gn: 'Urbano', pt: 'Urbano' },
	'rank.landuse.note.trees': { es: '% del departamento con árboles según Dynamic World (Sentinel-2, 10 m); al lado, las hectáreas. Incluye las plantaciones forestales de pino y eucalipto: no separa bosque nativo de bosque plantado. Clic en una barra para abrir el departamento.', en: 'Share of the department under trees by Dynamic World (Sentinel-2, 10 m), with hectares alongside. It includes pine and eucalyptus forest plantations: native and planted forest are not separated. Click a bar to open the department.', gn: '% del departamento con árboles según Dynamic World (Sentinel-2, 10 m); al lado, las hectáreas. Incluye las plantaciones forestales de pino y eucalipto: no separa bosque nativo de bosque plantado. Clic en una barra para abrir el departamento.', pt: '% do departamento com árvores segundo o Dynamic World (Sentinel-2, 10 m); ao lado, os hectares. Inclui as plantações florestais de pinus e eucalipto: não separa floresta nativa de floresta plantada. Clique numa barra para abrir o departamento.' },
	'rank.landuse.note.crops': { es: '% del departamento con cultivos agrícolas que no llegan a altura de árbol (soja, maíz, trigo, tabaco, barbechos), según Dynamic World (Sentinel-2, 10 m). La yerba y el té, por su porte de arbusto, pueden quedar en cultivos o en arbustos; el arroz inundado va a vegetación inundada. Las plantaciones de pino y eucalipto están en Árboles, no acá. Clic en una barra para abrir el departamento.', en: 'Share of the department under farm crops that do not reach tree height (soy, maize, wheat, tobacco, fallow), by Dynamic World (Sentinel-2, 10 m). Yerba mate and tea, being shrub-sized, may fall in crops or in shrubland; flooded rice goes to flooded vegetation. Pine and eucalyptus plantations are under Trees, not here. Click a bar to open the department.', gn: '% del departamento con cultivos agrícolas que no llegan a altura de árbol (soja, maíz, trigo, tabaco, barbechos), según Dynamic World (Sentinel-2, 10 m). La yerba y el té, por su porte de arbusto, pueden quedar en cultivos o en arbustos; el arroz inundado va a vegetación inundada. Las plantaciones de pino y eucalipto están en Árboles, no acá. Clic en una barra para abrir el departamento.', pt: '% do departamento com cultivos agrícolas que não chegam à altura de árvore (soja, milho, trigo, tabaco, pousio), segundo o Dynamic World (Sentinel-2, 10 m). A erva-mate e o chá, por seu porte de arbusto, podem cair em cultivos ou em arbustos; o arroz inundado vai para vegetação inundada. As plantações de pinus e eucalipto estão em Árvores, não aqui. Clique numa barra para abrir o departamento.' },
	'rank.landuse.note.built': { es: '% del departamento construido (edificios, calles y superficies impermeables), según Dynamic World (Sentinel-2, 10 m); al lado, las hectáreas. Clic en una barra para abrir el departamento.', en: 'Built-up share of the department (buildings, streets and sealed surfaces) by Dynamic World (Sentinel-2, 10 m), with hectares alongside. Click a bar to open the department.', gn: '% del departamento construido (edificios, calles y superficies impermeables), según Dynamic World (Sentinel-2, 10 m); al lado, las hectáreas. Clic en una barra para abrir el departamento.', pt: '% construído do departamento (edifícios, ruas e superfícies impermeáveis), segundo o Dynamic World (Sentinel-2, 10 m); ao lado, os hectares. Clique numa barra para abrir o departamento.' },
	'rank.forestry.title': { es: '¿Dónde hay más superficie apta para forestar?', en: 'Where is there most land suited to forestry?', gn: '¿Dónde hay más superficie apta para forestar?', pt: 'Onde há mais área apta para florestar?' },
	'rank.forestry.note': { es: 'Hectáreas con aptitud forestal alta (puntaje ≥ 70 sobre 100, escala fija) y, al lado, qué parte del departamento es. El puntaje es un modelo de nicho hecho con las plantaciones del Inventario DNDFI y con clima, suelo y pendiente, no una medición. Clic en una barra para abrir el departamento.', en: 'Hectares of high forestry aptitude (score ≥ 70 out of 100, fixed scale), with the share of the department alongside. The score is a niche model built from DNDFI inventory plantations plus climate, soil and slope, not a measurement. Click a bar to open the department.', gn: 'Hectáreas con aptitud forestal alta (puntaje ≥ 70 sobre 100, escala fija) y, al lado, qué parte del departamento es. El puntaje es un modelo de nicho hecho con las plantaciones del Inventario DNDFI y con clima, suelo y pendiente, no una medición. Clic en una barra para abrir el departamento.', pt: 'Hectares com aptidão florestal alta (pontuação ≥ 70 de 100, escala fixa) e, ao lado, a parte do departamento. A pontuação é um modelo de nicho, não uma medição. Clique numa barra para abrir o departamento.' },
	'rank.agri.title': { es: '¿Dónde hay más tierra con alto potencial agrícola?', en: 'Where is there most land with high farming potential?', gn: '¿Dónde hay más tierra con alto potencial agrícola?', pt: 'Onde há mais terra com alto potencial agrícola?' },
	'rank.agri.note': { es: 'Hectáreas con potencial agrícola alto (puntaje ≥ 70 sobre 100, escala fija) y, al lado, qué parte del departamento es. El puntaje combina suelo, pH, clima y pendiente; es un índice, no un rinde. Clic en una barra para abrir el departamento.', en: 'Hectares of high farming potential (score ≥ 70 out of 100, fixed scale), with the share of the department alongside. The score combines soil, pH, climate and slope; it is an index, not a yield. Click a bar to open the department.', gn: 'Hectáreas con potencial agrícola alto (puntaje ≥ 70 sobre 100, escala fija) y, al lado, qué parte del departamento es. El puntaje combina suelo, pH, clima y pendiente; es un índice, no un rinde. Clic en una barra para abrir el departamento.', pt: 'Hectares com potencial agrícola alto (pontuação ≥ 70 de 100, escala fixa) e, ao lado, a parte do departamento. A pontuação é um índice, não uma produtividade. Clique numa barra para abrir o departamento.' },
	'rank.service.title': { es: '¿Dónde faltan más servicios?', en: 'Where are services most lacking?', gn: '¿Dónde faltan más servicios?', pt: 'Onde faltam mais serviços?' },
	'rank.service.tab.nbi': { es: 'NBI', en: 'UBN', gn: 'NBI', pt: 'NBI' },
	'rank.service.tab.cloacas': { es: 'Sin cloacas', en: 'No sewerage', gn: 'Sin cloacas', pt: 'Sem esgoto' },
	'rank.service.tab.hacin': { es: 'Hacinamiento', en: 'Overcrowding', gn: 'Hacinamiento', pt: 'Superlotação' },
	'rank.service.note': { es: '% de los hogares del departamento con esa carencia (Censo 2022: hogares con la carencia sobre el total de hogares, sumados en sus hexágonos) y, al lado, cuántos hogares son. Clic en una barra para abrir el departamento.', en: 'Share of the department’s households with that deprivation (2022 Census: households with it over all households, summed over its hexagons) and, next to it, how many households. Click a bar to open the department.', gn: '% de los hogares del departamento con esa carencia (Censo 2022: hogares con la carencia sobre el total de hogares, sumados en sus hexágonos) y, al lado, cuántos hogares son. Clic en una barra para abrir el departamento.', pt: '% dos lares do departamento com essa carência (Censo 2022: lares com a carência sobre o total de lares, somados nos seus hexágonos) e, ao lado, quantos lares são. Clique numa barra para abrir o departamento.' },
	'rank.health.title': { es: '¿Dónde queda más lejos la atención de salud?', en: 'Where is healthcare furthest away?', gn: '¿Dónde queda más lejos la atención de salud?', pt: 'Onde o atendimento de saúde fica mais longe?' },
	'rank.health.note': { es: 'Minutos hasta el centro de salud más cercano, promedio de las personas del departamento (Oxford/MAP, ponderado con la población del Censo 2022) y, al lado, el % de personas con obra social o prepaga. Clic en una barra para abrir el departamento.', en: 'Minutes to the nearest healthcare facility, averaged over the department’s people (Oxford/MAP, weighted by 2022 Census population) and, next to it, the % of people with health insurance. Click a bar to open the department.', gn: 'Minutos hasta el centro de salud más cercano, promedio de las personas del departamento (Oxford/MAP, ponderado con la población del Censo 2022) y, al lado, el % de personas con obra social o prepaga. Clic en una barra para abrir el departamento.', pt: 'Minutos até o centro de saúde mais próximo, média das pessoas do departamento (Oxford/MAP, ponderado pela população do Censo 2022) e, ao lado, o % de pessoas com plano de saúde. Clique numa barra para abrir o departamento.' },
	'rank.educap.title': { es: '¿Cómo está la escolaridad?', en: 'How is schooling?', gn: '¿Cómo está la escolaridad?', pt: 'Como está a escolaridade?' },
	'rank.educap.tab.noschool': { es: 'Sin instrucción o primario incompleto', en: 'No schooling or incomplete primary', gn: 'Sin instrucción o primario incompleto', pt: 'Sem instrução ou fundamental incompleto' },
	'rank.educap.tab.univ': { es: 'Universitario completo', en: 'Completed university', gn: 'Universitario completo', pt: 'Universitário completo' },
	'rank.educap.note': { es: '% de la población de 18 años y más del departamento (Censo 2022), sumada en sus hexágonos; misma definición en las cuatro provincias. Clic en una barra para abrir el departamento.', en: 'Share of the department’s people aged 18+ (2022 Census), summed over its hexagons; the same definition in all four provinces. Click a bar to open the department.', gn: '% de la población de 18 años y más del departamento (Censo 2022), sumada en sus hexágonos; misma definición en las cuatro provincias. Clic en una barra para abrir el departamento.', pt: '% da população de 18 anos ou mais do departamento (Censo 2022), somada nos seus hexágonos; mesma definição nas quatro províncias. Clique numa barra para abrir o departamento.' },
	'rank.eduflow.title': { es: '¿Dónde se corta la trayectoria escolar?', en: 'Where do school paths break off?', gn: '¿Dónde se corta la trayectoria escolar?', pt: 'Onde se interrompe a trajetória escolar?' },
	'rank.eduflow.tab.dropout': { es: 'No asisten (13 a 18)', en: 'Not attending (13–18)', gn: 'No asisten (13 a 18)', pt: 'Não frequentam (13 a 18)' },
	'rank.eduflow.tab.teen': { es: 'Madres de 14 a 17', en: 'Mothers aged 14–17', gn: 'Madres de 14 a 17', pt: 'Mães de 14 a 17' },
	'rank.eduflow.note': { es: 'No asisten: % de las personas de 13 a 18 años que no van a la escuela. Madres: % de las mujeres de 14 a 17 años que tuvieron hijos. Censo 2022, sumado en los hexágonos del departamento. Clic en una barra para abrir el departamento.', en: 'Not attending: % of people aged 13–18 not in school. Mothers: % of women aged 14–17 who have had children. 2022 Census, summed over the department’s hexagons. Click a bar to open the department.', gn: 'No asisten: % de las personas de 13 a 18 años que no van a la escuela. Madres: % de las mujeres de 14 a 17 años que tuvieron hijos. Censo 2022, sumado en los hexágonos del departamento. Clic en una barra para abrir el departamento.', pt: 'Não frequentam: % das pessoas de 13 a 18 anos fora da escola. Mães: % das mulheres de 14 a 17 anos que tiveram filhos. Censo 2022, somado nos hexágonos do departamento. Clique numa barra para abrir o departamento.' },
	'rank.econ.title': { es: '¿Dónde hay más actividad económica?', en: 'Where is there most economic activity?', gn: '¿Dónde hay más actividad económica?', pt: 'Onde há mais atividade econômica?' },
	'rank.econ.note.viirs': { es: 'Radiancia nocturna media (VIIRS, nW/cm²/sr) sobre el área del departamento: un indicio de actividad y urbanización, no una medida económica. Clic en una barra para abrir el departamento.', en: 'Mean night-time radiance (VIIRS, nW/cm²/sr) over the department’s area: a hint of activity and urbanisation, not an economic measure. Click a bar to open the department.', gn: 'Radiancia nocturna media (VIIRS, nW/cm²/sr) sobre el área del departamento: un indicio de actividad y urbanización, no una medida económica. Clic en una barra para abrir el departamento.', pt: 'Radiância noturna média (VIIRS, nW/cm²/sr) sobre a área do departamento: um indício de atividade e urbanização, não uma medida econômica. Clique numa barra para abrir o departamento.' },
	'rank.socio.title': { es: '¿Dónde hay más necesidades básicas insatisfechas?', en: 'Where are basic needs most unmet?', gn: '¿Dónde hay más necesidades básicas insatisfechas?', pt: 'Onde há mais necessidades básicas insatisfeitas?' },
	'rank.socio.note': { es: '% de los hogares con necesidades básicas insatisfechas (Censo 2022), sumados en los hexágonos del departamento; misma definición en las cuatro provincias. Clic en una barra para abrir el departamento.', en: 'Share of households with unmet basic needs (2022 Census), summed over the department’s hexagons; the same definition in all four provinces. Click a bar to open the department.', gn: '% de los hogares con necesidades básicas insatisfechas (Censo 2022), sumados en los hexágonos del departamento; misma definición en las cuatro provincias. Clic en una barra para abrir el departamento.', pt: '% dos lares com necessidades básicas insatisfeitas (Censo 2022), somados nos hexágonos do departamento; mesma definição nas quatro províncias. Clique numa barra para abrir o departamento.' },
	'rank.econ.tab.empleo': { es: 'Empleo', en: 'Employment', gn: 'Empleo', pt: 'Emprego' },
	'rank.econ.tab.univ': { es: 'Universitarios', en: 'University', gn: 'Universidad', pt: 'Universitários' },
	'rank.econ.tab.viirs': { es: 'Luz nocturna', en: 'Night lights', gn: 'Luz nocturna', pt: 'Luz noturna' },
	'rank.econ.note': { es: '% de la población de 14 años y más que está ocupada (Censo 2022, definición INDEC): ocupados sobre población de 14+ sumados en los hexágonos del departamento, igual en las cuatro provincias. Clic en una barra para abrir el departamento.', en: '% of people aged 14+ who are employed (2022 Census, INDEC definition): employed over population 14+, summed over the department’s hexagons, the same in all four provinces. Click a bar to open the department.', gn: '% de la población de 14 años y más que está ocupada (Censo 2022, definición INDEC): ocupados sobre población de 14+ sumados en los hexágonos del departamento, igual en las cuatro provincias. Clic en una barra para abrir el departamento.', pt: '% da população de 14 anos ou mais que está ocupada (Censo 2022, definição INDEC): ocupados sobre população de 14+ somados nos hexágonos do departamento, igual nas quatro províncias. Clique numa barra para abrir o departamento.' },
	'rank.econ.note.univ': { es: '% de la población de 18 años y más con universitario completo (Censo 2022): sumado en los hexágonos del departamento, igual en las cuatro provincias. Clic en una barra para abrir el departamento.', en: '% of people aged 18+ with a completed university degree (2022 Census): summed over the department’s hexagons, the same in all four provinces. Click a bar to open the department.', gn: '% de la población de 18 años y más con universitario completo (Censo 2022): sumado en los hexágonos del departamento, igual en las cuatro provincias. Clic en una barra para abrir el departamento.', pt: '% da população de 18 anos ou mais com universitário completo (Censo 2022): somado nos hexágonos do departamento, igual nas quatro províncias. Clique numa barra para abrir o departamento.' },
	'rank.unit.land': { es: '% tierra', en: '% land', gn: '% tierra', pt: '% terra' },
	'rank.flood.tab.pct': { es: '% edificios', en: '% buildings', gn: '% edificios', pt: '% edifícios' },
	'rank.flood.tab.n': { es: 'Edificios', en: 'Buildings', gn: 'Edificios', pt: 'Edifícios' },
	'rank.flood.tab.land': { es: '% tierra', en: '% land', gn: '% tierra', pt: '% terra' },
	'rank.flood.title.n': { es: '¿Dónde hay más edificios en zona baja?', en: 'Where are there more buildings on low ground?', gn: '¿Dónde hay más edificios en zona baja?', pt: 'Onde há mais edifícios em terreno baixo?' },
	'rank.flood.note.n': { es: 'Cantidad de edificios de cada departamento (Google Open Buildings 2023) a menos de 5 m sobre el río o arroyo más cercano (HAND, MERIT Hydro). Mide magnitud: los departamentos con más población quedan arriba aunque su parte expuesta sea chica. Al lado, el total de edificios del departamento. Clic en una barra para abrir el departamento.', en: 'Number of each department’s buildings (Google Open Buildings 2023) less than 5 m above the nearest river or stream (HAND, MERIT Hydro). It measures magnitude: the most populated departments rank high even when their exposed share is small. Next to it, the department’s total buildings. Click a bar to open the department.', gn: 'Cantidad de edificios de cada departamento (Google Open Buildings 2023) a menos de 5 m sobre el río o arroyo más cercano (HAND, MERIT Hydro). Mide magnitud: los departamentos con más población quedan arriba aunque su parte expuesta sea chica. Al lado, el total de edificios del departamento. Clic en una barra para abrir el departamento.', pt: 'Quantidade de edifícios de cada departamento (Google Open Buildings 2023) a menos de 5 m acima do rio ou arroio mais próximo (HAND, MERIT Hydro). Mede magnitude: os departamentos mais populosos ficam no topo mesmo com uma parte exposta pequena. Ao lado, o total de edifícios do departamento. Clique numa barra para abrir o departamento.' },
	'rank.flood.title.land': { es: '¿Dónde hay más tierra baja?', en: 'Where is there more low ground?', gn: '¿Dónde hay más tierra baja?', pt: 'Onde há mais terra baixa?' },
	'rank.flood.note.land': { es: '% de la tierra de cada departamento —sin ríos ni embalses permanentes (JRC)— a menos de 5 m sobre el río o arroyo más cercano (HAND, MERIT Hydro). Lectura del suelo: no depende de cuánto está construido, y sube en los departamentos rurales sobre las terrazas del Paraná y del Uruguay. Al lado, % de los edificios en la misma franja. Clic en una barra para abrir el departamento.', en: 'Share of each department’s land —permanent rivers and reservoirs (JRC) excluded— less than 5 m above the nearest river or stream (HAND, MERIT Hydro). A reading of the land: it does not depend on how much is built, and rises in rural departments on the Paraná and Uruguay terraces. Next to it, share of buildings in the same band. Click a bar to open the department.', gn: '% de la tierra de cada departamento —sin ríos ni embalses permanentes (JRC)— a menos de 5 m sobre el río o arroyo más cercano (HAND, MERIT Hydro). Lectura del suelo: no depende de cuánto está construido, y sube en los departamentos rurales sobre las terrazas del Paraná y del Uruguay. Al lado, % de los edificios en la misma franja. Clic en una barra para abrir el departamento.', pt: '% da terra de cada departamento —sem rios nem represas permanentes (JRC)— a menos de 5 m acima do rio ou arroio mais próximo (HAND, MERIT Hydro). Leitura do solo: não depende de quanto está construído, e sobe nos departamentos rurais sobre os terraços do Paraná e do Uruguai. Ao lado, % dos edifícios na mesma faixa. Clique numa barra para abrir o departamento.' },
	'rank.unit.buildings': { es: 'edificios', en: 'buildings', gn: 'edificios', pt: 'edifícios' },
	'rank.unit.bldTotal': { es: 'edificios en total', en: 'buildings in total', gn: 'edificios en total', pt: 'edifícios no total' },
	'rank.unit.bldPct': { es: '% edificios', en: '% buildings', gn: '% edificios', pt: '% edifícios' },
	'rank.flood.tab.change': { es: 'Cambio', en: 'Change', gn: 'Cambio', pt: 'Mudança' },
	'rank.flood.title.change': { es: '¿Dónde lo nuevo se construye más en zona baja?', en: 'Where is new construction going more to low ground?', gn: '¿Dónde lo nuevo se construye más en zona baja?', pt: 'Onde o novo se constrói mais em terreno baixo?' },
	'rank.flood.note.change': { es: 'Diferencia, en puntos, entre el % de los edificios agregados en 2016–2023 que quedaron a menos de 5 m sobre el río o arroyo más cercano y el % del parque de 2016 en esa franja (Google Open Buildings Temporal, tendencia anual; HAND, MERIT Hydro). Rojo: el crecimiento se corre hacia zona baja; verde: se aleja. "—" donde se agregaron menos de 1.000 edificios en total o menos de 200 en zona baja. Arriba, la lectura del conjunto {ofRef}, comparando cuatro pares de años. El modelo detecta peor en los primeros años: leer como tendencia, no como conteo exacto. Al lado, los edificios nuevos en zona baja.', en: 'Difference, in points, between the share of buildings added in 2016–2023 that lie less than 5 m above the nearest river or stream and the share of the 2016 stock in that band (Google Open Buildings Temporal, annual trend; HAND, MERIT Hydro). Red: growth is drifting to low ground; green: away from it. "—" where fewer than 1,000 buildings were added in total or fewer than 200 on low ground. Above, the reading for {ref} as a whole, comparing four pairs of years. The model detects less in the early years: read it as a trend, not an exact count. Next to it, new buildings on low ground.', gn: 'Diferencia, en puntos, entre el % de los edificios agregados en 2016–2023 que quedaron a menos de 5 m sobre el río o arroyo más cercano y el % del parque de 2016 en esa franja (Google Open Buildings Temporal, tendencia anual; HAND, MERIT Hydro). Rojo: el crecimiento se corre hacia zona baja; verde: se aleja. "—" donde se agregaron menos de 1.000 edificios en total o menos de 200 en zona baja. Arriba, la lectura del conjunto {ofRef}, comparando cuatro pares de años. El modelo detecta peor en los primeros años: leer como tendencia, no como conteo exacto. Al lado, los edificios nuevos en zona baja.', pt: 'Diferença, em pontos, entre a % dos edifícios acrescentados em 2016–2023 que ficaram a menos de 5 m acima do rio ou arroio mais próximo e a % do estoque de 2016 nessa faixa (Google Open Buildings Temporal, tendência anual; HAND, MERIT Hydro). Vermelho: o crescimento se desloca para terreno baixo; verde: se afasta. "—" onde foram acrescentados menos de 1.000 edifícios no total ou menos de 200 em terreno baixo. Acima, a leitura do conjunto {ofRef}, comparando quatro pares de anos. O modelo detecta menos nos primeiros anos: ler como tendência, não como contagem exata. Ao lado, os edifícios novos em terreno baixo.' },
	'rank.unit.pts': { es: 'pts', en: 'pts', gn: 'pts', pt: 'pts' },
	'analysis.flood.lowBld': { es: 'Edificios en zona baja (HAND < 5 m)', en: 'Buildings on low ground (HAND < 5 m)', gn: 'Edificios en zona baja (HAND < 5 m)', pt: 'Edifícios em terreno baixo (HAND < 5 m)' },
	'analysis.flood.lowLand': { es: '% de tierra baja (HAND < 5 m)', en: '% low ground (HAND < 5 m)', gn: '% de tierra baja (HAND < 5 m)', pt: '% de terra baixa (HAND < 5 m)' },
	'analysis.flood.lowBldNew': { es: 'Edificios nuevos en zona baja 2016–2023', en: 'New buildings on low ground 2016–2023', gn: 'Edificios nuevos en zona baja 2016–2023', pt: 'Edifícios novos em terreno baixo 2016–2023' },
	'legend.lowBld.none': { es: 'Sin edificios en zona baja', en: 'No buildings on low ground', gn: 'Sin edificios en zona baja', pt: 'Sem edifícios em terreno baixo' },
	'legend.lowBld.c1': { es: '1–2 edificios', en: '1–2 buildings', gn: '1–2 edificios', pt: '1–2 edifícios' },
	'legend.lowBld.c2': { es: '3–9', en: '3–9', gn: '3–9', pt: '3–9' },
	'legend.lowBld.c3': { es: '10–49', en: '10–49', gn: '10–49', pt: '10–49' },
	'legend.lowBld.c4': { es: '50 o más', en: '50 or more', gn: '50 o más', pt: '50 ou mais' },
	'legend.lowLand.none': { es: 'Sin tierra baja', en: 'No low ground', gn: 'Sin tierra baja', pt: 'Sem terra baixa' },
	'legend.lowLand.c1': { es: 'Menos de 25 %', en: 'Under 25%', gn: 'Menos de 25 %', pt: 'Menos de 25 %' },
	'legend.lowLand.c2': { es: '25–75 %', en: '25–75%', gn: '25–75 %', pt: '25–75 %' },
	'legend.lowLand.c3': { es: '75 % o más', en: '75% or more', gn: '75 % o más', pt: '75 % ou mais' },
	'legend.lowNew.none': { es: 'Sin cambio', en: 'No change', gn: 'Sin cambio', pt: 'Sem mudança' },
	'legend.lowNew.down': { es: 'Menos que en 2016', en: 'Fewer than in 2016', gn: 'Menos que en 2016', pt: 'Menos que em 2016' },
	'legend.lowNew.c1': { es: '1–4 nuevos', en: '1–4 new', gn: '1–4 nuevos', pt: '1–4 novos' },
	'legend.lowNew.c2': { es: '5–19 nuevos', en: '5–19 new', gn: '5–19 nuevos', pt: '5–19 novos' },
	'legend.lowNew.c3': { es: '20 o más nuevos', en: '20 or more new', gn: '20 o más nuevos', pt: '20 ou mais novos' },
	'rank.change.prov.whole': { es: 'En el conjunto de', en: 'Across', gn: 'En el conjunto de', pt: 'No conjunto de' },
	'rank.change.prov.built': { es: 'de lo construido en 2016–2023 quedó en zona baja, frente a', en: 'of what was built in 2016–2023 is on low ground, against', gn: 'de lo construido en 2016–2023 quedó en zona baja, frente a', pt: 'do que foi construído em 2016–2023 ficou em terreno baixo, frente a' },
	'rank.change.prov.stock': { es: 'del parque de 2016.', en: 'of the 2016 stock.', gn: 'del parque de 2016.', pt: 'do estoque de 2016.' },
	'rank.change.label.toward': { es: 'el crecimiento se corre hacia zona baja', en: 'growth is drifting to low ground', gn: 'el crecimiento se corre hacia zona baja', pt: 'o crescimento se desloca para terreno baixo' },
	'rank.change.label.away': { es: 'el crecimiento se aleja de la zona baja', en: 'growth is drifting away from low ground', gn: 'el crecimiento se aleja de la zona baja', pt: 'o crescimento se afasta do terreno baixo' },
	'rank.change.label.none': { es: 'sin cambio: lo nuevo se reparte como lo existente', en: 'no change: new buildings are spread like the existing ones', gn: 'sin cambio: lo nuevo se reparte como lo existente', pt: 'sem mudança: o novo se distribui como o existente' },
	'rank.change.label.unclear': { es: 'sin tendencia clara: el signo cambia según los años que se comparen', en: 'no clear trend: the sign changes with the years compared', gn: 'sin tendencia clara: el signo cambia según los años que se comparen', pt: 'sem tendência clara: o sinal muda conforme os anos comparados' },
	'rank.change.noBars': { es: 'No se ordenan los departamentos: hay menos de 8 con suficientes edificios nuevos para compararlos, o su orden cambia según los años que se comparen. La lectura válida es la del conjunto {ofRef}.', en: 'Departments are not ranked: fewer than 8 have enough new buildings to compare, or their order changes with the years compared. The valid reading is {ref} as a whole.', gn: 'No se ordenan los departamentos: hay menos de 8 con suficientes edificios nuevos para compararlos, o su orden cambia según los años que se comparen. La lectura válida es la del conjunto {ofRef}.', pt: 'Os departamentos não são ordenados: menos de 8 têm edifícios novos suficientes para comparar, ou sua ordem muda conforme os anos comparados. A leitura válida é a do conjunto {ofRef}.' },
	'rank.unit.newLow': { es: 'nuevos en zona baja', en: 'new on low ground', gn: 'nuevos en zona baja', pt: 'novos em terreno baixo' },
	'rank.of': { es: 'de', en: 'of', gn: 'de', pt: 'de' },
	'rank.censo.tab.pct': { es: 'Cambio %', en: 'Change %', gn: 'Cambio %', pt: 'Mudança %' },
	'rank.censo.tab.people': { es: 'Personas', en: 'People', gn: 'Personas', pt: 'Pessoas' },
	'rank.censo.tab.total': { es: 'Población', en: 'Population', gn: 'Tavaygua', pt: 'População' },
	'rank.censo.title.pct': { es: '¿Cuánto creció la población entre 2010 y 2022?', en: 'How much did population grow between 2010 and 2022?', gn: '¿Cuánto creció la población entre 2010 y 2022?', pt: 'Quanto cresceu a população entre 2010 e 2022?' },
	'rank.censo.title.people': { es: '¿Dónde se sumó más gente entre 2010 y 2022?', en: 'Where were the most people added between 2010 and 2022?', gn: '¿Dónde se sumó más gente entre 2010 y 2022?', pt: 'Onde se somaram mais pessoas entre 2010 e 2022?' },
	'rank.censo.title.total': { es: '¿Dónde vive más gente? (Censo 2022)', en: 'Where do most people live? (2022 Census)', gn: '¿Dónde vive más gente? (Censo 2022)', pt: 'Onde vive mais gente? (Censo 2022)' },
	'rank.censo.note.pct': { es: 'Variación de la población entre los censos 2010 y 2022, con los totales oficiales de INDEC por departamento; al lado, la población 2022. Clic en un departamento: población y viviendas en los 4 censos (1991–2022) y su ubicación en el mapa.', en: 'Population change between the 2010 and 2022 censuses, from INDEC’s official department totals; next to it, the 2022 population. Click a department: population and dwellings in the 4 censuses (1991–2022) and its location on the map.', gn: 'Variación de la población entre los censos 2010 y 2022, con los totales oficiales de INDEC por departamento; al lado, la población 2022. Clic en un departamento: población y viviendas en los 4 censos (1991–2022) y su ubicación en el mapa.', pt: 'Variação da população entre os censos de 2010 e 2022, com os totais oficiais do INDEC por departamento; ao lado, a população de 2022. Clique em um departamento: população e moradias nos 4 censos (1991–2022) e sua localização no mapa.' },
	'rank.censo.note.people': { es: 'Personas ganadas o perdidas entre 2010 y 2022 (totales oficiales INDEC): la magnitud del cambio, que favorece a los departamentos grandes.', en: 'People gained or lost between 2010 and 2022 (INDEC official totals): the size of the change, which favours large departments.', gn: 'Personas ganadas o perdidas entre 2010 y 2022 (totales oficiales INDEC): la magnitud del cambio, que favorece a los departamentos grandes.', pt: 'Pessoas ganhas ou perdidas entre 2010 e 2022 (totais oficiais do INDEC): a magnitude da mudança, que favorece os departamentos grandes.' },
	'rank.censo.note.total': { es: 'Población del Censo 2022 (totales oficiales INDEC) y, al lado, viviendas.', en: '2022 Census population (INDEC official totals) and, next to it, dwellings.', gn: 'Población del Censo 2022 (totales oficiales INDEC) y, al lado, viviendas.', pt: 'População do Censo 2022 (totais oficiais do INDEC) e, ao lado, moradias.' },
	'rank.unit.people': { es: 'personas', en: 'people', gn: 'personas', pt: 'pessoas' },
	'rank.unit.dwellings': { es: 'viviendas', en: 'dwellings', gn: 'óga', pt: 'moradias' },
	'analysis.flood.clickHint': { es: 'Hacé click en una parcela para ver detalle', en: 'Click a parcel for details', gn: 'Ehesakutu peteĩ yvy ehecha hag̃ua', pt: 'Clique em uma parcela para ver detalhes' },
	'analysis.hex.clickHint': { es: 'Hacé click en un hexágono para ver el detalle', en: 'Click a hexagon for details', gn: 'Ehesakutu peteĩ hexágono ehecha hag̃ua', pt: 'Clique em um hexágono para ver o detalhe' },
	// No "Fuente:" prefix: the panel prints the label before it.
	'analysis.flood.source': { es: 'MERIT Hydro (HAND) + Google Open Buildings Temporal 2016–2023 + JRC Global Surface Water (Landsat, 1984–2021) + Sentinel-1 SAR (Copernicus)', en: 'MERIT Hydro (HAND) + Google Open Buildings Temporal 2016–2023 + JRC Global Surface Water (Landsat, 1984–2021) + Sentinel-1 SAR (Copernicus)', gn: 'MERIT Hydro (HAND) + Google Open Buildings Temporal 2016–2023 + JRC Global Surface Water (Landsat, 1984–2021) + Sentinel-1 SAR (Copernicus)', pt: 'MERIT Hydro (HAND) + Google Open Buildings Temporal 2016–2023 + JRC Global Surface Water (Landsat, 1984–2021) + Sentinel-1 SAR (Copernicus)' },
	'data.source.censo': { es: 'Fuente: INDEC — Censo Nacional de Población 2022', en: 'Source: INDEC — National Population Census 2022', gn: "Moñe'ẽha: INDEC — Censo Nacional 2022", pt: 'Fonte: INDEC — Censo Nacional de Población 2022' },
	'data.source.realEstate': { es: 'Fuente: Relevamiento de mercado inmobiliario', en: 'Source: Real estate market survey', gn: "Moñe'ẽha: Relevamiento óga ñemuhague", pt: 'Fonte: Levantamento do mercado imobiliário' },
	'data.source.buildings': { es: 'Fuente: Detección por IA sobre imágenes satelitales', en: 'Source: AI detection on satellite imagery', gn: "Moñe'ẽha: IA ohechaukáva satélite ra'ãnga rupi", pt: 'Fonte: Detecção por IA em imagens de satélite' },
	'data.source.catastro': { es: 'Fuente: Dirección General de Catastro, Misiones', en: 'Source: Dirección General de Catastro, Misiones', gn: "Moñe'ẽha: Catastro, Misiones", pt: 'Fonte: Dirección General de Catastro, Misiones' },
	'data.source.overture': { es: 'Fuente: Overture Maps Foundation via walkthru.earth (CC BY 4.0)', en: 'Source: Overture Maps Foundation via walkthru.earth (CC BY 4.0)', gn: "Moñe'ẽha: Overture Maps Foundation (CC BY 4.0)", pt: 'Fonte: Overture Maps Foundation via walkthru.earth (CC BY 4.0)' },
	// Per-layer data source (shown after the «Fuente:» label, so no prefix here).
	'layer.source.census2022': { es: 'INDEC — Censo Nacional 2022, repartido al hexágono según los edificios (método dasimétrico)', en: 'INDEC — 2022 National Census, apportioned to the hexagon by buildings (dasymetric method)', gn: 'INDEC — Censo Nacional 2022, repartido al hexágono según los edificios (método dasimétrico)', pt: 'INDEC — Censo Nacional 2022, distribuído ao hexágono segundo as edificações (método dasimétrico)' }, // gn: pendiente
	'layer.source.health': { es: 'Oxford / Malaria Atlas Project 2019 (Weiss et al. 2020) + OpenStreetMap + INDEC — Censo Nacional 2022', en: 'Oxford / Malaria Atlas Project 2019 (Weiss et al. 2020) + OpenStreetMap + INDEC — 2022 National Census', gn: 'Oxford / Malaria Atlas Project 2019 (Weiss et al. 2020) + OpenStreetMap + INDEC — Censo Nacional 2022', pt: 'Oxford / Malaria Atlas Project 2019 (Weiss et al. 2020) + OpenStreetMap + INDEC — Censo Nacional 2022' }, // gn: pendiente
	'layer.source.censoTemporal': { es: 'INDEC — Censos 1991, 2001, 2010 y 2022 · Rodríguez y de Grande (CONICET)', en: 'INDEC — 1991, 2001, 2010 and 2022 Censuses · Rodríguez and de Grande (CONICET)', gn: 'INDEC — Censos 1991, 2001, 2010 y 2022 · Rodríguez y de Grande (CONICET)', pt: 'INDEC — Censos 1991, 2001, 2010 e 2022 · Rodríguez e de Grande (CONICET)' }, // gn: pendiente
	'layer.source.agri': { es: 'SoilGrids v2 (ISRIC) + CHIRPS + ERA5 + FABDEM, 2019–2024', en: 'SoilGrids v2 (ISRIC) + CHIRPS + ERA5 + FABDEM, 2019–2024', gn: 'SoilGrids v2 (ISRIC) + CHIRPS + ERA5 + FABDEM, 2019–2024', pt: 'SoilGrids v2 (ISRIC) + CHIRPS + ERA5 + FABDEM, 2019–2024' }, // gn: pendiente
	'layer.source.forestry': { es: 'Inventario Nacional de Plantaciones Forestales (DNDFI 2026) + SoilGrids + CHIRPS + ERA5 + SRTM', en: 'National Forest Plantation Inventory (DNDFI 2026) + SoilGrids + CHIRPS + ERA5 + SRTM', gn: 'Inventario Nacional de Plantaciones Forestales (DNDFI 2026) + SoilGrids + CHIRPS + ERA5 + SRTM', pt: 'Inventário Nacional de Plantações Florestais (DNDFI 2026) + SoilGrids + CHIRPS + ERA5 + SRTM' }, // gn: pendiente
	'layer.source.landUse': { es: 'Dynamic World V1 (Google / World Resources Institute), Sentinel-2 a 10 m', en: 'Dynamic World V1 (Google / World Resources Institute), Sentinel-2 at 10 m', gn: 'Dynamic World V1 (Google / World Resources Institute), Sentinel-2 a 10 m', pt: 'Dynamic World V1 (Google / World Resources Institute), Sentinel-2 a 10 m' }, // gn: pendiente
	'layer.source.economic': { es: 'Luces nocturnas VIIRS + INDEC — Censo Nacional 2022 + Global Building Atlas', en: 'VIIRS night lights + INDEC — 2022 National Census + Global Building Atlas', gn: 'Luces nocturnas VIIRS + INDEC — Censo Nacional 2022 + Global Building Atlas', pt: 'Luzes noturnas VIIRS + INDEC — Censo Nacional 2022 + Global Building Atlas' }, // gn: pendiente
	'layer.source.accessibility': { es: 'Oxford / Malaria Atlas Project (superficie de fricción 2019, accesibilidad a ciudades 2015) + OpenStreetMap', en: 'Oxford / Malaria Atlas Project (2019 friction surface, 2015 accessibility to cities) + OpenStreetMap', gn: 'Oxford / Malaria Atlas Project (superficie de fricción 2019, accesibilidad a ciudades 2015) + OpenStreetMap', pt: 'Oxford / Malaria Atlas Project (superfície de fricção 2019, acessibilidade a cidades 2015) + OpenStreetMap' }, // gn: pendiente
	'layer.source.pm25': { es: 'ACAG V6.GL.02 (Dalhousie University; van Donkelaar et al. 2021), ~1 km, 1998–2022', en: 'ACAG V6.GL.02 (Dalhousie University; van Donkelaar et al. 2021), ~1 km, 1998–2022', gn: 'ACAG V6.GL.02 (Dalhousie University; van Donkelaar et al. 2021), ~1 km, 1998–2022', pt: 'ACAG V6.GL.02 (Dalhousie University; van Donkelaar et al. 2021), ~1 km, 1998–2022' }, // gn: pendiente
	'layer.source.deforestation': { es: 'Hansen Global Forest Change v1.12 (University of Maryland / Google), Landsat 30 m, 2001–2024', en: 'Hansen Global Forest Change v1.12 (University of Maryland / Google), Landsat 30 m, 2001–2024', gn: 'Hansen Global Forest Change v1.12 (University of Maryland / Google), Landsat 30 m, 2001–2024', pt: 'Hansen Global Forest Change v1.12 (University of Maryland / Google), Landsat 30 m, 2001–2024' }, // gn: pendiente
	'layer.source.eudr': { es: 'Hansen Global Forest Change v1.13 + MODIS MCD64A1 (área quemada) + MapBiomas Colección 2', en: 'Hansen Global Forest Change v1.13 + MODIS MCD64A1 (burned area) + MapBiomas Collection 2', gn: 'Hansen Global Forest Change v1.13 + MODIS MCD64A1 (área quemada) + MapBiomas Colección 2', pt: 'Hansen Global Forest Change v1.13 + MODIS MCD64A1 (área queimada) + MapBiomas Coleção 2' }, // gn: pendiente

	// ── EUDR variables ──
	'eudr.riskScore': { es: 'Score de riesgo EUDR', en: 'EUDR risk score', gn: 'EUDR riesgo', pt: 'Score de risco EUDR' },
	'eudr.lossPost2020': { es: 'Pérdida post-2020 (%)', en: 'Post-2020 loss (%)', gn: 'Pérdida post-2020', pt: 'Perda pós-2020 (%)' },
	'eudr.firePost2020': { es: 'Fuego post-2020, total (%)', en: 'Post-2020 fire, total (%)', gn: 'Fuego post-2020, opavave', pt: 'Fogo pós-2020, total (%)' },
	'eudr.fireNativePost2020': { es: 'Fuego sobre nativa leñosa (%)', en: 'Fire on native woody cover (%)', gn: 'Fuego ka’aguy ypykuére (%)', pt: 'Fogo sobre nativa lenhosa (%)' },
	'eudr.forest2020': { es: 'Cobertura arbórea 2000 (%)', en: 'Tree cover 2000 (%)', gn: "Ka'aguy 2000 (%)", pt: 'Cobertura arbórea 2000 (%)' },
	'eudr.forestCurrent': { es: 'Cobertura arbórea actual, aprox. (%)', en: 'Current tree cover, approx. (%)', gn: "Ka'aguy ko'ãga (%)", pt: 'Cobertura arbórea atual, aprox. (%)' },

	// ── Migrated radio/catastro to H3 ──
	'analysis.scores.type': { es: 'Tipo de consolidación', en: 'Consolidation type', gn: 'Tipo', pt: 'Tipo de consolidação' },
	'analysis.scores.typeLabel': { es: 'Etiqueta', en: 'Label', gn: 'Label', pt: 'Rótulo' },
	'analysis.socio.type': { es: 'Tipo sociodemografico', en: 'Sociodemographic type', gn: 'Tipo', pt: 'Tipo sociodemográfico' },
	'analysis.socio.typeLabel': { es: 'Etiqueta', en: 'Label', gn: 'Label', pt: 'Rótulo' },
	'analysis.economic.type': { es: 'Tipo económico', en: 'Economic type', gn: 'Tipo', pt: 'Tipo econômico' },
	'analysis.economic.typeLabel': { es: 'Etiqueta', en: 'Label', gn: 'Label', pt: 'Rótulo' },
	'analysis.accessibility.type': { es: 'Tipo de accesibilidad', en: 'Accessibility type', gn: 'Tipo', pt: 'Tipo de acessibilidade' },
	'analysis.accessibility.typeLabel': { es: 'Etiqueta', en: 'Label', gn: 'Label', pt: 'Rótulo' },
	'scores.urbanConsolidation': { es: 'Consolidacion urbana', en: 'Urban consolidation', gn: 'Consolidacion', pt: 'Consolidação urbana' },
	'scores.paving': { es: 'Pavimentación', en: 'Paving', gn: 'Tape', pt: 'Pavimentação' },
	'scores.serviceAccess': { es: 'Acceso a servicios', en: 'Service access', gn: 'Servicios', pt: 'Acesso a serviços' },
	'scores.commercial': { es: 'Vitalidad comercial', en: 'Commercial vitality', gn: 'Comercio', pt: 'Vitalidade comercial' },
	'scores.roadConnectivity': { es: 'Conectividad vial', en: 'Road connectivity', gn: 'Tape joapy', pt: 'Conectividade viária' },
	'scores.buildingMix': { es: 'Mix edilicio', en: 'Building mix', gn: 'Oga', pt: 'Mix edilício' },
	'scores.urbanization': { es: 'Urbanizacion', en: 'Urbanisation', gn: 'Tava', pt: 'Urbanização' },
	'scores.waterExposure': { es: 'Exposición hidrica', en: 'Water exposure', gn: 'Y', pt: 'Exposição hídrica' },
	'radio.densidad': { es: 'Habitantes por km² (estimado en el hexágono)', en: 'Inhabitants per km² (estimated in the hexagon)', gn: 'Habitantes por km² (estimado en el hexágono)', pt: 'Habitantes por km² (estimado no hexágono)' },
	'radio.nbi': { es: 'Hogares con NBI (% de hogares)', en: 'Households with UBN (% of households)', gn: 'Hogares con NBI (% de hogares)', pt: 'Lares com NBI (% dos lares)' },
	'radio.hacinamiento': { es: 'Hacinamiento (% de hogares)', en: 'Overcrowding (% of households)', gn: 'Hacinamiento (% de hogares)', pt: 'Superlotação (% dos lares)' },
	'radio.propietario': { es: 'Propietarios de la vivienda (% de hogares)', en: 'Homeowners (% of households)', gn: 'Propietarios de la vivienda (% de hogares)', pt: 'Proprietários da moradia (% dos lares)' },
	'radio.tamHogar': { es: 'Personas por hogar', en: 'People per household', gn: 'Personas por hogar', pt: 'Pessoas por lar' },
	'radio.computadora': { es: 'Con computadora (% de hogares)', en: 'With a computer (% of households)', gn: 'Con computadora (% de hogares)', pt: 'Com computador (% dos lares)' },
	'radio.empleo': { es: 'Ocupados (% de 14 años y más)', en: 'Employed (% of people aged 14+)', gn: 'Ocupados (% 14 ary ha hetave)', pt: 'Ocupados (% de 14 anos ou mais)' },
	'radio.actividad': { es: 'Activos (% de 14 años y más)', en: 'Economically active (% of people aged 14+)', gn: 'Activos (% 14 ary ha hetave)', pt: 'Ativos (% de 14 anos ou mais)' },
	'radio.universitario': { es: 'Universitario completo (% de 18 años y más)', en: 'Completed university (% of people aged 18+)', gn: 'Universidad opáva (% 18 ary ha hetave)', pt: 'Universitário completo (% de 18 anos ou mais)' },
	'radio.viirs': { es: 'Luces nocturnas', en: 'Night lights', gn: 'Luces', pt: 'Luzes noturnas' },
	'radio.buildingDensity': { es: 'Densidad edilicia', en: 'Building density', gn: 'Oga', pt: 'Densidade edilícia' },
	'radio.travelCapital': { es: 'Min. a capital', en: 'Min. to capital', gn: 'Capital', pt: 'Min. até capital' },
	'radio.travelCabecera': { es: 'Min. a cabecera', en: 'Min. to dept. seat', gn: 'Cabecera', pt: 'Min. até sede municipal' },
	'radio.distHospital': { es: 'Dist. hospital (km)', en: 'Dist. hospital (km)', gn: 'Hospital', pt: 'Dist. hospital (km)' },
	'radio.distSecundaria': { es: 'Dist. secundaria (km)', en: 'Dist. secondary school', gn: 'Secundaria', pt: 'Dist. ensino médio (km)' },
	'radio.distPrimaria': { es: 'Dist. ruta (km)', en: 'Dist. primary road (km)', gn: 'Tape', pt: 'Dist. rodovia (km)' },

	// ── EUDR in analysis menu ──
	'trade.eudr.analysis_title': { es: 'EUDR · Pérdida de bosque post-2020 (NEA argentino + Paraguay + sur de Brasil)', en: 'EUDR · Post-2020 forest loss (NE Argentina + Paraguay + southern Brazil)', gn: "EUDR · Ka'aguy ñehundi 2020 rire (NEA Argentina + Paraguay + Brasil sur)", pt: 'EUDR · Perda florestal pós-2020 (NEA argentino + Paraguai + sul do Brasil)' },
	'trade.eudr.analysis_desc': { es: 'Pérdida de cobertura arbórea posterior a 2020 por hexágono (Hansen GFC 30 m + MODIS fuego)', en: 'Tree-cover loss after 2020 per hexagon (Hansen GFC 30 m + MODIS fire)', gn: 'EUDR', pt: 'Perda de cobertura arbórea posterior a 2020 por hexágono (Hansen GFC 30 m + MODIS fogo)' },

	// ── Temporal toggle ──
	'temporal.current': { es: 'Actual', en: 'Current', gn: "Ko'ãga", pt: 'Atual' },
	'temporal.baseline': { es: 'Línea Base', en: 'Baseline', gn: 'Base', pt: 'Linha de Base' },
	'temporal.delta': { es: 'Cambio', en: 'Change', gn: 'Moambue', pt: 'Variação' },
	'temporal.hint.current': { es: 'Periodo reciente. Fuente: datos satelitales multisensor.', en: 'Recent period. Source: multi-sensor satellite data.', gn: "Ko'aga", pt: 'Período recente. Fonte: dados de satélite multissensor.' },
	'temporal.hint.baseline': { es: 'Periodo de referencia histórico. Mismas fuentes satelitales.', en: 'Historical reference period. Same satellite sources.', gn: "Ymaguare", pt: 'Período de referência histórico. Mesmas fontes satelitais.' },
	'temporal.hint.delta': { es: 'Diferencia actual vs línea base. Rojo = empeoró, verde = mejoró.', en: 'Current minus baseline. Red = worsened, green = improved.', gn: "Moambue actual vs base", pt: 'Diferença atual vs linha de base. Vermelho = piorou, verde = melhorou.' },
	// Per-layer temporal hints
	'temporal.hint.deforestation_dynamics.current': { es: 'Tasa de deforestación 2015-2024. Fuente: Hansen/Landsat.', en: 'Deforestation rate 2015-2024. Source: Hansen/Landsat.', gn: "2015-2024 okany", pt: 'Taxa de desmatamento 2015-2024. Fonte: Hansen/Landsat.' },
	'temporal.hint.deforestation_dynamics.baseline': { es: 'Tasa de deforestación 2001-2010 (pre-OTBN). Fuente: Hansen/Landsat.', en: 'Deforestation rate 2001-2010 (pre-OTBN). Source: Hansen/Landsat.', gn: "2001-2010 okany", pt: 'Taxa de desmatamento 2001-2010 (pré-OTBN). Fonte: Hansen/Landsat.' },
	'temporal.hint.deforestation_dynamics.delta': { es: 'Cambio en tasa: positivo = aceleró, negativo = frenó. Fuente: Hansen/Landsat.', en: 'Rate change: positive = accelerated, negative = slowed. Source: Hansen/Landsat.', gn: "Moambue okany", pt: 'Variação na taxa: positivo = acelerou, negativo = desacelerou. Fonte: Hansen/Landsat.' },
	'temporal.hint.pm25_drivers.current': { es: 'PM2.5 periodo 2013-2022. Fuente: ACAG V6 (satelital).', en: 'PM2.5 period 2013-2022. Source: ACAG V6 (satellite).', gn: "2013-2022 PM2.5", pt: 'PM2.5 período 2013-2022. Fonte: ACAG V6 (satelital).' },
	'temporal.hint.pm25_drivers.baseline': { es: 'PM2.5 periodo 2001-2010. Fuente: ACAG V6 (satelital).', en: 'PM2.5 period 2001-2010. Source: ACAG V6 (satellite).', gn: "2001-2010 PM2.5", pt: 'PM2.5 período 2001-2010. Fonte: ACAG V6 (satelital).' },
	'temporal.hint.pm25_drivers.delta': { es: 'Cambio en PM2.5: positivo = empeoró, negativo = mejoró.', en: 'PM2.5 change: positive = worsened, negative = improved.', gn: "Moambue PM2.5", pt: 'Variação no PM2.5: positivo = piorou, negativo = melhorou.' },
	// ── Cambio demográfico censal (censo_temporal, AR-only) ──
	'analysis.censo.title': { es: '¿Cómo cambió la población entre censos?', en: 'How did population change across censuses?', gn: "Mba'éichapa okambia tavayguakuéra censo-pe?", pt: 'Como mudou a população entre censos?' },
	'analysis.censo.desc': { es: 'Densidad de población y vivienda por hexágono en los 4 censos nacionales (1991–2022). Fuente: INDEC.', en: 'Population and housing density per hexagon across the 4 national censuses (1991–2022). Source: INDEC.', gn: "Tavaygua ha óga densidad censo 1991–2022 rupive. INDEC.", pt: 'Densidade de população e moradia por hexágono nos 4 censos nacionais (1991–2022). Fonte: INDEC.' },
	'legend.censo.low': { es: 'Baja densidad', en: 'Low density', gn: 'Michĩ densidad', pt: 'Baixa densidade' },
	'legend.censo.high': { es: 'Alta densidad', en: 'High density', gn: 'Tuicha densidad', pt: 'Alta densidade' },
	'censo.pobDens': { es: 'Densidad de población', en: 'Population density', gn: 'Tavaygua densidad', pt: 'Densidade populacional' },
	'censo.vivDens': { es: 'Densidad de viviendas', en: 'Housing density', gn: 'Óga densidad', pt: 'Densidade de moradias' },
	'temporal.hint.censo_temporal.current': { es: 'Densidad de población — Censo 2022 (INDEC).', en: 'Population density — 2022 Census (INDEC).', gn: "Tavaygua densidad — 2022 censo (INDEC).", pt: 'Densidade populacional — Censo 2022 (INDEC).' },
	'temporal.hint.censo_temporal.baseline': { es: 'Densidad de población — Censo 2010 (INDEC).', en: 'Population density — 2010 Census (INDEC).', gn: "Tavaygua densidad — 2010 censo (INDEC).", pt: 'Densidade populacional — Censo 2010 (INDEC).' },
	'temporal.hint.censo_temporal.delta': { es: 'Cambio 2010→2022: verde = creció, rojo = decreció.', en: 'Change 2010→2022: green = growth, red = decline.', gn: "Moambue 2010→2022: hovyũ = okakuaa, pytã = oguejy.", pt: 'Mudança 2010→2022: verde = cresceu, vermelho = caiu.' },
	'side.censoTemporal.title': { es: 'Trayectoria censal', en: 'Census trajectory', gn: 'Censo rape', pt: 'Trajetória censal' },
	'side.censoTemporal.subtitle': { es: 'Población y vivienda 1991 → 2022 en la selección', en: 'Population and housing 1991 → 2022 in the selection', gn: "Tavaygua ha óga 1991 → 2022 jeporavópe", pt: 'População e moradia 1991 → 2022 na seleção' },
	'side.censoTemporal.population': { es: 'Población', en: 'Population', gn: 'Tavaygua', pt: 'População' },
	'side.censoTemporal.dwellings': { es: 'Viviendas', en: 'Dwellings', gn: 'Óga', pt: 'Moradias' },
	'side.censoTemporal.change': { es: 'Cambio 1991→2022', en: 'Change 1991→2022', gn: 'Moambue 1991→2022', pt: 'Mudança 1991→2022' },
	'side.censoTemporal.loading': { es: 'Cargando…', en: 'Loading…', gn: 'Oñembohasahína…', pt: 'Carregando…' },
	'side.censoTemporal.error': { es: 'No se pudo cargar la trayectoria.', en: 'Could not load trajectory.', gn: "Ndaikatúi oñemyanyhẽ trayectoria.", pt: 'Não foi possível carregar a trajetória.' },
	'side.censoTemporal.empty': { es: 'Sin datos censales en la selección.', en: 'No census data in selection.', gn: "Ndaipóri censo datos jeporavópe.", pt: 'Sem dados censais na seleção.' },
	'temporal.legend.worse': { es: 'Peor', en: 'Worse', gn: 'Vaive', pt: 'Pior' },
	'temporal.legend.noChange': { es: 'Sin cambio', en: 'No change', gn: 'Oĩháicha', pt: 'Sem variação' },
	'temporal.legend.better': { es: 'Mejor', en: 'Better', gn: 'Iporãve', pt: 'Melhor' },

	// ── Satellite composite analyses ──
	'sat.locValue.title': { es: '¿Qué tan bien ubicada está esta zona?', en: 'How well located is this area?', gn: "Iporãpa oĩhápe ko yvy? (Misiones)", pt: 'Quão bem localizada está esta zona?' },
	'sat.locValue.desc': { es: 'Valor posicional: accesibilidad, salud, actividad económica, topografía y vialidad', en: 'Location value: accessibility, healthcare, economic activity, topography and roads', gn: "Yvy hepy", pt: 'Valor posicional: acessibilidade, saúde, atividade econômica, topografia e viabilidade' },
	'sat.locValue.score': { es: 'Score de valor', en: 'Value score', gn: 'Score', pt: 'Score de valor' },
	'sat.locValue.access20k': { es: 'Acceso a ciudad 20 mil hab.', en: 'Access to 20k city', gn: "Tape táva 20k", pt: 'Acesso a cidade de 20 mil hab.' },
	'sat.locValue.healthcare': { es: 'Acceso a salud', en: 'Healthcare access', gn: 'Tasyo', pt: 'Acesso à saúde' },
	'sat.locValue.nightlights': { es: 'Actividad económica (luces)', en: 'Economic activity (lights)', gn: "Tesa'y", pt: 'Atividade econômica (luzes)' },
	'sat.locValue.slope': { es: 'Pendiente del terreno', en: 'Terrain slope', gn: "Yvy sa'i", pt: 'Declividade do terreno' },
	'sat.locValue.roadDist': { es: 'Distancia a ruta pavimentada', en: 'Paved road distance', gn: "Tape mombyry", pt: 'Distância a rodovia pavimentada' },
	'sat.locValue.type': { es: 'Tipo posicional', en: 'Location type', gn: 'Tendaguépe laja', pt: 'Tipo posicional' },
	'sat.locValue.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'sat.agri.title': { es: '¿Qué potencial agrícola tiene?', en: 'What agricultural potential?', gn: "Mba'éichapa yvy temity?", pt: 'Qual o potencial agrícola?' },
	'sat.agri.desc': { es: 'Aptitud agroclimática: carbono orgánico, pH, arcilla, lluvia, calor acumulado y pendiente', en: 'Agroclimatic aptitude: organic carbon, pH, clay, rainfall, heat units and slope', gn: "Yvy porã temity", pt: 'Aptidão agroclimática: carbono orgânico, pH, argila, chuva, calor acumulado e declividade' },
	'sat.agri.soc': { es: 'Carbono orgánico del suelo', en: 'Soil organic carbon', gn: 'Carbono', pt: 'Carbono orgânico do solo' },
	'sat.agri.ph': { es: 'pH óptimo', en: 'Optimal pH', gn: "pH porã", pt: 'pH ótimo' },
	'sat.agri.phOptimal': { es: 'pH óptimo', en: 'Optimal pH', gn: "pH porã", pt: 'pH ótimo' },
	'sat.agri.clay': { es: 'Contenido de arcilla', en: 'Clay content', gn: 'Ñaũ', pt: 'Teor de argila' },
	'sat.agri.precip': { es: 'Precipitación anual', en: 'Annual rainfall', gn: 'Ama', pt: 'Precipitação anual' },
	'sat.agri.gdd': { es: 'Grados-día acumulados', en: 'Growing degree days', gn: "Aku mbytekue", pt: 'Graus-dia acumulados' },
	'sat.agri.slope': { es: 'Pendiente del terreno', en: 'Terrain slope', gn: "Yvy sa'i", pt: 'Declividade do terreno' },
	'sat.agri.score': { es: 'Aptitud compuesta (/100)', en: 'Composite suitability (/100)', gn: 'Aptitud', pt: 'Aptidão composta (/100)' },
	'sat.agri.nitrogen': { es: 'Nitrógeno', en: 'Nitrogen', gn: 'Nitrógeno', pt: 'Nitrogênio' },
	'sat.agri.elevation': { es: 'Elevación', en: 'Elevation', gn: "Yvy yvate", pt: 'Elevação' },
	'sat.agri.cec': { es: 'Cap. intercambio catiónico', en: 'Cation exchange capacity', gn: 'CEC', pt: 'Cap. de troca catiônica' },
	'sat.agri.type': { es: 'Tipo agroclimático', en: 'Agroclimatic type', gn: 'Laja', pt: 'Tipo agroclimático' },
	'sat.agri.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'sat.soilW.title': { es: '¿Cuánta agua tiene el suelo?', en: 'How much water does the soil hold?', gn: "Mbovy y oĩ yvýpe?", pt: 'Quanta água tem o solo?' },
	'sat.soilW.desc': { es: 'Disponibilidad de agua en el suelo: humedad del suelo (ERA5-Land), precipitación media anual (CHIRPS) y evapotranspiración real (MODIS MOD16). Período 2019-2024. Cubre Misiones e Itapúa.', en: 'Soil water availability: soil moisture (ERA5-Land), mean annual precipitation (CHIRPS) and actual evapotranspiration (MODIS MOD16). Period 2019-2024. Covers Misiones and Itapúa.', gn: "Yvy y", pt: 'Disponibilidade de água no solo: umidade do solo (ERA5-Land), precipitação média anual (CHIRPS) e evapotranspiração real (MODIS MOD16). Período 2019-2024. Cobre Misiones e Itapúa.' },
	'sat.soilW.score': { es: 'Score de disponibilidad hídrica', en: 'Water availability score', gn: 'Score', pt: 'Score de disponibilidade hídrica' },
	'sat.soilW.soilMoisture': { es: 'Humedad media del suelo', en: 'Mean soil moisture', gn: "Yvy yvytu", pt: 'Umidade média do solo' },
	'sat.soilW.drySeason': { es: 'Humedad en época seca (jun-ago)', en: 'Dry season soil moisture (Jun-Aug)', gn: "Yvytu kyrépe", pt: 'Umidade na época seca (jun-ago)' },
	'sat.soilW.precipitation': { es: 'Precipitación anual (CHIRPS)', en: 'Annual precipitation (CHIRPS)', gn: "Ama ary", pt: 'Precipitação anual (CHIRPS)' },
	'sat.soilW.actualEt': { es: 'Evapotranspiración real (MODIS)', en: 'Actual evapotranspiration (MODIS)', gn: "Y okakuaa porã", pt: 'Evapotranspiração real (MODIS)' },
	'sat.soilW.type': { es: 'Tipo hídrico', en: 'Water type', gn: 'Y laja', pt: 'Tipo hídrico' },
	'sat.soilW.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'legend.soilW.low': { es: 'Suelo seco / poca capacidad', en: 'Dry soil / low capacity', gn: "Yvy ko'ẽ", pt: 'Solo seco / baixa capacidade' },
	'legend.soilW.high': { es: 'Suelo húmedo / alta capacidad', en: 'Moist soil / high capacity', gn: "Yvy yvytu porã", pt: 'Solo úmido / alta capacidade' },
	'sat.forestry.title': { es: '¿Qué zonas se parecen a las que ya tienen plantaciones forestales?', en: 'Which areas resemble those that already have forestry plantations?', gn: "Mba'e tenda ojogua umi ka'aguy oĩmava rendápe?", pt: 'Quais zonas se assemelham às que já têm plantações florestais?' },
	'sat.forestry.desc': { es: 'Modelo SDM entrenado sobre el Inventario Nacional de Plantaciones Forestales (DNDFI 2026) como presencia. Dos nichos: comercial (pino/eucalyptus) en Misiones y Corrientes; especies nativas en Chaco y Formosa. Score = probabilidad de condiciones análogas a donde ya hay plantaciones. Excluye agua, urbano y bosque nativo maduro.', en: 'SDM trained on the National Forestry Plantation Inventory (DNDFI 2026) as presence. Two niches: commercial (pine/eucalyptus) in Misiones and Corrientes; native species in Chaco and Formosa. Score = probability of conditions analogous to where plantations already exist. Excludes water, urban and mature native forest.', gn: "Ka'aguy modelo (DNDFI 2026)", pt: 'Modelo SDM treinado sobre o Inventário Nacional de Plantações Florestais (DNDFI 2026) como presença. Dois nichos: comercial (pinho/eucalipto) em Misiones e Corrientes; espécies nativas no Chaco e Formosa. Score = probabilidade de condições análogas às onde já há plantações. Exclui água, urbano e floresta nativa madura.' },
	'sat.forestry.ph': { es: 'pH (ácido=mejor)', en: 'pH (acidic=better)', gn: 'pH', pt: 'pH (ácido=melhor)' },
	'sat.forestry.clay': { es: 'Arcilla (%)', en: 'Clay (%)', gn: 'Ñaũ', pt: 'Argila (%)' },
	'sat.forestry.precip': { es: 'Precipitación anual (mm)', en: 'Annual rainfall (mm)', gn: 'Ama', pt: 'Precipitação anual (mm)' },
	'sat.forestry.waterDeficit': { es: 'Déficit hídrico', en: 'Water deficit', gn: "Y ho'o", pt: 'Déficit hídrico' },
	'sat.forestry.frostDays': { es: 'Días de helada', en: 'Frost days', gn: "Ára ro'y", pt: 'Dias de geada' },
	'sat.forestry.slope': { es: 'Pendiente (°)', en: 'Slope (°)', gn: "Yvy sa'i", pt: 'Declividade (°)' },
	'sat.forestry.roadDist': { es: 'Distancia a ruta', en: 'Road distance', gn: "Tape mombyry", pt: 'Distância a rodovia' },
	'sat.forestry.access50k': { es: 'Acceso a ciudad 50 mil hab.', en: 'Access to 50k city', gn: "Tape táva 50k", pt: 'Acesso a cidade de 50 mil hab.' },
	'sat.forestry.soc': { es: 'Carbono orgánico', en: 'Organic carbon', gn: 'Carbono', pt: 'Carbono orgânico' },
	'sat.forestry.score': { es: 'Puntuación SDM (/100)', en: 'SDM score (/100)', gn: 'SDM', pt: 'Pontuação SDM (/100)' },
	'sat.forestry.elevation': { es: 'Elevación', en: 'Elevation', gn: "Yvy yvate", pt: 'Elevação' },
	'sat.forestry.gdd': { es: 'Grados-día', en: 'Growing degree days', gn: "Aku ára", pt: 'Graus-dia' },
	'sat.forestry.type': { es: 'Tipo de aptitud', en: 'Aptitude type', gn: 'Laja', pt: 'Tipo de aptidão' },
	'sat.forestry.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'forestry.overlay.toggle': { es: 'Plantaciones existentes (DNDFI 2026)', en: 'Existing plantations (DNDFI 2026)', gn: "Ka'aguy oĩmava (DNDFI 2026)", pt: 'Plantações existentes (DNDFI 2026)' },
	'forestry.overlay.note': { es: 'Inventario Nacional de Plantaciones Forestales — la misma base que define el score. En Chaco y Formosa son de especies nativas, un nicho distinto del comercial.', en: 'National Forestry Plantation Inventory — the same basis that defines the score. In Chaco and Formosa these are native species, a niche distinct from the commercial one.', gn: 'Inventario Nacional de Plantaciones Forestales. Chaco ha Formosa-pe nativas.', pt: 'Inventário Nacional de Plantações Florestais — a mesma base que define o score. No Chaco e Formosa são espécies nativas, um nicho distinto do comercial.' },
	'sat.deprivation.title': { es: '¿Dónde hay mayor carencia de servicios?', en: 'Where is service deprivation greatest?', gn: "Moõpa oĩve mba'e'ỹ?", pt: 'Onde há maior carência de serviços?' },
	'sat.deprivation.desc': { es: 'Carencia habitacional y de servicios (Censo 2022): NBI, sin cloacas, piso inadecuado, hacinamiento, hacinamiento crítico y sin computadora', en: 'Housing and service deprivation (Census 2022): UBN, no sewerage, inadequate floor, overcrowding, critical overcrowding and no computer', gn: "Mba'e'ỹ óga ha servicio (Censo 2022): NBI, ykuaa'ỹ, piso vai, ñembyaty ha computadora'ỹ", pt: 'Carência habitacional e de serviços (Censo 2022): NBI, sem esgoto, piso inadequado, superlotação, superlotação crítica e sem computador' },
	'sat.deprivation.nbi': { es: 'Hogares con NBI (% de hogares)', en: 'Households with UBN (% of households)', gn: 'Hogares con NBI (% de hogares)', pt: 'Lares com NBI (% dos lares)' },
	'sat.deprivation.sinCloacas': { es: 'Sin cloacas (% de hogares)', en: 'No sewerage (% of households)', gn: 'Sin cloacas (% de hogares)', pt: 'Sem esgoto (% dos lares)' },
	'sat.deprivation.piso': { es: 'Sin piso revestido (% de hogares)', en: 'Unfinished floor (% of households)', gn: 'Sin piso revestido (% de hogares)', pt: 'Sem piso revestido (% dos lares)' },
	'sat.deprivation.hacinamiento': { es: 'Hacinamiento (% de hogares)', en: 'Overcrowding (% of households)', gn: 'Hacinamiento (% de hogares)', pt: 'Superlotação (% dos lares)' },
	'sat.deprivation.hacinamientoCrit': { es: 'Hacinamiento crítico (% de hogares)', en: 'Critical overcrowding (% of households)', gn: 'Hacinamiento crítico (% de hogares)', pt: 'Superlotação crítica (% dos lares)' },
	'sat.deprivation.sinComputadora': { es: 'Sin computadora (% de hogares)', en: 'No computer (% of households)', gn: 'Sin computadora (% de hogares)', pt: 'Sem computador (% dos lares)' },
	'sat.isolation.title': { es: '¿Dónde hay mayor aislamiento geoespacial?', en: 'Where is geospatial isolation greatest?', gn: "Moõpa oĩve mombyry?", pt: 'Onde há maior isolamento geoespacial?' },
	'sat.isolation.desc': { es: 'Aislamiento geográfico: acceso a ciudades, salud, distancia a rutas, densidad vial, luces y población', en: 'Geographic isolation: access to cities, health, road distance, road density, lights and population', gn: "Mombyry", pt: 'Isolamento geográfico: acesso a cidades, saúde, distância a rodovias, densidade viária, luzes e população' },
	'sat.isolation.accessCities': { es: 'Acceso a ciudades', en: 'City access (min)', gn: "Táva gotyo", pt: 'Acesso a cidades' },
	'sat.isolation.accessHealth': { es: 'Acceso a salud', en: 'Health access (min)', gn: "Tesãi gotyo", pt: 'Acesso à saúde' },
	'sat.isolation.roadDist': { es: 'Distancia a ruta', en: 'Road distance', gn: "Tape gotyo", pt: 'Distância a rodovia' },
	'sat.isolation.roadDensity': { es: 'Densidad vial', en: 'Road density', gn: "Tape heta", pt: 'Densidade viária' },
	'sat.isolation.nightlights': { es: 'Actividad nocturna', en: 'Night-time activity', gn: "Tesa'y", pt: 'Atividade noturna' },
	'sat.isolation.popDensity': { es: 'Densidad poblacional', en: 'Pop. density', gn: "Yvypóra heta", pt: 'Densidade populacional' },
	'sat.isolation.type': { es: 'Tipo de aislamiento', en: 'Isolation type', gn: "Mombyry laja", pt: 'Tipo de isolamento' },
	'sat.isolation.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'sat.health.title': { es: '¿Dónde faltan servicios de salud?', en: 'Where are health services lacking?', gn: "Moõpa oĩ'ỹ tasyo?", pt: 'Onde faltam serviços de saúde?' },
	'sat.health.desc': { es: 'Brecha de acceso a salud: tiempo al centro de salud, demanda poblacional, cobertura y vulnerabilidad social', en: 'Health access gap: travel time to healthcare, population demand, coverage and social vulnerability', gn: "Tasyo rape", pt: 'Brecha de acesso à saúde: tempo ao centro de saúde, demanda populacional, cobertura e vulnerabilidade social' },
	'sat.health.score': { es: 'Score déficit salud', en: 'Health gap score', gn: 'Score', pt: 'Score déficit saúde' },
	'sat.health.time': { es: 'Tiempo al centro de salud más cercano', en: 'Time to the nearest healthcare facility', gn: 'Tiempo al centro de salud más cercano', pt: 'Tempo ao centro de saúde mais próximo' },
	'sat.health.coverage': { es: 'Con obra social o prepaga (% de personas)', en: 'With health insurance (% of people)', gn: 'Con obra social o prepaga (% de personas)', pt: 'Com plano de saúde (% das pessoas)' },
	'sat.health.nbi': { es: 'Hogares con NBI (% de hogares)', en: 'Households with UBN (% of households)', gn: 'Hogares con NBI (% de hogares)', pt: 'Lares com NBI (% dos lares)' },
	'sat.health.elderly': { es: '65 años y más (% de personas)', en: 'Aged 65+ (% of people)', gn: '65 años y más (% de personas)', pt: '65 anos ou mais (% das pessoas)' },
	'sat.health.children': { es: 'Menores de 18 años (% de personas)', en: 'Under 18 (% of people)', gn: 'Menores de 18 años (% de personas)', pt: 'Menores de 18 anos (% das pessoas)' },
	'sat.health.popDensity': { es: 'Habitantes por km² (estimado en el hexágono)', en: 'Inhabitants per km² (estimated in the hexagon)', gn: 'Habitantes por km² (estimado en el hexágono)', pt: 'Habitantes por km² (estimado no hexágono)' },
	'sat.health.roadDensity': { es: 'Densidad vial', en: 'Road density', gn: "Tape heta", pt: 'Densidade viária' },
	'sat.health.nightlights': { es: 'Luces nocturnas', en: 'Night lights', gn: "Tesa'y", pt: 'Luzes noturnas' },
	'sat.health.accessCities': { es: 'Acceso a ciudades', en: 'City access', gn: "Táva gotyo", pt: 'Acesso a cidades' },
	'sat.health.distPrimary': { es: 'Dist. a escuela primaria', en: 'Primary school distance', gn: "Mbo'ehao gotyo", pt: 'Dist. a escola primária' },
	'sat.health.type': { es: 'Tipo de acceso', en: 'Access type', gn: 'Tasyo laja', pt: 'Tipo de acesso' },
	'sat.health.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'sat.eduCap.title': { es: '¿Dónde hay menor capital educativo?', en: 'Where is education capital lowest?', gn: "Moõpa oĩ'ỹve mbo'ehao?", pt: 'Onde há menor capital educacional?' },
	'sat.eduCap.desc': { es: 'Capital educativo: nivel de instrucción acumulado de la población (sin instrucción, primaria, secundario, terciario, universitario)', en: 'Education capital: accumulated schooling level (no schooling, primary, secondary, tertiary, university)', gn: "Mbo'ehao capital", pt: 'Capital educacional: nível de instrução acumulado da população (sem instrução, ensino fundamental, médio, técnico, universitário)' },
	'sat.eduCap.noSchooling': { es: 'Sin instrucción o primario incompleto (% de 18 años y más)', en: 'No schooling or incomplete primary (% of people aged 18+)', gn: 'Sin instrucción o primario incompleto (% de 18 años y más)', pt: 'Sem instrução ou fundamental incompleto (% de 18 anos ou mais)' },
	'sat.eduCap.secondaryPlus': { es: 'Secundario completo o más (% de 18 años y más)', en: 'Completed secondary or higher (% of people aged 18+)', gn: 'Secundario completo o más (% de 18 años y más)', pt: 'Ensino médio completo ou mais (% de 18 anos ou mais)' },
	'sat.eduCap.higherEdu': { es: 'Terciario o universitario completo (% de 18 años y más)', en: 'Completed tertiary or university (% of people aged 18+)', gn: 'Terciario o universitario completo (% de 18 años y más)', pt: 'Superior completo (% de 18 anos ou mais)' },
	'sat.eduCap.university': { es: 'Universitario completo (% de 18 años y más)', en: 'Completed university (% of people aged 18+)', gn: 'Universitario completo (% de 18 años y más)', pt: 'Universitário completo (% de 18 anos ou mais)' },
	'sat.eduCap.type': { es: 'Tipo', en: 'Type', gn: 'Laja', pt: 'Tipo' },
	'sat.eduCap.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'sat.eduFlow.title': { es: '¿Dónde hay más problemas educativos?', en: 'Where are education problems greatest?', gn: "Moõpa oĩve mbo'ehao apañuãi?", pt: 'Onde há mais problemas educacionais?' },
	'sat.eduFlow.desc': { es: 'Flujo educativo: inasistencia primaria y secundaria, embarazo adolescente', en: 'Education flow: primary and secondary dropout, teen pregnancy', gn: "Mbo'ehao flujo", pt: 'Fluxo educacional: abandono escolar no ensino fundamental e médio, gravidez na adolescência' },
	'sat.eduFlow.dropoutPrimary': { es: 'No asisten a la escuela (% de 6 a 12 años)', en: 'Not attending school (% of children aged 6–12)', gn: 'No asisten a la escuela (% de 6 a 12 años)', pt: 'Não frequentam a escola (% de 6 a 12 anos)' },
	'sat.eduFlow.dropoutSecondary': { es: 'No asisten a la escuela (% de 13 a 18 años)', en: 'Not attending school (% of people aged 13–18)', gn: 'No asisten a la escuela (% de 13 a 18 años)', pt: 'Não frequentam a escola (% de 13 a 18 anos)' },
	'sat.eduFlow.nbi': { es: 'NBI', en: 'Unmet basic needs', gn: 'NBI', pt: 'NBI' },
	'sat.eduFlow.youthPct': { es: '% jóvenes 18-29', en: '% youth 18-29', gn: "Mitã 18-29", pt: '% jovens 18-29' },
	'sat.eduFlow.femaleHeaded': { es: 'Jefatura femenina', en: 'Female-headed', gn: "Kuña ñangareko", pt: 'Chefia feminina' },
	'sat.eduFlow.type': { es: 'Tipo', en: 'Type', gn: 'Laja', pt: 'Tipo' },
	'sat.eduFlow.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	// ── Productive Activity ──
	'sat.prodAct.title': { es: '¿Dónde se concentra la actividad económica?', en: 'Where is economic activity concentrated?', gn: "Moope tembiapo economico oime? (Misiones)", pt: 'Onde se concentra a atividade econômica?' },
	'sat.prodAct.desc': { es: '6 indicadores satelitales de actividad económica en valores reales: luces nocturnas (VIIRS), productividad vegetal (NPP), verdor (NDVI), superficie construida (GHSL), conversión forestal (Hansen) y temperatura superficial (LST). Cada hexágono muestra el valor real del pixel satelital. Comparación temporal con línea base.', en: '6 satellite economic activity indicators in real values: nightlights (VIIRS), vegetation productivity (NPP), greenness (NDVI), built surface (GHSL), forest conversion (Hansen) and surface temperature (LST). Each hexagon shows the real satellite pixel value. Temporal comparison with baseline.', gn: "6 tembiapo indicador", pt: '6 indicadores satelitais de atividade econômica em valores reais: luzes noturnas (VIIRS), produtividade vegetal (NPP), verdor (NDVI), superfície construída (GHSL), conversão florestal (Hansen) e temperatura superficial (LST). Cada hexágono mostra o valor real do pixel satelital. Comparação temporal com linha de base.' },
	'sat.prodAct.type': { es: 'Nivel', en: 'Level', gn: 'Nivel', pt: 'Nível' },
	'sat.prodAct.typeLabel': { es: 'Nivel de actividad', en: 'Activity level', gn: 'Nivel', pt: 'Nível de atividade' },
	'sat.prodAct.viirs': { es: 'Luces nocturnas (nW/cm2/sr)', en: 'Nightlights (nW/cm2/sr)', gn: 'Tesa pyharekue', pt: 'Luzes noturnas (nW/cm²/sr)' },
	'sat.prodAct.npp': { es: 'Productividad vegetal (gC/m2/ano)', en: 'Vegetation productivity (gC/m2/yr)', gn: 'NPP', pt: 'Produtividade vegetal (gC/m²/ano)' },
	'sat.prodAct.ndvi': { es: 'Verdor (NDVI)', en: 'Greenness (NDVI)', gn: 'NDVI', pt: 'Verdor (NDVI)' },
	'sat.prodAct.built': { es: 'Superficie construida', en: 'Built surface', gn: "Mba'e apopyra", pt: 'Superfície construída' },
	'sat.prodAct.loss': { es: 'Pérdida forestal', en: 'Forest loss', gn: "Ka'aguy okañy", pt: 'Perda florestal' },
	'sat.prodAct.lst': { es: 'Temperatura superficial (C)', en: 'Surface temperature (C)', gn: 'Aku', pt: 'Temperatura superficial (°C)' },
	'legend.prodAct.low': { es: 'Baja actividad', en: 'Low activity', gn: "Michimi", pt: 'Baixa atividade' },
	'legend.prodAct.high': { es: 'Alta actividad', en: 'High activity', gn: "Tuicha", pt: 'Alta atividade' },
	// ── Deforestation Dynamics ──
	'sat.deforest.title': { es: '¿Cómo avanza la deforestación?', en: 'How is deforestation progressing?', gn: "Mba'eichapa ka'aguy okany?", pt: 'Como avança o desmatamento?' },
	'sat.deforest.desc': { es: 'Dinámica de deforestación observada (Hansen/Landsat 2001-2024). Tasa de pérdida forestal anual, acumulada, y cambio respecto a la línea base (2001-2010).', en: 'Observed deforestation dynamics (Hansen/Landsat 2001-2024). Annual forest loss rate, cumulative, and change from baseline (2001-2010).', gn: "Ka'aguy okany 2001-2024", pt: 'Dinâmica de desmatamento observada (Hansen/Landsat 2001-2024). Taxa de perda florestal anual, acumulada e variação em relação à linha de base (2001-2010).' },
	'sat.deforest.type': { es: 'Presión', en: 'Pressure', gn: 'Presión', pt: 'Pressão' },
	'sat.deforest.typeLabel': { es: 'Nivel de presión', en: 'Pressure level', gn: 'Presión', pt: 'Nível de pressão' },
	'sat.deforest.lossRate': { es: 'Tasa de pérdida (%/año)', en: 'Loss rate (%/yr)', gn: "Okany (%/ary)", pt: 'Taxa de perda (%/ano)' },
	'sat.deforest.cumulative': { es: 'Pérdida acumulada (%)', en: 'Cumulative loss (%)', gn: "Opavave okany (%)", pt: 'Perda acumulada (%)' },
	'legend.deforest.none': { es: 'Sin pérdida detectada (0)', en: 'No loss detected (0)', gn: "Ndaipóri pérdida ojehecháva (0)", pt: 'Sem perda detectada (0)' },
	'legend.deforest.low': { es: 'Baja (≤0,5)', en: 'Low (≤0.5)', gn: "Michĩ (≤0,5)", pt: 'Baixa (≤0,5)' },
	'legend.deforest.mid': { es: 'Media (0,5–2)', en: 'Medium (0.5–2)', gn: "Mbyte (0,5–2)", pt: 'Média (0,5–2)' },
	'legend.deforest.high': { es: 'Alta (>2)', en: 'High (>2)', gn: "Guasu (>2)", pt: 'Alta (>2)' },
	'legend.landUse.low': { es: 'Poca cobertura nativa', en: 'Low native cover', gn: "Michĩ ka'aguy", pt: 'Pouca cobertura nativa' },
	'legend.landUse.high': { es: 'Alta cobertura nativa', en: 'High native cover', gn: "Tuicha ka'aguy", pt: 'Alta cobertura nativa' },
	// ── PM2.5 (concentracion ACAG + bandas OMS) ──
	'sat.pm25.title': { es: '¿Qué nivel de exposición a PM2.5 hay?', en: 'What level of PM2.5 exposure is there?', gn: "Mba'eichagua PM2.5 oĩ?", pt: 'Qual nível de exposição ao PM2.5 existe?' },
	'sat.pm25.desc': { es: 'Concentración media anual de PM2.5 (ACAG V6, ~1 km, 1998-2022) clasificada según la guía OMS 2021, con comparación 2001-2010 vs 2013-2022.', en: 'Annual mean PM2.5 concentration (ACAG V6, ~1 km, 1998-2022) classified against the WHO 2021 guideline, comparing 2001-2010 with 2013-2022.', gn: 'PM2.5 ary pukukue (ACAG V6) ha OMS 2021', pt: 'Concentração média anual de PM2.5 (ACAG V6, ~1 km, 1998-2022) classificada segundo a diretriz OMS 2021, com comparação 2001-2010 vs 2013-2022.' },
	'sat.pm25.type': { es: 'Tipo', en: 'Type', gn: 'Laja', pt: 'Tipo' },
	'sat.pm25.typeLabel': { es: 'Clasificación OMS', en: 'WHO classification', gn: 'OMS', pt: 'Classificação OMS' },
	'sat.pm25.pm25Mean': { es: 'PM2.5 medio (µg/m³)', en: 'Mean PM2.5 (µg/m³)', gn: 'PM2.5', pt: 'PM2.5 médio (µg/m³)' },
	'legend.scale.provincial': { es: 'Provincial', en: 'Provincial', gn: 'Tetãvore', pt: 'Provincial' },
	'legend.scale.regional': { es: 'Regional', en: 'Regional', gn: 'Región', pt: 'Regional' },
	'legend.scale.provincialNote': { es: 'Escala de {t} (P2–P98): colores relativos a la provincia', en: '{t} scale (P2–P98): colours relative to the province', gn: '{t} escala (P2–P98): sa\'y tetãvore rehegua', pt: 'Escala de {t} (P2–P98): cores relativas à província' },
	'legend.scale.minmaxNote': { es: 'Escala de {t} (mín–máx): colores relativos a la provincia', en: '{t} scale (min–max): colours relative to the province', gn: '{t} escala (mín–máx)', pt: 'Escala de {t} (mín–máx): cores relativas à província' },
	'legend.scale.regionalNote': { es: 'Escala regional fija: el mismo color es el mismo valor en Argentina, Paraguay y Brasil', en: 'Fixed regional scale: same colour = same value in Argentina, Paraguay and Brazil', gn: 'Escala regional fija: el mismo color es el mismo valor en Argentina, Paraguay y Brasil', pt: 'Escala regional fixa: a mesma cor é o mesmo valor na Argentina, no Paraguai e no Brasil' },

	'sat.landUse.title': { es: '¿Qué uso de suelo tiene esta zona?', en: 'What land use does this area have?', gn: "Mba'éichapa ojepuru ko yvy?", pt: 'Qual o uso do solo nesta zona?' },
	'sat.landUse.desc': { es: 'Composición de cobertura del suelo por hexágono (color = fracción arbórea): árboles, cultivos, pasturas, arbustos, construido, agua (Dynamic World 10m, Google/WRI — comparable entre Argentina, Paraguay y Brasil)', en: 'Land-cover composition per hexagon (colour = tree fraction): trees, crops, grass, shrub, built, water (Dynamic World 10m, Google/WRI — comparable across Argentina, Paraguay and Brazil)', gn: 'Composición de cobertura del suelo por hexágono (color = fracción arbórea): árboles, cultivos, pasturas, arbustos, construido, agua (Dynamic World 10m, Google/WRI — comparable entre Argentina, Paraguay y Brasil)', pt: 'Composição da cobertura do solo por hexágono (cor = fração arbórea): árvores, cultivos, pastagem, arbustos, construído, água (Dynamic World 10m, Google/WRI — comparável entre Argentina, Paraguai e Brasil)' },
	'sat.landUse.score': { es: 'Tipo de cobertura', en: 'Cover type', gn: 'Score', pt: 'Tipo de cobertura' },
	'sat.landUse.type': { es: 'Tipo', en: 'Type', gn: 'Laja', pt: 'Tipo' },
	'sat.landUse.typeLabel': { es: 'Tipo de cobertura', en: 'Cover type', gn: 'Laja', pt: 'Tipo de cobertura' },
	'sat.landUse.nativeForest': { es: 'Selva nativa (%)', en: 'Native forest (%)', gn: "Ka'aguy", pt: 'Floresta nativa (%)' },
	'sat.landUse.plantation': { es: 'Plantacion forestal (%)', en: 'Forest plantation (%)', gn: "Ka'aguy oñemity", pt: 'Plantação florestal (%)' },
	'sat.landUse.agriculture': { es: 'Agricultura (%)', en: 'Agriculture (%)', gn: 'Temity', pt: 'Agricultura (%)' },
	'sat.landUse.pasture': { es: 'Pastizal (%)', en: 'Pasture (%)', gn: 'Pasto', pt: 'Pastagem (%)' },
	'sat.landUse.grassland': { es: 'Pastizal natural (%)', en: 'Natural grassland (%)', gn: 'Pasto', pt: 'Pastagem natural (%)' },
	'sat.landUse.wetland': { es: 'Humedal (%)', en: 'Wetland (%)', gn: "Yvy y", pt: 'Área úmida (%)' },
	'sat.landUse.urban': { es: 'Urbano (%)', en: 'Urban (%)', gn: 'Tava', pt: 'Urbano (%)' },
	'sat.landUse.water': { es: 'Agua', en: 'Water', gn: 'Y', pt: 'Água' },
	'sat.landUse.mosaic': { es: 'Mosaico agropecuario (%)', en: 'Agricultural mosaic (%)', gn: 'Mosaiko', pt: 'Mosaico agropecuário (%)' },
	'sat.landUse.bare': { es: 'Suelo desnudo', en: 'Bare soil', gn: "Yvy ñu'ã", pt: 'Solo exposto' },
	'sat.landUse.trees': { es: 'Selva', en: 'Trees', gn: "Ka'aguy", pt: 'Floresta' },
	'sat.landUse.crops': { es: 'Cultivos', en: 'Crops', gn: 'Temity', pt: 'Cultivos' },
	'sat.landUse.built': { es: 'Construido', en: 'Built', gn: 'Táva', pt: 'Construído' },
	'sat.landUse.grass': { es: 'Pasturas', en: 'Grass', gn: 'Pasto', pt: 'Pastagens' },
	'sat.landUse.shrub': { es: 'Arbustos', en: 'Shrubs', gn: "Ka'avo", pt: 'Arbustos' },
	'sat.landUse.flooded': { es: 'Veg. inundada', en: 'Flooded veg.', gn: 'Ñana y', pt: 'Veg. inundada' },
	'sat.landUse.snow': { es: 'Nieve/hielo', en: 'Snow/ice', gn: 'Roy', pt: 'Neve/gelo' },

	// ── Radio-based analyses ──
	'analysis.investment.title': { es: '¿Cómo es el mercado inmobiliario en esta zona?', en: 'What is the real estate market like in this area?', gn: "Mba'éichapa yvy ñemuhague ko'ápe?", pt: 'Como é o mercado imobiliário nesta zona?' },
	'analysis.investment.desc': { es: 'Precios, oferta, oportunidad y atractivo del mercado inmobiliario por parcela', en: 'Prices, supply, opportunity and real estate market attractiveness per parcel', gn: "Hepy, teko porã ha ñemuhague yvy rupi", pt: 'Preços, oferta, oportunidade e atratividade do mercado imobiliário por parcela' },
	'analysis.risks.title': { es: '¿Dónde se concentran los riesgos naturales?', en: 'Where are natural hazards concentrated?', gn: "Moõpa oñembyaty mba'asy naturaléva?", pt: 'Onde se concentram os riscos naturais?' },
	'analysis.risks.desc': { es: 'Inundación, deslizamiento, erosión, pendiente y deforestación — riesgo integral por parcela', en: 'Flooding, landslide, erosion, slope and deforestation — integrated risk per parcel', gn: "Ysoguy, yvy ho'a, yvy ñembyai — mba'asy yvy rupi", pt: 'Inundação, deslizamento, erosão, declividade e desmatamento — risco integral por parcela' },
	'analysis.aptitude.title': { es: '¿Qué condiciones productivas tiene el suelo?', en: 'What productive conditions does the soil have?', gn: "Mba'éichapa yvy temitỹ rehegua?", pt: 'Que condições produtivas tem o solo?' },
	'analysis.aptitude.desc': { es: 'Suelo, lluvia, pendiente y aptitud agrícola — potencial productivo por parcela', en: 'Soil, rainfall, slope and agricultural aptitude — productive potential per parcel', gn: "Yvy, ama, yvy ho'a ha temitỹ teko porã", pt: 'Solo, chuva, declividade e aptidão agrícola — potencial produtivo por parcela' },
	'analysis.accessibility.title': { es: '¿Qué zonas están más aisladas?', en: 'Which areas are most isolated?', gn: "Mba'e yvýpa oĩ mombyryve?", pt: 'Quais zonas estão mais isoladas?' },
	'analysis.accessibility.desc': { es: 'Tiempo a capital, hospital, escuela y ruta — accesibilidad a servicios por hexágono', en: 'Time to capital, hospital, school and road — service accessibility per hexagon', gn: "Aravo taviguasu peve, hospital, mbo'ehao ha tape", pt: 'Tempo até capital, hospital, escola e rodovia — acessibilidade a serviços por hexágono' },
	'analysis.change.title': { es: '¿Qué zonas se están transformando más rápido?', en: 'Which areas are transforming fastest?', gn: "Mba'e yvýpa oñemoambue pya'evéva?", pt: 'Quais zonas estão se transformando mais rápido?' },
	'analysis.change.desc': { es: 'Parcelas nuevas, deforestación, densidad edilicia — dinámica de cambio', en: 'New parcels, deforestation, building density — change dynamics', gn: "Yvy pyahu, ka'aguy jepe'a, óga papapy", pt: 'Parcelas novas, desmatamento, densidade edilícia — dinâmica de mudança' },
	'analysis.socio.title': { es: '¿Dónde son mayores las carencias sociales?', en: 'Where are social deprivations greatest?', gn: "Moõpa oĩ mba'e'ỹ tuichavéva?", pt: 'Onde são maiores as carências sociais?' },
	'analysis.socio.desc': { es: 'Densidad, pobreza, hacinamiento, propiedad y conectividad — perfil censal por hexágono', en: 'Density, poverty, overcrowding, homeownership and connectivity — census profile per hexagon', gn: "Yvypóra papapy, mba'e'ỹ, oiko hatã ha ñanduti", pt: 'Densidade, pobreza, superlotação, propriedade e conectividade — perfil censitário por hexágono' },
	'analysis.forest.title': { es: '¿Dónde está la mayor riqueza forestal?', en: 'Where is forest cover richest?', gn: "Moõpa oĩ ka'aguy tuichavéva?", pt: 'Onde está a maior riqueza florestal?' },
	'analysis.forest.desc': { es: 'Cobertura arbórea, altura de dosel, NDVI, bosque nativo — perfil vegetal por parcela', en: 'Tree cover, canopy height, NDVI, native forest — vegetation profile per parcel', gn: "Ka'aguy, yvyra yvatekue, NDVI", pt: 'Cobertura arbórea, altura do dossel, NDVI, floresta nativa — perfil vegetal por parcela' },
	'analysis.economic.title': { es: '¿Dónde se concentra la actividad económica?', en: 'Where is economic activity concentrated?', gn: "Moõpa oñembyaty mba'apo?", pt: 'Onde se concentra a atividade econômica?' },
	'analysis.economic.desc': { es: 'Empleo, actividad, formación universitaria, luces nocturnas — dinamismo económico por hexágono', en: 'Employment, activity, university education, night lights — economic dynamism per hexagon', gn: "Mba'apo, teko, mbo'ehao guasu, pytũ rendy", pt: 'Emprego, atividade, formação universitária, luzes noturnas — dinamismo econômico por hexágono' },

	// ── Territorial Scores ──
	'analysis.scores.title': { es: '¿Dónde hay más infraestructura y servicios?', en: 'Where is there more infrastructure and services?', gn: "Moõpa oĩve mba'apo ha servicio?", pt: 'Onde há mais infraestrutura e serviços?' },
	'analysis.scores.desc': { es: 'Pavimentación, consolidación, servicios, comercio, conectividad, urbanización y más — 8 indicadores por hexágono', en: 'Paving, consolidation, services, commerce, connectivity, urbanisation and more — 8 indicators per hexagon', gn: "8 rechaukaha yvy rupi: tape, óga, servicio, tape joapy, mba'apo", pt: 'Pavimentação, consolidação, serviços, comércio, conectividade, urbanização e mais — 8 indicadores por hexágono' },
	'analysis.scores.selectDept': { es: 'Seleccioná un departamento', en: 'Select a department', gn: 'Eiporavo departamento', pt: 'Selecione um departamento' },
	'analysis.scores.clickHint': { es: 'Hacé click en el mapa para ver el perfil de la parcela', en: 'Click on the map to see the parcel profile', gn: 'Ehesakutu mapa-pe ehecha hag̃ua yvy rechaukaha', pt: 'Clique no mapa para ver o perfil da parcela' },
	'analysis.scores.selectIndicator': { es: 'Indicador del mapa', en: 'Map indicator', gn: "Mapa rechaukaha", pt: 'Indicador do mapa' },
	'analysis.scores.overall': { es: 'Promedio general', en: 'Overall average', gn: 'Mbytekue', pt: 'Média geral' },
	'analysis.scores.hexCount': { es: 'Zonas analizadas', en: 'Areas analysed', gn: 'Yvy ojehechauka', pt: 'Zonas analisadas' },

	'analysis.catastro.title': { es: '¿Cómo está parcelada la tierra?', en: 'How is the land divided into parcels?', gn: '¿Cómo está parcelada la tierra?', pt: 'Como está parcelada a terra?' },
	'analysis.catastro.desc': { es: 'Estructura catastral: densidad de parcelas urbanas y rurales, áreas medias y cambios registrados en el WFS de Catastro Misiones', en: 'Cadastral structure: urban and rural parcel density, average areas and changes registered in the Catastro Misiones WFS', gn: "Yvy ñemohenda táva ha ka'aguy rupi", pt: 'Estrutura cadastral: densidade de parcelas urbanas e rurais, áreas médias e mudanças registradas no WFS do Catastro Misiones' },
	'analysis.catastro.legend': { es: 'Total parcelas por radio', en: 'Total parcels per tract', gn: "Yvy opaite rupi", pt: 'Total de parcelas por setor' },
	'analysis.catastro.totalUrban': { es: 'Parcelas urbanas', en: 'Urban parcels', gn: "Yvy táva", pt: 'Parcelas urbanas' },
	'analysis.catastro.totalRural': { es: 'Parcelas rurales', en: 'Rural parcels', gn: "Yvy ka'aguy", pt: 'Parcelas rurais' },
	'analysis.catastro.avgAreaUrban': { es: 'Área media urbana', en: 'Avg urban area', gn: "Yvy mbytekue táva", pt: 'Área média urbana' },
	'analysis.catastro.avgAreaRural': { es: 'Área media rural', en: 'Avg rural area', gn: "Yvy mbytekue ka'aguy", pt: 'Área média rural' },
	'analysis.catastro.newParcels': { es: 'Nuevas (90 días)', en: 'New (90 days)', gn: "Ipyahu (90 ára)", pt: 'Novas (90 dias)' },
	'analysis.catastro.removedParcels': { es: 'Removidas', en: 'Removed', gn: "Oñembogueva", pt: 'Removidas' },
	'analysis.catastro.new90d': { es: 'nuevas 90d', en: 'new 90d', gn: "ipyahu 90 ára", pt: 'novas 90d' },
	'analysis.catastro.new7d': { es: 'Nuevas (7 días)', en: 'New (7 days)', gn: "Ipyahu (7 ára)", pt: 'Novas (7 dias)' },
	'analysis.catastro.totalParcels': { es: 'Total parcelas', en: 'Total parcels', gn: "Yvy opaite", pt: 'Total de parcelas' },
	'analysis.catastro.changeHistory': { es: 'Historial de cambios', en: 'Change history', gn: "Mba'e ojejapo vaekue", pt: 'Histórico de mudanças' },
	'analysis.catastro.backToDepts': { es: '\u2190 Departamentos', en: '\u2190 Departments', gn: "\u2190 Departamento", pt: '← Departamentos' },
	'analysis.catastro.topDepts': { es: 'Seleccioná un departamento', en: 'Select a department', gn: "Eiporavo departamento", pt: 'Selecione um departamento' },
	'analysis.catastro.howToReadTitle': { es: 'Cómo leer este mapa', en: 'How to read this map', gn: "Mba'eichapa ejapokuaa ko mapa", pt: 'Como ler este mapa' },
	'analysis.catastro.howToReadBody': {
		es: 'Las parcelas catastrales se muestran coloreadas según su tipo: cyan = urbana, verde = rural. Al seleccionar un departamento, el mapa muestra todas las parcelas registradas en la Dirección General de Catastro de Misiones. Hacé click en un radio censal para ver estadísticas detalladas.',
		en: 'Cadastral parcels are coloured by type: cyan = urban, green = rural. When you select a department, the map shows all parcels registered with the Misiones cadastre office. Click a census tract for detailed statistics.',
		gn: "Yvy oñembosa'y tipo rupi: cyan = táva, verde = ka'aguy. Eiporavo departamento, mapa ohechauka yvy opaite. Ehesakutu radio ehecha hag̃ua estadística.",
		pt: 'As parcelas cadastrais são mostradas coloridas por tipo: ciano = urbana, verde = rural. Ao selecionar um departamento, o mapa mostra todas as parcelas registradas na Dirección General de Catastro de Misiones. Clique em um setor censitário para ver estatísticas detalhadas.',
	},
	'analysis.catastro.implicationsTitle': { es: 'Implicancias', en: 'Implications', gn: "Mba'e he'ise", pt: 'Implicações' },
	'analysis.catastro.implicationsBody': {
		es: 'La cantidad de parcelas nuevas en los últimos 90 días indica la presión inmobiliaria sobre cada zona. Un crecimiento alto de parcelas urbanas puede señalar expansión urbana, demanda de servicios y necesidad de planificación.',
		en: 'The number of new parcels in the last 90 days indicates real estate pressure on each area. High growth of urban parcels may signal urban expansion, service demand and the need for planning.',
		gn: "Yvy pyahu 90 ára pe ohechauka presión inmobiliaria. Yvy táva oikuaaukáva ikatu he'ise táva oñembotuicha, oikotevẽ servicio ha planificación.",
		pt: 'A quantidade de parcelas novas nos últimos 90 dias indica a pressão imobiliária sobre cada zona. Um crescimento alto de parcelas urbanas pode sinalizar expansão urbana, demanda por serviços e necessidade de planejamento.',
	},
	'analysis.catastro.methodTitle': { es: 'Metodología', en: 'Methodology', gn: 'Metodología', pt: 'Metodologia' },
	'analysis.catastro.methodBody': {
		es: 'Datos catastrales de la Dirección General de Catastro de Misiones procesados a nivel de radio censal INDEC 2022. Cada parcela se clasifica como urbana o rural según su código catastral. Superficie media y conteo de parcelas nuevas (últimos 90 días) se calculan por radio censal. Actualización mensual.',
		en: 'Cadastral data from the Misiones General Cadastre Office processed at INDEC 2022 census tract level. Each parcel is classified as urban or rural by its cadastral code. Average area and new parcel counts (last 90 days) are computed per census tract. Monthly update.',
		gn: 'Catastro Misiones datos radio censal INDEC 2022 rupi. Yvy oñemboja urbano térã rural. Actualización jasy peteĩ.',
		pt: 'Dados cadastrais da Dirección General de Catastro de Misiones processados no nível de setor censitário INDEC 2022. Cada parcela é classificada como urbana ou rural segundo seu código cadastral. Superfície média e contagem de parcelas novas (últimos 90 dias) são calculados por setor censitário. Atualização mensal.',
	},
	'analysis.catastro.guideDeptTitle': { es: 'Guía rápida', en: 'Quick guide', gn: "Ñemoañete pya'e", pt: 'Guia rápido' },
	'analysis.catastro.guideDeptBody': {
		es: 'Las parcelas cyan son urbanas, las verdes son rurales. Hacé click en cualquier edificio o zona del mapa para seleccionar un radio censal y ver sus estadísticas: cantidad de parcelas, áreas medias, parcelas nuevas y calidad habitacional.',
		en: 'Cyan parcels are urban, green are rural. Click any building or map area to select a census tract and see its statistics: parcel counts, average areas, new parcels and housing quality.',
		gn: "Yvy cyan = táva, verde = ka'aguy. Ehesakutu óga térã mapa-pe eiporavo hag̃ua radio ha ehecha estadística.",
		pt: 'As parcelas ciano são urbanas, as verdes são rurais. Clique em qualquer edificação ou zona do mapa para selecionar um setor censitário e ver suas estatísticas: quantidade de parcelas, áreas médias, parcelas novas e qualidade habitacional.',
	},
	'analysis.catastro.vsAvg': { es: 'vs promedio provincial', en: 'vs provincial average', gn: 'vs tetã mbytekue', pt: 'vs média provincial' },
	'analysis.catastro.pressure': { es: 'Presión inmobiliaria', en: 'Real estate pressure', gn: "Yvy ñemuha reko", pt: 'Pressão imobiliária' },
	'analysis.catastro.parcels': { es: 'parcelas', en: 'parcels', gn: 'yvy', pt: 'parcelas' },
	'analysis.catastro.updateFreq': { es: 'Datos catastrales actualizados semanalmente', en: 'Cadastral data updated weekly', gn: "Mba'ekuaa catastro oñembopyahu arapokõindy", pt: 'Dados cadastrais atualizados semanalmente' },
	'analysis.catastro.clickDept': { es: 'Parcelas visibles en el mapa — hacé click en un radio para detalle', en: 'Parcels visible on map — click a tract for detail', gn: "Yvy ojehecha mapápe — eñemí peteĩ radio-pe", pt: 'Parcelas visíveis no mapa — clique em um setor para detalhes' },
	'analysis.catastro.thisRadio': { es: 'Este radio', en: 'This tract', gn: 'Ko radio', pt: 'Este setor' },
	'analysis.catastro.deptAvg': { es: 'Prom. depto', en: 'Dept avg', gn: 'Depto mbytekue', pt: 'Méd. depto' },
	'analysis.catastro.provAvg': { es: 'Prom. provincia', en: 'Province avg', gn: 'Tetã mbytekue', pt: 'Méd. provincial' },
	'analysis.catastro.housingTitle': { es: 'Calidad habitacional', en: 'Housing quality', gn: "Óga rekoporã", pt: 'Qualidade habitacional' },
	'analysis.catastro.h.agua': { es: 'Con agua de red', en: 'Water access', gn: "Y oĩva", pt: 'Com água de rede' },
	'analysis.catastro.h.cloacas': { es: 'Con cloacas', en: 'Sewerage', gn: "Ysyry guasu", pt: 'Com esgoto' },
	'analysis.catastro.h.alumbrado': { es: 'Con alumbrado', en: 'With street lights', gn: "Tape rendy oĩva", pt: 'Com iluminação' },
	'analysis.catastro.h.pavimento': { es: 'Con pavimento', en: 'With paved roads', gn: "Tape porã oĩva", pt: 'Com pavimento' },
	'analysis.catastro.h.hacinamiento': { es: 'Hacinamiento', en: 'Overcrowding', gn: "Hetaiterei", pt: 'Superlotação' },
	'analysis.catastro.h.nbi': { es: 'NBI', en: 'Unmet needs', gn: "Oikotevẽva", pt: 'NBI' },
	'analysis.catastro.petalHint': { es: 'Pétalo: más grande = mejor · punteada = prom. de la provincia de Misiones', en: 'Petal: larger = better · dashed = Misiones Province avg', gn: 'Pétalo: más grande = mejor · punteada = prom. de la provincia de Misiones', pt: 'Pétala: maior = melhor · pontilhada = méd. da província de Misiones' },
	'analysis.catastro.clearAll': { es: 'Limpiar', en: 'Clear all', gn: "Mopotĩ", pt: 'Limpar' },
	'analysis.catastro.legendUrban': { es: 'Parcela urbana', en: 'Urban parcel', gn: "Yvy táva", pt: 'Parcela urbana' },
	'analysis.catastro.legendRural': { es: 'Parcela rural', en: 'Rural parcel', gn: "Yvy ka'aguy", pt: 'Parcela rural' },
	'analysis.catastro.legendNew': { es: 'Nueva', en: 'New', gn: "Ipyahu", pt: 'Nova' },
	'analysis.catastro.legendRemoved': { es: 'Eliminada', en: 'Removed', gn: "Okañy", pt: 'Removida' },
	'data.updatedAt': { es: 'Procesado al', en: 'Processed', gn: 'Oñembopyahu', pt: 'Processado em' },
	'analysis.flood.methodTitle': { es: 'Metodología', en: 'Methodology', gn: "Mba'eichapa", pt: 'Metodologia' },
	'analysis.flood.methodRecurrence': {
		es: 'Presencia histórica de agua derivada de JRC Global Surface Water (Landsat, 1984–2021). Occurrence indica el % del tiempo con agua detectada; recurrence indica el % de años en que el agua vuelve a aparecer; estacionalidad indica cuántos meses al año hay agua sobre la superficie del suelo.',
		en: 'Historical water presence from JRC Global Surface Water (Landsat, 1984–2021). Occurrence indicates % of time with water detected; recurrence indicates % of years water reappears; seasonality indicates months per year with water on the land surface.',
		gn: "Y rehegua historia guive JRC Global Surface Water (Landsat, 1984–2021). Occurrence he'ise % ára y reheve; recurrence he'ise % ary y ojekuaa jey; estacionalidad he'ise mboy jasy y reheve ary pe.",
		pt: 'Presença histórica de água derivada do JRC Global Surface Water (Landsat, 1984–2021). Occurrence indica o % do tempo com água detectada; recurrence indica o % de anos em que a água reaparece; sazonalidade indica quantos meses por ano há água sobre a superfície do solo.',
	},
	'analysis.flood.methodExtent': {
		es: 'Porcentaje de la zona cubierto por agua en la observación más reciente (última imagen SAR procesada). Mide cuánto de la zona está inundado actualmente.',
		en: 'Percentage of the area covered by water in the most recent observation (latest processed SAR image). Measures how much of the area is currently flooded.',
		gn: "Mboy % yvy y guype oime ko'ag̃a imagen SAR ipahague rupi.",
		pt: 'Percentagem da zona coberta por água na observação mais recente (última imagem SAR processada). Mede quanto da zona está inundado atualmente.',
	},
	'analysis.flood.methodScore': {
		es: 'Índice compuesto 0–100: media geométrica de componentes validados por PCA (presencia histórica, recurrencia interanual, extensión actual). Fuentes: JRC Global Surface Water + Sentinel-1 SAR. Una zona permanentemente sobre un río tendrá score alto; una zona seca con inundación actual también.',
		en: 'Composite index 0–100: geometric mean of PCA-validated components (historical presence, year-to-year recurrence, current extent). Sources: JRC Global Surface Water + Sentinel-1 SAR. An area permanently on a river scores high; a dry area with current flooding also scores high.',
		gn: "Índice 0–100: media geométrica componente PCA rehegua (y rehegua, jey ary ha ary, ko'ag̃a tuichakue). JRC + Sentinel-1 SAR.",
		pt: 'Índice composto 0–100: média geométrica de componentes validados por PCA (presença histórica, recorrência interanual, extensão atual). Fontes: JRC Global Surface Water + Sentinel-1 SAR. Uma zona permanentemente sobre um rio terá score alto; uma zona seca com inundação atual também.',
	},
	// Explanatory panels — flood risk
	'analysis.flood.howToReadTitle': { es: 'Cómo leer este mapa', en: 'How to read this map', gn: "Mba'eichapa ejapokuaa ko mapa", pt: 'Como ler este mapa' },
	'analysis.flood.howToReadBody': {
		es: 'Los colores representan la presencia histórica de agua en cada zona (JRC Global Surface Water, Landsat 1984–2021). Oscuro = sin agua detectada en todo el período (0%); azul = baja presencia (<5%); amarillo = presencia moderada (5–20%); rojo = alta presencia (>20%). Seleccioná un departamento para ver el detalle a nivel de parcela catastral.',
		en: 'Colours represent historical water presence per area (JRC Global Surface Water, Landsat 1984–2021). Dark = no water detected in the whole period (0%); blue = low presence (<5%); yellow = moderate presence (5–20%); red = high presence (>20%). Select a department to view parcel-level detail.',
		gn: "Sa'y ohechauka y rehegua historia yvy peteĩ-pe (JRC, Landsat 1984–2021). Hũ = ndaipóri y ojehecháva (0%); hovy = michĩ (<5%); sa'yju = mbyte (5–20%); pytã = guasu (>20%). Eiporavo departamento ehecha hag̃ua yvy peteĩteĩ.",
		pt: 'As cores representam a presença histórica de água em cada zona (JRC Global Surface Water, Landsat 1984–2021). Escuro = sem água detectada em todo o período (0%); azul = baixa presença (<5%); amarelo = presença moderada (5–20%); vermelho = alta presença (>20%). Selecione um departamento para ver o detalhe.',
	},
	'analysis.flood.keyFindingsTitle': { es: 'Hallazgos clave', en: 'Key findings', gn: 'Ojejuhúva guasu', pt: 'Principais achados' },
	'analysis.flood.implicationsTitle': { es: 'Implicancias', en: 'Implications', gn: "Mba'e he'ise", pt: 'Implicações' },
	'analysis.flood.implicationsBody': {
		es: 'Las parcelas en zonas de riesgo alto pueden enfrentar anegamientos recurrentes, afectando el valor inmobiliario y la habitabilidad. La infraestructura de servicios básicos (agua, cloacas) en estas zonas requiere diseño resiliente.',
		en: 'Parcels in high-risk areas may face recurrent flooding, affecting property values and habitability. Basic service infrastructure (water, sewerage) in these areas requires resilient design.',
		gn: "Yvy mba'asy guasu rehegua ikatu oguereko ysoguy jey, ojapo hag̃ua óga ha tape ivaivéva.",
		pt: 'As parcelas em zonas de alto risco podem enfrentar inundações recorrentes, afetando o valor imobiliário e a habitabilidade. A infraestrutura de serviços básicos (água, esgoto) nessas zonas requer design resiliente.',
	},
	'analysis.flood.statusWet': { es: 'Agua detectada en superficie', en: 'Surface water detected', gn: "Y ojehecha yvy ári", pt: 'Água detectada na superfície' },
	'analysis.flood.statusDry': { es: 'Sin agua detectada en la última imagen', en: 'No water detected in latest image', gn: "Ndaipóri y ojehechaukáva", pt: 'Sem água detectada na última imagem' },
	'analysis.flood.statusDate': { es: 'Imagen SAR:', en: 'SAR image:', gn: "SAR ra'ãnga:", pt: 'Imagem SAR:' },
	'analysis.flood.howToReadDeptTitle': { es: 'Guía rápida', en: 'Quick guide', gn: "Ñemoañete pya'e", pt: 'Guia rápido' },
	'analysis.flood.howToReadDeptBody': {
		es: 'Cada parcela catastral está coloreada según la presencia histórica de agua (JRC, %). Hacé click en una parcela para ver las métricas físicas: presencia histórica, recurrencia interanual, estacionalidad y extensión actual. Podés seleccionar varias parcelas para compararlas.',
		en: 'Each cadastral parcel is coloured by historical water presence (JRC, %). Click a parcel to see the physical metrics: historical presence, year-to-year recurrence, seasonality and current extent. You can select multiple parcels to compare them.',
		gn: "Yvy peteĩteĩ oñembosa'y y historia rehegua rupi (JRC, %). Ehesakutu peteĩ ehecha hag̃ua métricas físicas. Ikatu eiporavo heta yvy ejojaha hag̃ua.",
		pt: 'Cada parcela cadastral está colorida pela presença histórica de água (JRC, %). Clique em uma parcela para ver as métricas físicas: presença histórica, recorrência interanual, sazonalidade e extensão atual. Você pode selecionar várias parcelas para compará-las.',
	},

	// Real estate analysis labels
	'analysis.re.provincial': { es: 'Resumen provincial', en: 'Provincial summary', gn: "Tetã guasu rehegua", pt: 'Resumo provincial' },
	'analysis.re.listings': { es: 'Avisos activos', en: 'Active listings', gn: "Mba'e oñevendéva", pt: 'Anúncios ativos' },
	'analysis.re.medianPrice': { es: 'Precio mediano USD/m²', en: 'Median price USD/m²', gn: 'Hepy mbytekue USD/m²', pt: 'Preço mediano USD/m²' },
	'analysis.re.medianTotal': { es: 'Precio mediano USD', en: 'Median price USD', gn: 'Hepy mbytekue USD', pt: 'Preço mediano USD' },
	'analysis.re.houses': { es: 'Casas', en: 'Houses', gn: 'Óga', pt: 'Casas' },
	'analysis.re.apartments': { es: 'Departamentos', en: 'Apartments', gn: "Óga guasu", pt: 'Apartamentos' },
	'analysis.re.lots': { es: 'Lotes', en: 'Lots', gn: 'Yvy', pt: 'Lotes' },
	'analysis.re.avgArea': { es: 'Superficie promedio', en: 'Average area', gn: "Yvy tuichakue", pt: 'Superfície média' },
	'analysis.re.vsMedian': { es: 'vs. mediana departamental', en: 'vs. department median', gn: 'vs. departamento mbytekue', pt: 'vs. mediana departamental' },
	'analysis.re.topDepts': { es: 'Top departamentos', en: 'Top departments', gn: 'Departamento iporãvéva', pt: 'Top departamentos' },
	'analysis.re.radioDetail': { es: 'Detalle del radio', en: 'Radio detail', gn: 'Radio rehegua', pt: 'Detalhe do setor' },
	'analysis.re.propertyTypes': { es: 'Tipos de propiedad', en: 'Property types', gn: "Mba'e lája", pt: 'Tipos de propriedade' },

	// ── Lasso / Zones ──────────────────────────────────────────────────────
	'lasso.toggle': { es: 'Lazo', en: 'Lasso', gn: 'Lazo', pt: 'Lasso' },
	'lasso.drawing': { es: 'Dibujando zona...', en: 'Drawing zone...', gn: 'Oñembosako\'i...' , pt: 'Desenhando zona...' },
	'lasso.cancel': { es: 'Cancelar lazo', en: 'Cancel lasso', gn: 'Eheja lazo', pt: 'Cancelar lasso' },
	'lasso.clearZones': { es: 'Limpiar zonas', en: 'Clear zones', gn: 'Emopotĩ zona', pt: 'Limpar zonas' },
	'side.radios': { es: 'Radios censales', en: 'Census tracts', gn: 'Radio censal', pt: 'Setores censitários' },
	'side.clearRadios': { es: 'Limpiar', en: 'Clear', gn: 'Emopotĩ', pt: 'Limpar' },
	'side.radioCensus.title': { es: 'Radios censales', en: 'Census tracts', gn: 'Radio censal', pt: 'Setores censitários' },
	'side.radioCensus.subtitle': { es: 'Distribución de indicadores censales en todos los radios', en: 'Distribution of census indicators across all tracts', gn: "Indicadores censales jehaipy opavave radio rehe", pt: 'Distribuição de indicadores censitários em todos os setores' },
	'side.radioCensus.position': { es: 'Este radio: {value} % · mayor que el {pct} % de los radios {ref}', en: 'This tract: {value} % · higher than {pct} % of the tracts {ref}', gn: 'Este radio: {value} % · mayor que el {pct} % de los radios {ref}', pt: 'Este setor: {value} % · maior que {pct} % dos setores {ref}' },
	'side.radioCensus.positionN': { es: 'Radio {n}: {value} % · mayor que el {pct} % de los radios {ref}', en: 'Tract {n}: {value} % · higher than {pct} % of the tracts {ref}', gn: 'Radio {n}: {value} % · mayor que el {pct} % de los radios {ref}', pt: 'Setor {n}: {value} % · maior que {pct} % dos setores {ref}' },
	'side.radioCensus.variable': { es: 'Variable', en: 'Variable', gn: 'Variable', pt: 'Variável' },
	'side.radioCensus.loading': { es: 'Cargando radios…', en: 'Loading tracts…', gn: 'Omyenyhẽhína radio…', pt: 'Carregando setores…' },
	'side.radioCensus.empty': { es: 'Sin datos de radios', en: 'No tract data', gn: "Ndaipóri radio dato", pt: 'Sem dados de setores' },
	'side.radioCensus.error': { es: 'No se pudieron cargar los radios', en: 'Could not load tracts', gn: "Ndaikatúi omyenyhẽ radio", pt: 'Não foi possível carregar os setores' },
	'source.title': { es: 'Fuentes', en: 'Sources', gn: "Moñe'ẽha", pt: 'Fontes' },
	'source.census':    { es: 'Datos: INDEC — Censo Nacional de Población 2022', en: 'Data: INDEC — National Population Census 2022', gn: 'Datos: INDEC — Censo Nacional 2022', pt: 'Dados: INDEC — Censo Nacional de Población 2022' },
	'source.census.br': { es: 'Datos: IBGE — Censo Demográfico 2022 (Agregados por Setores Censitários)', en: 'Data: IBGE — Demographic Census 2022 (Tract-level Aggregates)', gn: 'Datos: IBGE — Censo Demográfico 2022', pt: 'Dados: IBGE — Censo Demográfico 2022 (Agregados por Setores Censitários)' },
	'source.census.br.note': { es: 'Índice de saneamiento local (modo local — no comparable entre países): % agua de red general, % desagüe adecuado, % basura recolectada, % sin baño exclusivo.', en: 'Local sanitation index (local mode — not cross-country comparable): % piped water, % adequate sewage, % garbage collected, % no exclusive bathroom.', gn: 'Índice saneamiento local BR.', pt: 'Índice de saneamento local (modo local): % água de rede geral, % esgoto adequado, % lixo coletado, % sem banheiro exclusivo.' },
	'source.buildings': { es: 'Edificaciones: Google Open Buildings 2025 + VIDA', en: 'Buildings: Google Open Buildings 2025 + VIDA', gn: 'Óga: Google Open Buildings 2025 + VIDA', pt: 'Edificações: Google Open Buildings 2025 + VIDA' },
	'source.basemap': { es: 'Mapa: CARTO Dark Matter (© OpenStreetMap)', en: 'Basemap: CARTO Dark Matter (© OpenStreetMap)', gn: 'Mapa: CARTO Dark Matter (© OpenStreetMap)', pt: 'Mapa: CARTO Dark Matter (© OpenStreetMap)' },
	'source.terrain': { es: 'Relieve: Mapterhorn (Copernicus GLO-30, © DLR/Airbus, UE/ESA)', en: 'Terrain: Mapterhorn (Copernicus GLO-30, © DLR/Airbus, EU/ESA)', gn: 'Relieve: Mapterhorn (Copernicus GLO-30, © DLR/Airbus, UE/ESA)', pt: 'Relevo: Mapterhorn (Copernicus GLO-30, © DLR/Airbus, UE/ESA)' },
	'lasso.hint': { es: 'Dibujá una zona arrastrando sobre el mapa. El cálculo tarda unos segundos.', en: 'Draw a zone by dragging on the map. Calculation takes a few seconds.', gn: "Embosako'i zona mapa ári. Ohasa segundos.", pt: 'Desenhe uma zona arrastando sobre o mapa. O cálculo leva alguns segundos.' },
	'zone.title': { es: 'Zona', en: 'Zone', gn: 'Zona', pt: 'Zona' },
	'zone.population': { es: 'Población', en: 'Population', gn: 'Yvypóra', pt: 'População' },
	'zone.area': { es: 'Área km²', en: 'Area km²', gn: 'Yvy km²', pt: 'Área km²' },
	'zone.radios': { es: 'Radios', en: 'Radios', gn: 'Radio', pt: 'Setores' },
	'zone.noRadios': { es: 'Sin radios en la selección', en: 'No radios in selection', gn: "Radio'ỹ jeporavópe", pt: 'Sem setores na seleção' },
	'zone.petalNote': { es: 'Relativo al promedio {ofRef}, ponderado por población (línea punteada = promedio)', en: 'Relative to the population-weighted average {ofRef} (dashed line = average)', gn: 'Relativo al promedio {ofRef}, ponderado por población (línea punteada = promedio)', pt: 'Relativo à média {ofRef}, ponderada pela população (linha pontilhada = média)' },
	'zone.petalNoteMulti': { es: 'Cada zona, relativa al promedio de su provincia, ponderado por población (línea punteada = promedio): {list}', en: 'Each zone relative to its own province’s population-weighted average (dashed line = average): {list}', gn: 'Cada zona, relativa al promedio de su provincia, ponderado por población (línea punteada = promedio): {list}', pt: 'Cada zona relativa à média da sua província, ponderada pela população (linha pontilhada = média): {list}' },

	// ── Hex comparison / hex zones ──────────────────────────────────────
	'hex.comparison': { es: 'Comparación de hexágonos', en: 'Hexagon comparison', gn: 'Hexágono jojaha', pt: 'Comparação de hexágonos' },
	'hex.hexCount': { es: 'Hexágonos', en: 'Hexagons', gn: 'Hexágono', pt: 'Hexágonos' },
	'hexZone.title': { es: 'Zonas hexagonales', en: 'Hex zones', gn: 'Hexágono zona', pt: 'Zonas hexagonais' },
	'hex.resolution': { es: 'Resolución H3', en: 'H3 resolution', gn: 'H3 tuichakue', pt: 'Resolução H3' },
	'hex.loading': { es: 'Cargando hexágonos...', en: 'Loading hexagons...', gn: 'Oñemyenyhẽ hexágono...', pt: 'Carregando hexágonos...' },
	'hex.petalRelative': { es: 'Flor (lectura relativa):', en: 'Flower (relative reading):', gn: 'Yvoty:', pt: 'Flor (leitura relativa):' },
	'hex.avgOf': { es: 'promedio {ofRef}', en: 'average {ofRef}', gn: 'promedio {ofRef}', pt: 'média {ofRef}' },
	'hex.goalpostNote': { es: 'Valores /100: escala fija regional, idéntica en Argentina, Paraguay y Brasil — 100 es el techo del rango de referencia (NEA + región transfronteriza), no el máximo {ofRef}. Solo la flor de arriba compara contra el promedio {ofRef}.', en: 'Values /100: fixed regional scale, identical in Argentina, Paraguay and Brazil — 100 is the ceiling of the reference range (NEA + cross-border region), not the maximum {ofRef}. Only the flower above compares against the average {ofRef}.', gn: 'Valores /100: escala fija regional, idéntica en Argentina, Paraguay y Brasil — 100 es el techo del rango de referencia (NEA + región transfronteriza), no el máximo {ofRef}. Solo la flor de arriba compara contra el promedio {ofRef}.', pt: 'Valores /100: escala fixa regional, idêntica na Argentina, no Paraguai e no Brasil — 100 é o teto do intervalo de referência (NEA + região transfronteiriça), não o máximo {ofRef}. Apenas a flor acima compara contra a média {ofRef}.' },
	'hex.magTable': { es: 'Magnitud y valores estimados', en: 'Magnitude & estimated values', gn: 'Tuichakue ha papapy', pt: 'Magnitude e valores estimados' },
	'hex.buildings': { es: 'Edificios', en: 'Buildings', gn: 'Óga', pt: 'Edifícios' },
	'hex.base.pob_14_mas': { es: 'Personas de 14 años y más (estimado)', en: 'People aged 14+ (estimate)', gn: 'Tapicha 14 ary ha hetave (estimado)', pt: 'Pessoas de 14 anos ou mais (estimado)' },
	'hex.base.pob_18_mas': { es: 'Personas de 18 años y más (estimado)', en: 'People aged 18+ (estimate)', gn: 'Tapicha 18 ary ha hetave (estimado)', pt: 'Pessoas de 18 anos ou mais (estimado)' },
	'hex.base.hog_total': { es: 'Hogares (estimado)', en: 'Households (estimate)', gn: 'Hogares (estimado)', pt: 'Lares (estimado)' },
	'hex.base.pob_total': { es: 'Personas (estimado)', en: 'People (estimate)', gn: 'Personas (estimado)', pt: 'Pessoas (estimado)' },
	'hex.base.pob_6_12': { es: 'Chicos de 6 a 12 años (estimado)', en: 'Children aged 6–12 (estimate)', gn: 'Chicos de 6 a 12 años (estimado)', pt: 'Crianças de 6 a 12 anos (estimado)' },
	'hex.base.pob_13_18': { es: 'Personas de 13 a 18 años (estimado)', en: 'People aged 13–18 (estimate)', gn: 'Personas de 13 a 18 años (estimado)', pt: 'Pessoas de 13 a 18 anos (estimado)' },
	'hex.popEst': { es: 'Población estimada', en: 'Estimated population', gn: 'Tekohára papapy', pt: 'População estimada' },
	'hex.households': { es: 'Hogares estimados', en: 'Estimated households', gn: 'Óga papapy', pt: 'Domicílios estimados' },
	'hex.estPctNote': { es: 'Valores estimados por hexágono: el dato del radio censal se redistribuye a los hexágonos ponderando por edificios (método dasimétrico).', en: 'Estimated values per hexagon: the census-tract value is redistributed to hexagons weighted by buildings (dasymetric method).', gn: 'Papapy hexágono rehegua: radio censal-gui oñemboja\'o hexágono-pe óga rupive (dasimétrico).', pt: 'Valores estimados por hexágono: o dado do setor censitário é redistribuído aos hexágonos ponderando por edifícios (método dasimétrico).' },
	'hex.baseNote': { es: 'Cada % es la tasa del radio censal que cubre el hexágono (ponderada si son varios), sobre el total que dice su etiqueta: hogares o personas de cierta edad. Las filas «(estimado)» dicen cuántos de esos hogares o personas hay en el hexágono. «Sin cobertura»: el radio tiene menos de 30 en ese total, demasiado pocos para una tasa.', en: 'Each % is the rate of the census tract covering the hexagon (weighted if there are several), over the total its label names: households or people of a given age. The “(estimate)” rows say how many of those households or people are in the hexagon. “No data”: the tract has fewer than 30 in that total, too few for a rate.', gn: 'Cada % es la tasa del radio censal que cubre el hexágono (ponderada si son varios), sobre el total que dice su etiqueta: hogares o personas de cierta edad. Las filas «(estimado)» dicen cuántos de esos hogares o personas hay en el hexágono. «Sin cobertura»: el radio tiene menos de 30 en ese total, demasiado pocos para una tasa.', pt: 'Cada % é a taxa do setor censitário que cobre o hexágono (ponderada se forem vários), sobre o total que diz o seu rótulo: lares ou pessoas de certa idade. As linhas «(estimado)» dizem quantos desses lares ou pessoas há no hexágono. «Sem cobertura»: o setor tem menos de 30 nesse total, poucos demais para uma taxa.' },

	// ── Welcome panel ─────────────────────────────────────────────────────
	'side.onboarding.title': { es: 'Empezar a explorar', en: 'Start exploring', gn: "Eñepyrũ eheka", pt: 'Começar a explorar' },
	'side.onboarding.step1': { es: 'Elegí un lente arriba: Ambiente, Producción, Población o Economía', en: 'Choose a lens above: Environment, Production, Population or Economy', gn: "Eiporavo lente yvate gotyo", pt: 'Escolha uma lente acima: Ambiente, Produção, População ou Economia' },
	'side.onboarding.step2': { es: 'Seleccioná un análisis del menú y explorá el mapa por departamento', en: 'Select an analysis from the menu and explore the map by department', gn: "Eiporavo peteĩ mba'ekuaa ha ehecha mapa", pt: 'Selecione uma análise do menu e explore o mapa por departamento' },
	'side.onboarding.step3': { es: 'Hacé click en un hexágono para ver el detalle geoespacial completo', en: 'Click a hexagon to see the full geospatial profile', gn: "Ehesakutu hexágono ehecha hag̃ua", pt: 'Clique em um hexágono para ver o perfil geoespacial completo' },
	'side.welcome.desc': {
		es: 'Plataforma de análisis geoespacial que integra múltiples fuentes satelitales, censales y catastrales, en continua actualización, sobre el noreste argentino y sus regiones transfronterizas.',
		en: 'Geospatial analysis platform integrating multiple satellite, census and cadastral sources, continuously updated, for northeast Argentina and its cross-border regions.',
		gn: "Yvy rekokatu plataforma NEA-pe ha ñemboyvate jápa regiones ndive, heta moñe'ẽha satélite, censo ha catastro ndive, oñembohekopyahu hag̃uaitépe.",
		pt: 'Plataforma de análise geoespacial que integra múltiplas fontes satelitais, censitárias e cadastrais, em contínua atualização, sobre o nordeste argentino e suas regiões transfronteiriças.',
	},
	'side.welcome.footer.author': { es: 'Raimundo Elías Gómez', en: 'Raimundo Elías Gómez', gn: 'Raimundo Elías Gómez', pt: 'Raimundo Elías Gómez' },
	'side.welcome.footer.affiliation': { es: 'CONICET / FHyCS-UNaM / GEE Partner', en: 'CONICET / FHyCS-UNaM / GEE Partner', gn: 'CONICET / FHyCS-UNaM / GEE Partner', pt: 'CONICET / FHyCS-UNaM / GEE Partner' },
	'side.welcome.hidePanel': { es: 'Ocultar panel', en: 'Hide panel', gn: 'Emokañy panel', pt: 'Ocultar painel' },
	'side.welcome.showPanel': { es: 'Mostrar panel', en: 'Show panel', gn: 'Ehechauka panel', pt: 'Mostrar painel' },

	// ── CTA diagnóstico territorial ───────────────────────────────────────
	'cta.diagnostic.label': { es: '¿Necesitás un diagnóstico personalizado?', en: 'Need a custom geospatial diagnosis?', gn: "Reikotevẽ diagnóstico?", pt: 'Precisa de um diagnóstico personalizado?' },
	'cta.diagnostic.button': { es: 'Solicitar diagnóstico geoespacial', en: 'Request geospatial diagnosis', gn: 'Ejerure diagnóstico', pt: 'Solicitar diagnóstico geoespacial' },

	// ── EUDR product page ─────────────────────────────────────────────────
	'eudr.disclaimer_short': { es: 'Evaluación indicativa. No constituye certificación legal bajo Reg. (UE) 2023/1115.', en: 'Indicative assessment. Not legal certification under Reg. (EU) 2023/1115.', gn: 'EUDR', pt: 'Avaliação indicativa. Não constitui certificação legal sob Reg. (UE) 2023/1115.' },

	// ── EUDR check page ──────────────────────────────────────────────────
	'eudr.check.input_title': { es: 'Coordenadas', en: 'Coordinates', gn: 'Coordinates', pt: 'Coordenadas' },
	'eudr.check.check_btn': { es: 'Verificar', en: 'Check', gn: 'Check', pt: 'Verificar' },
	'eudr.check.try_example': { es: 'Probar con un ejemplo real (Misiones)', en: 'Try a real example (Misiones)', gn: 'Probar', pt: 'Testar com um exemplo real (Misiones)' },
	'eudr.check.clear_all': { es: '✕ Limpiar todo', en: '✕ Clear all', gn: '✕ Embogue', pt: '✕ Limpar tudo' },
	'eudr.check.checking': { es: 'Verificando...', en: 'Checking...', gn: 'Checking...', pt: 'Verificando...' },
	'eudr.check.click_map': { es: 'Click en el mapa para seleccionar ubicación', en: 'Click on the map to select location', gn: 'Click', pt: 'Clique no mapa para selecionar localização' },
	'eudr.check.legend_title': { es: 'Riesgo de deforestación post-2020 (score 0–100)', en: 'Post-2020 deforestation risk (score 0–100)', gn: 'Riesgo deforestación post-2020 (0–100)', pt: 'Risco de desmatamento pós-2020 (score 0–100)' },
	'eudr.check.legend_note': { es: 'Color por hexágono (~0,1 km²): 70% pérdida forestal + 20% fuego + 10% sin bosque (cutoff 31/12/2020)', en: 'Colour per hexagon (~0.1 km²): 70% forest loss + 20% fire + 10% no forest (cut-off 31/12/2020)', gn: 'Color hexágono rupive (~0,1 km²): 70% pérdida + 20% tata + 10% ka\'aguy\'ỹ', pt: 'Cor por hexágono (~0,1 km²): 70% perda florestal + 20% fogo + 10% sem floresta (corte 31/12/2020)' },
	'eudr.check.error_invalid': { es: 'Coordenadas inválidas', en: 'Invalid coordinates', gn: 'Error', pt: 'Coordenadas inválidas' },
	'eudr.check.error_bounds': { es: 'Coordenadas fuera del área de cobertura', en: 'Coordinates outside coverage area', gn: 'Error', pt: 'Coordenadas fora da área de cobertura' },
	'eudr.check.result_title': { es: 'Resultado', en: 'Result', gn: 'Result', pt: 'Resultado' },
	'eudr.check.risk_score': { es: 'Score de riesgo', en: 'Risk score', gn: 'Risk', pt: 'Score de risco' },
	'eudr.check.forest_2020': { es: 'Cobertura forestal 2020', en: 'Forest cover 2020', gn: 'Forest', pt: 'Cobertura florestal 2020' },
	'eudr.check.forest_current': { es: 'Cobertura actual', en: 'Current cover', gn: 'Forest', pt: 'Cobertura atual' },
	'eudr.check.loss_post_2020': { es: 'Pérdida post-2020', en: 'Loss post-2020', gn: 'Loss', pt: 'Perda pós-2020' },
	'eudr.check.fire_post_2020': { es: 'Fuego s/ nativa', en: 'Fire on native', gn: 'Tata ka’aguýre', pt: 'Fogo s/ nativa' },
	'eudr.check.fire_total': { es: 'total', en: 'total', gn: 'opavave', pt: 'total' },
	'eudr.check.province': { es: 'Provincia', en: 'Province', gn: 'Province', pt: 'Província' },
	'eudr.check.coordinates': { es: 'Coordenadas', en: 'Coordinates', gn: 'Coordinates', pt: 'Coordenadas' },
	'eudr.check.empty_title': { es: 'Seleccioná una ubicación', en: 'Select a location', gn: 'Select', pt: 'Selecione uma localização' },
	'eudr.check.empty_desc': { es: 'Hacé click en el mapa o ingresá coordenadas para verificar el estado de deforestación.', en: 'Click on the map or enter coordinates to check deforestation status.', gn: 'Click', pt: 'Clique no mapa ou insira coordenadas para verificar o status de desmatamento.' },
	'eudr.check.deforest_detected': { es: 'PÉRDIDA POST-2020 DETECTADA', en: 'POST-2020 LOSS DETECTED', gn: 'PÉRDIDA POST-2020', pt: 'PERDA PÓS-2020 DETECTADA' },
	'eudr.check.area_note': { es: 'Área evaluada: hexágono de ~0,1 km² (H3 res-9). Datos satelitales a 100 m: el resultado refleja ese hexágono, no necesariamente la parcela exacta.', en: 'Assessed area: ~0.1 km² hexagon (H3 res-9). 100 m satellite data: the result reflects that hexagon, not necessarily the exact plot.', gn: 'Área ojehecháva: hexágono ~0,1 km² (H3 res-9). Resultado he\'i hexágono rehe, ndaha\'éi parcela exacta.', pt: 'Área avaliada: hexágono de ~0,1 km² (H3 res-9). Dados satelitais a 100 m: o resultado reflete esse hexágono, não necessariamente a parcela exata.' },
	'eudr.check.vintage': { es: 'Datos satelitales', en: 'Satellite data', gn: 'Datos satelitales', pt: 'Dados satelitais' },
	'eudr.check.refreshed': { es: 'Última actualización', en: 'Last refreshed', gn: 'Última actualización', pt: 'Última atualização' },
	'eudr.check.methodology_link': { es: 'Ver metodología completa →', en: 'View full methodology →', gn: 'Ehecha metodología →', pt: 'Ver metodologia completa →' },
	'eudr.check.poly_title': { es: 'Analizar un polígono', en: 'Analyze a polygon', gn: 'Polígono', pt: 'Analisar um polígono' },
	'eudr.check.poly_upload': { es: 'Subir GeoJSON (parcela / lote)', en: 'Upload GeoJSON (plot / lot)', gn: 'Embohasa GeoJSON', pt: 'Enviar GeoJSON (parcela / lote)' },
	'eudr.check.poly_draw': { es: '🎯 Dibujar con lazo', en: '🎯 Draw with lasso', gn: '🎯 Embokuatia lazo-pe', pt: '🎯 Desenhar com laço' },
	'eudr.check.poly_draw_hint': { es: 'Mantené el mouse presionado y arrastrá sobre el mapa para dibujar el área. Al soltar, se cierra el polígono y se analiza.', en: 'Press and hold the mouse, then drag over the map to outline the area. Release to close the polygon and analyze.', gn: 'Embopypy mouse ha emboja mapa-re.', pt: 'Pressione e arraste o mouse sobre o mapa para desenhar a área. Solte para fechar o polígono e analisar.' },
	'eudr.check.poly_draw_cancel': { es: 'Cancelar lazo', en: 'Cancel lasso', gn: 'Ejoko lazo', pt: 'Cancelar laço' },
	'eudr.check.poly_result_title': { es: 'Resultado del polígono', en: 'Polygon result', gn: 'Polígono resultado', pt: 'Resultado do polígono' },
	'eudr.check.poly_area': { es: 'Área en cobertura', en: 'Area in coverage', gn: 'Área', pt: 'Área em cobertura' },
	'eudr.check.poly_deforested': { es: '% con pérdida post-2020', en: '% with post-2020 loss', gn: '% pérdida', pt: '% com perda pós-2020' },
	'eudr.check.poly_max_risk': { es: 'Riesgo máximo', en: 'Max risk', gn: 'Riesgo max', pt: 'Risco máximo' },
	'eudr.check.poly_mean_risk': { es: 'Riesgo medio', en: 'Mean risk', gn: 'Riesgo medio', pt: 'Risco médio' },
	'eudr.check.poly_cells': { es: 'Celdas (en cobertura / total)', en: 'Cells (in coverage / total)', gn: 'Celdas', pt: 'Células (em cobertura / total)' },
	'eudr.check.poly_deforested_cells': { es: 'Celdas con pérdida post-2020', en: 'Cells with post-2020 loss', gn: 'Celdas pérdida', pt: 'Células com perda pós-2020' },
	'eudr.check.loss_by_year': { es: 'Pérdida por año (post-cutoff EUDR)', en: 'Loss per year (post-EUDR cutoff)', gn: 'Pérdida ary rehegua', pt: 'Perda por ano (pós-cutoff EUDR)' },
	'eudr.check.batch_title': { es: 'Análisis en lote (CSV)', en: 'Batch analysis (CSV)', gn: 'CSV lote', pt: 'Análise em lote (CSV)' },
	'eudr.check.batch_upload': { es: 'Subir CSV (id, lat, lon)', en: 'Upload CSV (id, lat, lon)', gn: 'Embohasa CSV', pt: 'Enviar CSV (id, lat, lon)' },
	'eudr.check.batch_format': { es: 'Encabezados aceptados: id (opcional), lat/latitude/latitud, lon/lng/longitude. Hasta 10.000 filas, 5 MB.', en: 'Accepted headers: id (optional), lat/latitude, lon/lng/longitude. Up to 10,000 rows, 5 MB.', gn: 'Headers: id, lat, lon.', pt: 'Cabeçalhos aceitos: id (opcional), lat/latitude, lon/lng/longitude. Até 10.000 linhas, 5 MB.' },
	'eudr.check.batch_download': { es: 'parcelas procesadas — descargar CSV', en: 'plots processed — download CSV', gn: 'parcelas — emboguejy CSV', pt: 'parcelas processadas — baixar CSV' },
	'eudr.check.batch_outside': { es: 'fuera de cobertura', en: 'outside coverage', gn: 'fuera', pt: 'fora de cobertura' },
	'eudr.check.batch_err_size': { es: 'CSV demasiado grande (máx. 5 MB).', en: 'CSV too large (max 5 MB).', gn: 'CSV tuicha.', pt: 'CSV muito grande (máx. 5 MB).' },
	'eudr.check.batch_err_empty': { es: 'CSV vacío o sin filas válidas.', en: 'CSV empty or no valid rows.', gn: 'CSV nahániri.', pt: 'CSV vazio ou sem linhas válidas.' },
	'eudr.check.batch_err_cols': { es: 'El CSV debe tener columnas "lat" y "lon" (o latitude/longitude).', en: 'CSV must include "lat" and "lon" columns (or latitude/longitude).', gn: 'lat ha lon oĩva\'erã.', pt: 'CSV deve ter colunas "lat" e "lon" (ou latitude/longitude).' },
	'eudr.check.batch_err_too_many': { es: 'Máximo 10.000 filas por archivo.', en: 'Maximum 10,000 rows per file.', gn: '10000 max.', pt: 'Máximo 10.000 linhas por arquivo.' },
	'eudr.check.plant_managed': { es: 'Plantación forestal ({pct}%). La pérdida post-2020 puede ser un ciclo de cosecha, no deforestación de bosque nativo — verificá que la plantación sea anterior al 31/12/2020.', en: 'Forestry plantation ({pct}%). Post-2020 loss may be a harvest cycle, not native-forest deforestation — verify the plantation predates 31/12/2020.', gn: 'Plantación forestal ({pct}%). Pérdida post-2020 ikatu ha\'e cosecha, ndaha\'éi deforestación ka\'aguy nativo — emoañete plantación oĩ\'akue 31/12/2020 mboyve.', pt: 'Plantação florestal ({pct}%). A perda pós-2020 pode ser um ciclo de colheita, não desmatamento de floresta nativa — verifique se a plantação é anterior a 31/12/2020.' },
	'eudr.check.plant_zone': { es: 'Zona de plantación forestal ({pct}%).', en: 'Forestry plantation area ({pct}%).', gn: 'Plantación forestal rendaha ({pct}%).', pt: 'Área de plantação florestal ({pct}%).' },
	'eudr.check.plant_managed_confirmed': { es: 'En 2020 esta zona ya era plantación forestal ({pct}%). La pérdida post-2020 es compatible con un ciclo de cosecha, no con deforestación de bosque nativo. Indicativo — no constituye veredicto de cumplimiento EUDR.', en: 'In 2020 this area was already forestry plantation ({pct}%). The post-2020 loss is consistent with a harvest cycle rather than native-forest deforestation. Indicative — not an EUDR-compliance verdict.', gn: '2020-pe ko renda ha\'éma plantación forestal ({pct}%). Pérdida post-2020 ojokupyty cosecha ndive, ndaha\'éi deforestación ka\'aguy nativo. Indicativo añónte.', pt: 'Em 2020 esta zona já era plantação florestal ({pct}%). A perda pós-2020 é compatível com um ciclo de colheita, não com desmatamento de floresta nativa. Indicativo — não é um veredicto de conformidade EUDR.' },
	'eudr.check.plant_conversion': { es: 'Observación: en 2020 esta zona figuraba como cobertura nativa y hoy como plantación. Posible conversión post-2020 — requiere verificación en terreno o catastro. Indicativo, no es un veredicto.', en: 'Observation: in 2020 this area was native cover and is plantation today. Possible post-2020 conversion — requires field or cadastre verification. Indicative, not a verdict.', gn: 'Ojehecha: 2020-pe cobertura nativa, ko\'ágã plantación. Ikatu conversión post-2020 — eñemoañete terreno térã catastro. Indicativo añónte.', pt: 'Observação: em 2020 esta zona era cobertura nativa e hoje é plantação. Possível conversão pós-2020 — requer verificação em campo ou cadastro. Indicativo, não é um veredicto.' },
	'eudr.check.plant_forest_loss': { es: 'Pérdida sobre bosque cerrado (MapBiomas 2020, clase 3). Posible deforestación de bosque nativo post-2020 — verificar causa.', en: 'Loss over closed forest (MapBiomas 2020, class 3). Possible post-2020 native-forest deforestation — verify cause.', gn: 'Pérdida ka\'aguy hû\'î ári (MapBiomas 2020, clase 3). Ikatu deforestación post-2020 — eñemoañete.', pt: 'Perda sobre floresta fechada (MapBiomas 2020, classe 3). Possível desmatamento de floresta nativa pós-2020 — verificar causa.' },
	'eudr.check.plant_savanna_loss': { es: 'Pérdida sobre bosque abierto o inundable (MapBiomas 2020, clases 4 y 6). En la leyenda argentina son clases de bosque, igual que el bosque cerrado, con dosel más abierto o régimen de inundación. Cuenta como vegetación nativa leñosa.', en: 'Loss over open or flooded forest (MapBiomas 2020, classes 4 and 6). In the Argentine legend these are forest classes, like closed forest, with a more open canopy or a flooding regime. Counts as native woody vegetation.', gn: 'Pérdida ka\'aguy ojepe\'áva térã y ári (MapBiomas 2020, clase 4 ha 6). Leyenda argentina-pe ka\'aguy avei. Vegetación nativa leñosa ramo oñemomba\'e.', pt: 'Perda sobre floresta aberta ou inundável (MapBiomas 2020, classes 4 e 6). Na legenda argentina são classes de floresta, tal como a floresta fechada, com dossel mais aberto ou regime de inundação. Conta como vegetação nativa lenhosa.' },
	'eudr.check.plant_nodata': { es: 'Sin dato de plantación para esta zona. La distinción plantación / bosque nativo (MapBiomas) cubre el NEA argentino (Misiones, Corrientes, Chaco, Formosa); Paraguay y Brasil quedan sin dato.', en: 'No plantation data for this area. The plantation / native-forest distinction (MapBiomas) covers NE Argentina (Misiones, Corrientes, Chaco, Formosa); Paraguay and Brazil have no data.', gn: "Ndaipóri dato plantación ko rendápe. Distinción plantación / ka'aguy nativo oĩ NEA argentino-pe; Paraguay ha Brasil ndaipóri dato.", pt: 'Sem dado de plantação para esta área. A distinção plantação / floresta nativa (MapBiomas) cobre o NEA argentino (Misiones, Corrientes, Chaco, Formosa); Paraguai e Brasil ficam sem dado.' },
	'eudr.check.plant_nodata_loss': { es: '⚠️ Sin dato de plantación para esta zona (Paraguay/Brasil — la cobertura MapBiomas plantación/nativo está en el NEA argentino). Acá NO se pudo distinguir si la pérdida es sobre plantación (posible cosecha) o sobre bosque nativo. Si el lote es una forestación, podría ser un falso positivo — verificar en terreno o con catastro oficial.', en: '⚠️ No plantation data for this area (Paraguay/Brazil — MapBiomas plantation/native cover is in NE Argentina). Here the loss could NOT be distinguished between plantation (possible harvest) and native forest. If the plot is a plantation, it may be a false positive — verify on the ground or with the official cadastre.', gn: '⚠️ Ndaipóri dato plantación (Paraguay/Brasil). Ko\'ápe ndaikatúi oñembojoja plantación (ikatu cosecha) ha ka\'aguy nativo apytépe. Lote ha\'éramo forestación, ikatu falso positivo — emoañete terreno térã catastro-pe.', pt: '⚠️ Sem dado de plantação para esta área (Paraguai/Brasil — a cobertura MapBiomas está no NEA argentino). Aqui a perda NÃO pôde ser distinguida entre plantação (possível colheita) e floresta nativa. Se o lote for uma plantação, pode ser um falso positivo — verifique em campo ou com o cadastro oficial.' },
	'eudr.check.poly_plant_split': { es: 'Cobertura en 2020 de las celdas con pérdida arbórea: {harvest} sobre plantación forestal, {forest} sobre bosque cerrado, {savanna} sobre bosque abierto o inundable, {other} sobre otra cobertura (agro/pasturas/mosaico); {conversion} eran bosque en 2020 y hoy figuran como plantación (posible conversión, a verificar). Señal indicativa, no determina causa ni cumplimiento.', en: 'Cover in 2020 of the cells with tree-cover loss: {harvest} over forestry plantation, {forest} over closed forest, {savanna} over open or flooded forest, {other} over other cover (cropland/pasture/mosaic); {conversion} were forest in 2020 and are plantation today (possible conversion, to verify). Indicative — does not determine cause or compliance.', gn: 'Cobertura 2020-pe: {harvest} plantación forestal ári, {forest} ka\'aguy hû\'î ári, {savanna} ka\'aguy ojepe\'áva ári, {other} ambue cobertura ári; {conversion} ka\'aguy 2020-pe ha ko\'ágã plantación (ikatu conversión). Indicativo añónte.', pt: 'Cobertura em 2020 das células com perda arbórea: {harvest} sobre plantação florestal, {forest} sobre floresta fechada, {savanna} sobre floresta aberta ou inundável, {other} sobre outra cobertura (agro/pasto/mosaico); {conversion} eram floresta em 2020 e hoje são plantação (possível conversão, a verificar). Indicativa — não determina causa nem conformidade.' },
	'eudr.check.poly_coverage_warn': { es: 'Solo el {pct}% del polígono cae dentro del área de cobertura (10 provincias). Las métricas se calculan sobre esa porción.', en: 'Only {pct}% of the polygon falls within the coverage area (10 provinces). Metrics are computed over that portion.', gn: '{pct}% polígono oĩ cobertura-pe.', pt: 'Apenas {pct}% do polígono está dentro da área de cobertura (10 províncias). As métricas são calculadas sobre essa porção.' },
	'eudr.check.poly_invalid': { es: 'GeoJSON inválido o sin polígono. Subí un Feature/Geometry de tipo Polygon.', en: 'Invalid GeoJSON or no polygon. Upload a Polygon Feature/Geometry.', gn: 'GeoJSON nahániri.', pt: 'GeoJSON inválido ou sem polígono. Envie um Feature/Geometry do tipo Polygon.' },
	'eudr.check.poly_no_cells': { es: 'El polígono no cubre ninguna celda (¿muy chico o fuera del área?).', en: 'The polygon covers no cells (too small or outside the area?).', gn: 'Polígono ndoguerekói celda.', pt: 'O polígono não cobre nenhuma célula (muito pequeno ou fora da área?).' },
	'eudr.check.poly_too_big_file': { es: 'Archivo demasiado grande (máx. 1 MB).', en: 'File too large (max 1 MB).', gn: 'Archivo tuicha.', pt: 'Arquivo muito grande (máx. 1 MB).' },
	'eudr.check.poly_too_big_area': { es: 'Polígono demasiado grande para análisis interactivo (>100.000 ha). Dividí el área en polígonos más chicos.', en: 'Polygon too large for interactive analysis (>100,000 ha). Split the area into smaller polygons.', gn: 'Polígono tuicha (>100.000 ha).', pt: 'Polígono muito grande para análise interativa (>100.000 ha). Divida a área em polígonos menores.' },

	// ── Navigation (shared across pages) ─────────────────────────────────
	'nav.backToMap': { es: '← Volver al mapa', en: '← Back to map', gn: "← Ejevypa mapa-pe", pt: '← Voltar ao mapa' },
	'nav.printSave': { es: '↓ Imprimir / Guardar PDF', en: '↓ Print / Save PDF', gn: "↓ Emonguejy PDF", pt: '↓ Imprimir / Salvar PDF' },

	// ── Metodología pages ─────────────────────────────────────────────────
	'page.metodologia.title': { es: 'Metodología e indicadores', en: 'Methodology & indicators', gn: "Metodología ha indicadores", pt: 'Metodologia e indicadores' },
	'page.metodologia.kicker': { es: 'Spatia · noreste argentino y regiones transfronterizas', en: 'Spatia · northeast Argentina & cross-border regions', gn: "Spatia · noreste ha ñembyatyrã", pt: 'Spatia · nordeste argentino e regiões transfronteiriças' },
	'page.metodologia.desc': { es: 'Documentación metodológica de cada indicador: fuentes de datos, variables incluidas, método de normalización y limitaciones conocidas. Todos los análisis son reproducibles y auditables.', en: 'Methodological documentation for each indicator: data sources, included variables, normalisation method and known limitations. All analyses are reproducible and auditable.', gn: "Documentación metodológica ojeporúva: fuentes, variables ha limitaciones. Análisis oñemboguapýva ha ojapohápe.", pt: 'Documentação metodológica de cada indicador: fontes de dados, variáveis incluídas, método de normalização e limitações conhecidas. Todas as análises são reproduzíveis e auditáveis.' },
	'page.metodologia.arrowLink': { es: 'Ver metodología →', en: 'View methodology →', gn: "Ehecha metodología →", pt: 'Ver metodologia →' },
	'page.metodologia.kicker.detail': { es: 'Metodología · Spatia · análisis geoespacial abierto', en: 'Methodology · Spatia · open geospatial analysis', gn: "Metodología · Spatia · yvy rekokatu", pt: 'Metodologia · Spatia · análise geoespacial aberta' },
	'page.metodologia.downloadsText': { es: 'Los datos crudos de este análisis pueden descargarse en CSV o GeoJSON desde el panel lateral del mapa una vez seleccionado un departamento.', en: "The raw data for this analysis can be downloaded as CSV or GeoJSON from the map's side panel once a department is selected.", gn: "Mba'ekuaa opyta CSV o GeoJSON-pe oñemosẽ hag̃ua mapa-pe peteĩ departamento oiporávova guive.", pt: 'Os dados brutos desta análise podem ser baixados em CSV ou GeoJSON no painel lateral do mapa após a seleção de um departamento.' },
	'section.variables': { es: 'Variables incluidas', en: 'Variables included', gn: "Variables oñemboguapýva", pt: 'Variáveis incluídas' },
	'section.downloads': { es: 'Datos descargables', en: 'Downloadable data', gn: "Mba'ekuaa oñemosẽ hag̃ua", pt: 'Dados para download' },
	'section.citation': { es: 'Citación sugerida', en: 'Suggested citation', gn: "Omombe hag̃ua rehegua", pt: 'Citação sugerida' },
} as any;

class I18nStore {
	locale: Locale = $state(
		typeof window !== 'undefined'
			? ((localStorage.getItem('nealab-locale') as Locale) ?? 'es')
			: 'es'
	);

	t(key: string): string {
		const entry = dict[key];
		if (!entry) return key;
		return entry[this.locale] ?? entry['en'] ?? key;
	}

	setLocale(l: Locale) {
		this.locale = l;
		if (typeof window !== 'undefined') localStorage.setItem('nealab-locale', l);
	}
}

export const i18n = new I18nStore();
export function t(key: string): string { return i18n.t(key); }
