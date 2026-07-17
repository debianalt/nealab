export type Locale = 'es' | 'en' | 'gn' | 'pt';

const dict: Record<string, Record<Locale, string>> = {
	// Header
	'header.title': { es: 'nealab', en: 'nealab', gn: 'nealab', pt: 'nealab' },
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
	'legend.buildingVolumeNote': { es: 'medición GBA — comparable entre países', en: 'GBA measurement — cross-country comparable', gn: 'GBA — tetã joja', pt: 'medição GBA — comparável entre países' },
	'legend.buildingsCanvas': { es: 'Edificios — Global Building Atlas', en: 'Buildings — Global Building Atlas', gn: 'Óga — Global Building Atlas', pt: 'Edifícios — Global Building Atlas' },
	'analysis.scores.score': { es: 'Score de consolidación', en: 'Consolidation score', gn: 'Score consolidación', pt: 'Score de consolidação' },
	'analysis.socio.score': { es: 'Score sociodemográfico', en: 'Sociodemographic score', gn: 'Score sociodemográfico', pt: 'Score sociodemográfico' },
	'analysis.economic.score': { es: 'Score actividad económica', en: 'Economic activity score', gn: 'Score económico', pt: 'Score de atividade econômica' },
	'analysis.accessibility.score': { es: 'Score accesibilidad', en: 'Accessibility score', gn: 'Score accesibilidad', pt: 'Score de acessibilidade' },
	'legend.low': { es: 'Bajo', en: 'Low', gn: 'Michĩ', pt: 'Baixo' },
	'legend.medium': { es: 'Medio', en: 'Medium', gn: 'Mbyte', pt: 'Médio' },
	'legend.high': { es: 'Alto', en: 'High', gn: 'Yvate', pt: 'Alto' },
	'legend.veryLow': { es: 'Muy bajo', en: 'Very low', gn: 'Michĩmĩ', pt: 'Muito baixo' },
	'legend.lowRisk': { es: 'Bajo riesgo', en: 'Low risk', gn: 'Michĩ riesgo', pt: 'Baixo risco' },
	'legend.highRisk': { es: 'Alto riesgo', en: 'High risk', gn: 'Yvate riesgo', pt: 'Alto risco' },
	// Per-analysis legend labels
	'legend.flood.low': { es: 'Baja presencia histórica', en: 'Low historical presence', gn: 'Michĩ y historia rehegua', pt: 'Baixa presença histórica' },
	'legend.flood.high': { es: 'Alta presencia histórica', en: 'High historical presence', gn: 'Yvate y historia rehegua', pt: 'Alta presença histórica' },
	'legend.locValue.low': { es: 'Peor ubicación', en: 'Worse location', gn: 'Michĩ valor', pt: 'Pior localização' },
	'legend.locValue.high': { es: 'Mejor ubicación', en: 'Better location', gn: 'Tuicha valor', pt: 'Melhor localização' },
	'legend.agri.low': { es: 'Menor aptitud', en: 'Less suitable', gn: 'Michĩ aptitud', pt: 'Menor aptidão' },
	'legend.agri.high': { es: 'Mayor aptitud', en: 'More suitable', gn: 'Tuicha aptitud', pt: 'Maior aptidão' },
	'legend.forestry.low': { es: 'Menor precipitación anual', en: 'Lower annual rainfall', gn: "Michĩ ama", pt: 'Menor precipitação anual' },
	'legend.forestry.high': { es: 'Mayor precipitación anual', en: 'Higher annual rainfall', gn: "Tuicha ama", pt: 'Maior precipitação anual' },
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
	'legend.eudr.low': { es: 'Bajo riesgo EUDR', en: 'Low EUDR risk', gn: 'Michĩ EUDR', pt: 'Baixo risco EUDR' },
	'legend.eudr.high': { es: 'Alto riesgo EUDR', en: 'High EUDR risk', gn: 'Yvate EUDR', pt: 'Alto risco EUDR' },
	'legend.accessibility.low': { es: 'Cerca del capital', en: 'Near capital', gn: 'Peteĩ', pt: 'Perto da capital' },
	'legend.accessibility.high': { es: 'Lejos del capital', en: 'Far from capital', gn: 'Mombyry', pt: 'Longe da capital' },
	'legend.socio.low': { es: 'Bajo NBI', en: 'Low NBI', gn: 'Michĩ NBI', pt: 'Baixo NBI' },
	'legend.socio.high': { es: 'Alto NBI', en: 'High NBI', gn: 'Tuicha NBI', pt: 'Alto NBI' },
	'legend.economic.low': { es: 'Poca actividad nocturna', en: 'Low nighttime activity', gn: 'Michĩ', pt: 'Pouca atividade noturna' },
	'legend.economic.high': { es: 'Alta actividad nocturna', en: 'High nighttime activity', gn: 'Tuicha', pt: 'Alta atividade noturna' },
	'legend.scores.low': { es: 'Menor consolidación', en: 'Less consolidated', gn: 'Michĩ', pt: 'Menor consolidação' },
	'legend.scores.high': { es: 'Mayor consolidación', en: 'More consolidated', gn: 'Tuicha', pt: 'Maior consolidação' },
	'legend.range': { es: 'Score 0–100', en: 'Score 0–100', gn: 'Score 0–100', pt: 'Score 0–100' },
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
	'menu.group.comparable': { es: '↔ Comparables entre territorios', en: '↔ Comparable across territories', gn: '↔ Comparables entre territorios', pt: '↔ Comparáveis entre territórios' }, // gn: pendiente
	'menu.group.localOnly': { es: 'Solo {territory}', en: 'Only {territory}', gn: 'Solo {territory}', pt: 'Apenas {territory}' }, // gn: pendiente
	'menu.group.thisTerritory': { es: 'este territorio', en: 'this territory', gn: 'este territorio', pt: 'este território' }, // gn: pendiente
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
	'map.hint.zoomNeighbours': { es: 'Acercá para ver los hexágonos de los territorios vecinos', en: 'Zoom in to load neighbouring territories', gn: 'Acercá para ver los hexágonos de los territorios vecinos', pt: 'Aproxime para ver os hexágonos dos territórios vizinhos' }, // gn: pendiente
	'map.hint.aggregated': { es: 'Vista agregada — promedios espaciales · acercá para ver el detalle por hexágono', en: 'Aggregated overview — spatial means · zoom in for per-hex detail', gn: 'Vista agregada — promedios espaciales · acercá para ver el detalle por hexágono', pt: 'Vista agregada — médias espaciais · aproxime para ver o detalhe por hexágono' }, // gn: pendiente
	'map.basemap.map': { es: 'Mapa', en: 'Map', gn: 'Mapa', pt: 'Mapa' }, // gn: pendiente
	'map.basemap.satellite': { es: 'Satélite', en: 'Satellite', gn: 'Satélite', pt: 'Satélite' }, // gn: pendiente
	'map.opacity': { es: 'Opacidad', en: 'Opacity', gn: 'Opacidad', pt: 'Opacidade' }, // gn: pendiente

	// ── Analysis panel ──
	'analysis.noDeptData': { es: 'No hay datos departamentales para este territorio.', en: 'No department-level data for this territory.', gn: 'No hay datos departamentales para este territorio.', pt: 'Não há dados departamentais para este território.' }, // gn: pendiente
	'analysis.distByTerritory': { es: 'Distribución por territorio /100', en: 'Distribution by territory /100', gn: 'Distribución por territorio /100', pt: 'Distribuição por território /100' }, // gn: pendiente

	// ── Chart panels ──
	// Hints and empty states that were Spanish literals in the markup. The two *.note
	// keys carry <em> and render through {@html}, same as the methodology blocks.
	'chart.hexDblClick': { es: 'hex · doble-clic para limpiar', en: 'hex · double-click to clear', gn: 'hex · doble-clic para limpiar', pt: 'hex · duplo clique para limpar' }, // gn: pendiente
	'chart.loadError': { es: 'error al cargar datos', en: 'error loading data', gn: 'error al cargar datos', pt: 'erro ao carregar dados' }, // gn: pendiente
	'chart.bivariate.pts': { es: 'pts · arrastrá para seleccionar', en: 'pts · drag to select', gn: 'pts · arrastrá para seleccionar', pt: 'pts · arraste para selecionar' }, // gn: pendiente
	'chart.bivariate.pickY': { es: 'elegí un eje Y abajo', en: 'pick a Y axis below', gn: 'elegí un eje Y abajo', pt: 'escolha um eixo Y abaixo' }, // gn: pendiente
	'chart.bivariate.intro': { es: 'Seleccioná un segundo análisis en "Eje Y" para explorar correlaciones y seleccionar hexágonos en el mapa.', en: 'Select a second analysis under "Y axis" to explore correlations and select hexagons on the map.', gn: 'Seleccioná un segundo análisis en "Eje Y" para explorar correlaciones y seleccionar hexágonos en el mapa.', pt: 'Selecione uma segunda análise em "Eixo Y" para explorar correlações e selecionar hexágonos no mapa.' }, // gn: pendiente
	'chart.bivariate.noCommon': { es: 'sin hexágonos en común', en: 'no hexagons in common', gn: 'sin hexágonos en común', pt: 'sem hexágonos em comum' }, // gn: pendiente
	'chart.bump.hint': { es: 'pasá el mouse para explorar', en: 'hover to explore', gn: 'pasá el mouse para explorar', pt: 'passe o mouse para explorar' }, // gn: pendiente
	'chart.bump.axis': { es: 'posición relativa (#1 = mejor)', en: 'relative rank (#1 = best)', gn: 'posición relativa (#1 = mejor)', pt: 'posição relativa (#1 = melhor)' }, // gn: pendiente
	'chart.bump.note': { es: '#1 = mejor posición siempre · En <em>riesgo/aislamiento</em> (Inund., Amb., Acceso): #1 = menos expuesto · En <em>potencial</em> (resto): #1 = mayor valor', en: '#1 = best rank always · Under <em>risk/isolation</em> (Flood, Env., Access): #1 = least exposed · Under <em>potential</em> (the rest): #1 = highest value', gn: '#1 = mejor posición siempre · En <em>riesgo/aislamiento</em> (Inund., Amb., Acceso): #1 = menos expuesto · En <em>potencial</em> (resto): #1 = mayor valor', pt: '#1 = melhor posição sempre · Em <em>risco/isolamento</em> (Inund., Amb., Acesso): #1 = menos exposto · Em <em>potencial</em> (o resto): #1 = maior valor' }, // gn: pendiente
	'chart.bump.noData': { es: 'sin datos comparables disponibles', en: 'no comparable data available', gn: 'sin datos comparables disponibles', pt: 'sem dados comparáveis disponíveis' }, // gn: pendiente
	'chart.histogram.dragRange': { es: 'arrastrá para seleccionar rango', en: 'drag to select a range', gn: 'arrastrá para seleccionar rango', pt: 'arraste para selecionar o intervalo' }, // gn: pendiente
	'chart.parallel.noIntersection': { es: '0 hex en intersección', en: '0 hex in intersection', gn: '0 hex en intersección', pt: '0 hex na interseção' }, // gn: pendiente
	'chart.parallel.hint': { es: 'pasá el mouse sobre una variable y arrastrá', en: 'hover a variable and drag', gn: 'pasá el mouse sobre una variable y arrastrá', pt: 'passe o mouse sobre uma variável e arraste' }, // gn: pendiente
	'chart.parallel.note': { es: 'Pasá el mouse sobre una variable y <em>arrastrá para filtrar</em> por rango · El <em>×</em> sobre cada filtro activo lo limpia individualmente · "Limpiar todo" quita todos los filtros', en: 'Hover a variable and <em>drag to filter</em> by range · The <em>×</em> on each active filter clears it individually · "Clear all" removes every filter', gn: 'Pasá el mouse sobre una variable y <em>arrastrá para filtrar</em> por rango · El <em>×</em> sobre cada filtro activo lo limpia individualmente · "Limpiar todo" quita todos los filtros', pt: 'Passe o mouse sobre uma variável e <em>arraste para filtrar</em> por intervalo · O <em>×</em> sobre cada filtro ativo o limpa individualmente · "Limpar tudo" remove todos os filtros' }, // gn: pendiente
	'chart.moran.insufficient': { es: 'Datos insuficientes para calcular autocorrelación', en: 'Not enough data to compute autocorrelation', gn: 'Datos insuficientes para calcular autocorrelación', pt: 'Dados insuficientes para calcular autocorrelação' }, // gn: pendiente
	'chart.moran.loadLayer': { es: 'Cargá una capa para calcular autocorrelación espacial', en: 'Load a layer to compute spatial autocorrelation', gn: 'Cargá una capa para calcular autocorrelación espacial', pt: 'Carregue uma camada para calcular autocorrelação espacial' }, // gn: pendiente

	// ── Comparison panels ──
	'common.noMatches': { es: 'Sin coincidencias', en: 'No matches', gn: 'Sin coincidencias', pt: 'Sem correspondências' }, // gn: pendiente
	'common.noResults': { es: 'Sin resultados', en: 'No results', gn: 'Sin resultados', pt: 'Sem resultados' }, // gn: pendiente
	'panel.compareWith': { es: 'Comparar con…', en: 'Compare with…', gn: 'Comparar con…', pt: 'Comparar com…' }, // gn: pendiente
	'panel.selectDistrict': { es: 'Seleccioná un distrito', en: 'Select a district', gn: 'Seleccioná un distrito', pt: 'Selecione um distrito' }, // gn: pendiente
	'panel.percentileNote': { es: 'Percentiles calculados dentro de cada provincia. El eje 50 = mediana provincial.', en: 'Percentiles computed within each province. The 50 axis = provincial median.', gn: 'Percentiles calculados dentro de cada provincia. El eje 50 = mediana provincial.', pt: 'Percentis calculados dentro de cada província. O eixo 50 = mediana provincial.' }, // gn: pendiente
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
	'flood.petalNote': { es: 'Relativo al promedio provincial (50 = promedio). Mayor extensión = mayor riesgo.', en: 'Relative to the provincial average (50 = average). Larger extent = higher risk.', gn: 'Relativo al promedio provincial (50 = promedio). Mayor extensión = mayor riesgo.', pt: 'Relativo à média provincial (50 = média). Maior extensão = maior risco.' }, // gn: pendiente
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
	'label.br.sewerNetwork':     { es: 'Esgoto adecuado (%)',  en: 'Adequate sewage (%)',   gn: 'Ykua porã (%)',   pt: 'Esgoto adequado (%)' },
	'label.br.garbageCollected': { es: 'Basura recolectada (%)', en: 'Garbage collected (%)', gn: 'Taky jeheja (%)', pt: 'Lixo coletado (%)' },
	'label.br.literacy':         { es: 'Alfabetismo (%)',      en: 'Literacy (%)',          gn: 'Moñe\'ẽ porã (%)', pt: 'Alfabetismo (%)' },
	'label.br.noBathroom':       { es: 'Sin baño exclusivo (%)', en: 'No private bathroom (%)', gn: 'Ysyryrogue ỹre (%)', pt: 'Sem banheiro exclusivo (%)' },
	'label.sewerage': { es: 'Cloacas (%)', en: 'Sewerage (%)', gn: 'Ykuaa (%)', pt: 'Esgoto (%)' },
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
	'analysis.coverage.unavailable': { es: 'no disponible en este territorio', en: 'not available in this territory', gn: 'ndaipóri ko tetãme', pt: 'não disponível neste território' },
	'analysis.back': { es: '← Análisis', en: '← Analyses', gn: "← Mba'ekuaa", pt: '← Análises' },
	'analysis.noData': { es: 'Sin datos para este radio', en: 'No data for this radio', gn: "Mba'ekuaa'ỹ ko radio-pe", pt: 'Sem dados para este setor' },
	'analysis.loading': { es: 'Cargando datos...', en: 'Loading data...', gn: "Oñemyenyhẽ mba'ekuaa...", pt: 'Carregando dados...' },
	'analysis.petalHint': { es: 'Percentil del territorio (0-100). Mayor extensión = mayor intensidad relativa.', en: 'Territory percentile (0-100). Larger petal = higher relative intensity.', gn: 'Percentil tetãme (0-100).', pt: 'Percentil do território (0-100). Maior extensão = maior intensidade relativa.' },
	'analysis.comingSoon.body': { es: 'Este análisis está en desarrollo. Próximamente disponible con datos actualizados.', en: 'This analysis is under development. Coming soon with updated data.', gn: "Ko mba'ekuaa oñemoĩhína. Oguerahátama.", pt: 'Esta análise está em desenvolvimento. Em breve disponível com dados atualizados.' },

	// Flood risk analysis
	'analysis.floodRisk.title': { es: '¿Dónde es mayor el riesgo de inundación?', en: 'Where is flood risk highest?', gn: "Moõpa ysoguy mba'asy tuichavéva?", pt: 'Onde o risco de inundação é maior?' },
	'analysis.floodRisk.desc': { es: 'Presencia histórica de agua (JRC 1984–2021) e inundación actual (Sentinel-1 SAR) por hexágono', en: 'Historical water presence (JRC 1984–2021) and current flooding (Sentinel-1 SAR) per hexagon', gn: "Y rehegua historia guive (JRC 1984–2021) ha ko'ag̃a ysoguy (Sentinel-1 SAR) yvy rupi", pt: 'Presença histórica de água (JRC 1984–2021) e inundação atual (Sentinel-1 SAR) por hexágono' },
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
	'analysis.flood.clickHint': { es: 'Hacé click en una parcela para ver detalle', en: 'Click a parcel for details', gn: 'Ehesakutu peteĩ yvy ehecha hag̃ua', pt: 'Clique em uma parcela para ver detalhes' },
	'analysis.flood.source': { es: 'Fuente: JRC Global Surface Water (Landsat, 1984–2021) + Sentinel-1 SAR (Copernicus)', en: 'Source: JRC Global Surface Water (Landsat, 1984–2021) + Sentinel-1 SAR (Copernicus)', gn: 'Moñe\'ẽha: JRC Global Surface Water (Landsat, 1984–2021) + Sentinel-1 SAR (Copernicus)' , pt: 'Fonte: JRC Global Surface Water (Landsat, 1984–2021) + Sentinel-1 SAR (Copernicus)' },
	'data.source.censo': { es: 'Fuente: INDEC — Censo Nacional de Población 2022', en: 'Source: INDEC — National Population Census 2022', gn: "Moñe'ẽha: INDEC — Censo Nacional 2022", pt: 'Fonte: INDEC — Censo Nacional de Población 2022' },
	'data.source.realEstate': { es: 'Fuente: Relevamiento de mercado inmobiliario', en: 'Source: Real estate market survey', gn: "Moñe'ẽha: Relevamiento óga ñemuhague", pt: 'Fonte: Levantamento do mercado imobiliário' },
	'data.source.buildings': { es: 'Fuente: Detección por IA sobre imágenes satelitales', en: 'Source: AI detection on satellite imagery', gn: "Moñe'ẽha: IA ohechaukáva satélite ra'ãnga rupi", pt: 'Fonte: Detecção por IA em imagens de satélite' },
	'data.source.catastro': { es: 'Fuente: Dirección General de Catastro, Misiones', en: 'Source: Dirección General de Catastro, Misiones', gn: "Moñe'ẽha: Catastro, Misiones", pt: 'Fonte: Dirección General de Catastro, Misiones' },
	'data.source.overture': { es: 'Fuente: Overture Maps Foundation via walkthru.earth (CC BY 4.0)', en: 'Source: Overture Maps Foundation via walkthru.earth (CC BY 4.0)', gn: "Moñe'ẽha: Overture Maps Foundation (CC BY 4.0)", pt: 'Fonte: Overture Maps Foundation via walkthru.earth (CC BY 4.0)' },
	'data.source.satellite': { es: 'Fuente: MODIS, Landsat, ERA5, SoilGrids, VIIRS, Hansen GFC', en: 'Source: MODIS, Landsat, ERA5, SoilGrids, VIIRS, Hansen GFC', gn: "Moñe'ẽha: MODIS, Landsat, VIIRS", pt: 'Fonte: MODIS, Landsat, ERA5, SoilGrids, VIIRS, Hansen GFC' },

	// ── EUDR variables ──
	'eudr.riskScore': { es: 'Score de riesgo EUDR', en: 'EUDR risk score', gn: 'EUDR riesgo', pt: 'Score de risco EUDR' },
	'eudr.lossPost2020': { es: 'Pérdida post-2020 (%)', en: 'Post-2020 loss (%)', gn: 'Pérdida post-2020', pt: 'Perda pós-2020 (%)' },
	'eudr.firePost2020': { es: 'Fuego post-2020 (%)', en: 'Post-2020 fire (%)', gn: 'Fuego post-2020', pt: 'Fogo pós-2020 (%)' },
	'eudr.forest2020': { es: 'Cobertura forestal 2020 (%)', en: 'Forest cover 2020 (%)', gn: "Ka'aguy 2020 (%)", pt: 'Cobertura florestal 2020 (%)' },
	'eudr.forestCurrent': { es: 'Cobertura forestal actual (%)', en: 'Current forest cover (%)', gn: "Ka'aguy ko'ãga (%)", pt: 'Cobertura florestal atual (%)' },

	// ── Migrated radio/catastro to H3 ──
	'analysis.scores.type': { es: 'Tipo de consolidación', en: 'Consolidation type', gn: 'Tipo', pt: 'Tipo de consolidação' },
	'analysis.scores.typeLabel': { es: 'Etiqueta', en: 'Label', gn: 'Label', pt: 'Rótulo' },
	'analysis.socio.type': { es: 'Tipo sociodemografico', en: 'Sociodemographic type', gn: 'Tipo', pt: 'Tipo sociodemográfico' },
	'analysis.socio.typeLabel': { es: 'Etiqueta', en: 'Label', gn: 'Label', pt: 'Rótulo' },
	'analysis.economic.type': { es: 'Tipo economico', en: 'Economic type', gn: 'Tipo', pt: 'Tipo econômico' },
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
	'radio.densidad': { es: 'Densidad hab/km2', en: 'Pop. density/km2', gn: 'Densidad', pt: 'Densidade hab/km²' },
	'radio.nbi': { es: 'NBI (%)', en: 'UBN (%)', gn: 'NBI', pt: 'NBI (%)' },
	'radio.hacinamiento': { es: 'Hacinamiento (%)', en: 'Overcrowding (%)', gn: 'Hacinamiento', pt: 'Superlotação (%)' },
	'radio.propietario': { es: 'Propietarios (%)', en: 'Homeowners (%)', gn: 'Propietario', pt: 'Proprietários (%)' },
	'radio.tamHogar': { es: 'Tam. medio hogar', en: 'Avg household size', gn: 'Hogar', pt: 'Tam. médio do lar' },
	'radio.computadora': { es: 'Computadora (%)', en: 'Computer (%)', gn: 'Computadora', pt: 'Computador (%)' },
	'radio.densidad.pctl': { es: 'Densidad (pctl)', en: 'Density (pctl)', gn: 'Densidad (pctl)', pt: 'Densidade (pctl)' },
	'radio.nbi.pctl': { es: 'NBI (pctl)', en: 'UBN (pctl)', gn: 'NBI (pctl)', pt: 'NBI (pctl)' },
	'radio.hacinamiento.pctl': { es: 'Hacinamiento (pctl)', en: 'Overcrowding (pctl)', gn: 'Hacinamiento (pctl)', pt: 'Superlotação (pctl)' },
	'radio.propietario.pctl': { es: 'Propietarios (pctl)', en: 'Homeowners (pctl)', gn: 'Propietario (pctl)', pt: 'Proprietários (pctl)' },
	'radio.tamHogar.pctl': { es: 'Hogar (pctl)', en: 'Household size (pctl)', gn: 'Hogar (pctl)', pt: 'Lar (pctl)' },
	'radio.computadora.pctl': { es: 'Computadora (pctl)', en: 'Computer (pctl)', gn: 'Computadora (pctl)', pt: 'Computador (pctl)' },
	'radio.empleo': { es: 'Tasa empleo', en: 'Employment rate', gn: 'Empleo', pt: 'Taxa de emprego' },
	'radio.actividad': { es: 'Tasa actividad', en: 'Activity rate', gn: 'Actividad', pt: 'Taxa de atividade' },
	'radio.universitario': { es: 'Universitarios (%)', en: 'University (%)', gn: 'Universidad', pt: 'Universitários (%)' },
	'radio.viirs': { es: 'Luces nocturnas', en: 'Night lights', gn: 'Luces', pt: 'Luzes noturnas' },
	'radio.buildingDensity': { es: 'Densidad edilicia', en: 'Building density', gn: 'Oga', pt: 'Densidade edilícia' },
	'radio.travelCapital': { es: 'Min. a capital', en: 'Min. to capital', gn: 'Capital', pt: 'Min. até capital' },
	'radio.travelCabecera': { es: 'Min. a cabecera', en: 'Min. to dept. seat', gn: 'Cabecera', pt: 'Min. até sede municipal' },
	'radio.distHospital': { es: 'Dist. hospital (km)', en: 'Dist. hospital (km)', gn: 'Hospital', pt: 'Dist. hospital (km)' },
	'radio.distSecundaria': { es: 'Dist. secundaria (km)', en: 'Dist. secondary school', gn: 'Secundaria', pt: 'Dist. ensino médio (km)' },
	'radio.distPrimaria': { es: 'Dist. ruta (km)', en: 'Dist. primary road (km)', gn: 'Tape', pt: 'Dist. rodovia (km)' },

	// ── EUDR in analysis menu ──
	'trade.eudr.open_checker': { es: 'Abrir verificador EUDR', en: 'Open EUDR checker', gn: 'EUDR checker', pt: 'Abrir verificador EUDR' },
	'trade.eudr.analysis_title': { es: 'EUDR · Riesgo de deforestación (NEA argentino + Paraguay + sur de Brasil)', en: 'EUDR · Deforestation risk (NE Argentina + Paraguay + southern Brazil)', gn: 'EUDR · Deforestación (NEA Argentina + Paraguay + Brasil sur)', pt: 'EUDR · Risco de desmatamento (NEA argentino + Paraguai + sul do Brasil)' },
	'trade.eudr.analysis_desc': { es: 'Verificación de deforestación para parcelas de producción (Hansen GFC 30 m + MODIS fire)', en: 'Deforestation verification for production plots (Hansen GFC 30m + MODIS fire)', gn: 'EUDR', pt: 'Verificação de desmatamento para parcelas de produção (Hansen GFC 30 m + MODIS fire)' },

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
	'temporal.hint.deforestation_dynamics.delta': { es: 'Cambio en tasa: positivo = acelero, negativo = freno. Fuente: Hansen/Landsat.', en: 'Rate change: positive = accelerated, negative = slowed. Source: Hansen/Landsat.', gn: "Moambue okany", pt: 'Variação na taxa: positivo = acelerou, negativo = desacelerou. Fonte: Hansen/Landsat.' },
	'temporal.hint.pm25_drivers.current': { es: 'PM2.5 periodo 2013-2022. Fuente: ACAG V6 (satelital).', en: 'PM2.5 period 2013-2022. Source: ACAG V6 (satellite).', gn: "2013-2022 PM2.5", pt: 'PM2.5 período 2013-2022. Fonte: ACAG V6 (satelital).' },
	'temporal.hint.pm25_drivers.baseline': { es: 'PM2.5 periodo 2001-2010. Fuente: ACAG V6 (satelital).', en: 'PM2.5 period 2001-2010. Source: ACAG V6 (satellite).', gn: "2001-2010 PM2.5", pt: 'PM2.5 período 2001-2010. Fonte: ACAG V6 (satelital).' },
	'temporal.hint.pm25_drivers.delta': { es: 'Cambio en PM2.5: positivo = empeoro, negativo = mejoro.', en: 'PM2.5 change: positive = worsened, negative = improved.', gn: "Moambue PM2.5", pt: 'Variação no PM2.5: positivo = piorou, negativo = melhorou.' },
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
	'side.censoDept.title': { es: 'Departamentos — totales censales', en: 'Departments — census totals', gn: 'Departamento kuéra — censo papapy', pt: 'Departamentos — totais censais' },
	'side.censoDept.subtitle': { es: 'Totales oficiales INDEC por departamento · click para ver la trayectoria 1991→2022 y ubicarlo en el mapa', en: 'Official INDEC totals by department · click for the 1991→2022 trajectory and its location on the map', gn: 'INDEC papapy oficiál departamento rehegua · eiporavo ehecha hag̃ua 1991→2022 rape ha mapa-pe', pt: 'Totais oficiais INDEC por departamento · clique para ver a trajetória 1991→2022 e localizá-lo no mapa' },
	'temporal.hint.carbon_stock.current': { es: 'Biomasa aérea 2022 (ESA CCI), NPP 2022-2024 (MODIS), bosque remanente 2024 y tasa de deforestación 2021-2024 (Hansen). Estáticos: GEDI L4B, SoilGrids SOC, GFW Flux.', en: 'AGB 2022 (ESA CCI), NPP 2022-2024 (MODIS), standing tree cover 2024 and deforestation rate 2021-2024 (Hansen). Static: GEDI L4B, SoilGrids SOC, GFW Flux.', gn: "2022 AGB + NPP 2022-2024 + bosque 2024", pt: 'Biomassa aérea 2022 (ESA CCI), NPP 2022-2024 (MODIS), floresta remanescente 2024 e taxa de desmatamento 2021-2024 (Hansen). Estáticos: GEDI L4B, SoilGrids SOC, GFW Flux.' },
	'temporal.hint.carbon_stock.baseline': { es: 'Biomasa aérea promedio 2018-2020 (ESA CCI), NPP 2018-2020 (MODIS), bosque remanente 2020 y tasa de deforestación 2001-2020 (Hansen). Mismas fuentes, distinto periodo.', en: 'AGB average 2018-2020 (ESA CCI), NPP 2018-2020 (MODIS), standing tree cover 2020 and deforestation rate 2001-2020 (Hansen). Same sources, earlier period.', gn: "2018-2020 AGB + NPP + bosque", pt: 'Biomassa aérea média 2018-2020 (ESA CCI), NPP 2018-2020 (MODIS), floresta remanescente 2020 e taxa de desmatamento 2001-2020 (Hansen). Mesmas fontes, período anterior.' },
	'temporal.hint.carbon_stock.delta': { es: 'Cambio combinado: biomasa, productividad (NPP), bosque remanente y aceleración de deforestación. Verde = mejora (rebrote, más carbono, menos pérdida); rojo = degradación o pérdida acelerada.', en: 'Combined change: biomass, productivity (NPP), standing forest and deforestation acceleration. Green = improvement; red = degradation or accelerated loss.', gn: "Moambue biomasa + NPP + bosque + okany", pt: 'Variação combinada: biomassa, produtividade (NPP), floresta remanescente e aceleração do desmatamento. Verde = melhora; vermelho = degradação ou perda acelerada.' },
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
	'sat.deprivation.desc': { es: 'Carencia de infraestructura básica: agua, cloacas, techo, piso, combustible y hacinamiento', en: 'Basic infrastructure deprivation: water, sewage, roof, floor, fuel and overcrowding', gn: "Mba'e'ỹ", pt: 'Carência de infraestrutura básica: água, esgoto, teto, piso, combustível e superlotação' },
	'sat.deprivation.nbi': { es: 'Hogares con NBI', en: 'Households with UBN', gn: 'NBI', pt: 'Lares com NBI' },
	'sat.deprivation.sinAgua': { es: 'Sin red de agua', en: 'No water network', gn: "Y'ỹ", pt: 'Sem rede de água' },
	'sat.deprivation.sinCloacas': { es: 'Sin cloacas', en: 'No sewage', gn: "Cloaca'ỹ", pt: 'Sem esgoto' },
	'sat.deprivation.piso': { es: 'Sin piso revestido', en: 'Unfinished floor', gn: 'Yvy ñembojeguáava', pt: 'Sem piso revestido' },
	'sat.deprivation.hacinamiento': { es: 'Hacinamiento', en: 'Overcrowding', gn: "Heta óga peteĩme", pt: 'Superlotação' },
	'sat.deprivation.hacinamientoCrit': { es: 'Hacinamiento crítico', en: 'Critical overcrowding', gn: "Heta tuicha óga peteĩme", pt: 'Superlotação crítica' },
	'sat.deprivation.sinComputadora': { es: 'Sin computadora', en: 'No computer', gn: "Computadora'ỹ", pt: 'Sem computador' },
	'sat.deprivation.combustible': { es: 'Combustible precario', en: 'Precarious fuel', gn: "Tata'ỹ", pt: 'Combustível precário' },
	'sat.deprivation.sinTecho': { es: 'Techo inadecuado', en: 'Inadequate roof', gn: "Óga akã vai", pt: 'Teto inadequado' },
	'sat.deprivation.type': { es: 'Tipo de carencia', en: 'Deprivation type', gn: "Mba'e'ỹ laja", pt: 'Tipo de carência' },
	'sat.deprivation.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
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
	'sat.health.time': { es: 'Tiempo al centro de salud', en: 'Time to healthcare facility', gn: "Tasyo tape", pt: 'Tempo ao centro de saúde' },
	'sat.health.coverage': { es: 'Cobertura de salud', en: 'Health coverage', gn: "Tasyo rehegua", pt: 'Cobertura de saúde' },
	'sat.health.nbi': { es: 'Hogares con NBI', en: 'Households with UBN', gn: 'NBI', pt: 'Lares com NBI' },
	'sat.health.elderly': { es: 'Adultos mayores (%)', en: 'Elderly (%)', gn: "Tuja (%)", pt: 'Idosos (%)' },
	'sat.health.children': { es: 'Menores de 18 (%)', en: 'Under 18 (%)', gn: "Mitã (%)", pt: 'Menores de 18 (%)' },
	'sat.health.popDensity': { es: 'Densidad poblacional', en: 'Pop. density', gn: "Yvypóra tuichakue", pt: 'Densidade populacional' },
	'sat.health.roadDensity': { es: 'Densidad vial', en: 'Road density', gn: "Tape heta", pt: 'Densidade viária' },
	'sat.health.nightlights': { es: 'Luces nocturnas', en: 'Night lights', gn: "Tesa'y", pt: 'Luzes noturnas' },
	'sat.health.accessCities': { es: 'Acceso a ciudades', en: 'City access', gn: "Táva gotyo", pt: 'Acesso a cidades' },
	'sat.health.distPrimary': { es: 'Dist. a escuela primaria', en: 'Primary school distance', gn: "Mbo'ehao gotyo", pt: 'Dist. a escola primária' },
	'sat.health.type': { es: 'Tipo de acceso', en: 'Access type', gn: 'Tasyo laja', pt: 'Tipo de acesso' },
	'sat.health.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'sat.eduCap.title': { es: '¿Dónde hay menor capital educativo?', en: 'Where is education capital lowest?', gn: "Moõpa oĩ'ỹve mbo'ehao?", pt: 'Onde há menor capital educacional?' },
	'sat.eduCap.desc': { es: 'Capital educativo: nivel de instrucción acumulado de la población (sin instrucción, primaria, secundario, terciario, universitario)', en: 'Education capital: accumulated schooling level (no schooling, primary, secondary, tertiary, university)', gn: "Mbo'ehao capital", pt: 'Capital educacional: nível de instrução acumulado da população (sem instrução, ensino fundamental, médio, técnico, universitário)' },
	'sat.eduCap.noSchooling': { es: 'Sin instrucción', en: 'No schooling', gn: "Mbo'ehao'ỹ", pt: 'Sem instrução' },
	'sat.eduCap.secondaryPlus': { es: 'Secundario completo o más', en: 'Secondary or higher', gn: "Mokõiha+", pt: 'Ensino médio completo ou mais' },
	'sat.eduCap.higherEdu': { es: 'Educación superior', en: 'Higher education', gn: "Mbo'ehao yvate", pt: 'Educação superior' },
	'sat.eduCap.university': { es: 'Universitario', en: 'University', gn: "Universidad", pt: 'Universitário' },
	'sat.eduCap.type': { es: 'Tipo', en: 'Type', gn: 'Laja', pt: 'Tipo' },
	'sat.eduCap.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'sat.eduFlow.title': { es: '¿Dónde hay más problemas educativos?', en: 'Where are education problems greatest?', gn: "Moõpa oĩve mbo'ehao apañuãi?", pt: 'Onde há mais problemas educacionais?' },
	'sat.eduFlow.desc': { es: 'Flujo educativo: inasistencia primaria y secundaria, embarazo adolescente', en: 'Education flow: primary and secondary dropout, teen pregnancy', gn: "Mbo'ehao flujo", pt: 'Fluxo educacional: abandono escolar no ensino fundamental e médio, gravidez na adolescência' },
	'sat.eduFlow.dropoutPrimary': { es: 'Inasistencia 6-12 años', en: 'Dropout 6-12 yrs', gn: "Oheja 6-12", pt: 'Infrequência 6-12 anos' },
	'sat.eduFlow.dropoutSecondary': { es: 'Inasistencia 13-18 años', en: 'Dropout 13-18 yrs', gn: "Oheja 13-18", pt: 'Infrequência 13-18 anos' },
	'sat.eduFlow.teenPregnancy': { es: 'Maternidad adolescente', en: 'Teen pregnancy', gn: "Mitãkuña memby", pt: 'Maternidade adolescente' },
	'sat.eduFlow.nbi': { es: 'NBI', en: 'Unmet basic needs', gn: 'NBI', pt: 'NBI' },
	'sat.eduFlow.youthPct': { es: '% jóvenes 18-29', en: '% youth 18-29', gn: "Mitã 18-29", pt: '% jovens 18-29' },
	'sat.eduFlow.femaleHeaded': { es: 'Jefatura femenina', en: 'Female-headed', gn: "Kuña ñangareko", pt: 'Chefia feminina' },
	'sat.eduFlow.type': { es: 'Tipo', en: 'Type', gn: 'Laja', pt: 'Tipo' },
	'sat.eduFlow.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	// ── Carbon Stock & Balance ──
	'sat.carbon.title': { es: '¿Cuánto carbono almacenan estas zonas?', en: 'How much carbon do these areas store?', gn: "Mbovy carbono oguereko ko yvy?", pt: 'Quanto carbono essas zonas armazenam?' },
	'sat.carbon.desc': { es: 'Stock y balance de carbono: biomasa aerea (ESA CCI + GEDI), carbono del suelo, flujo de emisiones y remociones (Harris et al. 2021), valor economico a precio de mercado voluntario.', en: 'Carbon stock and balance: above-ground biomass (ESA CCI + GEDI), soil carbon, emission and removal flux (Harris et al. 2021), economic value at voluntary market price.', gn: "Carbono stock ha balance", pt: 'Estoque e balanço de carbono: biomassa aérea (ESA CCI + GEDI), carbono do solo, fluxo de emissões e remoções (Harris et al. 2021), valor econômico a preço de mercado voluntário.' },
	'sat.carbon.type': { es: 'Tipo', en: 'Type', gn: 'Laja', pt: 'Tipo' },
	'sat.carbon.typeLabel': { es: 'Clasificación', en: 'Classification', gn: 'Clasificación', pt: 'Classificação' },
	'sat.carbon.agbCci': { es: 'Biomasa aerea (ESA CCI)', en: 'Above-ground biomass (ESA CCI)', gn: 'AGB ESA', pt: 'Biomassa aérea (ESA CCI)' },
	'sat.carbon.agbGedi': { es: 'Biomasa aerea (GEDI lidar)', en: 'Above-ground biomass (GEDI lidar)', gn: 'AGB GEDI', pt: 'Biomassa aérea (GEDI lidar)' },
	'sat.carbon.totalCarbon': { es: 'Carbono total', en: 'Total carbon', gn: 'Carbono opavave', pt: 'Carbono total' },
	'sat.carbon.soc': { es: 'Carbono organico del suelo', en: 'Soil organic carbon', gn: 'Yvy carbono', pt: 'Carbono orgânico do solo' },
	'sat.carbon.emissions': { es: 'Emisiones brutas', en: 'Gross emissions', gn: "Emision tuicha", pt: 'Emissões brutas' },
	'sat.carbon.removals': { es: 'Remociones brutas', en: 'Gross removals', gn: "Oñembo'y", pt: 'Remoções brutas' },
	'sat.carbon.netFlux': { es: 'Balance neto de carbono', en: 'Net carbon balance', gn: 'Carbono balance', pt: 'Balanço líquido de carbono' },
	'sat.carbon.npp': { es: 'Productividad primaria neta', en: 'Net primary productivity', gn: 'NPP', pt: 'Produtividade primária líquida' },
	'sat.carbon.economicValue': { es: 'Valor teórico del carbono (USD/ha)*', en: 'Theoretical carbon value (USD/ha)*', gn: 'Valor USD/ha*', pt: 'Valor teórico do carbono (USD/ha)*' },
	'legend.carbon.low': { es: 'Bajo stock de carbono', en: 'Low carbon stock', gn: "Michĩ", pt: 'Baixo estoque de carbono' },
	'legend.carbon.high': { es: 'Alto stock de carbono', en: 'High carbon stock', gn: "Tuicha", pt: 'Alto estoque de carbono' },
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
	'legend.deforest.low': { es: 'Baja presión de deforestación', en: 'Low deforestation pressure', gn: "Michimi", pt: 'Baixa pressão de desmatamento' },
	'legend.deforest.high': { es: 'Alta presión de deforestación', en: 'High deforestation pressure', gn: "Tuicha", pt: 'Alta pressão de desmatamento' },
	'legend.landUse.low': { es: 'Poca cobertura nativa', en: 'Low native cover', gn: "Michĩ ka'aguy", pt: 'Pouca cobertura nativa' },
	'legend.landUse.high': { es: 'Alta cobertura nativa', en: 'High native cover', gn: "Tuicha ka'aguy", pt: 'Alta cobertura nativa' },
	// ── PM2.5 Predicted ──
	'sat.pm25pred.title': { es: '¿Cuál es la calidad del aire predicha?', en: 'What is the predicted air quality?', gn: "Mba'eichapa tataendy?", pt: 'Qual é a qualidade do ar prevista?' },
	'sat.pm25pred.desc': { es: 'PM2.5 predicho por machine learning en µg/m³ con escenarios de fuego. Modelo entrenado con 25 años de datos satelitales (ACAG V6). Incluye bandas de excedencia OMS.', en: 'ML-predicted PM2.5 in ug/m3 with fire scenarios. Model trained on 25 years of satellite data (ACAG V6). Includes WHO exceedance bands.', gn: 'PM2.5 predicho', pt: 'PM2.5 previsto por machine learning em µg/m³ com cenários de fogo. Modelo treinado com 25 anos de dados satelitais (ACAG V6). Inclui bandas de excedência OMS.' },
	'sat.pm25pred.type': { es: 'Banda OMS', en: 'WHO band', gn: 'OMS', pt: 'Banda OMS' },
	'sat.pm25pred.typeLabel': { es: 'Clasificación OMS', en: 'WHO classification', gn: 'OMS', pt: 'Classificação OMS' },
	'sat.pm25pred.pm25': { es: 'PM2.5 predicho (ug/m3)', en: 'Predicted PM2.5 (ug/m3)', gn: 'PM2.5', pt: 'PM2.5 previsto (µg/m³)' },
	'sat.pm25pred.fireHigh': { es: 'PM2.5 escenario fuego alto (ug/m3)', en: 'PM2.5 high-fire scenario (ug/m3)', gn: 'PM2.5 tata', pt: 'PM2.5 cenário fogo alto (µg/m³)' },
	'sat.pm25pred.whoBand': { es: 'Banda de excedencia OMS', en: 'WHO exceedance band', gn: 'OMS banda', pt: 'Banda de excedência OMS' },
	'legend.pm25pred.low': { es: 'Aire mas limpio', en: 'Cleaner air', gn: "Pora", pt: 'Ar mais limpo' },
	'legend.pm25pred.high': { es: 'Mayor contaminacion', en: 'More polluted', gn: "Vai", pt: 'Maior contaminação' },
	// ── PM2.5 Drivers ──
	'sat.pm25.title': { es: '¿Qué nivel de exposición a PM2.5 hay?', en: 'What level of PM2.5 exposure is there?', gn: "Mba'eichagua PM2.5 oĩ?", pt: 'Qual nível de exposição ao PM2.5 existe?' },
	'sat.pm25.desc': { es: 'Drivers de PM2.5 identificados por machine learning: intensidad de fuego regional, clima, terreno y vegetación. Panel de 25 años (ACAG V6, 1998-2022).', en: 'PM2.5 drivers identified by machine learning: regional fire intensity, climate, terrain and vegetation. 25-year panel (ACAG V6, 1998-2022).', gn: 'PM2.5 drivers', pt: 'Drivers de PM2.5 identificados por machine learning: intensidade de fogo regional, clima, terreno e vegetação. Painel de 25 anos (ACAG V6, 1998-2022).' },
	'sat.pm25.type': { es: 'Tipo', en: 'Type', gn: 'Laja', pt: 'Tipo' },
	'sat.pm25.typeLabel': { es: 'Clasificación OMS', en: 'WHO classification', gn: 'OMS', pt: 'Classificação OMS' },
	'sat.pm25.pm25Mean': { es: 'PM2.5 medio (µg/m³)', en: 'Mean PM2.5 (µg/m³)', gn: 'PM2.5', pt: 'PM2.5 médio (µg/m³)' },
	'sat.pm25.fire': { es: 'Contribución del fuego', en: 'Fire contribution', gn: 'Tata', pt: 'Contribuição do fogo' },
	'sat.pm25.climate': { es: 'Contribución del clima', en: 'Climate contribution', gn: 'Ára', pt: 'Contribuição do clima' },
	'sat.pm25.terrain': { es: 'Contribución del terreno', en: 'Terrain contribution', gn: 'Yvy', pt: 'Contribuição do terreno' },
	'sat.pm25.vegetation': { es: 'Contribución de la vegetación', en: 'Vegetation contribution', gn: "Ka'a", pt: 'Contribuição da vegetação' },
	'legend.pm25.low': { es: 'Baja exposición a PM2.5', en: 'Low PM2.5 exposure', gn: "Michĩ", pt: 'Baixa exposição ao PM2.5' },
	'legend.pm25.high': { es: 'Alta exposición a PM2.5', en: 'High PM2.5 exposure', gn: "Tuicha", pt: 'Alta exposição ao PM2.5' },

	'sat.landUse.title': { es: '¿Qué uso de suelo tiene esta zona?', en: 'What land use does this area have?', gn: "Mba'éichapa ojepuru ko yvy?", pt: 'Qual o uso do solo nesta zona?' },
	'sat.landUse.desc': { es: 'Composición de cobertura del suelo por hexágono (color = fracción arbórea): árboles, cultivos, pasturas, arbustos, construido, agua (Dynamic World 10m, Google/WRI — comparable entre territorios)', en: 'Land-cover composition per hexagon (colour = tree fraction): trees, crops, grass, shrub, built, water (Dynamic World 10m, Google/WRI — cross-territory comparable)', gn: "Yvy jepuru: ka'aguy, temity, pasto, ka'avo, táva, y (Dynamic World 10m)", pt: 'Composição da cobertura do solo por hexágono (cor = fração arbórea): árvores, cultivos, pastagem, arbustos, construído, água (Dynamic World 10m, Google/WRI — comparável entre territórios)' },
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

	'analysis.catastro.title': { es: '¿Cómo está parcelado el territorio?', en: 'How is the land divided into parcels?', gn: "Mba'éichapa oñembokoha yvy?", pt: 'Como está parcelado o território?' },
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
	'analysis.catastro.petalHint': { es: 'Pétalo: más grande = mejor · punteada = prom. provincial', en: 'Petal: larger = better · dashed = prov. avg', gn: "Pétalo: tuichavéva = iporãvéva", pt: 'Pétala: maior = melhor · pontilhada = méd. provincial' },
	'analysis.catastro.clearAll': { es: 'Limpiar', en: 'Clear all', gn: "Mopotĩ", pt: 'Limpar' },
	'analysis.catastro.legendUrban': { es: 'Parcela urbana', en: 'Urban parcel', gn: "Yvy táva", pt: 'Parcela urbana' },
	'analysis.catastro.legendRural': { es: 'Parcela rural', en: 'Rural parcel', gn: "Yvy ka'aguy", pt: 'Parcela rural' },
	'analysis.catastro.legendNew': { es: 'Nueva', en: 'New', gn: "Ipyahu", pt: 'Nova' },
	'analysis.catastro.legendRemoved': { es: 'Eliminada', en: 'Removed', gn: "Okañy", pt: 'Removida' },
	'data.updatedAt': { es: 'Procesado al', en: 'Processed', gn: 'Oñembopyahu', pt: 'Processado em' },
	'analysis.flood.methodTitle': { es: 'Metodologia', en: 'Methodology', gn: "Mba'eichapa", pt: 'Metodologia' },
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
		es: 'Los colores representan la presencia histórica de agua en cada zona (JRC Global Surface Water, Landsat 1984–2021). Verde = baja presencia (<5%); amarillo = presencia moderada (5–20%); rojo = alta presencia (>20%). Seleccioná un departamento para ver el detalle a nivel de parcela catastral.',
		en: 'Colours represent historical water presence per area (JRC Global Surface Water, Landsat 1984–2021). Green = low presence (<5%); yellow = moderate presence (5–20%); red = high presence (>20%). Select a department to view parcel-level detail.',
		gn: "Sa'y ohechauka y rehegua historia yvy peteĩ-pe (JRC, Landsat 1984–2021). Hovyũ = michĩ (<5%); sa'yju = mbyte (5–20%); pytã = guasu (>20%). Eiporavo departamento ehecha hag̃ua yvy peteĩteĩ.",
		pt: 'As cores representam a presença histórica de água em cada zona (JRC Global Surface Water, Landsat 1984–2021). Verde = baixa presença (<5%); amarelo = presença moderada (5–20%); vermelho = alta presença (>20%). Selecione um departamento para ver o detalhe.',
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
	'side.radioCensus.variable': { es: 'Variable', en: 'Variable', gn: 'Variable', pt: 'Variável' },
	'side.radioCensus.loading': { es: 'Cargando radios…', en: 'Loading tracts…', gn: 'Omyenyhẽhína radio…', pt: 'Carregando setores…' },
	'side.radioCensus.empty': { es: 'Sin datos de radios', en: 'No tract data', gn: "Ndaipóri radio dato", pt: 'Sem dados de setores' },
	'side.radioCensus.error': { es: 'No se pudieron cargar los radios', en: 'Could not load tracts', gn: "Ndaikatúi omyenyhẽ radio", pt: 'Não foi possível carregar os setores' },
	'source.title': { es: 'Fuentes', en: 'Sources', gn: "Moñe'ẽha", pt: 'Fontes' },
	'source.census':    { es: 'Datos: INDEC — Censo Nacional de Población 2022', en: 'Data: INDEC — National Population Census 2022', gn: 'Datos: INDEC — Censo Nacional 2022', pt: 'Dados: INDEC — Censo Nacional de Población 2022' },
	'source.census.br': { es: 'Datos: IBGE — Censo Demográfico 2022 (Agregados por Setores Censitários)', en: 'Data: IBGE — Demographic Census 2022 (Tract-level Aggregates)', gn: 'Datos: IBGE — Censo Demográfico 2022', pt: 'Dados: IBGE — Censo Demográfico 2022 (Agregados por Setores Censitários)' },
	'source.census.br.note': { es: 'Índice de saneamiento local (modo local — no comparable entre países): % agua de rede geral, % esgoto adequado, % lixo coletado, % sem banheiro exclusivo.', en: 'Local sanitation index (local mode — not cross-country comparable): % piped water, % adequate sewage, % garbage collected, % no exclusive bathroom.', gn: 'Índice saneamiento local BR.', pt: 'Índice de saneamento local (modo local): % água de rede geral, % esgoto adequado, % lixo coletado, % sem banheiro exclusivo.' },
	'source.buildings': { es: 'Edificaciones: Google Open Buildings 2025 + VIDA', en: 'Buildings: Google Open Buildings 2025 + VIDA', gn: 'Óga: Google Open Buildings 2025 + VIDA', pt: 'Edificações: Google Open Buildings 2025 + VIDA' },
	'source.basemap': { es: 'Mapa: CARTO Dark Matter (© OpenStreetMap)', en: 'Basemap: CARTO Dark Matter (© OpenStreetMap)', gn: 'Mapa: CARTO Dark Matter (© OpenStreetMap)', pt: 'Mapa: CARTO Dark Matter (© OpenStreetMap)' },
	'source.terrain': { es: 'Relieve: AWS Terrain Tiles (Mapzen)', en: 'Terrain: AWS Terrain Tiles (Mapzen)', gn: 'Relieve: AWS Terrain Tiles (Mapzen)', pt: 'Relevo: AWS Terrain Tiles (Mapzen)' },
	'lasso.hint': { es: 'Dibujá una zona arrastrando sobre el mapa. El cálculo tarda unos segundos.', en: 'Draw a zone by dragging on the map. Calculation takes a few seconds.', gn: "Embosako'i zona mapa ári. Ohasa segundos.", pt: 'Desenhe uma zona arrastando sobre o mapa. O cálculo leva alguns segundos.' },
	'zone.title': { es: 'Zona', en: 'Zone', gn: 'Zona', pt: 'Zona' },
	'zone.population': { es: 'Población', en: 'Population', gn: 'Yvypóra', pt: 'População' },
	'zone.area': { es: 'Área km²', en: 'Area km²', gn: 'Yvy km²', pt: 'Área km²' },
	'zone.radios': { es: 'Radios', en: 'Radios', gn: 'Radio', pt: 'Setores' },
	'zone.noRadios': { es: 'Sin radios en la selección', en: 'No radios in selection', gn: "Radio'ỹ jeporavópe", pt: 'Sem setores na seleção' },
	'zone.petalNote': { es: 'Relativo al promedio del territorio (línea punteada = promedio)', en: 'Relative to territory average (dashed line = average)', gn: 'Tetã mbytekue rehe (línea = mbytekue)', pt: 'Relativo à média do território (linha pontilhada = média)' },

	// ── Hex comparison / hex zones ──────────────────────────────────────
	'hex.comparison': { es: 'Comparación de hexágonos', en: 'Hexagon comparison', gn: 'Hexágono jojaha', pt: 'Comparação de hexágonos' },
	'hex.hexCount': { es: 'Hexágonos', en: 'Hexagons', gn: 'Hexágono', pt: 'Hexágonos' },
	'hexZone.title': { es: 'Zonas hexagonales', en: 'Hex zones', gn: 'Hexágono zona', pt: 'Zonas hexagonais' },
	'hex.resolution': { es: 'Resolución H3', en: 'H3 resolution', gn: 'H3 tuichakue', pt: 'Resolução H3' },
	'hex.loading': { es: 'Cargando hexágonos...', en: 'Loading hexagons...', gn: 'Oñemyenyhẽ hexágono...', pt: 'Carregando hexágonos...' },
	'hex.provAvg': { es: 'prom. del territorio', en: 'territory avg', gn: 'tetã mbytekue', pt: 'méd. do território' },
	'hex.provAvgProvince': { es: 'prom. de la provincia', en: 'province avg', gn: 'tetã mbytekue', pt: 'méd. da província' },
	'hex.provAvgDept': { es: 'prom. del departamento', en: 'department avg', gn: 'departamento mbytekue', pt: 'méd. do departamento' },
	'hex.provAvgState': { es: 'prom. del estado', en: 'state avg', gn: 'estado mbytekue', pt: 'méd. do estado' },
	'hex.petalRelative': { es: 'Flor (lectura relativa):', en: 'Flower (relative reading):', gn: 'Yvoty:', pt: 'Flor (leitura relativa):' },
	'hex.goalpostNote': { es: 'Valores /100: escala fija regional, idéntica en todos los territorios comparables — 100 es el techo del rango de referencia (NEA + región transfronteriza), no el máximo del territorio (provincia en AR, departamento en PY, estado en BR). Solo la flor de arriba compara contra el promedio local.', en: 'Values /100: fixed regional scale, identical across all comparable territories — 100 is the ceiling of the reference range (NEA + trans-boundary region), not the territory maximum (province in AR, department in PY, state in BR). Only the flower above compares against the local average.', gn: 'Papapy /100: escala fija regional, peteĩchagua opa tetãme — 100 ndaha\'éi tetã máximo (provincia AR, departamento PY, estado BR). Yvoty año ombojoja promedio local ndive.', pt: 'Valores /100: escala fixa regional, idêntica em todos os territórios comparáveis — 100 é o teto do intervalo de referência (NEA + região transfronteiriça), não o máximo do território (província na AR, departamento no PY, estado no BR). Apenas a flor acima compara contra a média local.' },
	'hex.percentileNote': { es: 'Percentiles provinciales (0–100): posición del hexágono frente al resto de la provincia. 50 = mediana; cuanto más alto, mayor carencia (100 = el peor de la provincia); más bajo, menor carencia.', en: 'Provincial percentiles (0–100): the hexagon\'s position relative to the rest of the province. 50 = median; higher means greater deprivation (100 = worst in the province), lower means less.', gn: 'Percentiles (0–100): 50 = mbytekue; ijyvateve = carencia tuichave.', pt: 'Percentis provinciais (0–100): posição do hexágono frente ao resto da província. 50 = mediana; quanto mais alto, maior carência (100 = o pior da província); mais baixo, menor.' },
	'hex.magTable': { es: 'Magnitud y valores estimados', en: 'Magnitude & estimated values', gn: 'Tuichakue ha papapy', pt: 'Magnitude e valores estimados' },
	'hex.buildings': { es: 'Edificios', en: 'Buildings', gn: 'Óga', pt: 'Edifícios' },
	'hex.popEst': { es: 'Población estimada', en: 'Estimated population', gn: 'Tekohára papapy', pt: 'População estimada' },
	'hex.households': { es: 'Hogares estimados', en: 'Estimated households', gn: 'Óga papapy', pt: 'Domicílios estimados' },
	'hex.estPctNote': { es: 'Valores estimados por hexágono: el dato del radio censal se redistribuye a los hexágonos ponderando por edificios (método dasimétrico).', en: 'Estimated values per hexagon: the census-tract value is redistributed to hexagons weighted by buildings (dasymetric method).', gn: 'Papapy hexágono rehegua: radio censal-gui oñemboja\'o hexágono-pe óga rupive (dasimétrico).', pt: 'Valores estimados por hexágono: o dado do setor censitário é redistribuído aos hexágonos ponderando por edifícios (método dasimétrico).' },

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

	// ── Trade section ─────────────────────────────────────────────────────
	'trade.nav.map': { es: 'Mapa', en: 'Map', gn: 'Mapa', pt: 'Mapa' },
	'trade.nav.trade': { es: 'Trade', en: 'Trade', gn: 'Trade', pt: 'Trade' },
	'trade.meta.title': { es: 'Datos geoespaciales para decisiones estratégicas', en: 'Geospatial data for strategic decisions', gn: 'Trade', pt: 'Dados geoespaciais para decisões estratégicas' },
	'trade.hero.subtitle': { es: 'Pipelines de datos satelitales y geoespaciales para compliance, análisis comercial y de riesgo', en: 'Satellite and geospatial data pipelines for compliance, business analytics, and risk assessment', gn: 'Trade', pt: 'Pipelines de dados satelitais e geoespaciais para compliance, análise comercial e análise de risco' },
	'trade.status.available': { es: 'Disponible', en: 'Available', gn: 'Available', pt: 'Disponível' },
	'trade.status.coming_soon': { es: 'Próximamente', en: 'Coming soon', gn: 'Coming soon', pt: 'Em breve' },
	'trade.eudr.card_title': { es: 'EUDR · Deforestación', en: 'EUDR · Deforestation', gn: 'EUDR · Deforestación', pt: 'EUDR · Desmatamento' },
	'trade.eudr.card_desc': { es: 'Verificación de deforestación para exportaciones de commodities argentinos a la UE. Regulación (UE) 2023/1115.', en: 'Deforestation verification for Argentine commodity exports to the EU. Regulation (EU) 2023/1115.', gn: 'EUDR', pt: 'Verificação de desmatamento para exportações de commodities argentinos à UE. Regulamento (UE) 2023/1115.' },
	'trade.eudr.card_cta': { es: 'Verificar parcela', en: 'Check parcel', gn: 'Check', pt: 'Verificar parcela' },
	'trade.radar.card_title': { es: 'Radar Empresarial', en: 'Business Radar', gn: 'Radar', pt: 'Radar Empresarial' },
	'trade.radar.card_desc': { es: 'Análisis competitivo: nuevas empresas, actividad económica y dinámica geoespacial por sector y localidad.', en: 'Competitive analysis: new businesses, economic activity, and geospatial dynamics by sector and locality.', gn: 'Radar', pt: 'Análise competitiva: novas empresas, atividade econômica e dinâmica geoespacial por setor e localidade.' },
	'trade.risk.card_title': { es: 'Riesgo Ambiental', en: 'Environmental Risk', gn: 'Risk', pt: 'Risco Ambiental' },
	'trade.risk.card_desc': { es: 'Evaluación de riesgo climático, hídrico y de incendios para inversiones y seguros en regiones específicas.', en: 'Climate, water, and fire risk assessment for investments and insurance in specific regions.', gn: 'Risk', pt: 'Avaliação de risco climático, hídrico e de incêndios para investimentos e seguros em regiões específicas.' },
	'trade.trust.title': { es: 'Fuentes de datos', en: 'Data sources', gn: 'Data', pt: 'Fontes de dados' },
	'trade.footer.tagline': { es: 'Análisis geoespacial desde el borde', en: 'Geospatial analysis from the edge', gn: 'nealab', pt: 'Análise geoespacial desde a borda' },
	'trade.footer.data': { es: 'Datos satelitales: MODIS, Landsat, Sentinel, VIIRS — servidos desde Cloudflare', en: 'Satellite data: MODIS, Landsat, Sentinel, VIIRS — served from Cloudflare', gn: 'Data', pt: 'Dados satelitais: MODIS, Landsat, Sentinel, VIIRS — servidos do Cloudflare' },

	// ── EUDR product page ─────────────────────────────────────────────────
	'eudr.hero.title': { es: 'EUDR Compliance Check', en: 'EUDR Compliance Check', gn: 'EUDR', pt: 'EUDR Compliance Check' },
	'eudr.hero.subtitle': { es: 'Análisis satelital de pérdida forestal post-2020 (Hansen GFC + MODIS) sobre el NEA argentino, Paraguay y el sur de Brasil. Soporte para due-diligence bajo el Reglamento (UE) 2023/1115.', en: 'Satellite analysis of post-2020 forest loss (Hansen GFC + MODIS) over NEA Argentina, Paraguay and southern Brazil. Supports due-diligence under EU Regulation 2023/1115.', gn: 'EUDR', pt: 'Análise satelital de perda florestal pós-2020 (Hansen GFC + MODIS) sobre o NEA argentino, Paraguai e sul do Brasil. Suporta due-diligence sob o Regulamento (UE) 2023/1115.' },
	'eudr.cta.try_demo': { es: 'Probar demo', en: 'Try demo', gn: 'Demo', pt: 'Testar demo' },
	'eudr.cta.contact': { es: 'Contactar', en: 'Contact us', gn: 'Contact', pt: 'Contatar' },
	'eudr.what.title': { es: '¿Qué es la EUDR?', en: 'What is the EUDR?', gn: 'EUDR', pt: 'O que é a EUDR?' },
	'eudr.what.regulation': { es: 'Regulación UE 2023/1115', en: 'EU Regulation 2023/1115', gn: 'EUDR', pt: 'Regulamento UE 2023/1115' },
	'eudr.what.regulation_desc': { es: 'Los importadores europeos deben demostrar que soja, carne y madera no provienen de tierras deforestadas. Aplica desde diciembre 2025.', en: 'EU importers must prove that soy, cattle, and wood products are not sourced from deforested land. Applies from December 2025.', gn: 'EUDR', pt: 'Os importadores europeus devem demonstrar que soja, carne e madeira não provêm de terras desmatadas. Aplica desde dezembro de 2025.' },
	'eudr.what.cutoff': { es: 'Fecha de corte: 31/12/2020', en: 'Cutoff date: 31/12/2020', gn: 'EUDR', pt: 'Data de corte: 31/12/2020' },
	'eudr.what.cutoff_desc': { es: 'Cualquier deforestación posterior al 31 de diciembre de 2020 hace que el producto no cumpla con la regulación.', en: 'Any deforestation after 31 December 2020 renders the product non-compliant with the regulation.', gn: 'EUDR', pt: 'Qualquer desmatamento posterior a 31 de dezembro de 2020 torna o produto não conforme com o regulamento.' },
	'eudr.what.penalty': { es: 'Multas de hasta 4%', en: 'Fines up to 4%', gn: 'EUDR', pt: 'Multas de até 4%' },
	'eudr.what.penalty_desc': { es: 'Las multas pueden alcanzar el 4% de la facturación anual del importador en la UE, más confiscación de mercadería y exclusión de licitaciones.', en: 'Fines can reach 4% of the importer\'s annual EU-wide turnover, plus confiscation of goods and exclusion from public procurement.', gn: 'EUDR', pt: 'As multas podem chegar a 4% do faturamento anual do importador na UE, mais confisco de mercadoria e exclusão de licitações.' },
	'eudr.how.title': { es: '¿Cómo funciona?', en: 'How it works', gn: 'EUDR', pt: 'Como funciona?' },
	'eudr.how.step1_title': { es: 'Ingresá coordenadas', en: 'Input coordinates', gn: 'EUDR', pt: 'Insira coordenadas' },
	'eudr.how.step1_desc': { es: 'Latitud/longitud de la parcela de producción, o hacé click en el mapa.', en: 'Latitude/longitude of the production plot, or click on the map.', gn: 'EUDR', pt: 'Latitude/longitude da parcela de produção, ou clique no mapa.' },
	'eudr.how.step2_title': { es: 'Análisis satelital', en: 'Satellite analysis', gn: 'EUDR', pt: 'Análise satelital' },
	'eudr.how.step2_desc': { es: 'Cruzamos Hansen GFC (30m) y MODIS fire data contra la línea base 2020.', en: 'We cross-reference Hansen GFC (30m) and MODIS fire data against the 2020 baseline.', gn: 'EUDR', pt: 'Cruzamos Hansen GFC (30m) e dados de fogo MODIS com a linha de base 2020.' },
	'eudr.how.step3_title': { es: 'Evaluación de riesgo', en: 'Risk assessment', gn: 'EUDR', pt: 'Avaliação de risco' },
	'eudr.how.step3_desc': { es: 'Score de riesgo 0-100 con evidencia satelital de pérdida forestal post-2020.', en: 'Risk score 0-100 with satellite evidence of post-2020 forest loss.', gn: 'EUDR', pt: 'Score de risco 0-100 com evidência satelital de perda florestal pós-2020.' },
	'eudr.data.title': { es: 'Datos y cobertura', en: 'Data and coverage', gn: 'EUDR', pt: 'Dados e cobertura' },
	'eudr.data.deforestation': { es: 'Deforestación', en: 'Deforestation', gn: 'EUDR', pt: 'Desmatamento' },
	'eudr.data.coverage': { es: 'Cobertura', en: 'Coverage', gn: 'EUDR', pt: 'Cobertura' },
	'eudr.data.update_freq': { es: 'Actualización mensual', en: 'Monthly updates', gn: 'EUDR', pt: 'Atualização mensal' },
	'eudr.pricing.title': { es: 'Planes', en: 'Pricing', gn: 'EUDR', pt: 'Planos' },
	'eudr.pricing.free': { es: 'Gratis', en: 'Free', gn: 'Free', pt: 'Grátis' },
	'eudr.pricing.contact': { es: 'Contactar', en: 'Contact', gn: 'Contact', pt: 'Contatar' },
	'eudr.pricing.custom': { es: 'A medida', en: 'Custom', gn: 'Custom', pt: 'Personalizado' },
	'eudr.pricing.demo_1': { es: 'Hasta 10 consultas/día', en: 'Up to 10 checks/day', gn: 'EUDR', pt: 'Até 10 consultas/dia' },
	'eudr.pricing.demo_2': { es: 'Cobertura: NEA argentino + Paraguay + sur de Brasil', en: 'Coverage: NEA Argentina + Paraguay + southern Brazil', gn: 'EUDR', pt: 'Cobertura: NEA argentino + Paraguai + sul do Brasil' },
	'eudr.pricing.demo_3': { es: 'Resolución H3 res-7 (~5 km²)', en: 'H3 res-7 resolution (~5 km²)', gn: 'EUDR', pt: 'Resolução H3 res-7 (~5 km²)' },
	'eudr.pricing.pro_1': { es: 'API REST ilimitada', en: 'Unlimited REST API', gn: 'EUDR', pt: 'API REST ilimitada' },
	'eudr.pricing.pro_2': { es: 'Monitoreo mensual de parcelas', en: 'Monthly parcel monitoring', gn: 'EUDR', pt: 'Monitoramento mensal de parcelas' },
	'eudr.pricing.pro_3': { es: 'Informes PDF para due diligence', en: 'PDF reports for due diligence', gn: 'EUDR', pt: 'Relatórios PDF para due diligence' },
	'eudr.pricing.ent_1': { es: 'Resolución premium (H3 res-9)', en: 'Premium resolution (H3 res-9)', gn: 'EUDR', pt: 'Resolução premium (H3 res-9)' },
	'eudr.pricing.ent_2': { es: 'Integración con su ERP/TMS', en: 'Integration with your ERP/TMS', gn: 'EUDR', pt: 'Integração com seu ERP/TMS' },
	'eudr.pricing.ent_3': { es: 'SLA y soporte dedicado', en: 'SLA and dedicated support', gn: 'EUDR', pt: 'SLA e suporte dedicado' },
	'eudr.disclaimer': { es: 'Esta herramienta proporciona evaluaciones indicativas basadas en datos satelitales. No constituye certificación legal de cumplimiento bajo el Reglamento (UE) 2023/1115. Los operadores deben realizar su propia diligencia debida según los Artículos 8-11 del Reglamento. Datos: Hansen/UMD Global Forest Change v1.12 (U. Maryland/NASA, 30m Landsat), MODIS MCD64A1 Burned Area (NASA, 500m). Fecha de corte EUDR: 31 de diciembre de 2020.', en: 'This tool provides indicative assessments based on satellite-derived data. It does not constitute legal compliance certification under EU Regulation 2023/1115. Operators must perform their own due diligence as required by Articles 8-11 of the Regulation. Data: Hansen/UMD Global Forest Change v1.12 (U. Maryland/NASA, 30m Landsat), MODIS MCD64A1 Burned Area (NASA, 500m). EUDR cutoff date: 31 December 2020.', gn: 'EUDR', pt: 'Esta ferramenta fornece avaliações indicativas baseadas em dados satelitais. Não constitui certificação legal de conformidade sob o Regulamento (UE) 2023/1115. Os operadores devem realizar sua própria diligência devida conforme os Artigos 8-11 do Regulamento. Dados: Hansen/UMD Global Forest Change v1.12 (U. Maryland/NASA, 30m Landsat), MODIS MCD64A1 Burned Area (NASA, 500m). Data de corte EUDR: 31 de dezembro de 2020.' },
	'eudr.disclaimer_short': { es: 'Evaluación indicativa. No constituye certificación legal bajo Reg. (UE) 2023/1115.', en: 'Indicative assessment. Not legal certification under Reg. (EU) 2023/1115.', gn: 'EUDR', pt: 'Avaliação indicativa. Não constitui certificação legal sob Reg. (UE) 2023/1115.' },

	// ── EUDR check page ──────────────────────────────────────────────────
	'eudr.check.title': { es: 'Verificar parcela', en: 'Check parcel', gn: 'Check', pt: 'Verificar parcela' },
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
	'eudr.check.fire_post_2020': { es: 'Fuego post-2020', en: 'Fire post-2020', gn: 'Fire', pt: 'Fogo pós-2020' },
	'eudr.check.province': { es: 'Provincia', en: 'Province', gn: 'Province', pt: 'Província' },
	'eudr.check.coordinates': { es: 'Coordenadas', en: 'Coordinates', gn: 'Coordinates', pt: 'Coordenadas' },
	'eudr.check.empty_title': { es: 'Seleccioná una ubicación', en: 'Select a location', gn: 'Select', pt: 'Selecione uma localização' },
	'eudr.check.empty_desc': { es: 'Hacé click en el mapa o ingresá coordenadas para verificar el estado de deforestación.', en: 'Click on the map or enter coordinates to check deforestation status.', gn: 'Click', pt: 'Clique no mapa ou insira coordenadas para verificar o status de desmatamento.' },
	'eudr.check.remaining': { es: 'checks restantes hoy', en: 'checks remaining today', gn: 'checks', pt: 'checks restantes hoje' },
	'eudr.check.limit_reached': { es: 'Límite diario alcanzado', en: 'Daily limit reached', gn: 'Limit', pt: 'Limite diário atingido' },
	'eudr.check.limit_cta': { es: 'Contactanos para acceso profesional', en: 'Contact us for professional access', gn: 'Contact', pt: 'Contate-nos para acesso profissional' },
	'eudr.check.deforest_detected': { es: 'PÉRDIDA POST-2020 DETECTADA', en: 'POST-2020 LOSS DETECTED', gn: 'PÉRDIDA POST-2020', pt: 'PERDA PÓS-2020 DETECTADA' },
	'eudr.check.area_note': { es: 'Área evaluada: hexágono de ~0,1 km² (H3 res-9). Datos satelitales a 100 m: el resultado refleja ese hexágono, no necesariamente la parcela exacta.', en: 'Assessed area: ~0.1 km² hexagon (H3 res-9). 100 m satellite data: the result reflects that hexagon, not necessarily the exact plot.', gn: 'Área ojehecháva: hexágono ~0,1 km² (H3 res-9). Resultado he\'i hexágono rehe, ndaha\'éi parcela exacta.', pt: 'Área avaliada: hexágono de ~0,1 km² (H3 res-9). Dados satelitais a 100 m: o resultado reflete esse hexágono, não necessariamente a parcela exata.' },
	'eudr.check.vintage': { es: 'Datos satelitales', en: 'Satellite data', gn: 'Datos satelitales', pt: 'Dados satelitais' },
	'eudr.check.refreshed': { es: 'Última actualización', en: 'Last refreshed', gn: 'Última actualización', pt: 'Última atualização' },
	'eudr.check.methodology_link': { es: 'Ver metodología completa →', en: 'View full methodology →', gn: 'Ehecha metodología →', pt: 'Ver metodologia completa →' },
	'eudr.layer_hint': { es: 'Acercá el zoom y paneá: el riesgo de deforestación se carga por hexágono solo en el área visible (escalable a cualquier región).', en: 'Zoom in and pan: deforestation risk loads per hexagon only for the visible area (scales to any region).', gn: 'Emboja ha emongarê: riesgo oñembyaty hexágono rupive área ojehecháva.', pt: 'Aproxime o zoom e navegue: o risco de desmatamento carrega por hexágono apenas na área visível (escalável a qualquer região).' },
	'eudr.check.cta_from_layer': { es: '🔎 Análisis detallado por punto o polígono (res-9) →', en: '🔎 Detailed point or polygon analysis (res-9) →', gn: '🔎 Análisis detallado →', pt: '🔎 Análise detalhada por ponto ou polígono (res-9) →' },
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
	'eudr.check.poly_report': { es: '📩 Solicitar informe técnico de soporte →', en: '📩 Request supporting technical report →', gn: '📩 Solicitá informe técnico de soporte →', pt: '📩 Solicitar relatório técnico de apoio →' },
	'eudr.check.plant_managed': { es: 'Plantación forestal ({pct}%). La pérdida post-2020 puede ser un ciclo de cosecha, no deforestación de bosque nativo — verificá que la plantación sea anterior al 31/12/2020.', en: 'Forestry plantation ({pct}%). Post-2020 loss may be a harvest cycle, not native-forest deforestation — verify the plantation predates 31/12/2020.', gn: 'Plantación forestal ({pct}%). Pérdida post-2020 ikatu ha\'e cosecha, ndaha\'éi deforestación ka\'aguy nativo — emoañete plantación oĩ\'akue 31/12/2020 mboyve.', pt: 'Plantação florestal ({pct}%). A perda pós-2020 pode ser um ciclo de colheita, não desmatamento de floresta nativa — verifique se a plantação é anterior a 31/12/2020.' },
	'eudr.check.plant_zone': { es: 'Zona de plantación forestal ({pct}%).', en: 'Forestry plantation area ({pct}%).', gn: 'Plantación forestal rendaha ({pct}%).', pt: 'Área de plantação florestal ({pct}%).' },
	'eudr.check.plant_managed_confirmed': { es: 'En 2020 esta zona ya era plantación forestal ({pct}%). La pérdida post-2020 es compatible con un ciclo de cosecha, no con deforestación de bosque nativo. Indicativo — no constituye veredicto de cumplimiento EUDR.', en: 'In 2020 this area was already forestry plantation ({pct}%). The post-2020 loss is consistent with a harvest cycle rather than native-forest deforestation. Indicative — not an EUDR-compliance verdict.', gn: '2020-pe ko renda ha\'éma plantación forestal ({pct}%). Pérdida post-2020 ojokupyty cosecha ndive, ndaha\'éi deforestación ka\'aguy nativo. Indicativo añónte.', pt: 'Em 2020 esta zona já era plantação florestal ({pct}%). A perda pós-2020 é compatível com um ciclo de colheita, não com desmatamento de floresta nativa. Indicativo — não é um veredicto de conformidade EUDR.' },
	'eudr.check.plant_conversion': { es: 'Observación: en 2020 esta zona figuraba como cobertura nativa y hoy como plantación. Posible conversión post-2020 — requiere verificación en terreno o catastro. Indicativo, no es un veredicto.', en: 'Observation: in 2020 this area was native cover and is plantation today. Possible post-2020 conversion — requires field or cadastre verification. Indicative, not a verdict.', gn: 'Ojehecha: 2020-pe cobertura nativa, ko\'ágã plantación. Ikatu conversión post-2020 — eñemoañete terreno térã catastro. Indicativo añónte.', pt: 'Observação: em 2020 esta zona era cobertura nativa e hoje é plantação. Possível conversão pós-2020 — requer verificação em campo ou cadastro. Indicativo, não é um veredicto.' },
	'eudr.check.plant_forest_loss': { es: 'Pérdida sobre bosque (formación forestal, MapBiomas 2020). Posible deforestación de bosque nativo post-2020 — verificar causa.', en: 'Loss over forest (forest formation, MapBiomas 2020). Possible post-2020 native-forest deforestation — verify cause.', gn: 'Pérdida ka\'aguy ári (MapBiomas 2020). Ikatu deforestación post-2020 — eñemoañete.', pt: 'Perda sobre floresta (formação florestal, MapBiomas 2020). Possível desmatamento de floresta nativa pós-2020 — verificar causa.' },
	'eudr.check.plant_savanna_loss': { es: 'Pérdida sobre monte / sabana nativa (MapBiomas 2020, formación sabánica). Es vegetación nativa leñosa abierta — no bosque cerrado. Su tratamiento bajo EUDR depende de la definición de bosque aplicada; verificar.', en: 'Loss over native savanna/woodland (MapBiomas 2020, savanna formation). This is open native woody vegetation, not closed forest. Its EUDR treatment depends on the forest definition applied; verify.', gn: 'Pérdida monte/sabana nativa ári (MapBiomas 2020). Vegetación nativa leñosa ojepe\'áva — ndaha\'éi ka\'aguy hû\'î. Eñemoañete.', pt: 'Perda sobre savana/cerrado nativo (MapBiomas 2020, formação savânica). É vegetação nativa lenhosa aberta, não floresta fechada. Seu tratamento sob a EUDR depende da definição de floresta aplicada; verificar.' },
	'eudr.check.plant_nodata': { es: 'Sin dato de plantación para esta zona. La distinción plantación / bosque nativo (MapBiomas) cubre el NEA argentino (Misiones, Corrientes, Chaco, Formosa); Paraguay y Brasil quedan sin dato.', en: 'No plantation data for this area. The plantation / native-forest distinction (MapBiomas) covers NE Argentina (Misiones, Corrientes, Chaco, Formosa); Paraguay and Brazil have no data.', gn: "Ndaipóri dato plantación ko rendápe. Distinción plantación / ka'aguy nativo oĩ NEA argentino-pe; Paraguay ha Brasil ndaipóri dato.", pt: 'Sem dado de plantação para esta área. A distinção plantação / floresta nativa (MapBiomas) cobre o NEA argentino (Misiones, Corrientes, Chaco, Formosa); Paraguai e Brasil ficam sem dado.' },
	'eudr.check.plant_nodata_loss': { es: '⚠️ Sin dato de plantación para esta zona (Paraguay/Brasil — la cobertura MapBiomas plantación/nativo está en el NEA argentino). Acá NO se pudo distinguir si la pérdida es sobre plantación (posible cosecha) o sobre bosque nativo. Si el lote es una forestación, podría ser un falso positivo — verificar en terreno o con catastro oficial.', en: '⚠️ No plantation data for this area (Paraguay/Brazil — MapBiomas plantation/native cover is in NE Argentina). Here the loss could NOT be distinguished between plantation (possible harvest) and native forest. If the plot is a plantation, it may be a false positive — verify on the ground or with the official cadastre.', gn: '⚠️ Ndaipóri dato plantación (Paraguay/Brasil). Ko\'ápe ndaikatúi oñembojoja plantación (ikatu cosecha) ha ka\'aguy nativo apytépe. Lote ha\'éramo forestación, ikatu falso positivo — emoañete terreno térã catastro-pe.', pt: '⚠️ Sem dado de plantação para esta área (Paraguai/Brasil — a cobertura MapBiomas está no NEA argentino). Aqui a perda NÃO pôde ser distinguida entre plantação (possível colheita) e floresta nativa. Se o lote for uma plantação, pode ser um falso positivo — verifique em campo ou com o cadastro oficial.' },
	'eudr.check.poly_plant_split': { es: 'Cobertura en 2020 de las celdas con pérdida arbórea: {harvest} sobre plantación forestal, {forest} sobre bosque (formación forestal), {savanna} sobre monte/sabana nativa, {other} sobre otra cobertura (agro/pasturas/mosaico); {conversion} eran bosque en 2020 y hoy figuran como plantación (posible conversión, a verificar). Señal indicativa, no determina causa ni cumplimiento.', en: 'Cover in 2020 of the cells with tree-cover loss: {harvest} over forestry plantation, {forest} over forest (forest formation), {savanna} over native savanna/woodland, {other} over other cover (cropland/pasture/mosaic); {conversion} were forest in 2020 and are plantation today (possible conversion, to verify). Indicative — does not determine cause or compliance.', gn: 'Cobertura 2020-pe: {harvest} plantación forestal ári, {forest} ka\'aguy ári, {savanna} monte/sabana ári, {other} ambue cobertura ári; {conversion} ka\'aguy 2020-pe ha ko\'ágã plantación (ikatu conversión). Indicativo añónte.', pt: 'Cobertura em 2020 das células com perda arbórea: {harvest} sobre plantação florestal, {forest} sobre floresta (formação florestal), {savanna} sobre savana/cerrado nativo, {other} sobre outra cobertura (agro/pasto/mosaico); {conversion} eram floresta em 2020 e hoje são plantação (possível conversão, a verificar). Indicativa — não determina causa nem conformidade.' },
	'eudr.check.req_title': { es: 'Solicitar informe técnico firmado', en: 'Request signed technical report', gn: 'Solicitar informe', pt: 'Solicitar relatório técnico assinado' },
	'eudr.check.req_intro': { es: 'Completá los datos. Al enviar se abre un mail prearmado a nealab@spatia.ar con el resumen del análisis.', en: 'Fill in your details. Submitting opens a prefilled email to nealab@spatia.ar with the analysis summary.', gn: 'Embokuatia datos. Embohasa email nealab@spatia.ar-pe.', pt: 'Preencha os dados. Ao enviar, abre-se um e-mail pré-formatado para nealab@spatia.ar com o resumo da análise.' },
	'eudr.check.req_name': { es: 'Nombre y apellido', en: 'Full name', gn: 'Téra', pt: 'Nome completo' },
	'eudr.check.req_email': { es: 'Email', en: 'Email', gn: 'Email', pt: 'E-mail' },
	'eudr.check.req_company': { es: 'Organización / empresa (opcional)', en: 'Organization / company (optional)', gn: 'Empresa', pt: 'Organização / empresa (opcional)' },
	'eudr.check.req_purpose': { es: 'Propósito / contexto del informe (opcional)', en: 'Purpose / context for the report (optional)', gn: 'Propósito', pt: 'Propósito / contexto (opcional)' },
	'eudr.check.req_send': { es: 'Abrir mail con la solicitud', en: 'Open email with the request', gn: 'Embohasa solicitud', pt: 'Abrir e-mail com a solicitação' },
	'eudr.check.req_cancel': { es: 'Cancelar', en: 'Cancel', gn: 'Ejoko', pt: 'Cancelar' },
	'eudr.check.req_note': { es: 'Al enviar se descarga el GeoJSON de tu polígono — adjuntalo al mail antes de mandarlo.', en: 'When submitting, the polygon GeoJSON is downloaded — attach it to the email before sending.', gn: 'GeoJSON ojedescarga-jepi.', pt: 'Ao enviar, o GeoJSON do polígono é baixado — anexe-o ao e-mail antes de enviar.' },
	'eudr.check.req_err_fields': { es: 'Nombre y email son obligatorios.', en: 'Name and email are required.', gn: 'Téra ha email ojeporu.', pt: 'Nome e e-mail são obrigatórios.' },
	'eudr.check.poly_coverage_warn': { es: 'Solo el {pct}% del polígono cae dentro del área de cobertura (10 provincias). Las métricas se calculan sobre esa porción.', en: 'Only {pct}% of the polygon falls within the coverage area (10 provinces). Metrics are computed over that portion.', gn: '{pct}% polígono oĩ cobertura-pe.', pt: 'Apenas {pct}% do polígono está dentro da área de cobertura (10 províncias). As métricas são calculadas sobre essa porção.' },
	'eudr.check.poly_invalid': { es: 'GeoJSON inválido o sin polígono. Subí un Feature/Geometry de tipo Polygon.', en: 'Invalid GeoJSON or no polygon. Upload a Polygon Feature/Geometry.', gn: 'GeoJSON nahániri.', pt: 'GeoJSON inválido ou sem polígono. Envie um Feature/Geometry do tipo Polygon.' },
	'eudr.check.poly_no_cells': { es: 'El polígono no cubre ninguna celda (¿muy chico o fuera del área?).', en: 'The polygon covers no cells (too small or outside the area?).', gn: 'Polígono ndoguerekói celda.', pt: 'O polígono não cobre nenhuma célula (muito pequeno ou fora da área?).' },
	'eudr.check.poly_too_big_file': { es: 'Archivo demasiado grande (máx. 1 MB).', en: 'File too large (max 1 MB).', gn: 'Archivo tuicha.', pt: 'Arquivo muito grande (máx. 1 MB).' },
	'eudr.check.poly_too_big_area': { es: 'Polígono demasiado grande para análisis interactivo (>100.000 ha). Contactanos para un informe STAN.', en: 'Polygon too large for interactive analysis (>100,000 ha). Contact us for a STAN report.', gn: 'Polígono tuicha. Contacto STAN.', pt: 'Polígono muito grande para análise interativa (>100.000 ha). Contate-nos para um relatório STAN.' },

	// ── Navigation (shared across pages) ─────────────────────────────────
	'nav.backToMap': { es: '← Volver al mapa', en: '← Back to map', gn: "← Ejevypa mapa-pe", pt: '← Voltar ao mapa' },
	'nav.printSave': { es: '↓ Imprimir / Guardar PDF', en: '↓ Print / Save PDF', gn: "↓ Emonguejy PDF", pt: '↓ Imprimir / Salvar PDF' },

	// ── Metodología pages ─────────────────────────────────────────────────
	'page.metodologia.title': { es: 'Metodología e indicadores', en: 'Methodology & indicators', gn: "Metodología ha indicadores", pt: 'Metodologia e indicadores' },
	'page.metodologia.kicker': { es: 'nealab · noreste argentino y regiones transfronterizas', en: 'nealab · northeast Argentina & cross-border regions', gn: "nealab · noreste ha ñembyatyrã", pt: 'nealab · nordeste argentino e regiões transfronteiriças' },
	'page.metodologia.desc': { es: 'Documentación metodológica de cada indicador: fuentes de datos, variables incluidas, método de normalización y limitaciones conocidas. Todos los análisis son reproducibles y auditables.', en: 'Methodological documentation for each indicator: data sources, included variables, normalisation method and known limitations. All analyses are reproducible and auditable.', gn: "Documentación metodológica ojeporúva: fuentes, variables ha limitaciones. Análisis oñemboguapýva ha ojapohápe.", pt: 'Documentação metodológica de cada indicador: fontes de dados, variáveis incluídas, método de normalização e limitações conhecidas. Todas as análises são reproduzíveis e auditáveis.' },
	'page.metodologia.arrowLink': { es: 'Ver metodología →', en: 'View methodology →', gn: "Ehecha metodología →", pt: 'Ver metodologia →' },
	'page.metodologia.kicker.detail': { es: 'Metodología · nealab · análisis geoespacial abierto', en: 'Methodology · nealab · open geospatial analysis', gn: "Metodología · nealab · yvy rekokatu", pt: 'Metodologia · nealab · análise geoespacial aberta' },
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
