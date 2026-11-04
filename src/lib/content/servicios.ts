import type { Locale } from '$lib/stores/i18n.svelte';
import { gnFromEs } from './gnFallback';

export interface ServiciosContent {
	pageTitle: string;
	metaDesc: string;
	ogTitle: string;
	ogDesc: string;
	kicker: string;
	subtitle: string;
	queEsTitle: string;
	queEsP1: string;
	queEsP2: string;
	marcoTitle: string;
	marcoP1: string;
	principiosTitle: string;
	principios: string[];
	queOfreceTitle: string;
	queOfreceIntro: string;
	queOfrece: string[];
	queOfreceFootnote: string;
	paraQuienesTitle: string;
	paraQuienes: string[];
	serviciosTitle: string;
	servicios: string[];
	limitesTitle: string;
	limitesWarning: string;
	limitesP1: string;
	limitesP2: string;
	limitesP3: string;
	limitesP4: string;
	limitesP5: string;
	liabilityText: string;
	termsLink: string;
	fuentesTitle: string;
	fuentesP1: string;
	contactoTitle: string;
	contactoContent: string;
	citationLabel: string;
	affil: string;
	printBrand: string;
	printGenerated: string;
}

const BASE: Record<'es' | 'en' | 'pt', ServiciosContent> = {
	es: {
		pageTitle: 'Spatia — análisis geoespacial abierto para el noreste argentino y sus regiones transfronterizas',
		metaDesc: 'Spatia es una plataforma de análisis geoespacial orientada al noreste argentino y sus regiones transfronterizas. Reúne análisis satelitales, censales y ambientales para investigación, gestión pública y cooperación internacional.',
		ogTitle: 'Spatia — servicios de análisis geoespacial',
		ogDesc: 'Plataforma pública de análisis geoespacial abierto. Análisis reproducible y acceso abierto. Hospedado en spatia.ar.',
		kicker: 'Análisis geoespacial abierto · Noreste argentino y regiones transfronterizas',
		subtitle: 'Plataforma de análisis geoespacial orientada al noreste argentino y sus regiones transfronterizas. Datos, métodos y código abiertos. Hospedado en <a href="/">spatia.ar</a>.',
		queEsTitle: 'Qué es Spatia',
		queEsP1: '<strong>Spatia</strong> es una plataforma de <strong>análisis geoespacial abierto</strong> desarrollada en el marco del <strong>Consejo Nacional de Investigaciones Científicas y Técnicas (CONICET, Argentina)</strong>, la <strong>Secretaría de Investigación y Posgrado (SINVyP)</strong> de la <strong>Facultad de Humanidades y Ciencias Sociales (FHyCS)</strong> de la <strong>Universidad Nacional de Misiones (UNaM)</strong>, y el acceso de nivel investigación al <strong>Google Earth Engine (GEE) Partner Tier</strong>. Reúne análisis satelitales, censales y ambientales sobre el noreste argentino y sus regiones transfronterizas para explorar de forma interactiva el estado ecológico, social y productivo de la región.',
		queEsP2: 'No constituye un sistema de recomendación, ni un sustituto del juicio experto o de la decisión política. Es una herramienta para visualizar capas de información pública, contrastarlas entre sí y enriquecer diagnósticos con evidencia cuantitativa reproducible.',
		marcoTitle: 'Marco institucional',
		marcoP1: 'Spatia es desarrollada por el Dr. Raimundo Elías Gómez, investigador del <strong>Consejo Nacional de Investigaciones Científicas y Técnicas (CONICET)</strong> en la Facultad de Humanidades y Ciencias Sociales de la Universidad Nacional de Misiones (FHyCS-UNaM). El software está registrado en la <strong>Dirección Nacional del Derecho de Autor (DNDA)</strong>. Los servicios profesionales se prestan como <strong>Servicio Tecnológico de Alto Nivel (STAN) de CONICET</strong>, autorizado en 2026 a través del Centro Científico Tecnológico CONICET Nordeste: <em>Asesoramiento y capacitación en análisis geoespaciales integrados socioeconómicos, productivos y ambientales</em>. Línea de investigación: <em>Huellas materiales del acuerdo Mercosur–Unión Europea en el Nordeste Argentino: flujos, agentes y transformaciones territoriales</em>.',
		principiosTitle: 'Principios',
		principios: [
			'<strong>Acceso abierto.</strong> Todos los datos que integra la plataforma provienen de fuentes públicas declaradas en la ficha metodológica de cada capa — entre otras: el Instituto Nacional de Estadística y Censos (INDEC), el Instituto Geográfico Nacional (IGN), OpenStreetMap (OSM), MapBiomas, el Joint Research Centre (JRC) de la Comisión Europea, Hansen Global Forest Change (GFC) y MODIS (Moderate Resolution Imaging Spectroradiometer). Cualquier persona puede consultar esas fuentes originales y reproducir los análisis siguiendo el pipeline documentado.',
			'<strong>Trazabilidad metodológica.</strong> Cada capa tiene su ficha técnica con fuentes, resolución, pipeline de procesamiento, supuestos y limitaciones conocidas. Las fichas están linkeadas desde el panel lateral de cada análisis.',
			'<strong>Reproducibilidad.</strong> El pipeline completo está documentado en un repositorio público. Los análisis pueden reproducirse y adaptarse a otras jurisdicciones sobre las mismas fuentes de datos abiertos.',
			'<strong>Actualización.</strong> La extensión actual de agua en Misiones, Corrientes, Chaco y Formosa se actualiza sola cada semana de octubre a abril y cada mes el resto del año, y un control semanal avisa si algún dato se atrasa. El PM2.5 se recalcula en mayo y noviembre. La deforestación usa la versión anual de Hansen Global Forest Change y se actualiza cuando se incorpora una versión nueva. El resto de las capas, y los demás territorios, se recalculan a mano cuando cambia la fuente. Cada capa muestra la fecha de sus datos.',
			'<strong>Acceso abierto a herramientas técnicas.</strong> La plataforma permite a gobiernos locales, organizaciones de investigación e instituciones técnicas acceder sin costo a herramientas técnicas de análisis e incorporar evidencia geoespacial reproducible a sus diagnósticos.',
			'<strong>Interoperabilidad.</strong> Cualquier vista o zona puede exportarse a CSV o GeoJSON. Se integra sin fricción con flujos de trabajo existentes en gobierno, academia y consultoría.',
		],
		queOfreceTitle: 'Qué ofrece',
		queOfreceIntro: 'Las capas de análisis cubren las cuatro provincias del NEA argentino, Itapúa y Alto Paraná; las censales y el potencial agrícola, sólo las cuatro provincias. En el resto de Paraguay y el sur de Brasil hay edificios, censo y límites. El chequeo EUDR por parcela cubre además Paraguay y el sur de Brasil. Spatia organiza sus análisis en cuatro grandes preguntas o "lentes", que pueden combinarse entre sí:',
		queOfrece: [
			'<strong>Ambiente y riesgo.</strong> Riesgo de inundación (edificios y tierra en zona baja según el relieve — MERIT Hydro HAND y Google Open Buildings —, presencia histórica de agua — JRC Global Surface Water — e inundación actual — Sentinel-1 SAR, radar de apertura sintética de la Agencia Espacial Europea, ESA), dinámica de deforestación, y calidad del aire (PM2.5 — material particulado fino de diámetro ≤ 2,5 micrómetros).',
			'<strong>Producción y suelo.</strong> Aptitud agrícola por cultivo en unidades físicas (NEA argentino), aptitud forestal (similitud con zonas ya plantadas) y uso y cobertura del suelo.',
			'<strong>Población y servicios.</strong> En las cuatro provincias argentinas (Misiones, Corrientes, Chaco y Formosa), con el censo del INDEC: carencia de servicios básicos (censo 2022), salud, capital y flujo educativo, perfil sociodemográfico y dinámica de población entre censos (1991–2022). El censo 2022 se reparte a hexágonos H3 por crosswalk dasimétrico y la serie 1991–2022 por interpolación areal. En las cuatro provincias, Itapúa y Alto Paraná: aislamiento (tiempo de viaje a la capital y a ciudades de 50 000 habitantes o más).',
			'<strong>Economía y comercio.</strong> Actividad económica (empleo, actividad, formación universitaria, luces nocturnas VIIRS y densidad de edificios; sólo en las cuatro provincias argentinas) y pérdida de bosque posterior a 2020, la fecha de corte del Reglamento de Deforestación de la Unión Europea (EUDR, por sus siglas en inglés).',
		],
		queOfreceFootnote: 'Todas las capas se proyectan sobre una grilla hexagonal <strong>H3</strong> (sistema de indexación geoespacial de código abierto desarrollado por Uber Technologies) de resolución 9 (~0,1 km² por hexágono, de unos 320 000 hexágonos en Misiones a más de un millón en Corrientes y en Chaco), con crosswalks documentados a radios censales para análisis cruzados.',
		paraQuienesTitle: 'Para quienes',
		paraQuienes: [
			'<strong>Organismos multilaterales</strong> — Programa de las Naciones Unidas para el Desarrollo (PNUD), Banco Interamericano de Desarrollo (BID), Banco Mundial, Organización de las Naciones Unidas para la Alimentación y la Agricultura (FAO), Comisión Económica para América Latina y el Caribe (CEPAL) — que necesiten diagnósticos geoespaciales rápidos y reproducibles sobre el noreste argentino y sus regiones transfronterizas, con datos curados y pipelines auditables.',
			'<strong>Gobiernos provinciales y municipales</strong> que busquen visualizar simultáneamente indicadores ambientales, sociales y productivos para planificación sectorial o negociación con terceros.',
			'<strong>Investigación académica</strong> con necesidad de datos curados listos para análisis, especialmente en ecología, geografía, economía ecológica, ciencias sociales y salud pública.',
			'<strong>Organizaciones de la sociedad civil</strong> que quieran fundamentar denuncias, propuestas de política o campañas de incidencia con evidencia cuantitativa rigurosa.',
			'<strong>Consultoras ambientales, agrícolas y forestales</strong> que necesiten evidencia geoespacial verificable para diagnósticos y estudios de base.',
			'<strong>Docentes y estudiantes</strong> que quieran introducirse al análisis geoespacial con una plataforma en español, con metodologías explicadas y datos reales.',
		],
		serviciosTitle: 'Servicios disponibles',
		servicios: [
			'<strong>Asesoría científica aplicada.</strong> Acompañamiento técnico y científico a proyectos de diagnóstico, planificación, evaluación de impacto o diseño de políticas, con interpretación experta y trabajo de campo.',
			'<strong>Diagnósticos territoriales con validación en terreno.</strong> Informes elaborados con interpretación científica contextualizada y verificación de campo sobre un recorte espacial solicitado.',
			'<strong>Capacitación y acompañamiento metodológico.</strong> Formación de equipos técnicos e institucionales en análisis geoespacial reproducible y diseño de flujos de trabajo propios sobre estándares abiertos.',
			'<strong>Independencia respecto de la plataforma.</strong> Los servicios profesionales consisten en asesoría científica basada en conocimiento experto y trabajo de campo. No incluyen la reventa de la plataforma pública, de sus datos ni de su capacidad de cómputo, que permanecen gratuitos y de acceso abierto. Lo que se trabaja para cada contratante (sus datos, resultados e informes) es confidencial: no se publica en Spatia ni se comparte con terceros sin su autorización por escrito.',
		],
		limitesTitle: 'Sobre los límites de este análisis',
		limitesWarning: 'Esta sección es importante y pedimos que se lea con atención antes de utilizar Spatia en cualquier decisión con consecuencias reales.',
		limitesP1: '<strong>Spatia es una herramienta analítica, no prescriptiva.</strong> Ningún análisis en esta plataforma constituye una recomendación de acción, una certificación técnica ni una consultoría profesional. Los scores, tipologías y rankings son síntesis cuantitativas de variables observables desde percepción remota, censos y fuentes administrativas. No sustituyen el juicio experto ni la responsabilidad de quien decide.',
		limitesP2: '<strong>Sin garantía de exactitud.</strong> La plataforma y toda la información que contiene se proveen tal cual (as-is), sin garantías expresas ni implícitas sobre exactitud, completitud, actualización o idoneidad para ningún propósito en particular. Los datos satelitales, censales y administrativos pueden contener errores, desfasajes temporales o limitaciones metodológicas documentadas en la ficha técnica de cada capa.',
		limitesP3: '<strong>Ningún análisis geoespacial reemplaza el trabajo de campo.</strong> La percepción remota captura condiciones físicas promedio en ventanas temporales definidas; no captura procesos sociales, conflictos de uso, restricciones jurídicas o cambios recientes que aún no se hayan incorporado a los datos. Las capas de Spatia requieren validación en terreno por parte de técnicos y responsables que conozcan la situación local.',
		limitesP4: 'Una zona clasificada con alta aptitud agrícola puede tener restricciones de uso, regímenes de tenencia o condiciones locales no reflejados en los datos disponibles. Una zona con bajo riesgo histórico de inundación puede estar experimentando cambios hidrológicos recientes. En todos los casos, el análisis geoespacial es un insumo diagnóstico, no un resultado de política.',
		limitesP5: '<strong>Cada clasificación disponible en Spatia debe entenderse como una hipótesis a contrastar, no como un resultado definitivo.</strong> El uso responsable implica leer la ficha metodológica de cada capa, reconocer sus limitaciones, y complementar el análisis con validación de campo.',
		liabilityText: '<strong>Responsabilidad.</strong> Bajo ninguna circunstancia el autor, el Consejo Nacional de Investigaciones Científicas y Técnicas (CONICET), la Universidad Nacional de Misiones (UNaM) ni su Facultad de Humanidades y Ciencias Sociales (FHyCS) serán responsables por daños directos, indirectos, incidentales o consecuentes derivados del uso de Spatia. Quien utilice esta plataforma en un contexto de gestión, inversión, investigación o política pública asume la responsabilidad de ese uso y debe complementarlo con validación profesional independiente.',
		termsLink: 'Leer términos y condiciones completos →',
		fuentesTitle: 'Fuentes y colaboraciones',
		fuentesP1: 'Spatia integra datos de fuentes públicas, entre ellas: INDEC (Censo Nacional de Población, Hogares y Viviendas 2022), IGN, JRC Global Surface Water, Hansen Global Forest Change, MODIS, MapBiomas (Argentina y Paraguay), CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data), ERA5 (reanálisis climático del Centro Europeo de Predicción Meteorológica a Medio Plazo — ECMWF, 5.ª generación), VIIRS (Visible Infrared Imaging Radiometer Suite, sensor NOAA/NASA), GHSL (Global Human Settlement Layer, Centro Común de Investigación de la Unión Europea), SoilGrids (ISRIC — International Soil Reference and Information Centre), misiones Sentinel de la Agencia Espacial Europea (ESA) / programa Copernicus, Global Building Atlas (GBA; Zhu et al., 2025, Earth System Science Data — ESSD), Overture Maps Foundation, OpenStreetMap, Dynamic World (Google / World Resources Institute), MERIT Hydro, Copernicus DEM GLO-30 (relieve, vía Mapterhorn), FABDEM, TerraClimate, Google Open Buildings, cartografía de plantaciones forestales (DNDFI-SAGyP, visor InForestal), conjunto PM2.5 de la Universidad de Dalhousie (ACAG — Atmospheric Composition Analysis Group), Oxford MAP (Malaria Atlas Project), Instituto Nacional de Estadística de Paraguay (INE) e Instituto Brasileiro de Geografia e Estatística (IBGE), entre otras. El procesamiento de la plataforma pública se realizó sobre Google Earth Engine como plataforma de cómputo, en su nivel de investigación (<strong>Partner Tier</strong>, uso no comercial).',
		contactoTitle: 'Contacto',
		contactoContent: '<strong>Raimundo Elías Gómez</strong><br />Investigador CONICET · Facultad de Humanidades y Ciencias Sociales, Universidad Nacional de Misiones (Argentina)<br /><a href="mailto:nealab@spatia.ar">nealab@spatia.ar</a>',
		citationLabel: 'Citación sugerida',
		affil: 'CONICET / FHyCS-UNaM · GEE Partner',
		printBrand: 'Spatia · análisis geoespacial abierto · spatia.ar',
		printGenerated: 'Documento generado el {date} desde spatia.ar/servicios',
	},

	en: {
		pageTitle: 'Spatia — open geospatial analysis for northeast Argentina and its cross-border regions',
		metaDesc: 'Spatia is a geospatial analysis platform focused on northeast Argentina and its cross-border regions. It brings together satellite, census and environmental analyses for research, public management and international cooperation.',
		ogTitle: 'Spatia — geospatial analysis services',
		ogDesc: 'Open public geospatial analysis platform. Reproducible analysis and open access. Hosted at spatia.ar.',
		kicker: 'Open geospatial analysis · Northeast Argentina & cross-border regions',
		subtitle: 'Geospatial analysis platform focused on northeast Argentina and its cross-border regions. Open data, methods and code. Hosted at <a href="/">spatia.ar</a>.',
		queEsTitle: 'What is Spatia',
		queEsP1: '<strong>Spatia</strong> is an <strong>open geospatial analysis</strong> platform developed within the framework of the <strong>National Scientific and Technical Research Council (CONICET, Argentina)</strong>, the <strong>Research and Postgraduate Secretariat (SINVyP)</strong> of the <strong>Faculty of Humanities and Social Sciences (FHyCS)</strong> at the <strong>National University of Misiones (UNaM)</strong>, and research-level access to the <strong>Google Earth Engine (GEE) Partner Tier</strong>. It brings together satellite, census and environmental analyses covering northeast Argentina and its cross-border regions to interactively explore the ecological, social and productive status of the region.',
		queEsP2: 'It is not a recommendation system, nor a substitute for expert judgement or political decision-making. It is a tool for visualising layers of public information, cross-referencing them, and enriching diagnostics with reproducible quantitative evidence.',
		marcoTitle: 'Institutional framework',
		marcoP1: 'Spatia is developed by Dr Raimundo Elías Gómez, researcher at the <strong>National Scientific and Technical Research Council of Argentina (CONICET)</strong>, based at the Faculty of Humanities and Social Sciences of the National University of Misiones (FHyCS-UNaM). The software is registered with Argentina\'s <strong>National Copyright Office (DNDA)</strong>. Professional services are provided as a <strong>CONICET High-Level Technological Service (STAN)</strong>, authorised in 2026 through the CONICET Northeast Scientific and Technological Centre: <em>Advice and training in integrated socioeconomic, productive and environmental geospatial analysis</em>. Research line: <em>Material footprints of the Mercosur–European Union agreement in Northeast Argentina: flows, agents and territorial transformations</em>.',
		principiosTitle: 'Principles',
		principios: [
			'<strong>Open access.</strong> All data integrated by the platform comes from public sources declared in the methodological fact sheet of each layer — including: the National Institute of Statistics and Censuses (INDEC), the National Geographic Institute (IGN), OpenStreetMap (OSM), MapBiomas, the Joint Research Centre (JRC) of the European Commission, Hansen Global Forest Change (GFC) and MODIS (Moderate Resolution Imaging Spectroradiometer). Anyone can consult those original sources and reproduce the analyses following the documented pipeline.',
			'<strong>Methodological traceability.</strong> Each layer has its technical fact sheet with sources, resolution, processing pipeline, assumptions and known limitations. The fact sheets are linked from the side panel of each analysis.',
			'<strong>Reproducibility.</strong> The complete pipeline is documented in a public repository. The analyses can be reproduced and adapted to other jurisdictions using the same open data sources.',
			'<strong>Updates.</strong> The current water extent in Misiones, Corrientes, Chaco and Formosa refreshes automatically every week from October to April and monthly the rest of the year, and a weekly check flags any data that falls behind. PM2.5 is recomputed in May and November. Deforestation uses the annual release of Hansen Global Forest Change and is updated when a new release is incorporated. The other layers, and the other territories, are recomputed by hand when the source changes. Each layer shows the date of its data.',
			'<strong>Open access to technical tools.</strong> The platform enables local governments, research organisations and technical institutions to access technical analysis tools free of charge and to incorporate reproducible geospatial evidence into their diagnostics.',
			'<strong>Interoperability.</strong> Any view or zone can be exported to CSV or GeoJSON. It integrates seamlessly with existing workflows in government, academia and consulting.',
		],
		queOfreceTitle: 'What it offers',
		queOfreceIntro: 'The analysis layers cover the four provinces of the Argentine NEA, Itapúa and Alto Paraná; the census layers and agricultural potential, only the four provinces. The rest of Paraguay and southern Brazil have buildings, census and boundaries. The parcel-level EUDR check also covers Paraguay and southern Brazil. Spatia organises its analyses around four broad questions or "lenses", which can be combined:',
		queOfrece: [
			'<strong>Environment & risk.</strong> Flood risk (buildings and land on low ground by terrain — MERIT Hydro HAND and Google Open Buildings —, historical water presence — JRC Global Surface Water — and current flooding — Sentinel-1 SAR, synthetic aperture radar of the European Space Agency, ESA), deforestation dynamics, and air quality (PM2.5 — fine particulate matter ≤ 2.5 microns in diameter).',
			'<strong>Production & land.</strong> Crop-by-crop agricultural suitability in physical units (Argentine NEA), forestry suitability (similarity to already planted areas), and land use and cover.',
			'<strong>Population & services.</strong> In the four Argentine provinces (Misiones, Corrientes, Chaco and Formosa), from the INDEC census: basic service deprivation (2022 census), health, educational capital and flow, sociodemographic profile and population change across censuses (1991–2022). The 2022 census is allocated to H3 hexagons by dasymetric crosswalk and the 1991–2022 series by areal interpolation. In the four provinces, Itapúa and Alto Paraná: isolation (travel time to the capital and to cities of 50,000 or more).',
			'<strong>Economy & trade.</strong> Economic activity (employment, activity, university education, VIIRS night lights and building density; only in the four Argentine provinces) and forest loss after 2020, the cut-off date of the EU Deforestation Regulation (EUDR).',
		],
		queOfreceFootnote: 'All layers are projected onto an <strong>H3</strong> hexagonal grid (open-source geospatial indexing system developed by Uber Technologies) at resolution 9 (~0.1 km² per hexagon, from about 320,000 hexagons in Misiones to over a million in Corrientes and in Chaco), with documented crosswalks to census tracts for cross-analysis.',
		paraQuienesTitle: 'Who it is for',
		paraQuienes: [
			'<strong>Multilateral organisations</strong> — United Nations Development Programme (UNDP), Inter-American Development Bank (IDB), World Bank, Food and Agriculture Organization (FAO), Economic Commission for Latin America and the Caribbean (ECLAC) — that need rapid and reproducible geospatial diagnostics on northeast Argentina and its cross-border regions, with curated data and auditable pipelines.',
			'<strong>Provincial and municipal governments</strong> seeking to simultaneously visualise environmental, social and productive indicators for sectoral planning or negotiations with third parties.',
			'<strong>Academic research</strong> requiring curated data ready for analysis, particularly in ecology, geography, ecological economics, social sciences and public health.',
			'<strong>Civil society organisations</strong> wishing to support complaints, policy proposals or advocacy campaigns with rigorous quantitative evidence.',
			'<strong>Environmental, agricultural and forestry consultancies</strong> requiring verifiable geospatial evidence for diagnostics and baseline studies.',
			'<strong>Teachers and students</strong> wishing to engage with geospatial analysis through a Spanish-language platform with explained methodologies and real data.',
		],
		serviciosTitle: 'Available services',
		servicios: [
			'<strong>Applied scientific advisory.</strong> Technical and scientific support for diagnostic, planning, impact assessment or policy design projects, with expert interpretation and fieldwork.',
			'<strong>Territorial diagnostics with field validation.</strong> Reports produced with contextualised scientific interpretation and on-the-ground verification for a requested spatial extent.',
			'<strong>Training and methodological support.</strong> Capacity-building for technical and institutional teams in reproducible geospatial analysis and the design of their own workflows on open standards.',
			'<strong>Independence from the platform.</strong> Professional services consist of scientific advisory grounded in expert knowledge and fieldwork. They do not include reselling the public platform, its data or its computing capacity, which remain free and openly accessible. Work done for each client (its data, results and reports) is confidential: it is neither published on Spatia nor shared with third parties without the client\'s written authorisation.',
		],
		limitesTitle: 'On the limits of this analysis',
		limitesWarning: 'This section is important and we ask that it be read carefully before using Spatia in any decision with real consequences.',
		limitesP1: '<strong>Spatia is an analytical, not prescriptive, tool.</strong> No analysis on this platform constitutes an action recommendation, technical certification or professional consultancy. The scores, typologies and rankings are quantitative summaries of variables observable through remote sensing, censuses and administrative sources. They do not replace expert judgement or the responsibility of the decision-maker.',
		limitesP2: '<strong>No accuracy guarantee.</strong> The platform and all information it contains are provided as-is, without express or implied warranties of accuracy, completeness, currency or fitness for any particular purpose. Satellite, census and administrative data may contain errors, time lags or methodological limitations documented in each layer\'s technical fact sheet.',
		limitesP3: '<strong>No geospatial analysis replaces fieldwork.</strong> Remote sensing captures average physical conditions over defined time windows; it does not capture social processes, land-use conflicts, legal restrictions or recent changes not yet incorporated into the data. Spatia\'s layers require on-the-ground validation by technicians and responsible parties who know the local situation.',
		limitesP4: 'An area classified as having high agricultural suitability may have use restrictions, tenure arrangements or local conditions not reflected in the available data. An area with a low historical flood risk may be experiencing recent hydrological changes. In all cases, geospatial analysis is a diagnostic input, not a policy outcome.',
		limitesP5: '<strong>Every classification available in Spatia should be understood as a hypothesis to be tested, not a definitive result.</strong> Responsible use entails reading each layer\'s methodological fact sheet, acknowledging its limitations, and complementing the analysis with field validation.',
		liabilityText: '<strong>Liability.</strong> Under no circumstances shall the author, the National Scientific and Technical Research Council (CONICET), the National University of Misiones (UNaM) or its Faculty of Humanities and Social Sciences (FHyCS) be liable for direct, indirect, incidental or consequential damages arising from the use of Spatia. Anyone using this platform in a management, investment, research or public policy context assumes responsibility for that use and must complement it with independent professional validation.',
		termsLink: 'Read full terms and conditions →',
		fuentesTitle: 'Sources and collaborations',
		fuentesP1: 'Spatia integrates data from public sources, including: INDEC (National Population, Household and Housing Census 2022), IGN, JRC Global Surface Water, Hansen Global Forest Change, MODIS, MapBiomas (Argentina and Paraguay), CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data), ERA5 (climate reanalysis by the European Centre for Medium-Range Weather Forecasts — ECMWF, 5th generation), VIIRS (Visible Infrared Imaging Radiometer Suite, NOAA/NASA sensor), GHSL (Global Human Settlement Layer, European Commission Joint Research Centre), SoilGrids (ISRIC — International Soil Reference and Information Centre), Sentinel missions by the European Space Agency (ESA) / Copernicus programme, Global Building Atlas (GBA; Zhu et al., 2025, Earth System Science Data — ESSD), Overture Maps Foundation, OpenStreetMap, Dynamic World (Google / World Resources Institute), MERIT Hydro, Copernicus DEM GLO-30 (relief, via Mapterhorn), FABDEM, TerraClimate, Google Open Buildings, forest plantation map (DNDFI-SAGyP, InForestal viewer), PM2.5 dataset from Dalhousie University (ACAG — Atmospheric Composition Analysis Group), Oxford MAP (Malaria Atlas Project), the National Institute of Statistics of Paraguay (INE) and the Brazilian Institute of Geography and Statistics (IBGE), among others. Processing of the public platform was carried out on Google Earth Engine as the computing platform, at its research level (<strong>Partner Tier</strong>, noncommercial use).',
		contactoTitle: 'Contact',
		contactoContent: '<strong>Raimundo Elías Gómez</strong><br />CONICET Researcher · Faculty of Humanities and Social Sciences, National University of Misiones (Argentina)<br /><a href="mailto:nealab@spatia.ar">nealab@spatia.ar</a>',
		citationLabel: 'Suggested citation',
		affil: 'CONICET / FHyCS-UNaM · GEE Partner',
		printBrand: 'Spatia · open geospatial analysis · spatia.ar',
		printGenerated: 'Document generated on {date} from spatia.ar/servicios',
	},

	pt: {
		pageTitle: 'Spatia — análise geoespacial aberta para o nordeste argentino e suas regiões transfronteiriças',
		metaDesc: 'Spatia é uma plataforma de análise geoespacial orientada ao nordeste argentino e suas regiões transfronteiriças. Reúne análises satelitais, censitárias e ambientais para pesquisa, gestão pública e cooperação internacional.',
		ogTitle: 'Spatia — serviços de análise geoespacial',
		ogDesc: 'Plataforma pública de análise geoespacial aberta. Análise reproduzível e acesso aberto. Hospedado em spatia.ar.',
		kicker: 'Análise geoespacial aberta · Nordeste argentino e regiões transfronteiriças',
		subtitle: 'Plataforma de análise geoespacial orientada ao nordeste argentino e suas regiões transfronteiriças. Dados, métodos e código abertos. Hospedado em <a href="/">spatia.ar</a>.',
		queEsTitle: 'O que é Spatia',
		queEsP1: '<strong>Spatia</strong> é uma plataforma de <strong>análise geoespacial aberta</strong> desenvolvida no âmbito do <strong>Conselho Nacional de Pesquisas Científicas e Técnicas (CONICET, Argentina)</strong>, da <strong>Secretaria de Pesquisa e Pós-graduação (SINVyP)</strong> da <strong>Faculdade de Humanidades e Ciências Sociais (FHyCS)</strong> da <strong>Universidade Nacional de Misiones (UNaM)</strong>, e com acesso de nível de pesquisa ao <strong>Google Earth Engine (GEE) Partner Tier</strong>. Reúne análises satelitais, censitárias e ambientais sobre o nordeste argentino e suas regiões transfronteiriças para explorar de forma interativa o estado ecológico, social e produtivo da região.',
		queEsP2: 'Não constitui um sistema de recomendação, nem um substituto do julgamento especializado ou da decisão política. É uma ferramenta para visualizar camadas de informação pública, confrontá-las entre si e enriquecer diagnósticos com evidências quantitativas reproduzíveis.',
		marcoTitle: 'Marco institucional',
		marcoP1: 'O Spatia é desenvolvido pelo Dr. Raimundo Elías Gómez, pesquisador do <strong>Conselho Nacional de Pesquisas Científicas e Técnicas da Argentina (CONICET)</strong> na Faculdade de Humanidades e Ciências Sociais da Universidade Nacional de Misiones (FHyCS-UNaM). O software está registrado na <strong>Direção Nacional do Direito Autoral da Argentina (DNDA)</strong>. Os serviços profissionais são prestados como <strong>Serviço Tecnológico de Alto Nível (STAN) do CONICET</strong>, autorizado em 2026 por meio do Centro Científico Tecnológico CONICET Nordeste: <em>Assessoria e capacitação em análises geoespaciais integradas socioeconômicas, produtivas e ambientais</em>. Linha de pesquisa: <em>Pegadas materiais do acordo Mercosul–União Europeia no Nordeste Argentino: fluxos, agentes e transformações territoriais</em>.',
		principiosTitle: 'Princípios',
		principios: [
			'<strong>Acesso aberto.</strong> Todos os dados integrados pela plataforma provêm de fontes públicas declaradas na ficha metodológica de cada camada — entre outras: o Instituto Nacional de Estatística e Censos (INDEC), o Instituto Geográfico Nacional (IGN), OpenStreetMap (OSM), MapBiomas, o Joint Research Centre (JRC) da Comissão Europeia, Hansen Global Forest Change (GFC) e MODIS (Moderate Resolution Imaging Spectroradiometer). Qualquer pessoa pode consultar essas fontes originais e reproduzir as análises seguindo o pipeline documentado.',
			'<strong>Rastreabilidade metodológica.</strong> Cada camada possui sua ficha técnica com fontes, resolução, pipeline de processamento, pressupostos e limitações conhecidas. As fichas estão vinculadas a partir do painel lateral de cada análise.',
			'<strong>Reprodutibilidade.</strong> O pipeline completo está documentado em um repositório público. As análises podem ser reproduzidas e adaptadas a outras jurisdições com base nas mesmas fontes de dados abertos.',
			'<strong>Atualização.</strong> A extensão atual de água em Misiones, Corrientes, Chaco e Formosa se atualiza sozinha toda semana de outubro a abril e todo mês no resto do ano, e uma verificação semanal avisa se algum dado atrasa. O PM2,5 é recalculado em maio e novembro. O desmatamento usa a versão anual do Hansen Global Forest Change e é atualizado quando uma versão nova é incorporada. As demais camadas, e os demais territórios, são recalculados manualmente quando a fonte muda. Cada camada mostra a data dos seus dados.',
			'<strong>Acesso aberto a ferramentas técnicas.</strong> A plataforma permite que governos locais, organizações de pesquisa e instituições técnicas acessem gratuitamente ferramentas técnicas de análise e incorporem evidências geoespaciais reproduzíveis a seus diagnósticos.',
			'<strong>Interoperabilidade.</strong> Qualquer visualização ou zona pode ser exportada para CSV ou GeoJSON. Integra-se sem fricção com fluxos de trabalho existentes em governo, academia e consultoria.',
		],
		queOfreceTitle: 'O que oferece',
		queOfreceIntro: 'As camadas de análise cobrem as quatro províncias do NEA argentino, Itapúa e Alto Paraná; as censitárias e o potencial agrícola, só as quatro províncias. No restante do Paraguai e no sul do Brasil há edificações, censo e limites. A verificação EUDR por parcela cobre também o Paraguai e o sul do Brasil. Spatia organiza suas análises em quatro grandes questões ou "lentes", que podem ser combinadas entre si:',
		queOfrece: [
			'<strong>Ambiente e risco.</strong> Risco de inundação (edifícios e terra em terreno baixo segundo o relevo — MERIT Hydro HAND e Google Open Buildings —, presença histórica de água — JRC Global Surface Water — e inundação atual — Sentinel-1 SAR, radar de abertura sintética da Agência Espacial Europeia, ESA), dinâmica de desmatamento, e qualidade do ar (PM2,5 — material particulado fino com diâmetro ≤ 2,5 micrômetros).',
			'<strong>Produção e solo.</strong> Aptidão agrícola por cultura em unidades físicas (NEA argentino), aptidão florestal (similaridade com áreas já plantadas) e uso e cobertura do solo.',
			'<strong>População e serviços.</strong> Nas quatro províncias argentinas (Misiones, Corrientes, Chaco e Formosa), com o censo do INDEC: carência de serviços básicos (censo 2022), saúde, capital e fluxo educacional, perfil sociodemográfico e dinâmica populacional entre censos (1991–2022). O censo 2022 é repartido em hexágonos H3 por crosswalk dassimétrico e a série 1991–2022 por interpolação areal. Nas quatro províncias, em Itapúa e no Alto Paraná: isolamento (tempo de viagem até a capital e até cidades de 50 mil habitantes ou mais).',
			'<strong>Economia e comércio.</strong> Atividade econômica (emprego, atividade, formação universitária, luzes noturnas VIIRS e densidade de edificações; só nas quatro províncias argentinas) e perda florestal posterior a 2020, a data de corte do Regulamento de Desmatamento da União Europeia (EUDR).',
		],
		queOfreceFootnote: 'Todas as camadas são projetadas sobre uma grade hexagonal <strong>H3</strong> (sistema de indexação geoespacial de código aberto desenvolvido pela Uber Technologies) de resolução 9 (~0,1 km² por hexágono, de cerca de 320.000 hexágonos em Misiones a mais de um milhão em Corrientes e no Chaco), com crosswalks documentados para setores censitários para análises cruzadas.',
		paraQuienesTitle: 'Para quem',
		paraQuienes: [
			'<strong>Organismos multilaterais</strong> — Programa das Nações Unidas para o Desenvolvimento (PNUD), Banco Interamericano de Desenvolvimento (BID), Banco Mundial, Organização das Nações Unidas para a Alimentação e a Agricultura (FAO), Comissão Econômica para a América Latina e o Caribe (CEPAL) — que necessitem de diagnósticos geoespaciais rápidos e reproduzíveis sobre o nordeste argentino e suas regiões transfronteiriças, com dados curados e pipelines auditáveis.',
			'<strong>Governos provinciais e municipais</strong> que busquem visualizar simultaneamente indicadores ambientais, sociais e produtivos para planejamento setorial ou negociação com terceiros.',
			'<strong>Pesquisa acadêmica</strong> com necessidade de dados curados prontos para análise, especialmente em ecologia, geografia, economia ecológica, ciências sociais e saúde pública.',
			'<strong>Organizações da sociedade civil</strong> que desejem fundamentar denúncias, propostas de política ou campanhas de incidência com evidências quantitativas rigorosas.',
			'<strong>Consultorias ambientais, agrícolas e florestais</strong> que necessitem de evidência geoespacial verificável para diagnósticos e estudos de base.',
			'<strong>Docentes e estudantes</strong> que desejem introduzir-se à análise geoespacial com uma plataforma em espanhol, com metodologias explicadas e dados reais.',
		],
		serviciosTitle: 'Serviços disponíveis',
		servicios: [
			'<strong>Assessoria científica aplicada.</strong> Acompanhamento técnico e científico a projetos de diagnóstico, planejamento, avaliação de impacto ou elaboração de políticas, com interpretação especializada e trabalho de campo.',
			'<strong>Diagnósticos territoriais com validação em campo.</strong> Relatórios elaborados com interpretação científica contextualizada e verificação de campo sobre um recorte espacial solicitado.',
			'<strong>Capacitação e acompanhamento metodológico.</strong> Formação de equipes técnicas e institucionais em análise geoespacial reproduzível e no desenho de seus próprios fluxos de trabalho sobre padrões abertos.',
			'<strong>Independência em relação à plataforma.</strong> Os serviços profissionais consistem em assessoria científica baseada em conhecimento especializado e trabalho de campo. Não incluem a revenda da plataforma pública, de seus dados nem de sua capacidade de computação, que permanecem gratuitos e de acesso aberto. O trabalho feito para cada contratante (seus dados, resultados e relatórios) é confidencial: não é publicado no Spatia nem compartilhado com terceiros sem sua autorização por escrito.',
		],
		limitesTitle: 'Sobre os limites desta análise',
		limitesWarning: 'Esta seção é importante e pedimos que seja lida com atenção antes de utilizar o Spatia em qualquer decisão com consequências reais.',
		limitesP1: '<strong>Spatia é uma ferramenta analítica, não prescritiva.</strong> Nenhuma análise nesta plataforma constitui uma recomendação de ação, certificação técnica ou consultoria profissional. Os scores, tipologias e rankings são sínteses quantitativas de variáveis observáveis por percepção remota, censos e fontes administrativas. Não substituem o julgamento especializado nem a responsabilidade de quem decide.',
		limitesP2: '<strong>Sem garantia de exatidão.</strong> A plataforma e todas as informações que contém são fornecidas no estado em que se encontram (as-is), sem garantias expressas ou implícitas de exatidão, completude, atualidade ou adequação a qualquer finalidade específica. Os dados satelitais, censitários e administrativos podem conter erros, defasagens temporais ou limitações metodológicas documentadas na ficha técnica de cada camada.',
		limitesP3: '<strong>Nenhuma análise geoespacial substitui o trabalho de campo.</strong> A percepção remota captura condições físicas médias em janelas temporais definidas; não captura processos sociais, conflitos de uso, restrições jurídicas ou mudanças recentes ainda não incorporadas aos dados. As camadas do Spatia requerem validação em campo por técnicos e responsáveis que conheçam a situação local.',
		limitesP4: 'Uma área classificada com alta aptidão agrícola pode ter restrições de uso, regimes de posse ou condições locais não refletidos nos dados disponíveis. Uma área com baixo risco histórico de inundação pode estar experimentando mudanças hidrológicas recentes. Em todos os casos, a análise geoespacial é um insumo diagnóstico, não um resultado de política.',
		limitesP5: '<strong>Cada classificação disponível no Spatia deve ser entendida como uma hipótese a ser testada, não como um resultado definitivo.</strong> O uso responsável implica ler a ficha metodológica de cada camada, reconhecer suas limitações e complementar a análise com validação de campo.',
		liabilityText: '<strong>Responsabilidade.</strong> Em nenhuma circunstância o autor, o Conselho Nacional de Pesquisas Científicas e Técnicas (CONICET), a Universidade Nacional de Misiones (UNaM) ou sua Faculdade de Humanidades e Ciências Sociais (FHyCS) serão responsáveis por danos diretos, indiretos, incidentais ou consequentes decorrentes do uso do Spatia. Quem utilizar esta plataforma em um contexto de gestão, investimento, pesquisa ou política pública assume a responsabilidade por esse uso e deve complementá-lo com validação profissional independente.',
		termsLink: 'Ler termos e condições completos →',
		fuentesTitle: 'Fontes e colaborações',
		fuentesP1: 'Spatia integra dados de fontes públicas, entre elas: INDEC (Censo Nacional de População, Domicílios e Moradias 2022), IGN, JRC Global Surface Water, Hansen Global Forest Change, MODIS, MapBiomas (Argentina e Paraguai), CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data), ERA5 (reanálise climática do Centro Europeu de Previsão de Tempo de Médio Prazo — ECMWF, 5.ª geração), VIIRS (Visible Infrared Imaging Radiometer Suite, sensor NOAA/NASA), GHSL (Global Human Settlement Layer, Centro Conjunto de Pesquisa da União Europeia), SoilGrids (ISRIC — International Soil Reference and Information Centre), missões Sentinel da Agência Espacial Europeia (ESA) / programa Copernicus, Global Building Atlas (GBA; Zhu et al., 2025, Earth System Science Data — ESSD), Overture Maps Foundation, OpenStreetMap, Dynamic World (Google / World Resources Institute), MERIT Hydro, Copernicus DEM GLO-30 (relevo, via Mapterhorn), FABDEM, TerraClimate, Google Open Buildings, cartografia de plantações florestais (DNDFI-SAGyP, visor InForestal), conjunto de dados PM2,5 da Universidade de Dalhousie (ACAG — Atmospheric Composition Analysis Group), Oxford MAP (Malaria Atlas Project), Instituto Nacional de Estatística do Paraguai (INE) e Instituto Brasileiro de Geografia e Estatística (IBGE), entre outras. O processamento da plataforma pública foi realizado no Google Earth Engine como plataforma de computação, em seu nível de pesquisa (<strong>Partner Tier</strong>, uso não comercial).',
		contactoTitle: 'Contato',
		contactoContent: '<strong>Raimundo Elías Gómez</strong><br />Pesquisador CONICET · Faculdade de Humanidades e Ciências Sociais, Universidade Nacional de Misiones (Argentina)<br /><a href="mailto:nealab@spatia.ar">nealab@spatia.ar</a>',
		citationLabel: 'Citação sugerida',
		affil: 'CONICET / FHyCS-UNaM · GEE Partner',
		printBrand: 'Spatia · análise geoespacial aberta · spatia.ar',
		printGenerated: 'Documento gerado em {date} a partir de spatia.ar/servicios',
	},
};

// gn = the Spanish text (see gnFromEs).
export const SERVICIOS: Record<Locale, ServiciosContent> = { ...BASE, gn: gnFromEs(BASE.es) };
