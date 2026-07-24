import type { Locale } from '$lib/stores/i18n.svelte';

// Narrative prose for the EUDR storymap (/eudr/informe). Mirrors the pattern of
// content/methodology.ts: one block per Record<Locale,string>, rendered with
// {@html}. es + en are authored; gn/pt fall back to es via pick().
type Block = Partial<Record<Locale, string>>;

export function pick(b: Block, l: Locale): string {
	return b[l] ?? b.es ?? '';
}

export const INFORME: Record<string, Block> = {
	kicker: {
		es: 'Informe · datos MapBiomas Argentina',
		en: 'Report · MapBiomas Argentina data'
	},
	title: {
		es: 'Cosecha o deforestación',
		en: 'Harvest or deforestation'
	},
	subtitle: {
		es: 'Un pre-diagnóstico del riesgo bajo el reglamento europeo para los bosques del norte argentino, con una línea base de MapBiomas para separar la cosecha de una plantación de la conversión de bosque nativo.',
		en: 'A pre-diagnosis of risk under the European regulation for the forests of northern Argentina, using a MapBiomas baseline to separate a plantation harvest from native-forest conversion.'
	},

	intro: {
		es: `<p>El Reglamento (UE) 2023/1115 condiciona el acceso al mercado europeo de siete materias primas a que su producción no haya implicado deforestación posterior al 31 de diciembre de 2020. Para soja, carne bovina y madera, el operador debe geolocalizar los predios de origen y demostrar que no hubo pérdida de bosque tras esa fecha.</p>
<p>Argentina exporta esas materias primas a la Unión Europea desde el nordeste del país, donde conviven frentes agropecuarios, plantaciones forestales y remanentes de bosque nativo. La exigencia traslada a productores y exportadores un problema de trazabilidad territorial.</p>`,
		en: `<p>Regulation (EU) 2023/1115 conditions access to the European market for seven commodities on their production not involving deforestation after 31 December 2020. For soy, beef and timber, operators must geolocate the plots of origin and show no forest loss occurred after that date.</p>
<p>Argentina exports these commodities to the European Union from the north-east, where agricultural frontiers, forestry plantations and remnants of native forest coexist. The requirement hands producers and exporters a territorial traceability problem.</p>`
	},

	problema: {
		es: `<p>Los productos globales de pérdida forestal detectan la desaparición de cobertura arbórea, pero no distinguen su causa. La cosecha de una plantación de pino o eucalipto figura como pérdida de bosque, igual que la conversión de un bosque nativo a un cultivo.</p>
<p>Para la lógica del reglamento, lo primero es admisible y lo segundo no. Separarlos exige saber qué había en cada punto antes de la fecha de corte.</p>`,
		en: `<p>Global forest-loss products detect the disappearance of tree cover but do not distinguish its cause. Harvesting a pine or eucalyptus plantation appears as forest loss, just like converting a native forest to cropland.</p>
<p>Under the regulation the first is admissible and the second is not. Telling them apart requires knowing what was on each point before the cut-off.</p>`
	},

	solucion: {
		es: `<p>Aquí interviene MapBiomas Argentina. Su Serie Anual de Mapas de Cobertura y Uso del Suelo clasifica el territorio con una clase propia de silvicultura, separada de las formaciones nativas. Leída en el año de corte, esa clase aporta la línea base que los productos globales no ofrecen.</p>`,
		en: `<p>This is where MapBiomas Argentina comes in. Its Annual Land Cover and Land Use series classifies the territory with its own forestry-plantation class, separate from native formations. Read at the cut-off year, that class supplies the baseline global products lack.</p>`
	},

	metodo: {
		es: `<p>El pre-diagnóstico cruza tres fuentes sobre una grilla hexagonal. La cobertura sale de la <strong>Colección 2</strong> de MapBiomas Argentina, serie 1985–2024, leída en el año de corte, <strong>2020</strong>, y en el último año disponible, <strong>2024</strong>. La pérdida forestal es de Hansen Global Forest Change v1.13, acumulada entre <strong>2021 y 2025</strong>. El área quemada es de MODIS MCD64A1, en ese mismo período.</p>
<p>La pérdida sobre lo que ya era plantación en 2020 es compatible con un ciclo de cosecha; la pérdida sobre bosque o monte nativo es la señal de riesgo relevante. Son señales indicativas, no veredictos de cumplimiento.</p>`,
		en: `<p>The pre-diagnosis crosses three sources on a hexagonal grid. Land cover comes from <strong>Collection 2</strong> of MapBiomas Argentina, series 1985–2024, read at the cut-off year, <strong>2020</strong>, and at the latest available year, <strong>2024</strong>. Forest loss is from Hansen Global Forest Change v1.13, accumulated between <strong>2021 and 2025</strong>. Burned area is from MODIS MCD64A1 over the same period.</p>
<p>Loss over what was already plantation in 2020 is consistent with a harvest cycle; loss over native forest or woodland is the relevant risk signal. These are indicative signals, not compliance verdicts.</p>`
	},

	hallazgosLead: {
		es: 'Qué muestran los datos',
		en: 'What the data show'
	},

	chartLead: {
		es: `<p>Al atribuir la pérdida posterior al corte a la cobertura registrada en 2020, el <strong>22,8 %</strong> de la pérdida de las cuatro provincias recae sobre superficie que ya era plantación, y el <strong>77,2 %</strong> sobre vegetación nativa. El peso de esta corrección es muy desigual entre provincias.</p>`,
		en: `<p>Attributing post-cut-off loss to the cover recorded in 2020, <strong>22.8%</strong> of the loss across the four provinces falls on land that was already plantation, and <strong>77.2%</strong> on native vegetation. The weight of this correction is very uneven across provinces.</p>`
	},

	chartAfter: {
		es: `<p>En Corrientes, más de nueve de cada diez hectáreas perdidas estaban sobre plantación preexistente. Sin la línea base de MapBiomas, esa pérdida se contaría como deforestación y elevaría el riesgo asignado al sector forestal. En Chaco y Formosa, en cambio, casi toda la pérdida ocurre sobre vegetación nativa.</p>
<p>Esa vegetación nativa es, enteramente, bosque. La leyenda de la Colección 2 agrupa las tres clases nativas leñosas dentro de la categoría bosques: bosque cerrado, bosque abierto y bosque inundable. El <strong>97,2 %</strong> de las 539.741 hectáreas perdidas sobre cobertura nativa corresponde a bosque cerrado: 257.310 ha en Chaco, 165.997 en Formosa, 91.244 en Misiones y 9.813 en Corrientes. El bosque cerrado abarca tanto la selva paranaense de Misiones como el bosque seco chaqueño. En Misiones la proporción es del 100 %, porque la provincia no registra bosque abierto ni inundable.</p>`,
		en: `<p>In Corrientes, more than nine of every ten hectares lost were over pre-existing plantation. Without the MapBiomas baseline, that loss would count as deforestation and inflate the risk assigned to the forestry sector. In Chaco and Formosa, by contrast, almost all the loss falls on native vegetation.</p>
<p>That native vegetation is entirely forest. The Collection 2 legend groups the three native woody classes inside the forests category: closed forest, open forest and flooded forest. <strong>97.2%</strong> of the 539,741 hectares lost over native cover is closed forest: 257,310 ha in Chaco, 165,997 in Formosa, 91,244 in Misiones and 9,813 in Corrientes. Closed forest covers both the Paraná rainforest of Misiones and the dry Chaco woodland. In Misiones the share is 100%, as the province records no open or flooded forest.</p>`
	},

	fuegoLead: {
		es: `<p>El índice pondera también el área quemada, bajo el supuesto de que el fuego acompaña a la conversión de bosque. Conviene no confundirla con la pérdida de cobertura: son dos magnitudes independientes. Misiones lo muestra bien, porque pierde bosque nativo con la mayor intensidad de las cuatro provincias y a la vez es la que menos se quema —el 0,5 % de su superficie—, ya que la selva húmeda no arde y su desmonte no pasa por el fuego.</p>
<p>En el nordeste argentino el supuesto se sostiene mal por otra razón: buena parte del fuego regional ocurre sobre pastizal y humedal, donde pertenece al régimen natural del ecosistema. La misma lectura de cobertura que separa cosecha de conversión dice sobre qué ardió cada incendio.</p>`,
		en: `<p>The index also weights burned area, on the assumption that fire tracks forest conversion. It should not be confused with cover loss: the two are independent quantities. Misiones shows this well, losing native forest at the highest intensity of the four provinces while burning the least of them — 0.5% of its surface — because humid rainforest does not burn and its clearing does not go through fire.</p>
<p>In north-eastern Argentina the assumption holds poorly for another reason: much of the regional fire occurs over grassland and wetland, where it belongs to the natural regime of the ecosystem. The same cover reading that separates harvest from conversion tells what each fire burned.</p>`
	},

	fuegoAfter: {
		es: `<p>Formosa y Corrientes tienen una superficie quemada semejante, cercana al 20 % y al 18 %. La diferencia está en qué ardió. En Formosa, donde el monte chaqueño cubre el 79 % del territorio, 15,5 puntos de esa quema ocurrieron sobre vegetación nativa leñosa. En Corrientes, cubierta en un 10 % por monte y en el resto por los esteros y pastizales del Iberá, sólo 1,0 punto. El fuego correntino es del humedal.</p>
<p>Por eso el score cuenta únicamente la fracción que ardió sobre vegetación nativa leñosa: en la región equivale al 5,3 % de la superficie, frente al 13,4 % del área quemada total. Formosa encabeza el riesgo regional; Corrientes, a la que la quema total situaba a la par, queda por debajo.</p>`,
		en: `<p>Formosa and Corrientes have a similar share of their surface burned, close to 20% and 18%. The difference lies in what burned. In Formosa, where Chaco woodland covers 79% of the territory, 15.5 points of that burning fell on native woody vegetation. In Corrientes, 10% woodland and otherwise the marshes and grasslands of the Iberá, only 1.0 point. Corrientes fire is wetland fire.</p>
<p>The score therefore counts only the fraction that burned over native woody vegetation: across the region that is 5.3% of the surface, against 13.4% for total burned area. Formosa leads regional risk; Corrientes, which total burning placed alongside it, falls below.</p>`
	},

	limitaciones: {
		es: `<p>La señal de pérdida indica desaparición de cobertura, no su causa: la atribución final requiere verificación documental o de campo. El desglose por tipo de cobertura hereda la exactitud de la clasificación de MapBiomas a 30 m y solo cubre el territorio argentino. La verificación formal bajo el reglamento exige geometría parcelaria oficial, trazabilidad documental y debida diligencia profesional independiente.</p>
<p>El filtro de fuego separa la cobertura sobre la que ocurre la quema, no su causa: un incendio sobre monte nativo puede ser accidental y uno sobre pastizal puede preceder a un desmonte. Fuera de las cuatro provincias argentinas no hay línea base de cobertura cargada, de modo que allí el score sigue ponderando el área quemada total.</p>`,
		en: `<p>The loss signal indicates the disappearance of cover, not its cause: final attribution requires documentary or field verification. The cover-type breakdown inherits the accuracy of the MapBiomas classification at 30 m and covers Argentine territory only. Formal verification under the regulation requires official parcel geometry, documentary traceability and independent professional due diligence.</p>
<p>The fire filter separates the cover on which burning occurs, not its cause: a fire over native woodland may be accidental, and one over grassland may precede clearing. Outside the four Argentine provinces there is no cover baseline loaded, so the score there still weights total burned area.</p>`
	},

	cta: {
		es: `<p>Para evaluar un predio concreto, la herramienta permite subir un polígono o coordenadas y ver el desglose a resolución de parcela.</p>`,
		en: `<p>To assess a specific holding, the tool lets you upload a polygon or coordinates and see the breakdown at parcel resolution.</p>`
	},
	ctaButton: {
		es: 'Abrir la herramienta por parcela →',
		en: 'Open the parcel-level tool →'
	},

	// Map scrolly step boxes. Each carries a mini split-bar (plantation vs native)
	// styled by ScrollyTextBox; .num highlights figures.
	stepOverview: {
		es: `<p>Cuatro provincias del nordeste: <span class="num">68.517</span> hexágonos de unos 5&nbsp;km². El color marca el riesgo de pérdida posterior a 2020; el mapa destaca los de mayor riesgo, que ocupan una porción pequeña del territorio.</p>
<div class="stepbar" role="img" aria-label="23% cosecha, 77% nativo"><span class="sbp" style="width:22.8%"></span><span class="sbn" style="width:77.2%"></span></div>
<div class="sbl"><span class="lp">23&nbsp;% cosecha</span><span class="ln">77&nbsp;% nativo</span></div>
<div class="sbcap">reparto de la pérdida en las cuatro provincias</div>`,
		en: `<p>Four north-eastern provinces: <span class="num">68,517</span> hexagons of about 5&nbsp;km². Colour marks the risk of post-2020 loss; the map highlights the highest-risk cells, which cover a small share of the territory.</p>
<div class="stepbar" role="img" aria-label="23% harvest, 77% native"><span class="sbp" style="width:22.8%"></span><span class="sbn" style="width:77.2%"></span></div>
<div class="sbl"><span class="lp">23% harvest</span><span class="ln">77% native</span></div>
<div class="sbcap">split of the loss across the four provinces</div>`
	},
	stepMisiones: {
		es: `<p>La pérdida aparece en el <span class="num">78&nbsp;%</span> de los hexágonos, y es además la más intensa de las cuatro provincias. De esa pérdida, un <span class="num">29&nbsp;%</span> cae sobre plantación que ya existía en 2020.</p>
<div class="stepbar"><span class="sbp" style="width:28.9%"></span><span class="sbn" style="width:71.1%"></span></div>
<div class="sbl"><span class="lp">29&nbsp;% cosecha</span><span class="ln">71&nbsp;% nativo</span></div>`,
		en: `<p>Loss appears in <span class="num">78%</span> of hexagons, and is also the most intense of the four provinces. Of that loss, <span class="num">29%</span> falls on plantation that already existed in 2020.</p>
<div class="stepbar"><span class="sbp" style="width:28.9%"></span><span class="sbn" style="width:71.1%"></span></div>
<div class="sbl"><span class="lp">29% harvest</span><span class="ln">71% native</span></div>`
	},
	stepCorrientes: {
		es: `<p>El caso extremo. El <span class="num">91&nbsp;%</span> de la pérdida ocurre sobre plantación que ya existía en 2020: un ciclo de cosecha, no deforestación. Sin la línea base de MapBiomas, un producto global la contaría como pérdida de bosque nativo.</p>
<div class="stepbar"><span class="sbp" style="width:91.2%"></span><span class="sbn" style="width:8.8%"></span></div>
<div class="sbl"><span class="lp">91&nbsp;% cosecha</span><span class="ln">9&nbsp;% nativo</span></div>`,
		en: `<p>The extreme case. <span class="num">91%</span> of the loss occurs over plantation that already existed in 2020: a harvest cycle, not deforestation. Without the MapBiomas baseline, a global product would count it as native-forest loss.</p>
<div class="stepbar"><span class="sbp" style="width:91.2%"></span><span class="sbn" style="width:8.8%"></span></div>
<div class="sbl"><span class="lp">91% harvest</span><span class="ln">9% native</span></div>`
	},
	stepChaco: {
		es: `<p>Casi el <span class="num">100&nbsp;%</span> de la pérdida es sobre vegetación nativa. No hay plantación comercial que confunda la señal: acá la pérdida posterior al corte recae, casi toda, sobre monte y bosque nativo.</p>
<div class="stepbar"><span class="sbn" style="width:100%"></span></div>
<div class="sbl"><span class="lp">≈0&nbsp;% cosecha</span><span class="ln">100&nbsp;% nativo</span></div>`,
		en: `<p>Almost <span class="num">100%</span> of the loss is over native vegetation. No commercial plantation muddies the signal: here post-cutoff loss falls almost entirely on native woodland and forest.</p>
<div class="stepbar"><span class="sbn" style="width:100%"></span></div>
<div class="sbl"><span class="lp">≈0% harvest</span><span class="ln">100% native</span></div>`
	},
	stepFormosa: {
		es: `<p>El patrón se repite. La pérdida posterior al corte recae, casi por completo, sobre el bosque y el monte nativo del Chaco húmedo y semiárido.</p>
<div class="stepbar"><span class="sbn" style="width:100%"></span></div>
<div class="sbl"><span class="lp">≈0&nbsp;% cosecha</span><span class="ln">100&nbsp;% nativo</span></div>`,
		en: `<p>The pattern repeats. Post-cutoff loss falls almost entirely on the native forest and woodland of the humid and semi-arid Chaco.</p>
<div class="stepbar"><span class="sbn" style="width:100%"></span></div>
<div class="sbl"><span class="lp">≈0% harvest</span><span class="ln">100% native</span></div>`
	}
};

// MapBiomas citation (CC-BY) — the plantation baseline uses Collection 2 (2020 and 2024).
export const CITA_MAPBIOMAS =
	'MapBiomas – Colección 2 de la Serie Anual de Mapas de Cobertura y Uso del Suelo de Argentina, consultada el 24 de julio de 2026 a través del enlace: https://argentina.mapbiomas.org';
