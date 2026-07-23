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
		es: `<p>El pre-diagnóstico combina la pérdida forestal (Hansen), el área quemada (MODIS) y la pérdida previa en un score de riesgo por hexágono, y lo cruza con la cobertura MapBiomas de 2020. La pérdida sobre lo que ya era plantación es compatible con un ciclo de cosecha; la pérdida sobre bosque o monte nativo es la señal de riesgo relevante.</p>
<p>Son señales indicativas, no veredictos de cumplimiento.</p>`,
		en: `<p>The pre-diagnosis combines forest loss (Hansen), burned area (MODIS) and prior loss into a per-hexagon risk score, and cross-references it with the 2020 MapBiomas cover. Loss over what was already plantation is consistent with a harvest cycle; loss over native forest or woodland is the relevant risk signal.</p>
<p>These are indicative signals, not compliance verdicts.</p>`
	},

	hallazgosLead: {
		es: 'Qué muestran los datos',
		en: 'What the data show'
	},

	chartLead: {
		es: `<p>Al atribuir la pérdida posterior al corte a la cobertura registrada en 2020, el <strong>17,4 %</strong> de la pérdida de las cuatro provincias recae sobre superficie que ya era plantación, y el <strong>82,6 %</strong> sobre vegetación nativa. El peso de esta corrección es muy desigual entre provincias.</p>`,
		en: `<p>Attributing post-cut-off loss to the cover recorded in 2020, <strong>17.4%</strong> of the loss across the four provinces falls on land that was already plantation, and <strong>82.6%</strong> on native vegetation. The weight of this correction is very uneven across provinces.</p>`
	},

	chartAfter: {
		es: `<p>En Corrientes, dos de cada tres hectáreas perdidas estaban sobre plantación preexistente. Sin la línea base de MapBiomas, esa pérdida se contaría como deforestación y elevaría el riesgo asignado al sector forestal. En Chaco y Formosa, en cambio, casi toda la pérdida ocurre sobre vegetación nativa.</p>`,
		en: `<p>In Corrientes, two of every three hectares lost were over pre-existing plantation. Without the MapBiomas baseline, that loss would count as deforestation and inflate the risk assigned to the forestry sector. In Chaco and Formosa, by contrast, almost all the loss falls on native vegetation.</p>`
	},

	limitaciones: {
		es: `<p>La señal de pérdida indica desaparición de cobertura, no su causa: la atribución final requiere verificación documental o de campo. La distinción por tipo de cobertura opera a nivel de hexágono, no de parcela, y solo cubre el territorio argentino. La verificación formal bajo el reglamento exige geometría parcelaria oficial, trazabilidad documental y debida diligencia profesional independiente.</p>
<p>El componente de área quemada (MODIS) no separa el fuego de origen antrópico del natural. En Corrientes y Formosa predominan los incendios de pastizal y humedal, que elevan el riesgo sin implicar deforestación; en esas provincias el peso del fuego debe leerse con esa salvedad.</p>`,
		en: `<p>The loss signal indicates the disappearance of cover, not its cause: final attribution requires documentary or field verification. The cover-type distinction works at the hexagon level, not the parcel level, and covers Argentine territory only. Formal verification under the regulation requires official parcel geometry, documentary traceability and independent professional due diligence.</p>
<p>The burned-area component (MODIS) does not separate anthropogenic from natural fire. In Corrientes and Formosa, grassland and wetland fires predominate and raise the risk score without implying deforestation; in those provinces the weight of fire should be read with that caveat.</p>`
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
		es: `<p>Cuatro provincias del nordeste: <span class="num">68.084</span> hexágonos de unos 5&nbsp;km². El color marca el riesgo de pérdida posterior a 2020; el riesgo alto se concentra en una porción pequeña del territorio.</p>
<div class="stepbar" role="img" aria-label="17% cosecha, 83% nativo"><span class="sbp" style="width:17.4%"></span><span class="sbn" style="width:82.6%"></span></div>
<div class="sbl"><span class="lp">17&nbsp;% cosecha</span><span class="ln">83&nbsp;% nativo</span></div>
<div class="sbcap">reparto de la pérdida en las cuatro provincias</div>`,
		en: `<p>Four north-eastern provinces: <span class="num">68,084</span> hexagons of about 5&nbsp;km². Colour marks the risk of post-2020 loss; high risk sits in a small share of the territory.</p>
<div class="stepbar" role="img" aria-label="17% harvest, 83% native"><span class="sbp" style="width:17.4%"></span><span class="sbn" style="width:82.6%"></span></div>
<div class="sbl"><span class="lp">17% harvest</span><span class="ln">83% native</span></div>
<div class="sbcap">split of the loss across the four provinces</div>`
	},
	stepMisiones: {
		es: `<p>La pérdida aparece en el <span class="num">76&nbsp;%</span> de los hexágonos, pero de baja intensidad por hexágono. De esa pérdida, un <span class="num">19&nbsp;%</span> cae sobre plantación que ya existía en 2020.</p>
<div class="stepbar"><span class="sbp" style="width:19.4%"></span><span class="sbn" style="width:80.6%"></span></div>
<div class="sbl"><span class="lp">19&nbsp;% cosecha</span><span class="ln">81&nbsp;% nativo</span></div>`,
		en: `<p>Loss appears in <span class="num">76%</span> of hexagons, but at low intensity per hexagon. Of that loss, <span class="num">19%</span> falls on plantation that already existed in 2020.</p>
<div class="stepbar"><span class="sbp" style="width:19.4%"></span><span class="sbn" style="width:80.6%"></span></div>
<div class="sbl"><span class="lp">19% harvest</span><span class="ln">81% native</span></div>`
	},
	stepCorrientes: {
		es: `<p>El caso extremo. El <span class="num">68&nbsp;%</span> de la pérdida ocurre sobre plantación que ya existía en 2020: un ciclo de cosecha, no deforestación. Sin la línea base de MapBiomas, un producto global la contaría como pérdida de bosque nativo.</p>
<div class="stepbar"><span class="sbp" style="width:68.3%"></span><span class="sbn" style="width:31.7%"></span></div>
<div class="sbl"><span class="lp">68&nbsp;% cosecha</span><span class="ln">32&nbsp;% nativo</span></div>`,
		en: `<p>The extreme case. <span class="num">68%</span> of the loss occurs over plantation that already existed in 2020: a harvest cycle, not deforestation. Without the MapBiomas baseline, a global product would count it as native-forest loss.</p>
<div class="stepbar"><span class="sbp" style="width:68.3%"></span><span class="sbn" style="width:31.7%"></span></div>
<div class="sbl"><span class="lp">68% harvest</span><span class="ln">32% native</span></div>`
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

// MapBiomas citation (CC-BY) — the plantation baseline uses Collection 1.
export const CITA_MAPBIOMAS =
	'MapBiomas – Colección 1 de la Serie Anual de Mapas de Cobertura y Uso del Suelo de Argentina, consultada a través de https://argentina.mapbiomas.org';
