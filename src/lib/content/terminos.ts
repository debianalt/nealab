import type { Locale } from '$lib/stores/i18n.svelte';
import { gnFromEs } from './gnFallback';

export interface TerminosContent {
	pageTitle: string;
	metaDesc: string;
	printBrand: string;
	kicker: string;
	title: string;
	subtitle: string;
	s1Title: string;
	s1P1: string;
	s1P2: string;
	s2Title: string;
	s2P1: string;
	s2P2: string;
	s2P3: string;
	s3Title: string;
	s3Intro: string;
	s3List: string[];
	s3P2: string;
	s4Title: string;
	s4Intro: string;
	s4List: string[];
	s4P2: string;
	s5Title: string;
	s5P1: string;
	s5P2: string;
	s6Title: string;
	s6Intro: string;
	s6List: string[];
	s6P2: string;
	s7Title: string;
	s7Intro: string;
	s7List: string[];
	s8Title: string;
	s8P1: string;
	s9Title: string;
	s9P1: string;
	s9P2: string;
	s10Title: string;
	s10P1: string;
	s11Title: string;
	s11P1: string;
	acceptText: string;
	acceptBtn: string;
	footerVersion: string;
	affil: string;
	printGenerated: string;
}

const BASE: Record<'es' | 'en' | 'pt', TerminosContent> = {
	es: {
		pageTitle: 'Términos y condiciones — Spatia',
		metaDesc: 'Términos y condiciones de uso de Spatia, plataforma de análisis geoespacial abierto. Exclusión de garantías, limitación de responsabilidad, ley aplicable.',
		printBrand: 'Spatia · términos y condiciones · spatia.ar',
		kicker: 'Términos y condiciones de uso · Spatia / spatia.ar',
		title: 'Términos y condiciones',
		subtitle: 'Última actualización: 3 de noviembre de 2026. Al usar Spatia aceptás estos términos en su totalidad. Si no los aceptás, no uses la plataforma.',
		s1Title: '1. Aceptación',
		s1P1: 'El uso de Spatia y de cualquier análisis, dato o visualización disponible en <strong>spatia.ar</strong> implica la aceptación plena e incondicional de estos términos y condiciones. Esta aceptación se produce en el momento en que el usuario accede a la plataforma y confirma haber leído y comprendido las condiciones aquí establecidas.',
		s1P2: 'Si usás Spatia en nombre de una organización, institución o empresa, aceptás estos términos en nombre de esa entidad y declarás tener autoridad para hacerlo.',
		s2Title: '2. Naturaleza del servicio',
		s2P1: 'Spatia es una <strong>herramienta de análisis geoespacial académico</strong> de acceso abierto, desarrollada en el marco del Consejo Nacional de Investigaciones Científicas y Técnicas (CONICET, Argentina), la Secretaría de Investigación y Posgrado (SINVyP) de la Facultad de Humanidades y Ciencias Sociales (FHyCS) y la Universidad Nacional de Misiones (UNaM). Integra datos públicos de fuentes declaradas para generar visualizaciones e indicadores sobre el noreste argentino y sus regiones transfronterizas.',
		s2P2: 'Spatia <strong>no es</strong> un sistema de recomendación, un sistema de certificación, un peritaje técnico ni una consultoría profesional de ningún tipo. Sus análisis son síntesis cuantitativas de variables observables desde percepción remota, censos y fuentes administrativas, y deben entenderse como <strong>hipótesis a contrastar</strong>, nunca como verdades a aplicar directamente.',
		s2P3: 'El acceso a Spatia es libre y gratuito. Los diagnósticos a medida, asesoramientos y capacitaciones se prestan aparte, como <strong>Servicio Tecnológico de Alto Nivel (STAN) del CONICET</strong> «Asesoramiento y capacitación en análisis geoespaciales integrados socioeconómicos, productivos y ambientales»; se contratan por separado y se rigen por las condiciones que se acuerden con cada contratante, no por estos términos: lo que se trabaja para un contratante (sus datos, resultados e informes) es <strong>confidencial</strong> y no se publica en Spatia ni se comparte con terceros, salvo que el contratante lo autorice por escrito.',
		s3Title: '3. Exclusión de garantías',
		s3Intro: 'La plataforma y toda la información que contiene se proveen <strong>tal cual (as-is)</strong>, sin garantías de ningún tipo, expresas ni implícitas, incluyendo pero no limitado a:',
		s3List: [
			'Exactitud o precisión de los datos',
			'Completitud o ausencia de errores',
			'Actualización o vigencia de la información',
			'Idoneidad para un propósito particular',
			'Ausencia de interrupciones o errores técnicos',
		],
		s3P2: 'Los datos satelitales, censales y administrativos que integra Spatia pueden contener errores, desfasajes temporales o limitaciones metodológicas documentadas en la ficha técnica de cada capa. Es responsabilidad del usuario verificar la adecuación de esa información para el uso que pretende darle.',
		s4Title: '4. Limitación de responsabilidad',
		s4Intro: 'En la máxima medida permitida por la legislación aplicable, <strong>Raimundo Elías Gómez, el Consejo Nacional de Investigaciones Científicas y Técnicas (CONICET), la Universidad Nacional de Misiones (UNaM) y su Facultad de Humanidades y Ciencias Sociales (FHyCS)</strong> no serán responsables por daños de ningún tipo derivados del uso o la imposibilidad de uso de Spatia, incluyendo pero no limitado a:',
		s4List: [
			'Daños directos, indirectos, incidentales, especiales o consecuentes',
			'Pérdidas económicas, pérdida de ganancias o de oportunidades de negocio',
			'Decisiones de inversión, gestión o política pública basadas en los análisis',
			'Consecuencias jurídicas, regulatorias o comerciales de cualquier índole',
			'Pérdida o corrupción de datos',
		],
		s4P2: 'Esta limitación aplica independientemente de que los daños fueran previsibles o de que se hubiera advertido sobre su posibilidad.',
		s5Title: '5. Marco institucional',
		s5P1: 'El software de Spatia es de <strong>titularidad compartida, en partes iguales, del Consejo Nacional de Investigaciones Científicas y Técnicas (CONICET) y de la Universidad Nacional de Misiones (UNaM)</strong>; su autor y responsable es Raimundo Elías Gómez. El CONICET y la UNaM, a través de la Facultad de Humanidades y Ciencias Sociales (FHyCS), son además el <strong>marco académico e institucional</strong> de la investigación que dio origen a Spatia. No son proveedores de la plataforma en sentido comercial, no avalan los análisis como documentos oficiales de ninguna institución pública o privada y no asumen responsabilidad por el uso que terceros hagan de la información disponible en la plataforma.',
		s5P2: 'La mención del CONICET, la UNaM y la FHyCS en esta plataforma refleja el origen académico del desarrollo, no un aval institucional sobre la exactitud, completitud o idoneidad de ningún análisis particular.',
		s6Title: '6. Uso permitido',
		s6Intro: 'Spatia puede usarse libremente para:',
		s6List: [
			'Consulta, exploración y visualización de datos geoespaciales públicos',
			'Investigación académica y producción científica',
			'Educación y docencia',
			'Diagnósticos no vinculantes como insumo de deliberación o planificación',
			'Generación de hipótesis que serán contrastadas con fuentes primarias y trabajo de campo',
		],
		s6P2: 'El uso en decisiones con consecuencias jurídicas, económicas, territoriales o de política pública requiere <strong>validación independiente</strong> por profesionales habilitados y trabajo situado en campo.',
		s7Title: '7. Uso prohibido',
		s7Intro: 'Está prohibido:',
		s7List: [
			'Presentar los análisis de Spatia como certificaciones técnicas, peritajes, informes oficiales o documentos con valor jurídico sin indicar explícitamente su naturaleza indicativa y sus limitaciones metodológicas.',
			'Usar Spatia como única fuente de due-diligence en contextos regulatorios que exigen certificación profesional (incluido pero no limitado al cumplimiento del Reglamento de Deforestación de la Unión Europea (EUDR, por sus siglas en inglés), evaluaciones de impacto ambiental o tasaciones).',
			'Reproducir o redistribuir los análisis de Spatia atribuyéndoles una precisión, autoridad o vigencia que el servicio no garantiza.',
		],
		s8Title: '8. Propiedad intelectual',
		s8P1: 'Los datos integrados por Spatia provienen de fuentes públicas declaradas en la ficha técnica de cada capa — entre otras, el Instituto Nacional de Estadística y Censos (INDEC), el Instituto Geográfico Nacional (IGN), el Joint Research Centre (JRC) de la Comisión Europea, Hansen Global Forest Change (GFC), MODIS (Moderate Resolution Imaging Spectroradiometer), MapBiomas y OpenStreetMap (OSM) — y se usan conforme a sus licencias originales. El pipeline de procesamiento, el código de la plataforma y las visualizaciones son de autoría de Raimundo Elías Gómez, están registrados en la Dirección Nacional del Derecho de Autor (DNDA) y el código fuente se publica bajo licencia AGPL-3.0 en <a href="https://github.com/debianalt/nealab">github.com/debianalt/nealab</a>. Pueden citarse según la referencia indicada en <a href="/servicios">spatia.ar/servicios</a>.',
		s9Title: '9. Privacidad y datos',
		s9P1: 'Spatia no recopila datos personales de los usuarios. El acceso a la plataforma puede generar registros técnicos estándar (dirección IP, timestamp, ruta de acceso) utilizados exclusivamente para monitoreo de infraestructura y detección de errores. Estos registros no se comparten con terceros ni se usan para identificar personas.',
		s9P2: 'La aceptación de estos términos se registra localmente en el navegador del usuario (localStorage) y no se transmite a ningún servidor.',
		s10Title: '10. Modificaciones',
		s10P1: 'Estos términos pueden modificarse. La versión vigente está siempre disponible en <strong>spatia.ar/terminos</strong>. Cambios sustanciales en las condiciones de responsabilidad o uso requerirán nueva aceptación explícita, que la plataforma solicitará automáticamente al usuario.',
		s11Title: '11. Ley aplicable y jurisdicción',
		s11P1: 'Estos términos y condiciones se rigen por las leyes de la <strong>República Argentina</strong>, en particular el Código Civil y Comercial de la Nación (Ley 26.994 y sus modificatorias). Para cualquier controversia derivada del uso de Spatia, las partes se someten a la jurisdicción de los <strong>tribunales ordinarios de la ciudad de Posadas, provincia de Misiones</strong>, con renuncia expresa a cualquier otro fuero que pudiera corresponder.',
		acceptText: 'Al hacer clic en el botón de abajo confirmás que leíste y aceptás estos términos y condiciones en su totalidad.',
		acceptBtn: 'Acepto los términos — entrar a Spatia',
		footerVersion: 'spatia.ar/terminos · Versión del 3 de noviembre de 2026',
		affil: 'CONICET / FHyCS-UNaM · GEE Partner',
		printGenerated: 'Documento generado el {date} desde spatia.ar/terminos',
	},

	en: {
		pageTitle: 'Terms and conditions — Spatia',
		metaDesc: 'Terms and conditions of use for Spatia, open geospatial analysis platform. Disclaimer of warranties, limitation of liability, applicable law.',
		printBrand: 'Spatia · terms and conditions · spatia.ar',
		kicker: 'Terms and conditions of use · Spatia / spatia.ar',
		title: 'Terms and conditions',
		subtitle: 'Last updated: 3 November 2026. By using Spatia you accept these terms in full. If you do not accept them, do not use the platform.',
		s1Title: '1. Acceptance',
		s1P1: 'The use of Spatia and any analysis, data or visualisation available at <strong>spatia.ar</strong> constitutes full and unconditional acceptance of these terms and conditions. This acceptance occurs at the moment the user accesses the platform and confirms having read and understood the conditions set out herein.',
		s1P2: 'If you use Spatia on behalf of an organisation, institution or company, you accept these terms on behalf of that entity and declare that you have the authority to do so.',
		s2Title: '2. Nature of the service',
		s2P1: 'Spatia is an open-access <strong>academic geospatial analysis tool</strong>, developed within the framework of the National Scientific and Technical Research Council (CONICET, Argentina), the Research and Postgraduate Secretariat (SINVyP) of the Faculty of Humanities and Social Sciences (FHyCS) and the National University of Misiones (UNaM). It integrates public data from declared sources to generate visualisations and indicators about northeast Argentina and its cross-border regions.',
		s2P2: 'Spatia is <strong>not</strong> a recommendation system, certification system, technical expert assessment or professional consultancy of any kind. Its analyses are quantitative summaries of variables observable through remote sensing, censuses and administrative sources, and must be understood as <strong>hypotheses to be tested</strong>, never as truths to be applied directly.',
		s2P3: 'Access to Spatia is free of charge. Tailored diagnostics, advice and training are provided separately, as a CONICET <strong>High-Level Technological Service (STAN)</strong> «Advice and training in integrated socio-economic, productive and environmental geospatial analysis»; they are contracted separately and governed by the conditions agreed with each client, not by these terms: work done for a client (its data, results and reports) is <strong>confidential</strong> and is neither published on Spatia nor shared with third parties unless the client authorises it in writing.',
		s3Title: '3. Disclaimer of warranties',
		s3Intro: 'The platform and all information it contains are provided <strong>as-is</strong>, without warranties of any kind, express or implied, including but not limited to:',
		s3List: [
			'Accuracy or precision of data',
			'Completeness or absence of errors',
			'Currency or validity of information',
			'Fitness for a particular purpose',
			'Absence of interruptions or technical errors',
		],
		s3P2: 'Satellite, census and administrative data integrated by Spatia may contain errors, time lags or methodological limitations documented in each layer\'s technical fact sheet. It is the user\'s responsibility to verify the suitability of that information for the intended use.',
		s4Title: '4. Limitation of liability',
		s4Intro: 'To the maximum extent permitted by applicable law, <strong>Raimundo Elías Gómez, the National Scientific and Technical Research Council (CONICET), the National University of Misiones (UNaM) and its Faculty of Humanities and Social Sciences (FHyCS)</strong> shall not be liable for damages of any kind arising from the use or inability to use Spatia, including but not limited to:',
		s4List: [
			'Direct, indirect, incidental, special or consequential damages',
			'Economic losses, loss of profit or business opportunities',
			'Investment, management or public policy decisions based on the analyses',
			'Legal, regulatory or commercial consequences of any kind',
			'Loss or corruption of data',
		],
		s4P2: 'This limitation applies regardless of whether the damages were foreseeable or whether their possibility had been warned against.',
		s5Title: '5. Institutional framework',
		s5P1: 'The Spatia software is <strong>jointly owned, in equal shares, by the National Scientific and Technical Research Council (CONICET) and the National University of Misiones (UNaM)</strong>; its author and the person responsible for it is Raimundo Elías Gómez. CONICET and UNaM, through the Faculty of Humanities and Social Sciences (FHyCS), also constitute the <strong>academic and institutional framework</strong> of the research that gave rise to Spatia. They are not providers of the platform in a commercial sense, do not endorse the analyses as official documents of any public or private institution, and assume no responsibility for the use that third parties make of the information available on the platform.',
		s5P2: 'The mention of CONICET, UNaM and FHyCS on this platform reflects the academic origin of the development, not an institutional endorsement of the accuracy, completeness or suitability of any particular analysis.',
		s6Title: '6. Permitted use',
		s6Intro: 'Spatia may be freely used for:',
		s6List: [
			'Consultation, exploration and visualisation of public geospatial data',
			'Academic research and scientific production',
			'Education and teaching',
			'Non-binding diagnostics as input for deliberation or planning',
			'Generation of hypotheses to be tested against primary sources and fieldwork',
		],
		s6P2: 'Use in decisions with legal, economic, territorial or public policy consequences requires <strong>independent validation</strong> by qualified professionals and situated fieldwork.',
		s7Title: '7. Prohibited use',
		s7Intro: 'The following are prohibited:',
		s7List: [
			'Presenting Spatia analyses as technical certifications, expert assessments, official reports or documents with legal force without explicitly indicating their indicative nature and methodological limitations.',
			'Using Spatia as the sole source of due diligence in regulatory contexts that require professional certification (including but not limited to EU Deforestation Regulation (EUDR) compliance, environmental impact assessments or valuations).',
			'Reproducing or redistributing Spatia analyses attributing to them an accuracy, authority or currency that the service does not guarantee.',
		],
		s8Title: '8. Intellectual property',
		s8P1: 'Data integrated by Spatia comes from public sources declared in each layer\'s technical fact sheet — including the National Institute of Statistics and Censuses (INDEC), the National Geographic Institute (IGN), the Joint Research Centre (JRC) of the European Commission, Hansen Global Forest Change (GFC), MODIS (Moderate Resolution Imaging Spectroradiometer), MapBiomas and OpenStreetMap (OSM) — and is used in accordance with their original licences. The processing pipeline, platform code and visualisations are authored by Raimundo Elías Gómez and registered with Argentina\'s National Copyright Office (DNDA); the source code is published under the AGPL-3.0 licence at <a href="https://github.com/debianalt/nealab">github.com/debianalt/nealab</a>. They may be cited as indicated in <a href="/en/servicios">spatia.ar/en/servicios</a>.',
		s9Title: '9. Privacy and data',
		s9P1: 'Spatia does not collect personal data from users. Access to the platform may generate standard technical logs (IP address, timestamp, access path) used exclusively for infrastructure monitoring and error detection. These logs are not shared with third parties or used to identify individuals.',
		s9P2: 'Acceptance of these terms is recorded locally in the user\'s browser (localStorage) and is not transmitted to any server.',
		s10Title: '10. Modifications',
		s10P1: 'These terms may be modified. The current version is always available at <strong>spatia.ar/terminos</strong>. Substantial changes to liability or use conditions will require new explicit acceptance, which the platform will automatically request from the user.',
		s11Title: '11. Applicable law and jurisdiction',
		s11P1: 'These terms and conditions are governed by the laws of the <strong>Argentine Republic</strong>, in particular the Civil and Commercial Code of the Nation (Law 26.994 and its amendments). For any dispute arising from the use of Spatia, the parties submit to the jurisdiction of the <strong>ordinary courts of the city of Posadas, province of Misiones</strong>, expressly waiving any other jurisdiction that might apply.',
		acceptText: 'By clicking the button below you confirm that you have read and accept these terms and conditions in full.',
		acceptBtn: 'I accept the terms — enter Spatia',
		footerVersion: 'spatia.ar/terminos · Version of 3 November 2026',
		affil: 'CONICET / FHyCS-UNaM · GEE Partner',
		printGenerated: 'Document generated on {date} from spatia.ar/terminos',
	},

	pt: {
		pageTitle: 'Termos e condições — Spatia',
		metaDesc: 'Termos e condições de uso do Spatia, plataforma de análise geoespacial aberta. Exclusão de garantias, limitação de responsabilidade, lei aplicável.',
		printBrand: 'Spatia · termos e condições · spatia.ar',
		kicker: 'Termos e condições de uso · Spatia / spatia.ar',
		title: 'Termos e condições',
		subtitle: 'Última atualização: 3 de novembro de 2026. Ao utilizar o Spatia, você aceita estes termos na íntegra. Caso não os aceite, não utilize a plataforma.',
		s1Title: '1. Aceitação',
		s1P1: 'O uso do Spatia e de qualquer análise, dado ou visualização disponível em <strong>spatia.ar</strong> implica a aceitação plena e incondicional destes termos e condições. Essa aceitação ocorre no momento em que o usuário acessa a plataforma e confirma ter lido e compreendido as condições aqui estabelecidas.',
		s1P2: 'Se você utilizar o Spatia em nome de uma organização, instituição ou empresa, aceita estes termos em nome dessa entidade e declara ter autoridade para fazê-lo.',
		s2Title: '2. Natureza do serviço',
		s2P1: 'Spatia é uma <strong>ferramenta de análise geoespacial acadêmica</strong> de acesso aberto, desenvolvida no âmbito do Conselho Nacional de Pesquisas Científicas e Técnicas (CONICET, Argentina), da Secretaria de Pesquisa e Pós-Graduação (SINVyP) da Faculdade de Humanidades e Ciências Sociais (FHyCS) e da Universidade Nacional de Misiones (UNaM). Integra dados públicos de fontes declaradas para gerar visualizações e indicadores sobre o nordeste argentino e suas regiões transfronteiriças.',
		s2P2: 'Spatia <strong>não é</strong> um sistema de recomendação, um sistema de certificação, uma perícia técnica nem uma consultoria profissional de qualquer natureza. Suas análises são sínteses quantitativas de variáveis observáveis por percepção remota, censos e fontes administrativas, e devem ser entendidas como <strong>hipóteses a serem testadas</strong>, nunca como verdades a serem aplicadas diretamente.',
		s2P3: 'O acesso ao Spatia é livre e gratuito. Os diagnósticos sob medida, assessorias e capacitações são prestados à parte, como <strong>Serviço Tecnológico de Alto Nível (STAN) do CONICET</strong> «Assessoria e capacitação em análises geoespaciais integradas socioeconômicas, produtivas e ambientais»; são contratados separadamente e regidos pelas condições acordadas com cada contratante, não por estes termos: o trabalho feito para um contratante (seus dados, resultados e relatórios) é <strong>confidencial</strong> e não é publicado no Spatia nem compartilhado com terceiros, salvo autorização por escrito do contratante.',
		s3Title: '3. Exclusão de garantias',
		s3Intro: 'A plataforma e todas as informações que contém são fornecidas <strong>no estado em que se encontram (as-is)</strong>, sem garantias de qualquer espécie, expressas ou implícitas, incluindo, mas não se limitando a:',
		s3List: [
			'Exatidão ou precisão dos dados',
			'Completude ou ausência de erros',
			'Atualidade ou vigência das informações',
			'Adequação a uma finalidade específica',
			'Ausência de interrupções ou erros técnicos',
		],
		s3P2: 'Os dados satelitais, censitários e administrativos integrados pelo Spatia podem conter erros, defasagens temporais ou limitações metodológicas documentadas na ficha técnica de cada camada. É responsabilidade do usuário verificar a adequação dessas informações para o uso que pretende fazer.',
		s4Title: '4. Limitação de responsabilidade',
		s4Intro: 'Na máxima extensão permitida pela legislação aplicável, <strong>Raimundo Elías Gómez, o Conselho Nacional de Pesquisas Científicas e Técnicas (CONICET), a Universidade Nacional de Misiones (UNaM) e sua Faculdade de Humanidades e Ciências Sociais (FHyCS)</strong> não serão responsáveis por danos de qualquer natureza decorrentes do uso ou da impossibilidade de uso do Spatia, incluindo, mas não se limitando a:',
		s4List: [
			'Danos diretos, indiretos, incidentais, especiais ou consequentes',
			'Perdas econômicas, perda de lucros ou de oportunidades de negócio',
			'Decisões de investimento, gestão ou política pública baseadas nas análises',
			'Consequências jurídicas, regulatórias ou comerciais de qualquer natureza',
			'Perda ou corrupção de dados',
		],
		s4P2: 'Esta limitação se aplica independentemente de os danos terem sido previsíveis ou de ter sido alertado sobre sua possibilidade.',
		s5Title: '5. Marco institucional',
		s5P1: 'O software do Spatia é de <strong>titularidade compartilhada, em partes iguais, do Conselho Nacional de Pesquisas Científicas e Técnicas (CONICET) e da Universidade Nacional de Misiones (UNaM)</strong>; seu autor e responsável é Raimundo Elías Gómez. O CONICET e a UNaM, por meio da Faculdade de Humanidades e Ciências Sociais (FHyCS), constituem também o <strong>marco acadêmico e institucional</strong> da pesquisa que deu origem ao Spatia. Não são fornecedores da plataforma em sentido comercial, não avalizam as análises como documentos oficiais de qualquer instituição pública ou privada e não assumem responsabilidade pelo uso que terceiros façam das informações disponíveis na plataforma.',
		s5P2: 'A menção ao CONICET, à UNaM e à FHyCS nesta plataforma reflete a origem acadêmica do desenvolvimento, não um aval institucional sobre a exatidão, completude ou adequação de qualquer análise específica.',
		s6Title: '6. Uso permitido',
		s6Intro: 'Spatia pode ser utilizado livremente para:',
		s6List: [
			'Consulta, exploração e visualização de dados geoespaciais públicos',
			'Pesquisa acadêmica e produção científica',
			'Educação e ensino',
			'Diagnósticos não vinculantes como insumo para deliberação ou planejamento',
			'Geração de hipóteses a serem testadas com fontes primárias e trabalho de campo',
		],
		s6P2: 'O uso em decisões com consequências jurídicas, econômicas, territoriais ou de política pública requer <strong>validação independente</strong> por profissionais habilitados e trabalho situado em campo.',
		s7Title: '7. Uso proibido',
		s7Intro: 'É proibido:',
		s7List: [
			'Apresentar as análises do Spatia como certificações técnicas, perícias, relatórios oficiais ou documentos com valor jurídico sem indicar explicitamente sua natureza indicativa e suas limitações metodológicas.',
			'Utilizar o Spatia como única fonte de due diligence em contextos regulatórios que exigem certificação profissional (incluindo, mas não se limitando à conformidade com o Regulamento de Desmatamento da União Europeia — EUDR, avaliações de impacto ambiental ou avaliações patrimoniais).',
			'Reproduzir ou redistribuir as análises do Spatia atribuindo-lhes uma precisão, autoridade ou atualidade que o serviço não garante.',
		],
		s8Title: '8. Propriedade intelectual',
		s8P1: 'Os dados integrados pelo Spatia provêm de fontes públicas declaradas na ficha técnica de cada camada — entre outras, o Instituto Nacional de Estatística e Censos (INDEC), o Instituto Geográfico Nacional (IGN), o Joint Research Centre (JRC) da Comissão Europeia, Hansen Global Forest Change (GFC), MODIS (Moderate Resolution Imaging Spectroradiometer), MapBiomas e OpenStreetMap (OSM) — e são utilizados conforme suas licenças originais. O pipeline de processamento, o código da plataforma e as visualizações são de autoria de Raimundo Elías Gómez e estão registrados na Direção Nacional do Direito Autoral da Argentina (DNDA); o código-fonte é publicado sob a licença AGPL-3.0 em <a href="https://github.com/debianalt/nealab">github.com/debianalt/nealab</a>. Podem ser citados conforme a referência indicada em <a href="/pt/servicios">spatia.ar/pt/servicios</a>.',
		s9Title: '9. Privacidade e dados',
		s9P1: 'Spatia não coleta dados pessoais dos usuários. O acesso à plataforma pode gerar registros técnicos padrão (endereço IP, timestamp, caminho de acesso) utilizados exclusivamente para monitoramento de infraestrutura e detecção de erros. Esses registros não são compartilhados com terceiros nem utilizados para identificar pessoas.',
		s9P2: 'A aceitação destes termos é registrada localmente no navegador do usuário (localStorage) e não é transmitida a nenhum servidor.',
		s10Title: '10. Modificações',
		s10P1: 'Estes termos podem ser modificados. A versão vigente está sempre disponível em <strong>spatia.ar/terminos</strong>. Alterações substanciais nas condições de responsabilidade ou de uso exigirão nova aceitação explícita, que a plataforma solicitará automaticamente ao usuário.',
		s11Title: '11. Lei aplicável e jurisdição',
		s11P1: 'Estes termos e condições são regidos pelas leis da <strong>República Argentina</strong>, em particular pelo Código Civil e Comercial da Nação (Lei n.º 26.994 e suas modificações). Para qualquer controvérsia decorrente do uso do Spatia, as partes se submetem à jurisdição dos <strong>tribunais ordinários da cidade de Posadas, província de Misiones</strong>, com renúncia expressa a qualquer outro foro que pudesse ser aplicável.',
		acceptText: 'Ao clicar no botão abaixo, você confirma que leu e aceita estes termos e condições na íntegra.',
		acceptBtn: 'Aceito os termos — entrar no Spatia',
		footerVersion: 'spatia.ar/terminos · Versão de 3 de novembro de 2026',
		affil: 'CONICET / FHyCS-UNaM · GEE Partner',
		printGenerated: 'Documento gerado em {date} a partir de spatia.ar/terminos',
	},
};

// gn = the Spanish text (see gnFromEs).
export const TERMINOS: Record<Locale, TerminosContent> = { ...BASE, gn: gnFromEs(BASE.es) };
