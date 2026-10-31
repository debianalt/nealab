// Which of the 24 territories a map point falls in — the map's click-to-select and
// hover hint. Bbox prefilter from TERRITORY_REGISTRY, then the territory's own
// point-in-polygon. The first four keep the order the click handler always used,
// so behaviour on the shared river banks (Misiones/Itapúa/Corrientes) is unchanged.
import { TERRITORY_REGISTRY } from '$lib/config';
import { isInsideItapua } from './itapua-pip';
import { isInsideAltoParana } from './alto_parana-pip';
import { isInsideMisiones } from './misiones-pip';
import { isInsideCorrientes } from './corrientes-pip';
import { isInsideChaco } from './chaco-pip';
import { isInsideFormosa } from './formosa-pip';
import { isInsideParanaBr } from './parana_br-pip';
import { isInsideSantaCatarinaBr } from './santa_catarina_br-pip';
import { isInsideRioGrandeSulBr } from './rio_grande_sul_br-pip';
import { isInsideConcepcion } from './concepcion-pip';
import { isInsideSanPedro } from './san_pedro-pip';
import { isInsideCordillera } from './cordillera-pip';
import { isInsideGuaira } from './guaira-pip';
import { isInsideCaaguazu } from './caaguazu-pip';
import { isInsideCaazapa } from './caazapa-pip';
import { isInsideMisionesPy } from './misiones_py-pip';
import { isInsideParaguari } from './paraguari-pip';
import { isInsideCentral } from './central-pip';
import { isInsideNeembucu } from './neembucu-pip';
import { isInsideAmambay } from './amambay-pip';
import { isInsideCanindeyu } from './canindeyu-pip';
import { isInsidePresidenteHayes } from './presidente_hayes-pip';
import { isInsideBoqueron } from './boqueron-pip';
import { isInsideAltoParaguay } from './alto_paraguay-pip';

const TESTS: Array<[string, (lat: number, lng: number) => boolean]> = [
	['itapua_py', isInsideItapua],
	['alto_parana_py', isInsideAltoParana],
	['misiones', isInsideMisiones],
	['corrientes', isInsideCorrientes],
	['chaco', isInsideChaco],
	['formosa', isInsideFormosa],
	['parana_br', isInsideParanaBr],
	['santa_catarina_br', isInsideSantaCatarinaBr],
	['rio_grande_sul_br', isInsideRioGrandeSulBr],
	['concepcion_py', isInsideConcepcion],
	['san_pedro_py', isInsideSanPedro],
	['cordillera_py', isInsideCordillera],
	['guaira_py', isInsideGuaira],
	['caaguazu_py', isInsideCaaguazu],
	['caazapa_py', isInsideCaazapa],
	['misiones_py', isInsideMisionesPy],
	['paraguari_py', isInsideParaguari],
	['central_py', isInsideCentral],
	['neembucu_py', isInsideNeembucu],
	['amambay_py', isInsideAmambay],
	['canindeyu_py', isInsideCanindeyu],
	['presidente_hayes_py', isInsidePresidenteHayes],
	['boqueron_py', isInsideBoqueron],
	['alto_paraguay_py', isInsideAltoParaguay],
];

export function territoryAt(lat: number, lng: number): string | null {
	for (const [id, inside] of TESTS) {
		const t = TERRITORY_REGISTRY[id];
		if (!t?.available) continue;
		const [w, s, e, n] = t.bbox;
		if (lng < w || lng > e || lat < s || lat > n) continue;
		if (inside(lat, lng)) return id;
	}
	return null;
}
