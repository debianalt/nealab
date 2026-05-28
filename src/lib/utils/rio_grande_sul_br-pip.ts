import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/rio_grande_sul_br_boundary.json';

export function isInsideRioGrandeSulBr(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
