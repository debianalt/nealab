import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/concepcion_py_boundary.json';

export function isInsideConcepcion(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
