import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/caazapa_py_boundary.json';

export function isInsideCaazapa(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
