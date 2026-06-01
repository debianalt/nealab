import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/amambay_py_boundary.json';

export function isInsideAmambay(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
