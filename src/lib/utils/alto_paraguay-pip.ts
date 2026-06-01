import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/alto_paraguay_py_boundary.json';

export function isInsideAltoParaguay(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
