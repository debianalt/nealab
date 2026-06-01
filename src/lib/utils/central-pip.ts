import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/central_py_boundary.json';

export function isInsideCentral(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
