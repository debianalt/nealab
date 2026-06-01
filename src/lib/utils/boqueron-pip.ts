import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/boqueron_py_boundary.json';

export function isInsideBoqueron(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
