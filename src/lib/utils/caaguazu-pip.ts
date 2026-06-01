import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/caaguazu_py_boundary.json';

export function isInsideCaaguazu(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
