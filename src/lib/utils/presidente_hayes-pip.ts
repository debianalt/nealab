import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/presidente_hayes_py_boundary.json';

export function isInsidePresidenteHayes(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
