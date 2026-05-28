import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/formosa_boundary.json';

export function isInsideFormosa(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
