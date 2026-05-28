import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/chaco_boundary.json';

export function isInsideChaco(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
