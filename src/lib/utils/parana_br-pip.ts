import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/parana_br_boundary.json';

export function isInsideParanaBr(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
