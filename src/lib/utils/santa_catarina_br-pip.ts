import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/santa_catarina_br_boundary.json';

export function isInsideSantaCatarinaBr(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
