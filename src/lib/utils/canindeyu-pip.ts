import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/canindeyu_py_boundary.json';

export function isInsideCanindeyu(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
