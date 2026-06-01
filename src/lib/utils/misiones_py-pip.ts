import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/misiones_py_boundary.json';

export function isInsideMisionesPy(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
