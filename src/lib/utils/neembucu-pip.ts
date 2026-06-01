import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/neembucu_py_boundary.json';

export function isInsideNeembucu(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
