import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/paraguari_py_boundary.json';

export function isInsideParaguari(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
