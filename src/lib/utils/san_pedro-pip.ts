import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/san_pedro_py_boundary.json';

export function isInsideSanPedro(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
