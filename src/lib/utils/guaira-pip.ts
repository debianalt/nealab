import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/guaira_py_boundary.json';

export function isInsideGuaira(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
