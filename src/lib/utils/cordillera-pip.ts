import { pointInGeoJsonFeature } from '$lib/utils/geometry';
import boundary from '$lib/data/cordillera_py_boundary.json';

export function isInsideCordillera(lat: number, lng: number): boolean {
	return pointInGeoJsonFeature([lng, lat], boundary);
}
