/**
 * Point-in-polygon test (ray casting algorithm).
 * Point and polygon vertices are [lng, lat] pairs.
 */
export function pointInPolygon(point: [number, number], polygon: [number, number][]): boolean {
	let inside = false;
	for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
		const [xi, yi] = polygon[i], [xj, yj] = polygon[j];
		if ((yi > point[1]) !== (yj > point[1]) &&
			point[0] < (xj - xi) * (point[1] - yi) / (yj - yi) + xi)
			inside = !inside;
	}
	return inside;
}

/**
 * Point-in-(Multi)Polygon test against a GeoJSON FeatureCollection or single Feature.
 * Handles both `Polygon` and `MultiPolygon` geometries; uses outer rings only
 * (territory boundaries don't have meaningful holes).
 */
export function pointInGeoJsonFeature(point: [number, number], data: any): boolean {
	const feature = data?.type === 'FeatureCollection' ? data.features?.[0] : data;
	const geom = feature?.geometry ?? feature;
	if (!geom) return false;
	if (geom.type === 'Polygon') {
		return pointInPolygon(point, geom.coordinates[0] as [number, number][]);
	}
	if (geom.type === 'MultiPolygon') {
		for (const poly of geom.coordinates as [number, number][][][]) {
			if (pointInPolygon(point, poly[0] as [number, number][])) return true;
		}
	}
	return false;
}
