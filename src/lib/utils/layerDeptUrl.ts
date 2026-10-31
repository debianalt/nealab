import { getSatGlobalUrl } from '$lib/config';
import { loadDeptSummary } from '$lib/utils/deptSummaries';
import { normDeptName } from '$lib/utils/deptBoundaries';

// Parquet of ANOTHER layer for the department open in the panel (its own key:
// census layers name some departments differently), else that layer's
// territory-wide parquet. Used by charts that join a second layer (bivariate Y,
// distance profile).
export async function layerDeptUrl(
	layerId: string,
	territoryPrefix: string,
	deptName: string | null,
	deptKey: string | null,
	deptUrl: (layerId: string, key: string) => string | null,
): Promise<string> {
	if (deptName && deptKey) {
		const summary = await loadDeptSummary(layerId, territoryPrefix);
		const want = normDeptName(deptName);
		const d = (summary?.departments as any[] | undefined)?.find(
			(x) => normDeptName(x.dpto ?? x.distrito ?? x.municipio) === want);
		const url = deptUrl(layerId, d?.parquetKey ?? deptKey);
		if (url) return url;
	}
	return getSatGlobalUrl(layerId, territoryPrefix);
}
