/**
 * EUDR Deforestation Check API — DEPRECATED (410 Gone).
 *
 * This endpoint served H3 res-7 data from D1, which became inconsistent with
 * the res-9 analysis the UI at /eudr/check runs client-side against the R2
 * parquets. Deprecated 2026-07-08; the D1 import was removed from the CI
 * workflow at the same time. Use the interactive tool at
 * https://www.spatia.ar/eudr/check instead.
 */

const GONE_BODY = JSON.stringify({
	error: 'This API endpoint has been retired.',
	detail:
		'The /api/eudr/check endpoint returned H3 res-7 aggregates that are coarser than the res-9 analysis now used by the platform. Use the interactive tool at https://www.spatia.ar/eudr/check for point, polygon and batch analysis.',
	deprecated_on: '2026-07-08',
});

const HEADERS: Record<string, string> = {
	'Content-Type': 'application/json',
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Methods': 'POST, OPTIONS',
	'Access-Control-Allow-Headers': 'Content-Type, x-api-key',
};

export const onRequestOptions: PagesFunction = async () => {
	return new Response(null, { status: 204, headers: HEADERS });
};

export const onRequest: PagesFunction = async ({ request }) => {
	if (request.method === 'OPTIONS') {
		return new Response(null, { status: 204, headers: HEADERS });
	}
	return new Response(GONE_BODY, { status: 410, headers: HEADERS });
};
