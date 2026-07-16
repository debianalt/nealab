import type { ParamMatcher } from '@sveltejs/kit';
import { PREFIXED_LOCALES } from '$lib/utils/locale-path';

// `es` is absent from PREFIXED_LOCALES on purpose: Spanish lives on the unprefixed
// path (/servicios), so /es/servicios must 404 rather than become a duplicate of it.
// That also keeps the existing URLs — the ones linked from Zenodo and LinkedIn — intact.
export const match: ParamMatcher = (param) => (PREFIXED_LOCALES as readonly string[]).includes(param);
