import { LANG_ENTRIES } from '$lib/utils/locale-path';

export const ssr = true;
export const prerender = true;

export function entries() {
	return LANG_ENTRIES;
}
