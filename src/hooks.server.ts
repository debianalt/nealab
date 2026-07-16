import type { Handle } from '@sveltejs/kit';

// app.html hardcoded lang="es" and served it to all four locales. SvelteKit has no
// placeholder for the <html> attributes, so it gets substituted here. Hooks run during
// prerendering, so the attribute lands in the built HTML.
export const handle: Handle = async ({ event, resolve }) => {
	const lang = event.params.lang ?? 'es';

	return resolve(event, {
		transformPageChunk: ({ html }) => html.replace('%lang%', lang),
	});
};
