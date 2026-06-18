import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	preprocess: vitePreprocess(),
	kit: {
		adapter: adapter({
			pages: 'build',
			assets: 'build',
			fallback: '404.html',
			precompress: false,
			strict: true
		}),
		// Poll _app/version.json every 60s so an open tab learns about a new deploy
		// (sets the `updated` state → +layout shows a "recargar" banner). Avoids users
		// running a stale build until they hard-refresh.
		version: {
			pollInterval: 60_000
		}
	}
};

export default config;
