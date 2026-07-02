import { sveltekit } from '@sveltejs/kit/vite';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	optimizeDeps: {
		exclude: ['@duckdb/duckdb-wasm'],
		esbuildOptions: {
			target: 'esnext'
		}
	},
	worker: {
		format: 'es'
	},
	build: {
		target: 'esnext'
	},
	server: {
		proxy: {
			'/r2': {
				target: 'https://cdn.spatia.ar',
				changeOrigin: true,
				rewrite: (path) => path.replace(/^\/r2/, '')
			},
			'/api/terrain': {
				target: 'https://s3.amazonaws.com',
				changeOrigin: true,
				rewrite: (path) => path.replace('/api/terrain', '/elevation-tiles-prod/terrarium')
			},
			'/api': {
				target: 'http://localhost:8788',
				changeOrigin: true
			}
		}
	}
});
