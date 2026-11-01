// maplibre-gl 6 is ESM-only and, under a bundler, needs its worker URL set once
// before the first map. Import maplibre through this module so that always happens.
import { setWorkerUrl, setWorkerCount } from 'maplibre-gl';
import workerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url';

setWorkerUrl(workerUrl);

// maplibre-gl 6 runs ONE worker outside Safari (util/worker_pool.ts). Spatia keeps
// ~90 sources (24 territory outlines, buildings PMTiles of every territory, radios,
// basemap): over Villeta (Central PY) that single worker took ~4,800 tile loads,
// sat ~900 replies behind, and the terrain and hexagon tiles never came back — the
// department painted nothing (0 hexagons in 4/4 runs; 9,365 with 4 workers, 2/2).
// Measured 1-nov-2026. Set before the first map: the pool is created lazily.
const cores = typeof navigator !== 'undefined' ? navigator.hardwareConcurrency || 4 : 4;
setWorkerCount(Math.min(4, Math.max(2, cores - 1)));

export * from 'maplibre-gl';
