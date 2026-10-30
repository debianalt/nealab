// maplibre-gl 6 is ESM-only and, under a bundler, needs its worker URL set once
// before the first map. Import maplibre through this module so that always happens.
import { setWorkerUrl } from 'maplibre-gl';
import workerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url';

setWorkerUrl(workerUrl);

export * from 'maplibre-gl';
