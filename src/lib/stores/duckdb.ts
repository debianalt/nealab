import type { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import type { Table } from 'apache-arrow';

let db: AsyncDuckDB | null = null;
let conn: AsyncDuckDBConnection | null = null;
let initPromise: Promise<void> | null = null;
let initError: string | null = null;

const INIT_TIMEOUT_MS = 20_000;

// Serve the DuckDB-WASM bundles from R2 (same origin as the parquets) instead of the
// jsDelivr CDN. Ad-blockers / privacy extensions commonly block jsDelivr — that stalled
// the worker/wasm fetch so init hit the 20s timeout → "error al iniciar el motor de datos"
// (reported in the user's NORMAL browser, not just incognito). R2 is demonstrably reachable
// for these users (the app's parquet queries already go there). WASM is uploaded gzipped
// with Content-Encoding so the wire size matches jsDelivr (~8 MB). mvp + eh only — the
// coi/pthread bundle needs COOP/COEP headers we don't set.
const R2_DUCKDB = 'https://cdn.spatia.ar/data/duckdb/v1_32_0';
const R2_BUNDLES = {
	mvp: { mainModule: `${R2_DUCKDB}/duckdb-mvp.wasm`, mainWorker: `${R2_DUCKDB}/duckdb-browser-mvp.worker.js` },
	eh: { mainModule: `${R2_DUCKDB}/duckdb-eh.wasm`, mainWorker: `${R2_DUCKDB}/duckdb-browser-eh.worker.js` },
};

async function createWorkerBlob(url: string): Promise<Worker> {
	const res = await fetch(url);
	const blob = new Blob([await res.text()], { type: 'application/javascript' });
	return new Worker(URL.createObjectURL(blob), { type: 'module' });
}

export async function initDuckDB(): Promise<void> {
	if (db) return;
	if (initPromise) return initPromise;

	initPromise = Promise.race([
		(async () => {
			const duckdb = await import('@duckdb/duckdb-wasm');

			// selectBundle feature-detects eh vs mvp; modules/workers are served from R2.
			const bundle = await duckdb.selectBundle(R2_BUNDLES);

			// Worker is cross-origin (R2) → wrap in a blob URL (same as before).
			const worker = await createWorkerBlob(bundle.mainWorker!);
			const logger = new duckdb.ConsoleLogger();

			db = new duckdb.AsyncDuckDB(logger, worker);
			await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

			conn = await db.connect();

			// Install and load httpfs for remote Parquet access (parallel saves ~200ms)
			await Promise.all([conn.query(`INSTALL httpfs`), conn.query(`LOAD httpfs`)]);
		})(),
		new Promise<void>((_, reject) =>
			setTimeout(() => reject(new Error('DuckDB init timeout')), INIT_TIMEOUT_MS)
		),
	]).catch((e) => {
		initError = e instanceof Error ? e.message : 'Error initializing data engine';
		initPromise = null;
		throw e;
	});

	return initPromise;
}

export async function query(sql: string): Promise<Table> {
	if (!conn) throw new Error('DuckDB not initialized. Call initDuckDB() first.');
	return await conn.query(sql);
}

export function isReady(): boolean {
	return db !== null && conn !== null;
}

export function getInitError(): string | null {
	return initError;
}
