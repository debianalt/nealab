// One rule to aggregate a set of cells, shared by the lasso zones, the department they are
// read against and the zone's sheet across layers (7-nov-2026). A zone, its department and
// the ranking must say the same thing about the same cells.
import type { HexVariable } from '$lib/config';

/** Column a variable's mean is weighted by: the census base (households, people) or a plain
 *  denominator (low_bld_pct over the cell's buildings). */
export function weightColOf(v: HexVariable): string | undefined {
	return v.baseCol ?? v.weightCol;
}

export interface RowAggregate {
	/** Per variable: mean over the cells with a value, weighted by weightColOf when set. */
	rawValues: number[];
	/** Per variable: Σ over the cells (what a count variable, aggregation 'sum', reports). */
	totals: number[];
	/** Cells with at least one value. */
	cellsWithData: number;
}

/**
 * Mean of each variable over the cells that HAVE a value (a cell without data is not a zero).
 * A rate with a census base reads Σ rate·base / Σ base, like the radios it covers; a cell
 * without base is left out of that mean. A plain denominator (weightCol) does the same, except
 * that a parquet published before the column existed (the value undefined) falls back to the
 * unweighted mean instead of leaving the variable empty.
 */
export function aggregateRows(rows: Iterable<Record<string, any> | undefined>, numVars: HexVariable[]): RowAggregate {
	const sums = new Array(numVars.length).fill(0);
	const ns = new Array(numVars.length).fill(0);
	const totals = new Array(numVars.length).fill(0);
	const wcols = numVars.map(weightColOf);
	let cellsWithData = 0;
	for (const data of rows) {
		if (!data) continue;
		let any = false;
		for (let v = 0; v < numVars.length; v++) {
			const x = data[numVars[v].col];
			if (typeof x !== 'number' || !Number.isFinite(x)) continue;
			any = true;
			totals[v] += x;
			const wc = wcols[v];
			let w = 1;
			if (wc) {
				const raw = data[wc];
				if (numVars[v].baseCol || (raw !== undefined && raw !== null)) {
					w = Number(raw);
					if (!(w > 0)) continue;
				}
			}
			sums[v] += x * w; ns[v] += w;
		}
		if (any) cellsWithData++;
	}
	return { rawValues: sums.map((s, v) => (ns[v] > 0 ? s / ns[v] : NaN)), totals, cellsWithData };
}

/** A percentage or a rate in % (%, % campañas, %/yr…): compared in points, not as a ratio. */
export function isPercentUnit(v: HexVariable): boolean {
	return typeof v.unit === 'string' && v.unit.trim().startsWith('%');
}

export type RefCompare =
	| { kind: 'pts'; value: number }     // zone − department, in percentage points
	| { kind: 'ratio'; value: number }   // zone / department
	| { kind: 'diff'; value: number }    // zone − department, in the variable's unit (signed values)
	| { kind: 'share'; value: number };  // % of the department's total in the zone (counts)

/**
 * How a zone value reads against its department. A count: the zone's share of the department
 * total (a ratio of totals is always < 1 and says nothing). A percentage: the difference in
 * points (0,5 % against 0,2 % is «+0,3 pts», not «×2,5»). Anything else: the ratio when both
 * are positive, the difference otherwise (deltas, signed values).
 */
export function compareToRef(v: HexVariable, zoneMean: number, refMean: number, zoneTotal: number, refTotal: number): RefCompare | null {
	if (v.aggregation === 'sum') {
		return Number.isFinite(zoneTotal) && refTotal > 0 ? { kind: 'share', value: 100 * zoneTotal / refTotal } : null;
	}
	if (!Number.isFinite(zoneMean) || !Number.isFinite(refMean)) return null;
	if (isPercentUnit(v)) return { kind: 'pts', value: zoneMean - refMean };
	if (zoneMean > 0 && refMean > 0) return { kind: 'ratio', value: zoneMean / refMean };
	return { kind: 'diff', value: zoneMean - refMean };
}
