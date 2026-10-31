// Open hexagon fact sheet (ficha): which cell, and the department/territory it
// belongs to (the per-department parquets are resolved from that context).
export interface FichaContext {
	territoryPrefix: string;
	territoryId: string;
	deptName: string | null;
	deptKey: string | null;
	// layer → parquetKey of the department file that holds the cell, when it is not the
	// one deptName leads to (?h3= links; src/lib/data/ficha_link_index.json).
	keys?: Record<string, string>;
}

class FichaStore {
	h3: string | null = $state(null);
	ctx: FichaContext | null = $state(null);

	open(h3: string, ctx: FichaContext) {
		this.h3 = h3;
		this.ctx = ctx;
	}

	close() {
		this.h3 = null;
		this.ctx = null;
	}
}

export const fichaStore = new FichaStore();
