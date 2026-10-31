import { TERRITORY_REGISTRY, getDefaultTerritory, type TerritoryConfig, type CountryId } from '$lib/config';

export class TerritoryStore {
	activeTerritory: TerritoryConfig = $state(getDefaultTerritory());
	compareTerritory: TerritoryConfig | null = $state(null);
	compareModeActive: boolean = $state(false);
	regionalMode: boolean = $state(true);
	// null = trinational scope (all available territories, no country-first funnel)
	countryFilter: CountryId | null = $state(null);
	// Start view (fresh load / "Spatia"): no territory picked by the user yet, so the
	// map marks none and draws the outline of the whole area. Picking one, by click,
	// building, URL or a lens, ends it.
	homeView: boolean = $state(true);

	setTerritory(id: string) {
		const t = TERRITORY_REGISTRY[id];
		if (!t || !t.available) return;
		this.activeTerritory = t;
		this.countryFilter = t.country;
		this.homeView = false;
		// Exit compare mode when switching primary territory (but NOT regional mode — Option D)
		this.compareTerritory = null;
		this.compareModeActive = false;
	}

	enterCompareMode(territoryId: string) {
		const t = TERRITORY_REGISTRY[territoryId];
		if (!t || !t.available) return;
		this.compareTerritory = t;
		this.compareModeActive = true;
	}

	exitCompareMode() {
		this.compareTerritory = null;
		this.compareModeActive = false;
	}

	enterRegionalMode(country: CountryId | null = null) {
		this.compareTerritory = null;
		this.compareModeActive = false;
		this.countryFilter = country;
		this.regionalMode = true;
		// null = trinational: keep the default primary (Misiones) so the
		// regional engine routes Itapúa+Alto Paraná into the compare/regional
		// slots (the cross-frontier deforestation surface).
		if (country) {
			const first = Object.values(TERRITORY_REGISTRY).find(t => t.country === country && t.available);
			if (first) this.activeTerritory = first;
		}
	}

	enterCountryView(country: CountryId) {
		this.enterRegionalMode(country);
	}

	exitRegionalMode() {
		this.regionalMode = false;
		this.countryFilter = 'ar';
	}

	get availableCount(): number {
		return Object.values(TERRITORY_REGISTRY).filter(t => t.available).length;
	}
}
