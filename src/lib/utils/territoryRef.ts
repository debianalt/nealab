// Name of a territory as its real administrative unit, for UI text that would
// otherwise say "the territory": "la provincia de Misiones", "el departamento de
// Itapúa", "el estado de Paraná". The petals and rankings compare against one of
// these units; the text has to say which.
import { TERRITORY_REGISTRY } from '$lib/config';

// nominative ("la provincia de X"), genitive ("de la provincia de X"), locative ("en la provincia de X")
type Form = 'nom' | 'gen' | 'in';

// Portuguese takes the article with some Brazilian state names.
const PT_BR_OF: Record<string, string> = {
	parana_br: 'do Paraná',
	santa_catarina_br: 'de Santa Catarina',
	rio_grande_sul_br: 'do Rio Grande do Sul',
};

export function territoryRef(id: string, locale: string, form: Form = 'nom'): string {
	const t = TERRITORY_REGISTRY[id];
	if (!t) return '';
	// Misiones exists twice: the Argentine province and the Paraguayan department.
	const name = id === 'misiones_py' ? `${t.label} (Paraguay)` : t.label;
	const c = t.country;
	if (locale === 'en') {
		const nom = c === 'ar' ? `${name} Province` : c === 'py' ? `${name} Department` : `the state of ${name}`;
		return form === 'gen' ? `of ${nom}` : form === 'in' ? `in ${nom}` : nom;
	}
	if (locale === 'pt') {
		const nom = c === 'ar' ? `a província de ${name}` : c === 'py' ? `o departamento de ${name}` : `o estado ${PT_BR_OF[id] ?? `de ${name}`}`;
		if (form === 'nom') return nom;
		const fem = nom.startsWith('a ');
		if (form === 'in') return `${fem ? 'na' : 'no'} ${nom.slice(2)}`;
		return `${fem ? 'da' : 'do'} ${nom.slice(2)}`;
	}
	// es, and gn (whose UI text for these notes is Spanish)
	const nom = c === 'ar' ? `la provincia de ${name}` : c === 'py' ? `el departamento de ${name}` : `el estado de ${name}`;
	if (form === 'nom') return nom;
	if (form === 'in') return `en ${nom}`;
	return nom.startsWith('el ') ? `del ${nom.slice(3)}` : `de ${nom}`;
}

/** Fills {ref} / {ofRef} / {inRef} in an i18n text with the territory's unit. */
export function withRef(text: string, id: string, locale: string): string {
	return text
		.replaceAll('{ofRef}', territoryRef(id, locale, 'gen'))
		.replaceAll('{inRef}', territoryRef(id, locale, 'in'))
		.replaceAll('{ref}', territoryRef(id, locale, 'nom'));
}
