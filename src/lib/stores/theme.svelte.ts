// Fondo del mapa y tema del sitio: Claro (atlas científico) · Oscuro (vitrina) ·
// Satélite (imagen bajo los datos, con el juego oscuro de colores y paneles: un panel
// blanco sobre la imagen encandila). Dos temas de color, tres fondos.
//
// The inline script in src/app.html sets <html data-theme> before the first paint
// (no flash) from the same localStorage key and the same default: keep FONDO_DEFAULT,
// the key and the theme-color values in sync with it.

export type Fondo = 'claro' | 'oscuro' | 'satelite';
export type Tema = 'claro' | 'oscuro';

export const FONDO_KEY = 'spatia_fondo';
// Claro for everyone on a first visit (decided 31-oct-2026: the atlas, not the vitrina,
// is what an official or an evaluator should see), not prefers-color-scheme.
export const FONDO_DEFAULT: Fondo = 'claro';
const THEME_COLOR: Record<Tema, string> = { claro: '#fbfaf7', oscuro: '#0a0c12' };

const isFondo = (v: unknown): v is Fondo => v === 'claro' || v === 'oscuro' || v === 'satelite';
export const temaDe = (f: Fondo): Tema => (f === 'claro' ? 'claro' : 'oscuro');

function readFondo(): Fondo {
	try {
		const v = typeof localStorage !== 'undefined' ? localStorage.getItem(FONDO_KEY) : null;
		if (isFondo(v)) return v;
	} catch { /* private window, blocked storage */ }
	return FONDO_DEFAULT;
}

class ThemeStore {
	fondo: Fondo = $state(readFondo());
	tema: Tema = $derived(temaDe(this.fondo));

	set(f: Fondo) {
		this.fondo = f;
		try { localStorage.setItem(FONDO_KEY, f); } catch { /* not persisted: still applies */ }
		if (typeof document === 'undefined') return;
		const t = temaDe(f);
		document.documentElement.dataset.theme = t;
		document.documentElement.dataset.fondo = f;
		document.querySelector('meta[name="theme-color"]')?.setAttribute('content', THEME_COLOR[t]);
	}
}

export const themeStore = new ThemeStore();
