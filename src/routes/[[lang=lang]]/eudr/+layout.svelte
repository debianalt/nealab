<script lang="ts">
	import '../../../app.css';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { lp } from '$lib/utils/locale-path';
	let { children } = $props();
	// gn reads the Spanish text (see gnFallback.ts).
	const NAV: Record<string, { map: string; report: string }> = {
		es: { map: '← Mapa', report: 'Informe EUDR' },
		en: { map: '← Map', report: 'EUDR report' },
		pt: { map: '← Mapa', report: 'Relatório EUDR' },
	};
	const nav = $derived(NAV[i18n.locale] ?? NAV.es);
</script>

<div class="min-h-screen bg-bg text-text font-sans">
	<header class="sticky top-0 z-50 border-b border-border" style="background: rgba(var(--c-bg-rgb), 0.88); backdrop-filter: blur(8px);">
		<div class="px-4 py-2.5 flex items-center gap-3">
			<a href="/" class="text-[17px] font-bold text-white tracking-wide hover:text-white/80 transition-colors">Spatia</a>
			<span class="text-white/20">&middot;</span>
			<a href="/" class="text-[13px] text-white/40 hover:text-white transition-colors">{nav.map}</a>
			<span class="text-white/20">&middot;</span>
			<a href={lp('/eudr/informe', i18n.locale)} class="text-[13px] text-white/40 hover:text-white transition-colors">{nav.report}</a>
		</div>
	</header>
	<main class="max-w-6xl mx-auto px-6">
		{@render children()}
	</main>
</div>
