<script lang="ts">
	import '../app.css';
	import TermsModal from '$lib/components/TermsModal.svelte';
	import { terms } from '$lib/stores/terms.svelte';
	import { page } from '$app/stores';
	import { updated } from '$app/state';
	import { browser } from '$app/environment';
	import { stripLocale } from '$lib/utils/locale-path';
	let { children } = $props();

	const EXCLUDED = ['/terminos', '/servicios'];
	// Matched on the unprefixed path, or /en/terminos would miss and the modal would
	// cover the very pages it is meant to leave alone.
	const basePath = $derived(stripLocale($page.url.pathname));
</script>

<div class="relative w-full min-h-screen bg-bg">
	<!-- `browser &&`: terms.accepted is false server-side (the store guards its localStorage
	     read), so without this the modal's blocking overlay gets baked into the prerendered
	     HTML of every non-EXCLUDED route and is what crawlers see. -->
	{#if browser && !terms.accepted && !EXCLUDED.includes(basePath)}
		<TermsModal />
	{/if}
	{@render children()}

	<!-- New-deploy notice: version.json is polled (svelte.config: version.pollInterval),
	     so an open tab flips updated.current on a new build and can reload into the
	     latest instead of silently running stale code. -->
	{#if updated.current}
		<div class="fixed bottom-4 left-1/2 -translate-x-1/2 z-[100] flex items-center gap-3 px-4 py-2 rounded-full bg-black/90 backdrop-blur-md border border-yellow-400/50 text-[12px] text-white shadow-2xl">
			<span>Nueva versión disponible</span>
			<button onclick={() => location.reload()}
				class="px-3 py-1 rounded-full bg-yellow-400 text-black font-bold hover:bg-yellow-300 transition-colors cursor-pointer">
				Recargar
			</button>
		</div>
	{/if}
</div>
