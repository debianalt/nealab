<script lang="ts">
	// The one contact call of the site: «Pedir un diagnóstico a medida». A mailto with
	// what the person was looking at (layer, place, zones, hexagon, link) and four
	// questions; «copiar email» as fallback when no mail client is set up. The platform
	// itself stays free; this is the door to tailored, paid work (06_diagnostico_a_medida.md).
	import { i18n } from '$lib/stores/i18n.svelte';
	import { lp } from '$lib/utils/locale-path';
	import { REQUEST_DIAGNOSTIC } from '$lib/content/requestDiagnostic';
	import { buildRequestMailto, REQUEST_EMAIL, type RequestContext } from '$lib/utils/requestMail';

	let {
		ctx = {},
		variant = 'panel',
	}: {
		ctx?: RequestContext;
		variant?: 'panel' | 'welcome';
	} = $props();

	const t = $derived(REQUEST_DIAGNOSTIC[i18n.locale] ?? REQUEST_DIAGNOSTIC.es);
	const currentHref = () => (typeof location !== 'undefined' ? location.href : '');
	const href = $derived(buildRequestMailto(t, { ...ctx, href: ctx.href ?? currentHref() }, i18n.locale));
	// The URL changes after render (replaceState on every selection): rebuild on click.
	function refresh(e: MouseEvent) {
		(e.currentTarget as HTMLAnchorElement).href = buildRequestMailto(t, { ...ctx, href: ctx.href ?? currentHref() }, i18n.locale);
	}

	let copied = $state(false);
	async function copyEmail() {
		try { await navigator.clipboard.writeText(REQUEST_EMAIL); copied = true; setTimeout(() => (copied = false), 2000); } catch { /* clipboard blocked */ }
	}
</script>

<div class="rd" class:rd-welcome={variant === 'welcome'} data-export-hide>
	{#if variant === 'welcome'}<div class="rd-label">{t.welcomeLabel}</div>{/if}
	<div class="rd-row">
		<a class="rd-btn" {href} onclick={refresh}>{t.button}</a>
		<button class="rd-copy" onclick={copyEmail} title={REQUEST_EMAIL}>{copied ? `${REQUEST_EMAIL} ✓` : t.copy}</button>
	</div>
	<details class="rd-what">
		<summary>{t.whatTitle}</summary>
		<p>{t.what} <a href={lp('/servicios', i18n.locale)}>{t.servicesLink}</a></p>
	</details>
</div>

<style>
	.rd { margin: 10px 0; font-size: 12px; }
	.rd-welcome { margin-top: 14px; padding: 10px 12px; border: 1px solid rgba(var(--c-fg-rgb), 0.08); border-radius: 6px; }
	.rd-label { font-size: 11.5px; color: var(--s-ink-2); margin-bottom: 8px; line-height: 1.4; }
	.rd-row { display: flex; gap: 6px; align-items: center; }
	.rd-btn {
		flex: 1; text-align: center; padding: 6px 10px; border-radius: 4px; font-size: 12px; text-decoration: none;
		color: var(--s-ink-1); border: 1px solid var(--s-cat-1); background: rgba(196, 87, 47, 0.14);
	}
	.rd-btn:hover { background: rgba(196, 87, 47, 0.26); }
	.rd-copy {
		font: inherit; font-size: 11.5px; padding: 6px 8px; border-radius: 4px; cursor: pointer; white-space: nowrap;
		color: var(--s-ink-2); background: none; border: 1px solid rgba(var(--c-fg-rgb), 0.12);
	}
	.rd-copy:hover { color: var(--s-ink-1); }
	.rd-what { margin-top: 5px; }
	.rd-what summary { font-size: 11.5px; color: var(--s-ink-3); cursor: pointer; list-style: none; padding: 2px 0; }
	.rd-what summary::-webkit-details-marker { display: none; }
	.rd-what summary::before { content: '▸ '; font-size: 10px; }
	.rd-what[open] summary::before { content: '▾ '; }
	.rd-what p { font-size: 11.5px; line-height: 1.5; color: var(--s-ink-2); margin: 3px 0 0; }
	.rd-what a { color: var(--s-ink-1); }
</style>
