<script lang="ts">
	import { page } from '$app/state';
	import { i18n } from '$lib/stores/i18n.svelte';
</script>

<svelte:head>
	<title>{page.status} — Spatia</title>
</svelte:head>

<!-- Theme tokens (app.css), not fixed dark colours: the site defaults to Claro. -->
<div class="err">
	<div class="box">
		<p class="code">{page.status}</p>
		<p class="msg">{i18n.t(page.status === 404 ? 'error.notFound' : 'error.generic')}</p>
		<a href="/" class="back">{i18n.t('error.back')}</a>
	</div>
</div>

<style>
	.err { min-height: 100vh; display: flex; align-items: center; justify-content: center; background: var(--c-bg); padding: 0 16px; }
	.box { text-align: center; display: flex; flex-direction: column; align-items: center; gap: 24px; }
	.code { font-size: 3.75rem; font-weight: 700; color: var(--c-ink-4); margin: 0; }
	.msg { font-size: 0.875rem; color: var(--c-ink-2); max-width: 28rem; margin: 0; }
	.back { display: inline-block; padding: 8px 24px; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--c-ink-1); border: 1px solid var(--c-line-2); border-radius: 4px; text-decoration: none; }
	.back:hover { border-color: var(--c-line-3); }
</style>
