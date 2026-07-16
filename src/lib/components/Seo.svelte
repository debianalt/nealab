<script lang="ts">
	// Per-route metadata. Only renders into the HTML on routes with ssr = true —
	// `/` is ssr=false and falls back to the tags in app.html.
	// og:image / og:type / twitter:card are identical site-wide and live in app.html.
	import { page } from '$app/state';

	interface Props {
		title: string;
		description: string;
		/** Defaults to `title` — pass only when the social title differs. */
		ogTitle?: string;
		/** Defaults to `description`. */
		ogDescription?: string;
		noindex?: boolean;
	}

	let { title, description, ogTitle, ogDescription, noindex = false }: Props = $props();

	const url = $derived(`https://spatia.ar${page.url.pathname}`);
</script>

<svelte:head>
	<title>{title}</title>
	<meta name="description" content={description} />
	<link rel="canonical" href={url} />
	<meta property="og:title" content={ogTitle ?? title} />
	<meta property="og:description" content={ogDescription ?? description} />
	<meta property="og:url" content={url} />
	{#if noindex}
		<meta name="robots" content="noindex" />
	{/if}
</svelte:head>
