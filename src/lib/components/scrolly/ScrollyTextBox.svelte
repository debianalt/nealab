<script lang="ts">
	/**
	 * ScrollyTextBox — narrative card for scrollytelling steps.
	 * Restyled for nealab: dark glass panel, Roboto Condensed body, JetBrains Mono
	 * title, amber `.num` highlight for figures.
	 */
	import type { Snippet } from 'svelte';

	interface SourceLink {
		text: string;
		url: string;
	}

	interface Props {
		title?: string;
		active?: boolean;
		variant?: 'light' | 'dark';
		source?: SourceLink | null;
		children?: Snippet;
	}

	let { title = '', active = true, variant = 'dark', source = null, children }: Props = $props();
</script>

<div class="scrolly-text-box" class:active class:light={variant === 'light'}>
	{#if title}
		<h2 class="box-title">{title}</h2>
	{/if}
	<div class="box-content">
		{@render children?.()}
	</div>
	{#if source}
		<div class="box-footer">
			<a href={source.url} target="_blank" rel="noopener noreferrer" class="source-link">
				{source.text} →
			</a>
		</div>
	{/if}
</div>

<style>
	.scrolly-text-box {
		max-width: 440px;
		background: rgba(10, 12, 18, 0.92);
		backdrop-filter: blur(10px);
		-webkit-backdrop-filter: blur(10px);
		border: 1px solid #24252b;
		border-left: 3px solid #199e70;
		padding: 1.6rem 1.8rem;
		border-radius: 10px;
		box-shadow: 0 12px 40px rgba(0, 0, 0, 0.55);
		opacity: 0.72;
		transform: translateY(8px);
		transition: all 0.35s cubic-bezier(0.25, 0.46, 0.45, 0.94);
		pointer-events: auto;
	}

	.scrolly-text-box.active {
		opacity: 1;
		transform: translateY(0);
		border-color: #334155;
	}

	.scrolly-text-box.light {
		background: rgba(255, 255, 255, 0.96);
		border-color: rgba(0, 0, 0, 0.1);
	}

	.box-title {
		font-family: 'JetBrains Mono', monospace;
		font-size: 1.08rem;
		font-weight: 700;
		letter-spacing: 0.01em;
		color: #22c39a;
		margin: 0 0 0.6rem 0;
	}

	.box-content {
		font-family: 'Roboto Condensed', system-ui, sans-serif;
		font-size: 1.08rem;
		line-height: 1.6;
		color: rgba(255, 255, 255, 0.9);
	}

	/* embedded mini split-bar (plantation vs native) */
	.box-content :global(.stepbar) {
		display: flex;
		gap: 2px;
		height: 13px;
		margin: 1rem 0 0.5rem;
	}
	.box-content :global(.stepbar span) {
		display: block;
		height: 100%;
		border-radius: 3px;
	}
	.box-content :global(.sbp) {
		background: #199e70;
	}
	.box-content :global(.sbn) {
		background: #9085e9;
	}
	.box-content :global(.sbl) {
		display: flex;
		justify-content: space-between;
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.74rem;
	}
	.box-content :global(.sbl .lp) {
		color: #22c39a;
	}
	.box-content :global(.sbl .ln) {
		color: #a78bfa;
	}
	.box-content :global(.sbcap) {
		margin-top: 0.4rem;
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.68rem;
		color: rgba(255, 255, 255, 0.4);
	}

	.light .box-content {
		color: #1e293b;
	}

	.box-content :global(p) {
		margin: 0;
	}
	.box-content :global(p + p) {
		margin-top: 0.75em;
	}
	.box-content :global(strong) {
		font-weight: 700;
		color: #fff;
	}
	.light .box-content :global(strong) {
		color: #0a0a0a;
	}
	.box-content :global(.num) {
		font-family: 'JetBrains Mono', monospace;
		font-weight: 700;
		color: #22c39a;
	}

	.box-footer {
		margin-top: 0.9rem;
		padding-top: 0.7rem;
		border-top: 1px solid rgba(255, 255, 255, 0.12);
		font-family: 'JetBrains Mono', monospace;
		font-size: 0.72rem;
	}

	.source-link {
		color: rgba(255, 255, 255, 0.55);
		text-decoration: none;
	}
	.source-link:hover {
		color: #22c39a;
	}

	@media (max-width: 600px) {
		.scrolly-text-box {
			max-width: 320px;
			padding: 1.15rem 1.3rem;
		}
		.box-content {
			font-size: 0.95rem;
		}
	}
</style>
