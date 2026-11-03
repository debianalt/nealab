<script lang="ts">
	import { terms } from '$lib/stores/terms.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { lp } from '$lib/utils/locale-path';
	import { ANALYSIS_REGISTRY } from '$lib/config';
	// Counted, not written: the modal said «23 análisis» while there were 15.
	const nAnalyses = ANALYSIS_REGISTRY.filter((a) => a.status === 'available').length;
</script>

<div class="overlay">
	<div class="modal">
		<div class="kicker">spatia.ar</div>
		<h1 class="title">{i18n.t('terms.modal.title')}</h1>

		<p class="platform-intro">{i18n.t('terms.modal.intro')}</p>

		<div class="platform-stats">
			<span>{i18n.t('terms.modal.grid')}</span>
			<span class="stat-sep">·</span>
			<span>{i18n.t('terms.modal.analyses').replace('{n}', String(nAnalyses))}</span>
			<span class="stat-sep">·</span>
			<span>CONICET / FHyCS-UNaM · GEE Partner</span>
		</div>

		<div class="stats-divider">{i18n.t('terms.modal.conditions')}</div>

		<ul class="points">
			{#each [1, 2, 3, 4, 5] as n}
				<li>
					<span class="label">{i18n.t(`terms.modal.p${n}.label`)}</span>
					{i18n.t(`terms.modal.p${n}`)}
				</li>
			{/each}
		</ul>

		<div class="actions">
			<a class="btn-secondary" href={lp('/terminos', i18n.locale)}>{i18n.t('terms.modal.read')}</a>
			<button class="btn-primary" onclick={() => terms.accept()}>
				{i18n.t('terms.modal.accept')}
			</button>
		</div>
	</div>
</div>

<style>
	.overlay {
		position: fixed;
		inset: 0;
		background: rgba(var(--c-bg-rgb), 0.88);
		backdrop-filter: blur(4px);
		z-index: 9999;
		display: flex;
		align-items: center;
		justify-content: center;
		padding: 32px 24px;
		overflow-y: auto;
	}

	.modal {
		background: var(--c-modal);
		border: 1px solid rgba(var(--c-fg-rgb), 0.15);
		max-width: 560px;
		width: 100%;
		padding: 48px 40px 40px;
		font-family: var(--font-sans);
		max-height: calc(100vh - 64px);
		overflow-y: auto;
		display: flex;
		flex-direction: column;
	}

	.kicker {
		font-size: 10px;
		color: rgba(var(--c-fg-rgb), 0.4);
		text-transform: uppercase;
		letter-spacing: 0.12em;
		margin-bottom: 12px;
	}

	.title {
		font-size: 22px;
		font-weight: 700;
		color: var(--c-ink-0);
		margin: 0 0 16px;
		line-height: 1.2;
	}

	.platform-intro {
		font-size: 12px;
		color: rgba(var(--c-fg-rgb), 0.75);
		margin: 0 0 14px;
		line-height: 1.65;
	}

	.platform-stats {
		display: flex;
		flex-wrap: wrap;
		gap: 4px 0;
		font-size: 10px;
		color: rgba(var(--c-fg-rgb), 0.35);
		letter-spacing: 0.04em;
		margin-bottom: 28px;
	}

	.stat-sep {
		margin: 0 8px;
		color: rgba(var(--c-fg-rgb), 0.18);
	}

	.stats-divider {
		font-size: 9px;
		text-transform: uppercase;
		letter-spacing: 0.1em;
		color: rgba(var(--c-fg-rgb), 0.25);
		border-top: 1px solid rgba(var(--c-fg-rgb), 0.08);
		padding-top: 16px;
		margin-bottom: 16px;
	}

	.points {
		list-style: none;
		padding: 0;
		margin: 0 0 32px;
		display: flex;
		flex-direction: column;
		gap: 12px;
		flex: 1;
	}

	.points li {
		font-size: 12px;
		color: rgba(var(--c-fg-rgb), 0.7);
		line-height: 1.55;
		padding-left: 14px;
		position: relative;
	}

	.points li::before {
		content: '—';
		position: absolute;
		left: 0;
		color: rgba(var(--c-fg-rgb), 0.3);
	}

	.label {
		color: var(--c-ink-0);
		font-weight: 700;
	}

	.actions {
		display: flex;
		flex-direction: column;
		gap: 10px;
		position: sticky;
		bottom: 0;
		background: var(--c-modal);
		padding-top: 20px;
		margin-top: auto;
	}

	.btn-primary {
		background: var(--c-ink-0);
		color: var(--c-bg);
		border: none;
		padding: 12px 20px;
		font-family: inherit;
		font-size: 12px;
		font-weight: 700;
		letter-spacing: 0.04em;
		cursor: pointer;
		text-align: center;
		transition: opacity 0.15s;
	}

	.btn-primary:hover {
		opacity: 0.85;
	}

	.btn-secondary {
		display: block;
		border: 1px solid rgba(var(--c-fg-rgb), 0.25);
		color: rgba(var(--c-fg-rgb), 0.6);
		padding: 10px 20px;
		font-family: inherit;
		font-size: 11px;
		letter-spacing: 0.04em;
		text-decoration: none;
		text-align: center;
		transition: all 0.15s;
	}

	.btn-secondary:hover {
		border-color: rgba(var(--c-fg-rgb), 0.6);
		color: var(--c-ink-0);
	}
</style>
