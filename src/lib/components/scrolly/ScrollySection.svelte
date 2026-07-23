<script lang="ts">
	/**
	 * ScrollySection — sticky visual + scrolling text track.
	 * Adapted from the scrolly-sveltekit skill for nealab (dark default, simplified
	 * text box props). Sticky logic preserves first/last step past the section.
	 */
	import ScrollyHelper from './ScrollyHelper.svelte';
	import ScrollyTextBox from './ScrollyTextBox.svelte';
	import { onMount } from 'svelte';

	interface SourceLink {
		text: string;
		url: string;
	}

	interface Step {
		text?: string;
		title?: string;
		raw?: boolean;
		source?: SourceLink;
	}

	interface Props {
		activeStep?: number;
		steps: Step[];
		backgroundColor?: string;
		showTextBoxes?: boolean;
		textBoxVariant?: 'light' | 'dark';
		textBoxPosition?: 'center' | 'left' | 'right';
		firstStepOffset?: number;
		onStepEnter?: (stepIndex: number, direction: 'up' | 'down') => void;
		onScrollProgress?: (progress: number) => void;
		children?: import('svelte').Snippet<[{ activeStep: number }]>;
	}

	let {
		activeStep = $bindable(0),
		steps,
		backgroundColor = '#0a0a0a',
		showTextBoxes = true,
		textBoxVariant = 'dark',
		textBoxPosition = 'left',
		firstStepOffset = 0,
		onStepEnter,
		onScrollProgress,
		children
	}: Props = $props();

	let rawStep = $state<number | undefined>(undefined);
	let internalStep = $state(0);
	let previousStep = $state<number | undefined>(undefined);

	$effect(() => {
		if (rawStep !== undefined) internalStep = rawStep;
		activeStep = internalStep;
	});

	$effect(() => {
		if (internalStep !== previousStep) {
			const direction = previousStep === undefined || internalStep > previousStep ? 'down' : 'up';
			onStepEnter?.(internalStep, direction);
			previousStep = internalStep;
		}
	});

	let viewportHeight = $state(800);
	let stepHeight = $derived(Math.max(viewportHeight * 0.7, 520));
	let stepGap = $derived(Math.max(viewportHeight * 0.25, 240));
	let triggerOffset = $derived(Math.round(viewportHeight * 0.25));
	let sectionEl: HTMLElement | undefined = $state();

	onMount(() => {
		viewportHeight = window.innerHeight;
		const handleResize = () => (viewportHeight = window.innerHeight);
		window.addEventListener('resize', handleResize);

		const handleScroll = () => {
			if (!sectionEl || !onScrollProgress) return;
			const rect = sectionEl.getBoundingClientRect();
			const scrolledPast = -rect.top;
			const scrollableDistance = rect.height - viewportHeight;
			const progress = Math.max(0, Math.min(1, scrolledPast / scrollableDistance));
			onScrollProgress(progress);
		};

		if (onScrollProgress) {
			window.addEventListener('scroll', handleScroll, { passive: true });
			handleScroll();
		}

		return () => {
			window.removeEventListener('resize', handleResize);
			if (onScrollProgress) window.removeEventListener('scroll', handleScroll);
		};
	});
</script>

<section bind:this={sectionEl} class="scroll-section" style:background={backgroundColor} style={`--vh:${viewportHeight}px`}>
	<div class="scroll-inner">
		<div class="visual-layer">
			{#if children}
				{@render children({ activeStep: internalStep })}
			{/if}
		</div>

		<div class="text-track">
			<ScrollyHelper bind:value={rawStep} top={triggerOffset} bottom={triggerOffset}>
				{#each steps as step, i}
					{@const isFirst = i === 0}
					{@const isLast = i === steps.length - 1}
					{@const active = internalStep === i}
					<div
						class="step"
						class:active
						class:step-first={isFirst}
						style:min-height={isFirst && step.raw ? `${viewportHeight}px` : `${stepHeight}px`}
						style:margin-bottom={`${isLast ? viewportHeight * 0.5 : stepGap}px`}
						style:margin-top={isFirst && firstStepOffset > 0 ? `${viewportHeight * (-1 + firstStepOffset)}px` : undefined}
					>
						{#if step.raw && step.text}
							<div class="step-raw">{@html step.text}</div>
						{:else if showTextBoxes && step.text}
							<div
								class="step-content"
								class:position-left={textBoxPosition === 'left'}
								class:position-right={textBoxPosition === 'right'}
							>
								<ScrollyTextBox title={step.title || ''} source={step.source || null} {active} variant={textBoxVariant}>
									{@html step.text}
								</ScrollyTextBox>
							</div>
						{/if}
					</div>
				{/each}
			</ScrollyHelper>
		</div>
	</div>
</section>

<style>
	.scroll-section {
		position: relative;
		width: 100%;
		isolation: isolate;
	}
	.scroll-inner {
		position: relative;
	}
	.visual-layer {
		position: sticky;
		top: 0;
		width: 100%;
		height: 100vh;
		/* NOT min-height:var(--vh): --vh defaults to 800 until the parent onMount
		   measures innerHeight, which inflates the sticky container above the real
		   viewport while the map initialises — the canvas then ends up taller than
		   what's visible and the framed content is clipped at the bottom. */
		min-height: 0;
		display: flex;
		align-items: center;
		justify-content: center;
		z-index: 1;
	}
	.text-track {
		position: relative;
		z-index: 2;
		max-width: 1200px;
		margin: 0 auto;
		padding: 0 1.5rem calc(var(--vh) * 0.4);
		/* The track box sits above the sticky visual (z-index 1). Without this it would
		   swallow every click over its 1200px column before it reaches the map canvas
		   underneath. The interactive children (.step-content, .step-raw, the text box)
		   re-enable pointer-events, so cards stay clickable and the rest falls through. */
		pointer-events: none;
	}
	.step {
		display: flex;
		align-items: center;
		justify-content: center;
		opacity: 0.35;
		transform: translateY(6px);
		transition: opacity 220ms ease, transform 220ms ease;
		pointer-events: none;
	}
	.step.active {
		opacity: 1;
		transform: translateY(0);
	}
	.step.step-first {
		margin-top: calc(var(--vh) * -1);
	}
	.step-raw {
		pointer-events: auto;
		width: 100%;
		display: flex;
		align-items: center;
		justify-content: center;
	}
	.step-content {
		pointer-events: auto;
		max-width: 480px;
		width: 100%;
		padding: 0 0.5rem;
	}
	.step-content.position-left {
		margin-right: auto;
	}
	.step-content.position-right {
		margin-left: auto;
	}
	@media (max-width: 768px) {
		.text-track {
			padding: calc(var(--vh) * 0.18) 1rem calc(var(--vh) * 0.25);
		}
		.step-content {
			max-width: 360px;
		}
		.step-content.position-left,
		.step-content.position-right {
			margin: 0 auto;
		}
	}
</style>
