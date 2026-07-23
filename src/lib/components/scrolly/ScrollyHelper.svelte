<script lang="ts">
	/**
	 * ScrollyHelper — IntersectionObserver-based step tracker.
	 * Tracks which child element is most visible and exposes `value` (step index),
	 * `undefined` when nothing is in view. Adapted to Svelte 5 snippets.
	 */
	import { onMount } from 'svelte';
	import type { Snippet } from 'svelte';

	interface Props {
		value?: number;
		root?: Element | null;
		top?: number;
		bottom?: number;
		increments?: number;
		children?: Snippet;
	}

	let {
		value = $bindable(undefined),
		root = null,
		top = 0,
		bottom = 0,
		increments = 100,
		children
	}: Props = $props();

	let container: HTMLDivElement;
	let nodes: NodeListOf<Element>;
	let intersectionObservers: IntersectionObserver[] = [];
	const steps: number[] = [];
	const threshold: number[] = [];

	function mostInView() {
		let maxRatio = 0;
		let maxIndex = 0;
		for (let i = 0; i < steps.length; i++) {
			if (steps[i] > maxRatio) {
				maxRatio = steps[i];
				maxIndex = i;
			}
		}
		if (maxRatio > 0) value = maxIndex;
		else value = undefined;
	}

	function createObserver(node: Element, index: number) {
		const handleIntersect = (entries: IntersectionObserverEntry[]) => {
			steps[index] = entries[0].intersectionRatio;
			mostInView();
		};
		const marginTop = top ? top * -1 : 0;
		const marginBottom = bottom ? bottom * -1 : 0;
		const rootMargin = `${marginTop}px 0px ${marginBottom}px 0px`;
		const options = { root, rootMargin, threshold };
		intersectionObservers[index]?.disconnect();
		const io = new IntersectionObserver(handleIntersect, options);
		io.observe(node);
		intersectionObservers[index] = io;
	}

	function update() {
		if (!nodes?.length) return;
		nodes.forEach((node, index) => createObserver(node, index));
	}

	$effect(() => {
		top;
		bottom;
		update();
	});

	onMount(() => {
		for (let i = 0; i <= increments; i++) threshold.push(i / increments);
		nodes = container.querySelectorAll(':scope > *:not(iframe)');
		update();
		return () => intersectionObservers.forEach((io) => io?.disconnect());
	});
</script>

<div bind:this={container}>
	{@render children?.()}
</div>
