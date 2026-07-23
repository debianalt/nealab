import type { Action } from 'svelte/action';

// Minimal scroll-reveal: adds `reveal-init` immediately and `reveal-in` when the
// element scrolls into view (once). The page defines the two :global classes.
export const reveal: Action<HTMLElement, { threshold?: number; delay?: number } | undefined> = (
	node,
	params
) => {
	const threshold = params?.threshold ?? 0.14;
	node.classList.add('reveal-init');
	if (params?.delay) node.style.transitionDelay = `${params.delay}ms`;

	const io = new IntersectionObserver(
		(entries) => {
			for (const e of entries) {
				if (e.isIntersecting) {
					node.classList.add('reveal-in');
					io.unobserve(node);
				}
			}
		},
		{ threshold, rootMargin: '0px 0px -8% 0px' }
	);
	io.observe(node);

	return { destroy: () => io.disconnect() };
};
