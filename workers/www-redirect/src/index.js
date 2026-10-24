// 301 from www.spatia.ar to the canonical host spatia.ar, keeping path and query.
// Bound only to the www.spatia.ar/* route (see wrangler.toml); the Pages site is untouched.
export default {
	fetch(request) {
		const url = new URL(request.url);
		return Response.redirect(`https://spatia.ar${url.pathname}${url.search}`, 301);
	}
};
