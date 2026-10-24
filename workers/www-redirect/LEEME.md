# spatia-www-redirect

Worker mínimo que responde 301 de `www.spatia.ar/*` a `https://spatia.ar` (misma ruta y parámetros), para que Google indexe un solo host. La URL canónica de cada página ya apunta a `spatia.ar`.

- Desplegar: `npx wrangler deploy` desde esta carpeta.
- Quitar: `npx wrangler delete spatia-www-redirect`.
- Se usa un Worker porque el token de wrangler no tiene permiso para reglas de redirección de la zona. Una regla en el panel de Cloudflare haría lo mismo; si se crea, borrar este Worker.
